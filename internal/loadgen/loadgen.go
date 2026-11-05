// Package loadgen is the load generator and verifier behind the lifecycle acceptance tests (design §16):
// producers with acks=all and idempotence write records that carry (producer, sequence, hash), keep a
// ledger of the records the cluster acknowledged, and the verifier reads the topic back and proves that
// every acknowledged record is present, intact and not duplicated. It is a client of the public endpoint
// and knows nothing of the platform.
package loadgen

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Conn describes how to reach the cluster.
type Conn struct {
	Bootstrap string    // host:port, as published by the API
	User      string    // SCRAM-SHA-512 user; empty for no authentication
	Password  string    //
	CAFile    string    // PEM CA to trust; empty for no TLS
	DialAddr  string    // if set, every connection goes to this address (tests that resolve names locally)
	Extra     []kgo.Opt // test hooks
}

func (c Conn) opts() ([]kgo.Opt, error) {
	opts := []kgo.Opt{kgo.SeedBrokers(c.Bootstrap), kgo.ClientID("streamcell-loadgen")}
	if c.User != "" {
		opts = append(opts, kgo.SASL(scram.Auth{User: c.User, Pass: c.Password}.AsSha512Mechanism()))
	}
	var tlsCfg *tls.Config
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("no certificate in " + c.CAFile)
		}
		tlsCfg = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	switch {
	case c.DialAddr != "":
		// Every connection goes to one address (a local relay), but the certificate is still verified against
		// the broker's own host name, exactly as a client that resolved the name would do.
		d := &net.Dialer{Timeout: 10 * time.Second}
		opts = append(opts, kgo.Dialer(func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := d.DialContext(ctx, network, c.DialAddr)
			if err != nil || tlsCfg == nil {
				return conn, err
			}
			host, _, _ := net.SplitHostPort(addr)
			cfg := tlsCfg.Clone()
			cfg.ServerName = host
			tc := tls.Client(conn, cfg)
			if err := tc.HandshakeContext(ctx); err != nil {
				conn.Close()
				return nil, fmt.Errorf("tls handshake with %s: %w", host, err)
			}
			return tc, nil
		}))
	case tlsCfg != nil:
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}
	return append(opts, c.Extra...), nil
}

// Record layout: "<producer>|<seq>|<sha256(payload)>|<payload>".
func encode(producer string, seq int64, payload []byte) []byte {
	h := sha256.Sum256(payload)
	return []byte(fmt.Sprintf("%s|%d|%s|%s", producer, seq, hex.EncodeToString(h[:]), hex.EncodeToString(payload)))
}

type decoded struct {
	producer string
	seq      int64
	ok       bool // the hash matches the payload
}

func decode(v []byte) (decoded, error) {
	parts := strings.SplitN(string(v), "|", 4)
	if len(parts) != 4 {
		return decoded{}, errors.New("malformed record")
	}
	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return decoded{}, err
	}
	payload, err := hex.DecodeString(parts[3])
	if err != nil {
		return decoded{producer: parts[0], seq: seq}, nil
	}
	h := sha256.Sum256(payload)
	return decoded{producer: parts[0], seq: seq, ok: hex.EncodeToString(h[:]) == parts[2]}, nil
}

// ProduceOptions configures Produce.
type ProduceOptions struct {
	Topic     string
	Producers int           // independent clients, each with its own producer id
	Rate      int           // total records per second
	Size      int           // payload bytes
	Ledger    string        // file the acknowledged records are appended to
	Duration  time.Duration // 0: until ctx is done
	RunID     string        // makes producer names unique across runs
}

// ProduceResult counts what happened.
type ProduceResult struct {
	Sent, Acked, Failed int64
	FirstError          string
}

// Produce writes until ctx is done (or Duration passes), then flushes and returns. A record is in the
// ledger if and only if the broker acknowledged it. Records that failed are counted, not retried: with
// idempotence a failed record may still be in the log, which the verifier tolerates (it only requires
// the acknowledged ones).
func Produce(ctx context.Context, c Conn, o ProduceOptions) (ProduceResult, error) {
	if o.Producers <= 0 {
		o.Producers = 2
	}
	if o.Rate <= 0 {
		o.Rate = 100
	}
	if o.Size <= 0 {
		o.Size = 128
	}
	if o.RunID == "" {
		o.RunID = hex.EncodeToString(randBytes(3))
	}
	base, err := c.opts()
	if err != nil {
		return ProduceResult{}, err
	}
	f, err := os.OpenFile(o.Ledger, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return ProduceResult{}, err
	}
	defer f.Close()
	var ledgerMu sync.Mutex
	w := bufio.NewWriter(f)
	var res ProduceResult
	var firstErr atomic.Value

	if o.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, o.Duration)
		defer cancel()
	}
	var wg sync.WaitGroup
	for p := 0; p < o.Producers; p++ {
		name := fmt.Sprintf("%s-p%d", o.RunID, p)
		cl, err := kgo.NewClient(append(base,
			kgo.DefaultProduceTopic(o.Topic),
			kgo.RequiredAcks(kgo.AllISRAcks()),
			kgo.RecordDeliveryTimeout(3*time.Minute),
			kgo.ProducerBatchMaxBytes(1<<20),
			kgo.MaxBufferedRecords(20000),
		)...)
		if err != nil {
			return res, err
		}
		wg.Add(1)
		go func(name string, cl *kgo.Client) {
			defer wg.Done()
			defer cl.Close()
			per := time.Second / time.Duration(max(o.Rate/o.Producers, 1))
			tick := time.NewTicker(per)
			defer tick.Stop()
			var seq int64
			var inflight sync.WaitGroup
			for {
				select {
				case <-ctx.Done():
					fctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
					_ = cl.Flush(fctx)
					cancel()
					// Records still unresolved after the flush are abandoned (counted as neither acked nor
					// failed): waiting for them forever would hang the test behind a cluster that is down.
					done := make(chan struct{})
					go func() { inflight.Wait(); close(done) }()
					select {
					case <-done:
					case <-time.After(30 * time.Second):
					}
					return
				case <-tick.C:
				}
				seq++
				cur := seq
				payload := randBytes(o.Size)
				rec := &kgo.Record{Key: []byte(name), Value: encode(name, cur, payload)}
				atomic.AddInt64(&res.Sent, 1)
				inflight.Add(1)
				cl.Produce(context.Background(), rec, func(r *kgo.Record, err error) {
					defer inflight.Done()
					if err != nil {
						atomic.AddInt64(&res.Failed, 1)
						firstErr.CompareAndSwap(nil, err.Error())
						return
					}
					atomic.AddInt64(&res.Acked, 1)
					ledgerMu.Lock()
					fmt.Fprintf(w, "%s %d %d %d\n", name, cur, r.Partition, r.Offset)
					ledgerMu.Unlock()
				})
			}
		}(name, cl)
	}
	wg.Wait()
	ledgerMu.Lock()
	err = w.Flush()
	ledgerMu.Unlock()
	if v, ok := firstErr.Load().(string); ok {
		res.FirstError = v
	}
	return res, err
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// Report is the verifier's verdict.
type Report struct {
	Acked      int      // records in the ledger
	InLog      int      // records read from the topic
	Lost       []string // acknowledged but not in the log
	Duplicated []string // present more than once
	Corrupt    []string // payload does not match its hash
	Reordered  []string // sequence went backwards within a partition
	Unacked    int      // present in the log but never acknowledged (allowed)
}

// OK is true when no acknowledged record was lost and nothing was duplicated, corrupted or reordered.
func (r Report) OK() bool {
	return len(r.Lost) == 0 && len(r.Duplicated) == 0 && len(r.Corrupt) == 0 && len(r.Reordered) == 0
}

func (r Report) String() string {
	return fmt.Sprintf("acked=%d in-log=%d lost=%d duplicated=%d corrupt=%d reordered=%d (in the log but not in this ledger: %d — earlier runs and unacknowledged records)",
		r.Acked, r.InLog, len(r.Lost), len(r.Duplicated), len(r.Corrupt), len(r.Reordered), r.Unacked)
}

type key struct {
	producer string
	seq      int64
}

// Verify reads the whole topic and checks it against the ledger.
func Verify(ctx context.Context, c Conn, topic, ledger string) (Report, error) {
	var rep Report
	acked := map[key]bool{}
	f, err := os.Open(ledger)
	if err != nil {
		return rep, err
	}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fs := strings.Fields(sc.Text())
		if len(fs) < 2 {
			continue
		}
		seq, err := strconv.ParseInt(fs[1], 10, 64)
		if err != nil {
			f.Close()
			return rep, fmt.Errorf("bad ledger line %q", sc.Text())
		}
		acked[key{fs[0], seq}] = true
	}
	f.Close()
	rep.Acked = len(acked)

	opts, err := c.opts()
	if err != nil {
		return rep, err
	}
	cl, err := kgo.NewClient(append(opts, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadUncommitted()))...)
	if err != nil {
		return rep, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	// The cluster may still be settling after an operation: retry for a while, never forever.
	var ends kadm.ListedOffsets
	for attempt := 0; ; attempt++ {
		lctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		ends, err = adm.ListEndOffsets(lctx, topic)
		cancel()
		if err == nil && ends.Error() == nil {
			break
		}
		if attempt >= 10 || ctx.Err() != nil {
			if err == nil {
				err = ends.Error()
			}
			return rep, fmt.Errorf("end offsets: %w", err)
		}
		time.Sleep(5 * time.Second)
	}
	remaining := map[int32]int64{}
	ends.Each(func(e kadm.ListedOffset) {
		if e.Offset > 0 {
			remaining[e.Partition] = e.Offset
		}
	})
	seen := map[key]int{}
	lastSeq := map[string]int64{} // producer|partition
	deadline := time.Now().Add(5 * time.Minute)
	for len(remaining) > 0 && time.Now().Before(deadline) {
		fctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		fetches := cl.PollFetches(fctx)
		cancel()
		if ctx.Err() != nil {
			return rep, ctx.Err()
		}
		if errs := fetches.Errors(); len(errs) > 0 && fetches.NumRecords() == 0 {
			if errors.Is(errs[0].Err, context.DeadlineExceeded) {
				continue
			}
			return rep, fmt.Errorf("fetch: %v", errs[0].Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			rep.InLog++
			d, err := decode(r.Value)
			if err != nil {
				rep.Corrupt = append(rep.Corrupt, fmt.Sprintf("p%d@%d: %v", r.Partition, r.Offset, err))
			} else {
				k := key{d.producer, d.seq}
				seen[k]++
				if !d.ok {
					rep.Corrupt = append(rep.Corrupt, fmt.Sprintf("%s#%d", d.producer, d.seq))
				}
				lk := fmt.Sprintf("%s|%d", d.producer, r.Partition)
				if prev, ok := lastSeq[lk]; ok && d.seq < prev {
					rep.Reordered = append(rep.Reordered, fmt.Sprintf("%s#%d after #%d in partition %d", d.producer, d.seq, prev, r.Partition))
				}
				lastSeq[lk] = d.seq
			}
			if end, ok := remaining[r.Partition]; ok && r.Offset+1 >= end {
				delete(remaining, r.Partition)
			}
		})
	}
	if len(remaining) > 0 {
		return rep, fmt.Errorf("did not reach the end of partitions %v within 5 minutes", remaining)
	}
	for k := range acked {
		if seen[k] == 0 {
			rep.Lost = append(rep.Lost, fmt.Sprintf("%s#%d", k.producer, k.seq))
		}
	}
	for k, n := range seen {
		if n > 1 {
			rep.Duplicated = append(rep.Duplicated, fmt.Sprintf("%s#%d x%d", k.producer, k.seq, n))
		}
		if !acked[k] {
			rep.Unacked++
		}
	}
	sort.Strings(rep.Lost)
	sort.Strings(rep.Duplicated)
	return rep, nil
}
