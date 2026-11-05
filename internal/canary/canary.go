// Package canary probes every serving cluster of a cell through its PUBLIC endpoint, the way a tenant
// reaches it (design §17): it produces to a topic with one partition per broker using acks=all, reads
// the records back, and reports attempts, successes and the 99th percentile latency. The control plane's
// health evaluator turns those reports into DEGRADED and back. The canary is a client of the cluster
// and a super user of it (the platform-canary principal); it does not touch Kubernetes.
package canary

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/avilashs/kafka_as_service/internal/loadgen"
	"github.com/avilashs/kafka_as_service/internal/platform/store"
)

// Topic is the canary's topic. The "__" prefix is reserved for the platform: tenants can neither see nor use it.
const Topic = "__platform_canary"

// Target is one cluster to probe.
type Target struct {
	ClusterID  string
	Bootstrap  string
	CAPEM      string
	Partitions int
}

// Prober probes the targets it is given.
type Prober struct {
	Targets  func(ctx context.Context) ([]Target, error)
	Password func(ctx context.Context, clusterID string) (string, error) // "" with User "" means no authentication
	User     string
	Report   func(ctx context.Context, rs []store.CanaryResult) error
	Log      *slog.Logger

	// ReplicationFactor and MinISR of the canary topic (3 and 2 in a cluster; tests use a single broker).
	ReplicationFactor int16
	MinISR            int

	// DialAddr sends every connection to one address (tests and kind, where names resolve to a local relay).
	DialAddr string
	// RoundEvery is the probe period (2 s), RoundTimeout how long a round trip may take (5 s), ReportEvery
	// how often results are sent (15 s). Warmup: a cluster seen for the first time is probed but not
	// reported for this long, so the minutes after creation do not count against it.
	RoundEvery, RoundTimeout, ReportEvery, Warmup time.Duration
	// ReconnectEvery: clients are recreated this often (default 2 min), so that the whole path (route, TLS,
	// authentication) is exercised by new connections too. A NetworkPolicy removed under an established
	// connection does not break it (conntrack), the kind run showed a "cut" the canary never noticed.
	ReconnectEvery time.Duration

	mu     sync.Mutex
	probes map[string]*probe
}

func (p *Prober) defaults() {
	if p.RoundEvery == 0 {
		p.RoundEvery = 2 * time.Second
	}
	if p.RoundTimeout == 0 {
		p.RoundTimeout = 5 * time.Second
	}
	if p.ReportEvery == 0 {
		p.ReportEvery = 15 * time.Second
	}
	if p.Warmup == 0 {
		p.Warmup = 60 * time.Second
	}
	if p.ReconnectEvery == 0 {
		p.ReconnectEvery = 2 * time.Minute
	}
	if p.Log == nil {
		p.Log = slog.Default()
	}
	if p.ReplicationFactor == 0 {
		p.ReplicationFactor = 3
	}
	if p.MinISR == 0 {
		p.MinISR = 2
	}
	if p.probes == nil {
		p.probes = map[string]*probe{}
	}
}

type acc struct {
	attempts, successes int
	lat                 []time.Duration
}

type probe struct {
	t        Target
	pw       string
	caFile   string
	prod     *kgo.Client
	cons     *kgo.Client
	adm      *kadm.Client
	topicOK  bool
	partsOK  int
	firstSee time.Time
	cur      acc
	seq      int64
	run      string
	mu       sync.Mutex
	got      map[string]time.Time // record id -> arrival
	cancel   context.CancelFunc

	connectedAt time.Time

	lastErr string    // last failure that was logged, and when: a dead cluster must not flood the log
	lastLog time.Time //
}

// logFailure says why rounds fail. Silent failures cost a whole CI run to diagnose (M5), so they are logged at
// WARN; the same message is repeated at most every 30 s.
func (pr *probe) logFailure(log *slog.Logger, err error) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	msg := err.Error()
	if msg == pr.lastErr && time.Since(pr.lastLog) < 30*time.Second {
		return
	}
	pr.lastErr, pr.lastLog = msg, time.Now()
	log.Warn("canary round failed", "cluster", pr.t.ClusterID, "err", msg)
}

// stop ends the probe's loop and closes its clients without waiting for a cluster that does not answer.
func (pr *probe) stop() {
	pr.cancel()
	pr.drop()
}

// drop detaches the clients at once (a later connect is not affected) and closes them in the background:
// closing a client of a frozen cluster can take a long time.
func (pr *probe) drop() {
	pr.mu.Lock()
	prod, cons := pr.prod, pr.cons
	pr.prod, pr.cons = nil, nil
	pr.mu.Unlock()
	go func() {
		if prod != nil {
			prod.Close()
		}
		if cons != nil {
			cons.Close()
		}
	}()
}

// Run probes until ctx is done. Every cluster has its own loop, so a frozen cluster delays neither the
// others nor the reports.
func (p *Prober) Run(ctx context.Context) {
	p.defaults()
	go func() {
		t := time.NewTicker(p.ReportEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				p.flush(ctx)
			}
		}
	}()
	for {
		ts, err := p.Targets(ctx)
		if err != nil {
			p.Log.Warn("canary: cannot list targets", "err", err)
		} else {
			p.reconcile(ctx, ts)
		}
		select {
		case <-ctx.Done():
			p.closeAll()
			return
		case <-time.After(5 * time.Second):
		}
	}
}

func (p *Prober) closeAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, pr := range p.probes {
		pr.stop()
		delete(p.probes, id)
	}
}

// reconcile starts the loop of new targets and stops those that are gone or changed.
func (p *Prober) reconcile(ctx context.Context, ts []Target) {
	p.mu.Lock()
	defer p.mu.Unlock()
	live := map[string]Target{}
	for _, t := range ts {
		live[t.ClusterID] = t
	}
	for id, pr := range p.probes {
		if t, ok := live[id]; !ok || t.Bootstrap != pr.t.Bootstrap || t.CAPEM != pr.t.CAPEM {
			p.Log.Info("canary: stopped probing", "cluster", id)
			pr.stop()
			delete(p.probes, id)
		}
	}
	for id, t := range live {
		pr := p.probes[id]
		if pr == nil {
			pctx, cancel := context.WithCancel(ctx)
			pr = &probe{t: t, firstSee: time.Now(), run: hex.EncodeToString(randb(4)), got: map[string]time.Time{}, cancel: cancel}
			p.probes[id] = pr
			p.Log.Info("canary: probing", "cluster", id, "endpoint", t.Bootstrap, "partitions", t.Partitions)
			go func() {
				for {
					select {
					case <-pctx.Done():
						return
					case <-time.After(p.RoundEvery):
						p.round(pctx, pr)
					}
				}
			}()
		}
		pr.mu.Lock()
		pr.t.Partitions = t.Partitions
		pr.mu.Unlock()
	}
}

func randb(n int) []byte { b := make([]byte, n); _, _ = rand.Read(b); return b }

// connect (re)creates the clients of a probe.
func (p *Prober) connect(ctx context.Context, pr *probe) error {
	pw := ""
	if p.User != "" {
		var err error
		if pw, err = p.Password(ctx, pr.t.ClusterID); err != nil {
			return fmt.Errorf("credentials: %w", err)
		}
	}
	conn := loadgen.Conn{Bootstrap: pr.t.Bootstrap, User: p.User, Password: pw, DialAddr: p.DialAddr}
	if pr.t.CAPEM != "" {
		f, err := os.CreateTemp("", "canary-ca-*.pem")
		if err != nil {
			return err
		}
		defer f.Close()
		if _, err := f.WriteString(pr.t.CAPEM); err != nil {
			return err
		}
		conn.CAFile = f.Name()
		defer os.Remove(f.Name())
	}
	opts, err := conn.Options()
	if err != nil {
		return err
	}
	prod, err := kgo.NewClient(append(opts, kgo.DefaultProduceTopic(Topic), kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.RecordDeliveryTimeout(p.RoundTimeout), kgo.ProduceRequestTimeout(p.RoundTimeout),
		kgo.RecordRetries(1))...)
	if err != nil {
		return err
	}
	cons, err := kgo.NewClient(append(opts, kgo.ConsumeTopics(Topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.FetchMaxWait(500*time.Millisecond))...)
	if err != nil {
		prod.Close()
		return err
	}
	pr.mu.Lock()
	pr.prod, pr.cons, pr.adm = prod, cons, kadm.NewClient(prod)
	pr.mu.Unlock()
	pr.topicOK = false
	pr.connectedAt = time.Now()
	go consume(cons, pr)
	return nil
}

func consume(cl *kgo.Client, pr *probe) {
	for {
		fs := cl.PollFetches(context.Background())
		if fs.IsClientClosed() {
			return
		}
		now := time.Now()
		fs.EachRecord(func(r *kgo.Record) {
			pr.mu.Lock()
			pr.got[string(r.Value)] = now
			pr.mu.Unlock()
		})
	}
}

func (p *Prober) ensureTopic(ctx context.Context, pr *probe, parts int) error {
	cctx, cancel := context.WithTimeout(ctx, p.RoundTimeout)
	defer cancel()
	if !pr.topicOK {
		resp, err := pr.adm.CreateTopic(cctx, int32(parts), p.ReplicationFactor,
			map[string]*string{"min.insync.replicas": strp(strconv.Itoa(p.MinISR)), "retention.ms": strp("3600000")}, Topic)
		// kadm returns the per-topic error as err as well; an existing topic is the normal case after a reconnect.
		if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			return err
		}
		if resp.Err != nil && !errors.Is(resp.Err, kerr.TopicAlreadyExists) {
			return resp.Err
		}
		pr.topicOK, pr.partsOK = true, parts
	}
	if parts > pr.partsOK { // the cluster grew: one partition per broker still
		if _, err := pr.adm.UpdatePartitions(cctx, parts, Topic); err != nil {
			return err
		}
		pr.partsOK = parts
	}
	return nil
}

func strp(s string) *string { return &s }

// round sends one record to every partition and waits for them to come back.
func (p *Prober) round(parent context.Context, pr *probe) {
	// A frozen or unreachable cluster must show up as failures, not as silence: attempts are counted before
	// anything can hang, and the whole round is bounded.
	pr.mu.Lock()
	n := pr.t.Partitions
	pr.cur.attempts += n
	connected := pr.prod != nil
	pr.mu.Unlock()
	if connected && time.Since(pr.connectedAt) > p.ReconnectEvery {
		pr.drop()
		connected = false
	}
	ctx, cancel := context.WithTimeout(parent, 3*p.RoundTimeout)
	defer cancel()
	fail := func(err error) {
		pr.logFailure(p.Log, err)
		pr.drop() // a fresh connection next round; a frozen cluster must not hold the loop
	}
	if !connected {
		if err := p.connect(ctx, pr); err != nil {
			fail(err)
			return
		}
	}
	if err := p.ensureTopic(ctx, pr, n); err != nil {
		fail(err)
		return
	}
	pr.seq++
	pr.mu.Lock()
	prod := pr.prod // close() clears the field while stragglers of an abandoned round may still run
	pr.mu.Unlock()
	if prod == nil {
		pr.logFailure(p.Log, errors.New("connection closed before the round started"))
		return
	}
	type sent struct {
		id string
		at time.Time
		ok bool
	}
	results := make([]sent, n)
	oks := make([]atomic.Bool, n) // written by the producing goroutines, which may outlive the round
	var wg sync.WaitGroup
	for part := 0; part < n; part++ {
		id := fmt.Sprintf("%s-%d-%d", pr.run, pr.seq, part)
		results[part] = sent{id: id, at: time.Now()}
		wg.Add(1)
		go func(part int) {
			defer wg.Done()
			rec := &kgo.Record{Partition: int32(part), Value: []byte(id), Key: []byte("canary")}
			rec.Topic = Topic
			if err := prod.ProduceSync(ctx, rec).FirstErr(); err == nil {
				oks[part].Store(true)
			} else {
				pr.logFailure(p.Log, fmt.Errorf("produce to partition %d: %w", part, err))
			}
		}(part)
	}
	// ProduceSync does not return on a frozen broker when its context ends (the request is in flight), so the
	// round waits for it only until its own deadline; closing the clients then fails the stragglers.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		pr.logFailure(p.Log, errors.New("produce did not complete within the round"))
		pr.drop()
	}
	for i := range results {
		results[i].ok = oks[i].Load()
	}
	deadline := time.Now().Add(p.RoundTimeout)
	var lat []time.Duration
	okCount := 0
	for _, r := range results {
		if !r.ok {
			continue
		}
		for {
			pr.mu.Lock()
			at, seen := pr.got[r.id]
			pr.mu.Unlock()
			if seen {
				okCount++
				lat = append(lat, at.Sub(r.at))
				break
			}
			if time.Now().After(deadline) || ctx.Err() != nil {
				pr.logFailure(p.Log, errors.New("a record was acknowledged but not read back in time"))
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	pr.mu.Lock()
	pr.cur.successes += okCount
	pr.cur.lat = append(pr.cur.lat, lat...)
	// forget what has been matched; keep the map small
	for k := range pr.got {
		delete(pr.got, k)
	}
	pr.mu.Unlock()
}

// flush reports what each probe has collected since the last report.
func (p *Prober) flush(ctx context.Context) {
	p.mu.Lock()
	var out []store.CanaryResult
	for id, pr := range p.probes {
		pr.mu.Lock()
		a := pr.cur
		if time.Since(pr.firstSee) < p.Warmup {
			pr.cur = acc{} // still warming up: probed, not reported
			pr.mu.Unlock()
			continue
		}
		pr.cur = acc{}
		pr.mu.Unlock()
		if a.attempts == 0 {
			continue
		}
		out = append(out, store.CanaryResult{ClusterID: id, At: time.Now().UTC(), Attempts: a.attempts, Successes: a.successes, P99ms: p99ms(a.lat)})
	}
	p.mu.Unlock()
	if len(out) == 0 {
		return
	}
	if err := p.Report(ctx, out); err != nil {
		p.Log.Warn("canary: report failed; this window is lost", "err", err)
	}
}

func p99ms(l []time.Duration) int {
	if len(l) == 0 {
		return 0
	}
	sort.Slice(l, func(i, j int) bool { return l[i] < l[j] })
	return int(l[(len(l)*99+99)/100-1] / time.Millisecond)
}
