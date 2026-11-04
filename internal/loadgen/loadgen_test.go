//go:build e2e

package loadgen

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// The verifier is only worth something if it can fail. This test runs against a real single-node Kafka
// (docker) and checks both directions: a clean run verifies, and a lost, a duplicated and a corrupted
// record are each reported.
func TestVerifierFindsWhatIsWrong(t *testing.T) {
	image := os.Getenv("STREAMCELL_E2E_KAFKA_IMAGE")
	if image == "" {
		image = "mirror.gcr.io/apache/kafka:4.1.0"
	}
	name := "streamcell-loadgen-test"
	_ = exec.Command("docker", "rm", "-f", name).Run()
	if out, err := exec.Command("docker", "run", "-d", "--name", name, "--network", "host", image).CombinedOutput(); err != nil {
		t.Skipf("docker not available: %v %s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })
	c := Conn{Bootstrap: "localhost:9092"}
	ctx := context.Background()

	var adm *kadm.Client
	for i := 0; i < 60; i++ {
		cl, err := kgo.NewClient(kgo.SeedBrokers(c.Bootstrap))
		if err == nil {
			a := kadm.NewClient(cl)
			if _, err := a.CreateTopic(ctx, 3, 1, nil, "load"); err == nil {
				adm = a
				break
			}
			cl.Close()
		}
		time.Sleep(time.Second)
	}
	if adm == nil {
		t.Fatal("broker did not start")
	}

	ledger := filepath.Join(t.TempDir(), "ledger")
	res, err := Produce(ctx, c, ProduceOptions{Topic: "load", Producers: 3, Rate: 300, Size: 64, Ledger: ledger, Duration: 4 * time.Second})
	if err != nil || res.Acked < 600 || res.Failed != 0 {
		t.Fatalf("produce: %+v %v", res, err)
	}
	rep, err := Verify(ctx, c, "load", ledger)
	if err != nil || !rep.OK() || rep.Acked != int(res.Acked) || rep.InLog < rep.Acked {
		t.Fatalf("clean run: %s %v", rep, err)
	}
	t.Log("clean:", rep)

	// A record the broker "acknowledged" but that is not in the log.
	f, _ := os.OpenFile(ledger, os.O_APPEND|os.O_WRONLY, 0o644)
	fmt.Fprintln(f, "ghost-p0 1 0 0")
	f.Close()
	rep, err = Verify(ctx, c, "load", ledger)
	if err != nil || len(rep.Lost) != 1 || !strings.HasPrefix(rep.Lost[0], "ghost-p0#1") || rep.OK() {
		t.Fatalf("lost record not found: %s %v", rep, err)
	}

	// A duplicate and a corrupted record, written without idempotence.
	cl, _ := kgo.NewClient(kgo.SeedBrokers(c.Bootstrap), kgo.DisableIdempotentWrite(), kgo.RequiredAcks(kgo.AllISRAcks()))
	defer cl.Close()
	dup := encode("dupe-p0", 1, []byte("payload"))
	bad := []byte(strings.Replace(string(encode("bad-p0", 1, []byte("payload"))), "|", "|0", 2)) // breaks the hash
	for _, v := range [][]byte{dup, dup, bad} {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "load", Key: []byte("k"), Value: v}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	rep, err = Verify(ctx, c, "load", ledger)
	if err != nil || len(rep.Duplicated) != 1 || len(rep.Corrupt) == 0 {
		t.Fatalf("duplicate/corrupt not found: %s %v", rep, err)
	}
}
