//go:build e2e

package loadgen

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
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

// The lifecycle scenario reaches the brokers over TLS with SCRAM, through a relay that every connection is
// sent to while the certificate is checked against the broker's own name. This test proves that path
// against a real broker (SASL_SSL with a PEM keystore and a SCRAM user created at format time).
func TestProduceAndVerifyOverTLSAndSCRAM(t *testing.T) {
	image := os.Getenv("STREAMCELL_E2E_KAFKA_IMAGE")
	if image == "" {
		image = "mirror.gcr.io/apache/kafka:4.1.0"
	}
	dir := t.TempDir()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"}, NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, _ := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	caCert, _ := x509.ParseCertificate(caDER)
	srvKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	srvDER, _ := x509.CreateCertificate(rand.Reader, &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "b0.test.local"},
		DNSNames: []string{"b0.test.local"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}, caCert, &srvKey.PublicKey, caKey)
	srvKeyDER, _ := x509.MarshalPKCS8PrivateKey(srvKey)
	write := func(name string, blocks ...*pem.Block) {
		var b []byte
		for _, bl := range blocks {
			b = append(b, pem.EncodeToMemory(bl)...)
		}
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("ca.pem", &pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	write("server.pem", &pem.Block{Type: "PRIVATE KEY", Bytes: srvKeyDER}, &pem.Block{Type: "CERTIFICATE", Bytes: srvDER})
	props := `process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:19096
listeners=SECURE://:19095,CONTROLLER://:19096
advertised.listeners=SECURE://b0.test.local:19095
listener.security.protocol.map=SECURE:SASL_SSL,CONTROLLER:PLAINTEXT
inter.broker.listener.name=SECURE
controller.listener.names=CONTROLLER
sasl.enabled.mechanisms=SCRAM-SHA-512
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
listener.name.secure.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="u" password="pw";
ssl.keystore.type=PEM
ssl.keystore.location=/it/server.pem
ssl.truststore.type=PEM
ssl.truststore.location=/it/ca.pem
ssl.endpoint.identification.algorithm=
ssl.client.auth=none
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.dirs=/tmp/kraft
`
	if err := os.WriteFile(filepath.Join(dir, "server.properties"), []byte(props), 0o644); err != nil {
		t.Fatal(err)
	}
	name := "streamcell-loadgen-tls-test"
	_ = exec.Command("docker", "rm", "-f", name).Run()
	script := `/opt/kafka/bin/kafka-storage.sh format -t "$(/opt/kafka/bin/kafka-storage.sh random-uuid)" -c /it/server.properties --add-scram 'SCRAM-SHA-512=[name=u,password=pw]' && exec /opt/kafka/bin/kafka-server-start.sh /it/server.properties`
	if out, err := exec.Command("docker", "run", "-d", "--name", name, "--network", "host", "-v", dir+":/it", "--entrypoint", "bash", image, "-c", script).CombinedOutput(); err != nil {
		t.Skipf("docker not available: %v %s", err, out)
	}
	t.Cleanup(func() {
		if t.Failed() {
			out, _ := exec.Command("docker", "logs", "--tail", "40", name).CombinedOutput()
			t.Logf("broker log:\n%s", out)
		}
		_ = exec.Command("docker", "rm", "-f", name).Run()
	})

	c := Conn{Bootstrap: "b0.test.local:19095", User: "u", Password: "pw", CAFile: filepath.Join(dir, "ca.pem"), DialAddr: "127.0.0.1:19095"}
	ctx := context.Background()
	var ok bool
	for i := 0; i < 60 && !ok; i++ {
		opts, _ := c.opts()
		cl, err := kgo.NewClient(opts...)
		if err == nil {
			_, err = kadm.NewClient(cl).CreateTopic(ctx, 3, 1, nil, "load")
			cl.Close()
			ok = err == nil
		}
		if !ok {
			time.Sleep(time.Second)
		}
	}
	if !ok {
		t.Fatal("could not reach the broker over TLS with SCRAM")
	}
	ledger := filepath.Join(dir, "ledger")
	res, err := Produce(ctx, c, ProduceOptions{Topic: "load", Producers: 2, Rate: 200, Size: 64, Ledger: ledger, Duration: 3 * time.Second})
	if err != nil || res.Acked < 300 || res.Failed != 0 {
		t.Fatalf("produce: %+v %v", res, err)
	}
	rep, err := Verify(ctx, c, "load", ledger)
	if err != nil || !rep.OK() || rep.Acked != int(res.Acked) {
		t.Fatalf("verify: %s %v", rep, err)
	}
	t.Log(rep)

	// A wrong CA must fail, not silently skip verification.
	other, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	oDER, _ := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &other.PublicKey, other)
	if err := os.WriteFile(filepath.Join(dir, "other.pem"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: oDER}), 0o644); err != nil {
		t.Fatal(err)
	}
	bad := c
	bad.CAFile = filepath.Join(dir, "other.pem")
	bctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if _, err := Verify(bctx, bad, "load", ledger); err == nil {
		t.Fatal("verification with the wrong CA succeeded")
	}
}
