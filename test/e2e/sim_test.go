//go:build e2e

package e2e

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
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

var (
	kafkaGVR     = schema.GroupVersionResource{Group: "kafka.strimzi.io", Version: "v1", Resource: "kafkas"}
	kafkaUserGVR = schema.GroupVersionResource{Group: "kafka.strimzi.io", Version: "v1", Resource: "kafkausers"}
	nodePoolGVR  = schema.GroupVersionResource{Group: "kafka.strimzi.io", Version: "v1", Resource: "kafkanodepools"}
)

// opSim is a TEST-ONLY stand-in for two controllers that envtest lacks:
//   - Strimzi: marks a Kafka CR Ready for its current generation (after readyDelay),
//     publishes the external listener's bootstrap address, creates the cluster CA
//     certificate Secret (<cluster>-cluster-ca-cert, a fresh self-signed CA per cluster)
//     and marks KafkaUser CRs Ready, as the cluster and user operators do;
//   - the namespace controller: finalizes Terminating namespaces once our objects are gone.
//
// It never creates or deletes platform objects itself.
type opSim struct {
	dyn        dynamic.Interface
	kube       kubernetes.Interface
	readyDelay atomic.Int64 // nanoseconds
	mu         sync.Mutex
	firstSeen  map[string]time.Time
}

func (s *opSim) setReadyDelay(d time.Duration) { s.readyDelay.Store(int64(d)) }

func (s *opSim) run(ctx context.Context) {
	s.firstSeen = map[string]time.Time{}
	t := time.NewTicker(150 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.reconcileKafkas(ctx)
			s.reconcileUsers(ctx)
			s.confirmPodTermination(ctx)
			s.finalizeNamespaces(ctx)
		}
	}
}

func (s *opSim) reconcileKafkas(ctx context.Context) {
	list, err := s.dyn.Resource(kafkaGVR).Namespace("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return
	}
	for i := range list.Items {
		k := &list.Items[i]
		if k.GetDeletionTimestamp() != nil {
			continue
		}
		og, _, _ := unstructured.NestedInt64(k.Object, "status", "observedGeneration")
		if og == k.GetGeneration() {
			continue
		}
		key := fmt.Sprintf("%s/%s/%d", k.GetNamespace(), k.GetName(), k.GetGeneration())
		s.mu.Lock()
		first, ok := s.firstSeen[key]
		if !ok {
			first = time.Now()
			s.firstSeen[key] = first
		}
		s.mu.Unlock()
		if time.Since(first) < time.Duration(s.readyDelay.Load()) {
			continue
		}
		version, _, _ := unstructured.NestedString(k.Object, "spec", "kafka", "version")
		bootstrap := ""
		listeners, _, _ := unstructured.NestedSlice(k.Object, "spec", "kafka", "listeners")
		for _, l := range listeners {
			lm := l.(map[string]any)
			if lm["name"] == "external" {
				bootstrap, _, _ = unstructured.NestedString(lm, "configuration", "bootstrap", "host")
			}
		}
		k.Object["status"] = map[string]any{
			"observedGeneration": k.GetGeneration(),
			"conditions": []any{map[string]any{
				"type": "Ready", "status": "True", "lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
			}},
			"kafkaVersion": version,
			"listeners":    []any{map[string]any{"name": "external", "bootstrapServers": bootstrap + ":443"}},
		}
		s.ensureClusterCA(ctx, k.GetNamespace())
		_, _ = s.dyn.Resource(kafkaGVR).Namespace(k.GetNamespace()).UpdateStatus(ctx, k, metav1.UpdateOptions{})
	}
}

// ensureClusterCA creates the CA Secrets Strimzi would create for the cluster: the cluster CA certificate
// (brokers' server certificates are signed by it) and the clients CA (certificate and key; the cell agent
// signs client certificates with it).
func (s *opSim) ensureClusterCA(ctx context.Context, ns string) {
	create := func(name string, data map[string][]byte) {
		if _, err := s.kube.CoreV1().Secrets(ns).Get(ctx, name, metav1.GetOptions{}); err == nil {
			return
		}
		_, _ = s.kube.CoreV1().Secrets(ns).Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}, Data: data}, metav1.CreateOptions{})
	}
	crt, _ := testCA(ns + "-cluster-ca")
	create(ns+"-cluster-ca-cert", map[string][]byte{"ca.crt": []byte(crt)})
	if _, err := s.kube.CoreV1().Secrets(ns).Get(ctx, ns+"-clients-ca-cert", metav1.GetOptions{}); err != nil {
		ccrt, ckey := testCA(ns + "-clients-ca")
		create(ns+"-clients-ca-cert", map[string][]byte{"ca.crt": []byte(ccrt)})
		create(ns+"-clients-ca", map[string][]byte{"ca.key": []byte(ckey), "ca.crt": []byte(ccrt)})
	}
}

// testCA returns a fresh self-signed CA certificate and its PKCS#8 key, in PEM form.
func testCA(cn string) (certPEM, keyPEM string) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	kder, _ := x509.MarshalPKCS8PrivateKey(key)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kder}))
}

// reconcileUsers marks KafkaUser CRs Ready once the user secret they reference exists.
func (s *opSim) reconcileUsers(ctx context.Context) {
	list, err := s.dyn.Resource(kafkaUserGVR).Namespace("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return
	}
	for i := range list.Items {
		u := &list.Items[i]
		if u.GetDeletionTimestamp() != nil {
			continue
		}
		if og, _, _ := unstructured.NestedInt64(u.Object, "status", "observedGeneration"); og == u.GetGeneration() {
			continue
		}
		secretName, _, _ := unstructured.NestedString(u.Object, "spec", "authentication", "password", "valueFrom", "secretKeyRef", "name")
		if typ, _, _ := unstructured.NestedString(u.Object, "spec", "authentication", "type"); typ != "tls-external" {
			if _, err := s.kube.CoreV1().Secrets(u.GetNamespace()).Get(ctx, secretName, metav1.GetOptions{}); err != nil {
				continue // Strimzi reports NotReady until the referenced secret exists
			}
		}
		u.Object["status"] = map[string]any{
			"observedGeneration": u.GetGeneration(),
			"username":           u.GetName(),
			"conditions": []any{map[string]any{
				"type": "Ready", "status": "True", "lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
			}},
		}
		_, _ = s.dyn.Resource(kafkaUserGVR).Namespace(u.GetNamespace()).UpdateStatus(ctx, u, metav1.UpdateOptions{})
	}
}

func (s *opSim) confirmPodTermination(ctx context.Context) {
	pods, err := s.kube.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return
	}
	zero := int64(0)
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.DeletionTimestamp != nil {
			_ = s.kube.CoreV1().Pods(p.Namespace).Delete(ctx, p.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero})
		}
	}
}

func (s *opSim) finalizeNamespaces(ctx context.Context) {
	nss, err := s.kube.CoreV1().Namespaces().List(ctx, metav1.ListOptions{LabelSelector: "streamcell.dev/inventory"})
	if err != nil {
		return
	}
	for i := range nss.Items {
		ns := &nss.Items[i]
		if ns.DeletionTimestamp == nil || len(ns.Spec.Finalizers) == 0 {
			continue
		}
		// The real namespace controller deletes all content; here the agent must
		// already have deleted everything it created, so only finalize when empty.
		empty := true
		for _, gvr := range []schema.GroupVersionResource{kafkaGVR, nodePoolGVR} {
			l, err := s.dyn.Resource(gvr).Namespace(ns.Name).List(ctx, metav1.ListOptions{})
			if err != nil || len(l.Items) > 0 {
				empty = false
			}
		}
		if np, err := s.kube.NetworkingV1().NetworkPolicies(ns.Name).List(ctx, metav1.ListOptions{}); err != nil || len(np.Items) > 0 {
			empty = false
		}
		if pods, err := s.kube.CoreV1().Pods(ns.Name).List(ctx, metav1.ListOptions{}); err != nil || len(pods.Items) > 0 {
			empty = false
		}
		if !empty {
			continue
		}
		ns.Spec.Finalizers = nil
		if _, err := s.kube.CoreV1().Namespaces().Finalize(ctx, ns, metav1.UpdateOptions{}); err != nil && !apierrors.IsNotFound(err) {
			continue
		}
	}
}
