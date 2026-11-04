package admission

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Certs is a CA and a serving certificate signed by it, PEM encoded.
type Certs struct{ CA, Cert, Key []byte }

// Validity of generated certificates. Long on purpose: an expired webhook certificate
// makes a fail-closed webhook refuse every bind in tenant namespaces. Rotation (and an
// expiry alert) arrive with cert-manager integration in M6; see docs/milestones/m2.md.
const Validity = 10 * 365 * 24 * time.Hour

// Generate creates a CA and a serving certificate valid for the given DNS names and IPs.
func Generate(dnsNames []string, ips []net.IP) (*Certs, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	now := time.Now().Add(-time.Minute)
	caTmpl := &x509.Certificate{
		SerialNumber: serial(), Subject: pkix.Name{CommonName: "streamcell-admission-ca"},
		NotBefore: now, NotAfter: now.Add(Validity), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	caCert, _ := x509.ParseCertificate(caDER)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial(), Subject: pkix.Name{CommonName: dnsNames[0]},
		NotBefore: now, NotAfter: now.Add(Validity), DNSNames: dnsNames, IPAddresses: ips,
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}
	return &Certs{
		CA:   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		Cert: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		Key:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}, nil
}

func serial() *big.Int {
	n, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	return n
}

// EnsureInCluster is the certgen job: it makes sure Secret ns/secretName holds a valid
// serving certificate for the Service, and that the named ValidatingWebhookConfiguration
// trusts its CA. Safe to run repeatedly; an existing valid certificate is kept.
func EnsureInCluster(ctx context.Context, kube kubernetes.Interface, ns, secretName, service, webhookConfig string) error {
	names := []string{service, service + "." + ns, service + "." + ns + ".svc", service + "." + ns + ".svc.cluster.local"}
	secrets := kube.CoreV1().Secrets(ns)
	existing, err := secrets.Get(ctx, secretName, metav1.GetOptions{})
	var ca []byte
	switch {
	case err == nil && validFor(existing.Data["tls.crt"], names, 30*24*time.Hour):
		ca = existing.Data["ca.crt"]
	case err == nil || apierrors.IsNotFound(err):
		c, gerr := Generate(names, nil)
		if gerr != nil {
			return gerr
		}
		sec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: ns},
			Type:       corev1.SecretTypeTLS,
			Data:       map[string][]byte{"tls.crt": c.Cert, "tls.key": c.Key, "ca.crt": c.CA},
		}
		if err == nil {
			sec.ResourceVersion = existing.ResourceVersion
			_, err = secrets.Update(ctx, sec, metav1.UpdateOptions{})
		} else {
			_, err = secrets.Create(ctx, sec, metav1.CreateOptions{})
		}
		if err != nil {
			return fmt.Errorf("write secret: %w", err)
		}
		ca = c.CA
	default:
		return fmt.Errorf("read secret: %w", err)
	}
	vwc := kube.AdmissionregistrationV1().ValidatingWebhookConfigurations()
	cfg, err := vwc.Get(ctx, webhookConfig, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read webhook configuration %s: %w", webhookConfig, err)
	}
	changed := false
	for i := range cfg.Webhooks {
		if !bytes.Equal(cfg.Webhooks[i].ClientConfig.CABundle, ca) {
			cfg.Webhooks[i].ClientConfig.CABundle = ca
			changed = true
		}
	}
	if changed {
		if _, err := vwc.Update(ctx, cfg, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("patch caBundle: %w", err)
		}
	}
	return nil
}

func validFor(certPEM []byte, names []string, margin time.Duration) bool {
	blk, _ := pem.Decode(certPEM)
	if blk == nil {
		return false
	}
	c, err := x509.ParseCertificate(blk.Bytes)
	if err != nil || time.Now().Add(margin).After(c.NotAfter) {
		return false
	}
	for _, n := range names {
		if c.VerifyHostname(n) != nil {
			return false
		}
	}
	return true
}

// FileCertSource serves a certificate from files, re-reading them when they change
// (Secret volume updates), so rotation needs no restart.
type FileCertSource struct {
	CertFile, KeyFile string
	mu                sync.Mutex
	mtime             time.Time
	cert              *tls.Certificate
}

// GetCertificate implements tls.Config.GetCertificate.
func (f *FileCertSource) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	st, err := os.Stat(f.CertFile)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cert == nil || !st.ModTime().Equal(f.mtime) {
		c, err := tls.LoadX509KeyPair(f.CertFile, f.KeyFile)
		if err != nil {
			return nil, err
		}
		f.cert, f.mtime = &c, st.ModTime()
	}
	return f.cert, nil
}
