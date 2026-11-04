package agent

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/avilashs/kafka_as_service/internal/platform/engine"
	"github.com/avilashs/kafka_as_service/internal/platform/labels"
)

const (
	certSecretPrefix = "client-cert-"
	annCSRHash       = labels.Prefix + "csr-sha256"
	annCAHash        = labels.Prefix + "ca-sha256"
	defaultCertDays  = 90
)

// certOutputs signs the bundle's certificate requests that are not signed yet and returns every issued
// certificate as an output. The signed certificate is kept in a Secret of the cluster namespace (public
// data) so that it is stable across agent restarts: an unchanged request and CA never produce a new serial.
// A request whose CA does not exist yet is skipped; the next status round tries again.
func (a *Agent) certOutputs(ctx context.Context, b *engine.Bundle) map[string]string {
	if b == nil || len(b.CertRequests) == 0 {
		return nil
	}
	out := map[string]string{}
	for _, r := range b.CertRequests {
		pemCert, err := a.issue(ctx, r)
		if err != nil {
			a.Log.Warn("client certificate not issued", "request", r.Name, "ns", r.Namespace, "err", err)
			continue
		}
		out[engine.CertOutputPrefix+r.Name] = pemCert
	}
	return out
}

func sum(b ...[]byte) string {
	h := sha256.New()
	for _, x := range b {
		h.Write(x)
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (a *Agent) issue(ctx context.Context, r engine.CertRequest) (string, error) {
	caCertSec, err := a.Kube.CoreV1().Secrets(r.Namespace).Get(ctx, r.CACertSecret, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("CA certificate secret: %w", err)
	}
	caKeySec, err := a.Kube.CoreV1().Secrets(r.Namespace).Get(ctx, r.CAKeySecret, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("CA key secret: %w", err)
	}
	caCertPEM, caKeyPEM := caCertSec.Data[r.CACertKey], caKeySec.Data[r.CAKeyKey]
	want := sum([]byte(r.CSR), []byte(r.CommonName))
	caHash := sum(caCertPEM)

	name := certSecretPrefix + r.Name
	existing, err := a.Kube.CoreV1().Secrets(r.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err == nil && existing.Annotations[annCSRHash] == want && existing.Annotations[annCAHash] == caHash {
		if crt := existing.Data["tls.crt"]; len(crt) > 0 && !expiringSoon(crt) {
			return string(crt), nil
		}
	} else if err != nil && !apierrors.IsNotFound(err) {
		return "", err
	}

	crt, err := signCSR(r, caCertPEM, caKeyPEM)
	if err != nil {
		return "", err
	}
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: r.Namespace,
			Labels:      map[string]string{labels.Inventory: r.Namespace},
			Annotations: map[string]string{annCSRHash: want, annCAHash: caHash}},
		Data: map[string][]byte{"tls.crt": crt},
	}
	if _, err := a.Kube.CoreV1().Secrets(r.Namespace).Create(ctx, sec, metav1.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return "", err
		}
		cur, gerr := a.Kube.CoreV1().Secrets(r.Namespace).Get(ctx, name, metav1.GetOptions{})
		if gerr != nil {
			return "", gerr
		}
		cur.Data, cur.Annotations = sec.Data, sec.Annotations
		if _, err := a.Kube.CoreV1().Secrets(r.Namespace).Update(ctx, cur, metav1.UpdateOptions{}); err != nil {
			return "", err
		}
	}
	return string(crt), nil
}

func expiringSoon(crtPEM []byte) bool {
	blk, _ := pem.Decode(crtPEM)
	if blk == nil {
		return true
	}
	c, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		return true
	}
	life := c.NotAfter.Sub(c.NotBefore)
	return time.Until(c.NotAfter) < life/3
}

// signCSR issues a client certificate for the CSR's public key. Everything but the public key is decided
// here: subject, key usage, validity.
func signCSR(r engine.CertRequest, caCertPEM, caKeyPEM []byte) ([]byte, error) {
	blk, _ := pem.Decode([]byte(r.CSR))
	if blk == nil || blk.Type != "CERTIFICATE REQUEST" {
		return nil, errors.New("the request is not a PEM CERTIFICATE REQUEST")
	}
	csr, err := x509.ParseCertificateRequest(blk.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse CSR: %w", err)
	}
	if err := csr.CheckSignature(); err != nil { // proof that the requester holds the private key
		return nil, fmt.Errorf("CSR signature: %w", err)
	}
	cb, _ := pem.Decode(caCertPEM)
	if cb == nil {
		return nil, errors.New("CA certificate is not PEM")
	}
	ca, err := x509.ParseCertificate(cb.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse CA certificate: %w", err)
	}
	kb, _ := pem.Decode(caKeyPEM)
	if kb == nil {
		return nil, errors.New("CA key is not PEM")
	}
	key, err := x509.ParsePKCS8PrivateKey(kb.Bytes)
	if err != nil {
		if k1, e1 := x509.ParsePKCS1PrivateKey(kb.Bytes); e1 == nil {
			key, err = k1, nil
		} else if k2, e2 := x509.ParseECPrivateKey(kb.Bytes); e2 == nil {
			key, err = k2, nil
		}
	}
	if err != nil {
		return nil, fmt.Errorf("parse CA key: %w", err)
	}
	days := r.ValidDays
	if days <= 0 {
		days = defaultCertDays
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return nil, err
	}
	now := time.Now()
	notAfter := now.Add(time.Duration(days) * 24 * time.Hour)
	if notAfter.After(ca.NotAfter) {
		notAfter = ca.NotAfter
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: r.CommonName},
		NotBefore: now.Add(-5 * time.Minute), NotAfter: notAfter,
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, csr.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("sign: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), nil
}
