// Command streamcell-admission runs the pods/binding guard webhook and its certificate job.
//
//	streamcell-admission serve   [--addr :8443] [--tls-dir /tls]
//	streamcell-admission certgen --namespace streamcell-system --service streamcell-admission \
//	                             --secret streamcell-admission-tls --webhook streamcell-binding-guard
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/avilashs/kafka_as_service/internal/admission"
)

func main() { os.Exit(run()) }

func run() int {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: streamcell-admission serve|certgen [flags]")
		return 2
	}
	cmd, args := os.Args[1], os.Args[2:]
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	rc, err := rest.InClusterConfig()
	if err != nil {
		rc, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(), nil).ClientConfig()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "kubeconfig:", err)
		return 2
	}
	kube, err := kubernetes.NewForConfig(rc)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch cmd {
	case "certgen":
		fs := flag.NewFlagSet("certgen", flag.ExitOnError)
		ns := fs.String("namespace", "streamcell-system", "namespace of the Service and Secret")
		svc := fs.String("service", "streamcell-admission", "Service name the webhook is reached at")
		secret := fs.String("secret", "streamcell-admission-tls", "Secret to store the serving certificate in")
		webhook := fs.String("webhook", "streamcell-binding-guard", "ValidatingWebhookConfiguration to inject the CA bundle into")
		_ = fs.Parse(args)
		if err := admission.EnsureInCluster(ctx, kube, *ns, *secret, *svc, *webhook); err != nil {
			fmt.Fprintln(os.Stderr, "certgen:", err)
			return 1
		}
		log.Info("certificates ready", "secret", *secret, "webhook", *webhook)
		return 0
	case "serve":
		fs := flag.NewFlagSet("serve", flag.ExitOnError)
		addr := fs.String("addr", ":8443", "listen address")
		dir := fs.String("tls-dir", "/tls", "directory with tls.crt and tls.key")
		_ = fs.Parse(args)
		src := &admission.FileCertSource{CertFile: filepath.Join(*dir, "tls.crt"), KeyFile: filepath.Join(*dir, "tls.key")}
		srv := &http.Server{
			Addr: *addr, Handler: (&admission.Guard{Kube: kube, Log: log}).Handler(), ReadHeaderTimeout: 10 * time.Second,
			TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: src.GetCertificate},
		}
		go func() {
			log.Info("binding guard listening", "addr", *addr)
			if err := srv.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("serve", "err", err)
				stop()
			}
		}()
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
		return 0
	}
	fmt.Fprintln(os.Stderr, "unknown command", cmd)
	return 2
}
