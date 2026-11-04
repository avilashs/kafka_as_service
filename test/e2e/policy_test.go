//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/avilashs/kafka_as_service/internal/platform/engine"
)

var (
	vapGVR     = schema.GroupVersionResource{Group: "admissionregistration.k8s.io", Version: "v1", Resource: "validatingadmissionpolicies"}
	vapBindGVR = schema.GroupVersionResource{Group: "admissionregistration.k8s.io", Version: "v1", Resource: "validatingadmissionpolicybindings"}
	cmGVR      = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
)

// adminUser is the identity the test process (acting as the cell agent) runs as.
var adminUser string

// installPolicies applies deploy/admission/policies/*.yaml exactly as shipped, except that
// the provisioner allowlist is set to the test process's own identity, and waits until
// the apiserver enforces them.
func installPolicies() error {
	ctx := context.Background()
	review, err := kube.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("self subject review: %w", err)
	}
	adminUser = review.Status.UserInfo.Username
	if _, err := kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "streamcell-system"}}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	files, _ := filepath.Glob(filepath.Join(repoRoot, "deploy/admission/policies/*.yaml"))
	if len(files) == 0 {
		return fmt.Errorf("no policy files found under %s", repoRoot)
	}
	for _, f := range files {
		if err := applyFile(ctx, f, func(u *unstructured.Unstructured) {
			if u.GetKind() == "ConfigMap" && u.GetName() == "streamcell-admission-params" {
				_ = unstructured.SetNestedField(u.Object, adminUser, "data", "provisioners")
			}
		}); err != nil {
			return fmt.Errorf("%s: %w", f, err)
		}
	}
	// The policies are enforced asynchronously: probe until a bad pod is refused.
	if _, err := kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: "policy-probe", Labels: map[string]string{"streamcell.dev/inventory": "policy-probe"}}}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		_, err := kube.CoreV1().Pods("policy-probe").Create(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "probe"},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "x"}}},
		}, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
		if err != nil && strings.Contains(err.Error(), "streamcell placement policy") {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("pod placement policy was not enforced within 60s")
}

func applyFile(ctx context.Context, path string, mutate func(*unstructured.Unstructured)) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	dec := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096)
	for {
		u := &unstructured.Unstructured{}
		if err := dec.Decode(&u.Object); err != nil {
			if err.Error() == "EOF" {
				return nil
			}
			return err
		}
		if len(u.Object) == 0 {
			continue
		}
		if mutate != nil {
			mutate(u)
		}
		var gvr schema.GroupVersionResource
		switch u.GetKind() {
		case "ValidatingAdmissionPolicy":
			gvr = vapGVR
		case "ValidatingAdmissionPolicyBinding":
			gvr = vapBindGVR
		case "ConfigMap":
			gvr = cmGVR
		default:
			return fmt.Errorf("unsupported kind %s", u.GetKind())
		}
		data, _ := json.Marshal(u.Object)
		force := true
		var ri = dyn.Resource(gvr)
		var err error
		if ns := u.GetNamespace(); ns != "" {
			_, err = ri.Namespace(ns).Patch(ctx, u.GetName(), types.ApplyPatchType, data, metav1.PatchOptions{FieldManager: "e2e", Force: &force})
		} else {
			_, err = ri.Patch(ctx, u.GetName(), types.ApplyPatchType, data, metav1.PatchOptions{FieldManager: "e2e", Force: &force})
		}
		if err != nil {
			return fmt.Errorf("apply %s %s: %w", u.GetKind(), u.GetName(), err)
		}
	}
}

// asUser returns a client that sends requests as another identity (impersonation), so
// tests can act as "someone who is not the cell agent" against the real apiserver.
func asUser(t *testing.T, user string) kubernetes.Interface {
	t.Helper()
	cfg := rest.CopyConfig(restCfg)
	cfg.Impersonate = rest.ImpersonationConfig{UserName: user, Groups: []string{"system:masters"}}
	return kubernetes.NewForConfigOrDie(cfg)
}

// tenantNamespace creates a namespace labelled as a tenant namespace (in policy scope).
func tenantNamespace(t *testing.T, name string) {
	t.Helper()
	_, err := kube.CoreV1().Namespaces().Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: name, Labels: map[string]string{"streamcell.dev/inventory": name}}}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
}

// podTemplates turns the pod templates of a rendered bundle into Pods, so the policy is
// tested against what the engine really renders. Keys: "brokers", "controllers",
// "cruise-control", "entity-operator".
func podTemplates(t *testing.T, b *engine.Bundle, clusterID string) map[string]*corev1.Pod {
	t.Helper()
	out := map[string]*corev1.Pod{}
	conv := func(name string, tmpl map[string]any) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: clusterID + "-" + name + "-0", Namespace: clusterID},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "registry.k8s.io/pause:3.10"}}},
		}
		if md, ok := tmpl["metadata"].(map[string]any); ok {
			if l, ok := md["labels"].(map[string]any); ok {
				pod.Labels = map[string]string{}
				for k, v := range l {
					pod.Labels[k] = v.(string)
				}
			}
		}
		for field, dst := range map[string]any{"affinity": &pod.Spec.Affinity, "tolerations": &pod.Spec.Tolerations, "topologySpreadConstraints": &pod.Spec.TopologySpreadConstraints} {
			if v, ok := tmpl[field]; ok {
				raw, _ := json.Marshal(v)
				if err := json.Unmarshal(raw, dst); err != nil {
					t.Fatalf("convert %s: %v", field, err)
				}
			}
		}
		out[name] = pod
	}
	for _, o := range b.Objects {
		switch o.GetKind() {
		case "KafkaNodePool":
			tmpl, _, _ := unstructured.NestedMap(o.Object, "spec", "template", "pod")
			conv(o.GetName(), tmpl)
		case "Kafka":
			cc, _, _ := unstructured.NestedMap(o.Object, "spec", "cruiseControl", "template", "pod")
			conv("cruise-control", cc)
			eo, _, _ := unstructured.NestedMap(o.Object, "spec", "entityOperator", "template", "pod")
			conv("entity-operator", eo)
		}
	}
	return out
}
