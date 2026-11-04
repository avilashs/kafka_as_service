//go:build e2e

package e2e

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/avilashs/kafka_as_service/internal/platform/secrets"
)

// activeCluster creates a cluster and waits until it is ACTIVE.
func (e *env) activeCluster(name string) string {
	e.t.Helper()
	opID, id := e.createCluster(name, "create-"+name+"-"+randHex(3))
	e.waitOp(opID, "SUCCEEDED", 60*time.Second)
	return id
}

func (e *env) userOp(method, path string, body any) apiResp {
	e.t.Helper()
	return e.call(method, path, "k-"+randHex(6), body)
}

// mustUserOp runs a user mutation to completion.
func (e *env) mustUserOp(method, path string, body any) {
	e.t.Helper()
	r := e.userOp(method, path, body)
	if r.Code != http.StatusAccepted {
		e.t.Fatalf("%s %s: %d %v", method, path, r.Code, r.Body)
	}
	e.waitOp(r.Body["id"].(string), "SUCCEEDED", 60*time.Second)
}

func (e *env) credentials(clusterID, user string) (map[string]any, int) {
	r := e.call("GET", "/v1/clusters/"+clusterID+"/scram-users/"+user+"/credentials", "", nil)
	return r.Body, r.Code
}

// dumpDatabase returns every row of every table, as text.
func dumpDatabase(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	rows, err := st.Pool.Query(ctx, `SELECT table_name FROM information_schema.tables WHERE table_schema='public'`)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var n string
		_ = rows.Scan(&n)
		tables = append(tables, n)
	}
	rows.Close()
	if len(tables) < 8 {
		t.Fatalf("only %d tables found; the scan would prove nothing", len(tables))
	}
	var sb strings.Builder
	for _, tb := range tables {
		r, err := st.Pool.Query(ctx, fmt.Sprintf(`SELECT x::text FROM %q x`, tb))
		if err != nil {
			t.Fatalf("dump %s: %v", tb, err)
		}
		for r.Next() {
			var row string
			_ = r.Scan(&row)
			sb.WriteString(row)
			sb.WriteByte('\n')
		}
		r.Close()
	}
	return sb.String()
}

func k8sPassword(t *testing.T, clusterID, user string) string {
	t.Helper()
	sec, err := kube.CoreV1().Secrets(clusterID).Get(context.Background(), "user-"+user, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("secret user-%s: %v", user, err)
	}
	return string(sec.Data["password"])
}

func waitFor(t *testing.T, what string, d time.Duration, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// Users: create, read credentials, rotate, delete — and the password never reaches the database.
func TestM3UserLifecycle(t *testing.T) {
	e := newEnv(t)
	e.startWorker()
	id := e.activeCluster("userlife")
	ctx := context.Background()
	base := "/v1/clusters/" + id + "/scram-users"

	e.mustUserOp("POST", base, map[string]any{"name": "app", "acls": []any{
		map[string]any{"resource": map[string]any{"type": "topic", "name": "orders", "patternType": "prefix"}, "operations": []string{"Read", "Write", "Describe"}},
	}})
	e.mustUserOp("POST", base, map[string]any{"name": "root", "admin": true})

	// Both exist, with the ACLs asked for; the listing carries no credentials.
	r := e.call("GET", base, "", nil)
	if r.Code != 200 || strings.Contains(r.Raw, "password") {
		t.Fatalf("list users: %d %s", r.Code, r.Raw)
	}
	users := r.Body["users"].([]any)
	if len(users) != 2 || users[0].(map[string]any)["name"] != "app" || users[1].(map[string]any)["admin"] != true {
		t.Fatalf("users: %v", users)
	}
	// The cluster description does not leak the user list either: users have their own endpoints.
	if c := e.call("GET", "/v1/clusters/"+id, "", nil); strings.Contains(c.Raw, `"users"`) || strings.Contains(c.Raw, "secretVersion") {
		t.Errorf("cluster spec exposes users: %s", c.Raw)
	}

	// Credentials: from the secret manager, equal to what the cell got, strong.
	cred, code := e.credentials(id, "app")
	if code != 200 {
		t.Fatalf("credentials: %d %v", code, cred)
	}
	pw1 := cred["password"].(string)
	if len(pw1) < 43 || cred["username"] != "app" || cred["mechanism"] != "SCRAM-SHA-512" {
		t.Fatalf("credentials: %v", cred)
	}
	cell := e.cellID
	stored, v1, err := secretsSt.Get(ctx, secrets.UserPath(cell, id, "app"))
	if err != nil || stored["password"] != pw1 || v1 != 1 {
		t.Fatalf("secret manager holds %v v%d (%v), API returned %s", stored, v1, err, pw1)
	}
	waitFor(t, "cell secret", 10*time.Second, func() bool { return k8sPassword(t, id, "app") == pw1 })

	// KafkaUser objects carry the ACLs and no password.
	ku, err := dyn.Resource(kafkaUserGVR).Namespace(id).Get(ctx, "app", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	acls, _, _ := unstructured.NestedSlice(ku.Object, "spec", "authorization", "acls")
	if len(acls) != 1 {
		t.Errorf("KafkaUser app: %v", acls)
	}
	if b, _ := ku.MarshalJSON(); strings.Contains(string(b), pw1) {
		t.Error("KafkaUser contains the password")
	}
	kr, _ := dyn.Resource(kafkaUserGVR).Namespace(id).Get(ctx, "root", metav1.GetOptions{})
	racls, _, _ := unstructured.NestedSlice(kr.Object, "spec", "authorization", "acls")
	for _, a := range racls { // admin must never get broker-level operations
		m := a.(map[string]any)
		if m["resource"].(map[string]any)["type"] == "cluster" {
			for _, op := range m["operations"].([]any) {
				if op == "Alter" || op == "AlterConfigs" || op == "ClusterAction" || op == "All" {
					t.Errorf("admin user has cluster operation %v", op)
				}
			}
		}
	}

	// The password exists nowhere in the database: scan every table.
	dump := dumpDatabase(t)
	if strings.Contains(dump, pw1) {
		t.Fatal("a user password appears in the platform database")
	}

	// Rotation: new password, version 2, cell secret follows, old one is gone.
	e.mustUserOp("POST", base+"/app/rotate", nil)
	cred2, _ := e.credentials(id, "app")
	pw2 := cred2["password"].(string)
	if pw2 == pw1 || cred2["secretVersion"].(float64) != 2 {
		t.Fatalf("rotation: %v", cred2)
	}
	waitFor(t, "rotated cell secret", 10*time.Second, func() bool { return k8sPassword(t, id, "app") == pw2 })
	if d := dumpDatabase(t); strings.Contains(d, pw1) || strings.Contains(d, pw2) {
		t.Fatal("a rotated password appears in the platform database")
	}

	// Delete: the user, its KafkaUser, its cell secret and its stored secret all go.
	e.mustUserOp("DELETE", base+"/app", nil)
	if _, code := e.credentials(id, "app"); code != http.StatusNotFound {
		t.Errorf("credentials of deleted user: %d", code)
	}
	if _, _, err := secretsSt.Get(ctx, secrets.UserPath(cell, id, "app")); err != secrets.ErrNotFound {
		t.Errorf("secret of deleted user still stored (%v)", err)
	}
	waitFor(t, "KafkaUser and cell secret removed", 10*time.Second, func() bool {
		_, e1 := dyn.Resource(kafkaUserGVR).Namespace(id).Get(ctx, "app", metav1.GetOptions{})
		_, e2 := kube.CoreV1().Secrets(id).Get(ctx, "user-app", metav1.GetOptions{})
		return apierrors.IsNotFound(e1) && apierrors.IsNotFound(e2)
	})
	if _, code := e.credentials(id, "root"); code != 200 {
		t.Errorf("the other user was affected: %d", code)
	}

	// Deleting the cluster removes every secret it kept.
	opID := e.call("DELETE", "/v1/clusters/"+id, "del-"+randHex(3), nil).Body["id"].(string)
	e.waitOp(opID, "SUCCEEDED", 120*time.Second)
	if _, _, err := secretsSt.Get(ctx, secrets.UserPath(cell, id, "root")); err != secrets.ErrNotFound {
		t.Errorf("cluster delete left a secret behind (%v)", err)
	}
	assertDeleted(t, e, id)
}

// Bad requests are refused before they take the cluster lock; replays and races behave.
func TestM3UserValidationAndIdempotency(t *testing.T) {
	e := newEnv(t)
	e.startWorker()
	id := e.activeCluster("uservalid")
	base := "/v1/clusters/" + id + "/scram-users"
	topic := func(ops ...string) map[string]any {
		return map[string]any{"resource": map[string]any{"type": "topic", "name": "t"}, "operations": ops}
	}
	cluster := func(ops ...string) map[string]any {
		return map[string]any{"resource": map[string]any{"type": "cluster"}, "operations": ops}
	}
	bad := []struct {
		name string
		body map[string]any
	}{
		{"reserved prefix", map[string]any{"name": "platform-admin"}},
		{"upper case", map[string]any{"name": "Bob"}},
		{"one letter", map[string]any{"name": "a"}},
		{"unknown field", map[string]any{"name": "okuser", "superuser": true}},
		{"cluster Alter", map[string]any{"name": "okuser", "acls": []any{cluster("Alter")}}},
		{"cluster AlterConfigs", map[string]any{"name": "okuser", "acls": []any{cluster("AlterConfigs")}}},
		{"cluster ClusterAction", map[string]any{"name": "okuser", "acls": []any{cluster("ClusterAction")}}},
		{"cluster All", map[string]any{"name": "okuser", "acls": []any{cluster("All")}}},
		{"reserved topic prefix", map[string]any{"name": "okuser", "acls": []any{map[string]any{"resource": map[string]any{"type": "topic", "name": "__consumer_offsets"}, "operations": []string{"Read"}}}}},
		{"no operations", map[string]any{"name": "okuser", "acls": []any{topic()}}},
		{"unknown operation", map[string]any{"name": "okuser", "acls": []any{topic("Root")}}},
		{"prefix star", map[string]any{"name": "okuser", "acls": []any{map[string]any{"resource": map[string]any{"type": "topic", "name": "*", "patternType": "prefix"}, "operations": []string{"Read"}}}}},
	}
	for _, tc := range bad {
		if r := e.userOp("POST", base, tc.body); r.Code != http.StatusUnprocessableEntity && r.Code != http.StatusBadRequest {
			t.Errorf("%s: got %d %v, want 4xx", tc.name, r.Code, r.Body)
		}
	}
	if n := countRows(t, `SELECT count(*) FROM operations WHERE cluster_id=$1 AND type LIKE '%ScramUser'`, id); n != 0 {
		t.Errorf("%d operations were created by invalid requests", n)
	}
	if r := e.userOp("POST", base+"/ghost/rotate", nil); r.Code != http.StatusNotFound {
		t.Errorf("rotate unknown user: %d", r.Code)
	}
	if r := e.userOp("DELETE", base+"/ghost", nil); r.Code != http.StatusNotFound {
		t.Errorf("delete unknown user: %d", r.Code)
	}
	// Users can't be smuggled in at cluster creation (they would have no secret).
	body := createBody("smuggle")
	body["spec"].(map[string]any)["users"] = []any{map[string]any{"name": "sneaky"}}
	if r := e.call("POST", "/v1/clusters", "smuggle-"+randHex(3), body); r.Code != http.StatusUnprocessableEntity {
		t.Errorf("users in create: %d %v", r.Code, r.Body)
	}

	// Idempotency: same key and body replays the operation; the user is created once.
	key := "same-" + randHex(4)
	body1 := map[string]any{"name": "dup"}
	r1 := e.call("POST", base, key, body1)
	r2 := e.call("POST", base, key, body1)
	if r1.Code != 202 || r2.Code != 202 || r1.Body["id"] != r2.Body["id"] || r2.Header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay: %d %v / %d %v", r1.Code, r1.Body, r2.Code, r2.Body)
	}
	if r3 := e.call("POST", base, key, map[string]any{"name": "other"}); r3.Code != http.StatusUnprocessableEntity {
		t.Errorf("same key, different body: %d", r3.Code)
	}
	e.waitOp(r1.Body["id"].(string), "SUCCEEDED", 60*time.Second)
	// A second create of the same name is a validation error, not a second user.
	if r := e.userOp("POST", base, body1); r.Code != http.StatusUnprocessableEntity {
		t.Errorf("duplicate user: %d %v", r.Code, r.Body)
	}
	if n := countRows(t, `SELECT count(*) FROM operations WHERE cluster_id=$1 AND type='CreateScramUser'`, id); n != 1 {
		t.Errorf("%d CreateScramUser operations, want 1", n)
	}

	// Per-resource lock: while one change is in flight a second is refused with 409.
	sim.setReadyDelay(2 * time.Second)
	defer sim.setReadyDelay(0)
	first := e.userOp("POST", base, map[string]any{"name": "slow"})
	if first.Code != 202 {
		t.Fatalf("slow create: %d %v", first.Code, first.Body)
	}
	if r := e.userOp("POST", base, map[string]any{"name": "second"}); r.Code != http.StatusConflict {
		t.Errorf("concurrent change: %d %v, want 409", r.Code, r.Body)
	}
	e.waitOp(first.Body["id"].(string), "SUCCEEDED", 60*time.Second)
}

// A tenant can see only its own clusters' users, credentials and CA.
func TestM3TenantScoping(t *testing.T) {
	e := newEnv(t)
	e.startWorker()
	id := e.activeCluster("scoped")
	e.mustUserOp("POST", "/v1/clusters/"+id+"/scram-users", map[string]any{"name": "app"})
	other := e.otherTenant()
	for _, p := range []string{"/scram-users", "/scram-users/app/credentials", "/ca-certificate", "/bootstrap-brokers"} {
		if r := e.callAs(other, "GET", "/v1/clusters/"+id+p, "", nil); r.Code != http.StatusNotFound {
			t.Errorf("other tenant GET %s: %d, want 404", p, r.Code)
		}
	}
	for _, c := range []struct{ method, p string }{{"POST", "/scram-users/app/rotate"}, {"DELETE", "/scram-users/app"}} {
		if r := e.callAs(other, c.method, "/v1/clusters/"+id+c.p, "x-"+randHex(3), nil); r.Code != http.StatusNotFound {
			t.Errorf("other tenant %s %s: %d, want 404", c.method, c.p, r.Code)
		}
	}
	if r := e.callAs(other, "POST", "/v1/clusters/"+id+"/scram-users", "x-"+randHex(3), map[string]any{"name": "evil"}); r.Code != http.StatusNotFound {
		t.Errorf("other tenant create user: %d", r.Code)
	}
	if _, code := e.credentials(id, "app"); code != 200 {
		t.Errorf("owner lost access: %d", code)
	}
	if r := e.call("GET", "/v1/clusters/"+id+"/scram-users/app/credentials", "", nil); r.Header.Get("Cache-Control") != "no-store" {
		t.Errorf("credentials are cacheable: %q", r.Header.Get("Cache-Control"))
	}
}

// Every cluster has its own CA, served as PEM.
func TestM3ClusterCA(t *testing.T) {
	e := newEnv(t)
	e.startWorker()
	a, b := e.activeCluster("ca-a"), e.activeCluster("ca-b")
	get := func(id string) *x509.Certificate {
		var raw string
		waitFor(t, "CA of "+id, 10*time.Second, func() bool {
			r := e.call("GET", "/v1/clusters/"+id+"/ca-certificate", "", nil)
			raw = r.Raw
			return r.Code == 200
		})
		blk, _ := pem.Decode([]byte(raw))
		if blk == nil || blk.Type != "CERTIFICATE" {
			t.Fatalf("not a PEM certificate: %q", raw)
		}
		c, err := x509.ParseCertificate(blk.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	ca, cb := get(a), get(b)
	if !ca.IsCA || !cb.IsCA || ca.Equal(cb) || string(ca.RawSubjectPublicKeyInfo) == string(cb.RawSubjectPublicKeyInfo) {
		t.Error("clusters must not share a CA")
	}
}
