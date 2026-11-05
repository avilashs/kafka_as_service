package kafka

import (
	"encoding/json"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/avilashs/kafka_as_service/internal/platform/engine"
)

func withUsers(users ...User) Spec {
	s := okSpec()
	s.Users = users
	return s
}

func TestUserValidation(t *testing.T) {
	e := New()
	acl := func(typ, name string, ops ...string) []ACL {
		return []ACL{{Resource: ACLResource{Type: typ, Name: name}, Operations: ops}}
	}
	cases := []struct {
		name string
		u    []User
		want string // "" = valid
	}{
		{"plain user", []User{{Name: "alice"}}, ""},
		{"admin user", []User{{Name: "ops", Admin: true}}, ""},
		{"read-only on a topic prefix", []User{{Name: "rdr", ACLs: []ACL{{Resource: ACLResource{Type: "topic", Name: "orders-", PatternType: "prefix"}, Operations: []string{"Read", "Describe"}}}}}, ""},
		{"uppercase name", []User{{Name: "Alice"}}, "must match"},
		{"name too short", []User{{Name: "a"}}, "must match"},
		{"reserved platform prefix", []User{{Name: "platform-canary"}}, "reserved"},
		{"duplicate", []User{{Name: "alice"}, {Name: "alice"}}, "duplicated"},
		{"cluster Alter (would allow reassignments and ACL changes)", []User{{Name: "xuser", ACLs: acl("cluster", "", "Alter")}}, "not allowed on a cluster"},
		{"cluster AlterConfigs", []User{{Name: "xuser", ACLs: acl("cluster", "", "AlterConfigs")}}, "not allowed on a cluster"},
		{"cluster ClusterAction (acting as a broker)", []User{{Name: "xuser", ACLs: acl("cluster", "", "ClusterAction")}}, "not allowed on a cluster"},
		{"cluster All", []User{{Name: "xuser", ACLs: acl("cluster", "", "All")}}, "not allowed on a cluster"},
		{"named cluster resource", []User{{Name: "xuser", ACLs: acl("cluster", "kafka-cluster", "Describe")}}, "no name"},
		{"platform topic", []User{{Name: "xuser", ACLs: acl("topic", "__platform_canary", "Read")}}, "reserved for the platform"},
		{"internal topic", []User{{Name: "xuser", ACLs: acl("topic", "__consumer_offsets", "Read")}}, "reserved for the platform"},
		{"unknown resource type", []User{{Name: "xuser", ACLs: acl("delegationToken", "t", "Read")}}, "not one of"},
		{"unknown operation", []User{{Name: "xuser", ACLs: acl("topic", "t", "Destroy")}}, "not allowed on a topic"},
		{"no operations", []User{{Name: "xuser", ACLs: acl("topic", "t")}}, "at least one operation"},
		{"group Write makes no sense", []User{{Name: "xuser", ACLs: acl("group", "g", "Write")}}, "not allowed on a group"},
		{"bad resource name", []User{{Name: "xuser", ACLs: acl("topic", "a b", "Read")}}, "invalid"},
		{"prefix star", []User{{Name: "xuser", ACLs: []ACL{{Resource: ACLResource{Type: "topic", Name: "*", PatternType: "prefix"}, Operations: []string{"Read"}}}}}, "cannot be"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := e.Validate(spec(t, engine.TierShared, withUsers(c.u...)), nil)
			if c.want == "" && err != nil {
				t.Fatalf("unexpected: %v", err)
			}
			if c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)) {
				t.Fatalf("want error containing %q, got %v", c.want, err)
			}
		})
	}
	t.Run("too many users", func(t *testing.T) {
		var us []User
		for i := 0; i < maxUsers+1; i++ {
			us = append(us, User{Name: "user-" + strings.Repeat("a", 3) + string(rune('a'+i%26)) + string(rune('a'+i/26))})
		}
		if err := e.Validate(spec(t, engine.TierShared, withUsers(us...)), nil); err == nil || !strings.Contains(err.Error(), "at most") {
			t.Fatalf("got %v", err)
		}
	})
}

// The admin profile holds cluster Alter, which Kafka needs for CreateAcls/DeleteAcls and which
// PlatformAuthorizer restricts to exactly those two requests (plugins/kafka/it/it.sh proves that against a
// real broker). Nothing else broker-level may be in it, and tenants cannot grant Alter themselves.
func TestAdminACLsAreBoundedAndTenantsCannotGrantAlter(t *testing.T) {
	sawAlter := false
	for _, a := range adminACLs() {
		if a.Resource.Type == "cluster" {
			for _, op := range a.Operations {
				switch op {
				case "Alter":
					sawAlter = true
				case "AlterConfigs", "ClusterAction", "All", "Create":
					t.Errorf("admin gets cluster operation %s", op)
				}
			}
			continue
		}
		if err := validateACL(a); err != nil {
			t.Errorf("the admin ACL set must itself be valid: %v", err)
		}
	}
	if !sawAlter {
		t.Error("the admin profile must hold cluster Alter, or the tenant cannot manage ACLs")
	}
	for _, op := range []string{"Alter", "AlterConfigs", "ClusterAction", "Create", "All"} {
		if err := validateACL(ACL{Resource: ACLResource{Type: "cluster"}, Operations: []string{op}}); err == nil {
			t.Errorf("a tenant-defined ACL may not grant cluster %s", op)
		}
	}
}

func TestUserManager(t *testing.T) {
	e := New()
	raw := spec(t, engine.TierShared, okSpec()).Spec
	next, name, err := e.AddUser(raw, json.RawMessage(`{"name":"alice","admin":true}`))
	if err != nil || name != "alice" {
		t.Fatalf("AddUser: %v %q", err, name)
	}
	if _, _, err := e.AddUser(next, json.RawMessage(`{"name":"alice"}`)); err == nil {
		t.Fatal("duplicate user accepted")
	}
	if _, _, err := e.AddUser(next, json.RawMessage(`{"name":"bob","password":"hunter2"}`)); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("a user request must not accept a password field: %v", err)
	}
	if _, err := e.RotateUser(next, "nobody"); err == nil {
		t.Fatal("rotating an unknown user succeeded")
	} else if _, ok := err.(*engine.NotFoundError); !ok {
		t.Fatalf("want NotFoundError, got %T", err)
	}
	list, _ := e.ListUsers(next)
	if !strings.Contains(string(list), `"alice"`) || strings.Contains(string(list), "secret") || strings.Contains(string(list), "password") {
		t.Fatalf("list = %s", list)
	}
	back, err := e.RemoveUser(next, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if list, _ := e.ListUsers(back); string(list) != "[]" {
		t.Fatalf("after removal: %s", list)
	}
}

func TestRenderUsers(t *testing.T) {
	s := withUsers(User{Name: "alice", Admin: true, SecretVersion: 3},
		User{Name: "reader", ACLs: []ACL{{Resource: ACLResource{Type: "topic", Name: "orders", PatternType: "literal"}, Operations: []string{"Read"}}}, SecretVersion: 1})
	b := render(t, engine.TierShared, s)

	ku := find(b, "KafkaUser", "alice")
	if ku == nil {
		t.Fatal("no KafkaUser alice")
	}
	if typ, _, _ := unstructured.NestedString(ku.Object, "spec", "authentication", "type"); typ != "scram-sha-512" {
		t.Errorf("auth type = %s", typ)
	}
	ref, _, _ := unstructured.NestedStringMap(ku.Object, "spec", "authentication", "password", "valueFrom", "secretKeyRef")
	if ref["name"] != "user-alice" || ref["key"] != "password" {
		t.Errorf("password ref = %v", ref)
	}
	if ku.GetLabels()["strimzi.io/cluster"] != "kc-0123456789" {
		t.Errorf("KafkaUser is not bound to the cluster: %v", ku.GetLabels())
	}
	if ku.GetAnnotations()["streamcell.dev/password-version"] != "3" {
		t.Errorf("annotations = %v", ku.GetAnnotations())
	}
	acls, _, _ := unstructured.NestedSlice(ku.Object, "spec", "authorization", "acls")
	if len(acls) != len(adminACLs()) {
		t.Errorf("alice has %d ACLs, want the %d admin ACLs", len(acls), len(adminACLs()))
	}
	r := find(b, "KafkaUser", "reader")
	racls, _, _ := unstructured.NestedSlice(r.Object, "spec", "authorization", "acls")
	if len(racls) != 1 {
		t.Errorf("reader ACLs = %v", racls)
	}

	// Secrets are references, never values.
	if len(b.Secrets) != 3 { // alice, reader and the platform's canary principal
		t.Fatalf("secrets = %+v", b.Secrets)
	}
	if pk := find(b, "KafkaUser", PlatformCanaryUser); pk == nil {
		t.Error("the platform canary user is not rendered")
	} else if _, has, _ := unstructured.NestedMap(pk.Object, "spec", "authorization"); has {
		t.Error("the canary is a super user and carries no ACLs")
	}
	if k := find(b, "Kafka", "kc-0123456789"); k != nil {
		az, _, _ := unstructured.NestedMap(k.Object, "spec", "kafka", "authorization")
		found := false
		for _, su := range az["superUsers"].([]any) {
			found = found || su == PlatformCanaryUser
		}
		if !found {
			t.Errorf("superUsers lacks the canary: %v", az["superUsers"])
		}
	}
	for _, m := range b.Secrets {
		ref := m.Data["password"]
		if !strings.HasPrefix(ref.Path, "streamcell/cell-1/kc-0123456789/users/") || ref.Key != "password" {
			t.Errorf("secret ref = %+v", ref)
		}
	}
	all, _ := json.Marshal(b)
	for _, bad := range []string{"hunter2", "\"password\":\"", "stringData"} {
		if strings.Contains(string(all), bad) {
			t.Errorf("the rendered bundle contains %q", bad)
		}
	}
	if len(b.Outputs) != 1 || b.Outputs[0].Name != "cluster-ca.crt" || b.Outputs[0].ObjectName != "kc-0123456789-cluster-ca-cert" {
		t.Errorf("outputs = %+v", b.Outputs)
	}
}

func TestRenderChangesWhenAPasswordIsRotated(t *testing.T) {
	a, _ := json.Marshal(render(t, engine.TierShared, withUsers(User{Name: "alice", SecretVersion: 1})))
	b, _ := json.Marshal(render(t, engine.TierShared, withUsers(User{Name: "alice", SecretVersion: 2})))
	if string(a) == string(b) {
		t.Fatal("a rotation must change the rendered state, or the agent would never re-materialise the secret")
	}
}

func TestObserveStatusWaitsForUsers(t *testing.T) {
	e := New()
	rs := spec(t, engine.TierShared, withUsers(User{Name: "alice"}))
	ready := json.RawMessage(`{"observedGeneration":1,"conditions":[{"type":"Ready","status":"True"}]}`)
	obs := engine.Observed{Generation: 1, Objects: []engine.ObjectStatus{
		{Kind: "Kafka", Name: "kc-0123456789", Exists: true, Generation: 1, Status: ready},
		{Kind: "KafkaUser", Name: "alice", Exists: true, Generation: 1, Status: json.RawMessage(`{"observedGeneration":1,"conditions":[{"type":"Ready","status":"False","reason":"InvalidResource","message":"password secret missing"}]}`)},
	}}
	st, _ := e.ObserveStatus(rs, obs)
	if st.Ready || !strings.Contains(st.Reason, "alice") || !strings.Contains(st.Reason, "password secret missing") {
		t.Fatalf("a failing user must keep the cluster not ready: %+v", st)
	}
	obs.Objects[1].Status = ready
	if st, _ := e.ObserveStatus(rs, obs); !st.Ready {
		t.Fatalf("all ready: %+v", st)
	}
	obs.Objects[1].Generation = 2 // user changed, operator has not caught up
	if st, _ := e.ObserveStatus(rs, obs); st.Ready {
		t.Fatal("must wait for the user's current generation")
	}
}
