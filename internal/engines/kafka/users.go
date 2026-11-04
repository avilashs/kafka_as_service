package kafka

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/avilashs/kafka_as_service/internal/platform/engine"
	"github.com/avilashs/kafka_as_service/internal/platform/labels"
	"github.com/avilashs/kafka_as_service/internal/platform/secrets"
)

// User is a client principal of the cluster. Credentials are never part of the spec: the SCRAM
// password lives in the secret manager at secrets.UserPath, and SecretVersion only records which
// version the rendered state refers to (it changes on rotation).
type User struct {
	Name  string `json:"name"`
	Admin bool   `json:"admin,omitempty"`
	ACLs  []ACL  `json:"acls,omitempty"`
	// Auth is "scram" (default) or "tls". A tls user authenticates with a client certificate that the cell
	// signs for the public key in CSR; the private key never leaves the client.
	Auth          string `json:"auth,omitempty"`
	CSR           string `json:"csr,omitempty"`
	SecretVersion int    `json:"secretVersion,omitempty"`
}

// Authentication mechanisms of a user.
const (
	AuthSCRAM = "scram"
	AuthTLS   = "tls"
)

func (u User) isTLS() bool { return u.Auth == AuthTLS }

// PrincipalName is the name the broker authorizes. A certificate subject "CN=x" makes principal "User:CN=x".
func (u User) principalName() string {
	if u.isTLS() {
		return "CN=" + u.Name
	}
	return u.Name
}

// ACL grants operations on a resource to the user.
type ACL struct {
	Resource   ACLResource `json:"resource"`
	Operations []string    `json:"operations"`
}

// ACLResource names what the ACL applies to.
type ACLResource struct {
	Type        string `json:"type"` // topic | group | transactionalId | cluster
	Name        string `json:"name,omitempty"`
	PatternType string `json:"patternType,omitempty"` // literal (default) | prefix
}

const (
	maxUsers        = 50
	maxACLsPerUser  = 50
	reservedPrefix  = "__"
	platformPrefix  = "__platform"
	userNamePattern = `^[a-z][a-z0-9-]{0,30}[a-z0-9]$`
)

var (
	userNameRE     = regexp.MustCompile(userNamePattern)
	resourceNameRE = regexp.MustCompile(`^[A-Za-z0-9._*-]{1,249}$`)
)

// allowedOps lists, per resource type, the operations a tenant-defined ACL may grant. In particular
// no cluster ACL may grant Alter, AlterConfigs, ClusterAction or Create: those would let a tenant change
// broker configuration, reassign partitions, act as a broker or create topics with reserved names (design §15.1).
var allowedOps = map[string][]string{
	"topic":           {"Read", "Write", "Create", "Delete", "Alter", "Describe", "DescribeConfigs", "AlterConfigs", "All"},
	"group":           {"Read", "Delete", "Describe", "All"},
	"transactionalId": {"Write", "Describe", "All"},
	"cluster":         {"Describe", "DescribeConfigs", "IdempotentWrite"},
}

// adminACLs is what "admin: true" means: everything a tenant needs to run its own topics, groups and
// transactions, and nothing that touches the brokers. It is the customer admin principal of the design.
func adminACLs() []ACL {
	return []ACL{
		{Resource: ACLResource{Type: "topic", Name: "*", PatternType: "literal"}, Operations: []string{"All"}},
		{Resource: ACLResource{Type: "group", Name: "*", PatternType: "literal"}, Operations: []string{"All"}},
		{Resource: ACLResource{Type: "transactionalId", Name: "*", PatternType: "literal"}, Operations: []string{"All"}},
		// Alter on the cluster is how Kafka authorizes CreateAcls/DeleteAcls. PlatformAuthorizer grants it to
		// those two requests only and denies it for everything else (reassignment, leader election, features...).
		// Create is NOT here: cluster-wide Create would bypass the per-topic checks; topic Create comes with "All".
		{Resource: ACLResource{Type: "cluster"}, Operations: []string{"Alter", "Describe", "DescribeConfigs", "IdempotentWrite"}},
	}
}

func validateUsers(users []User) error {
	if len(users) > maxUsers {
		return engine.Invalid("at most %d users per cluster", maxUsers)
	}
	seen := map[string]bool{}
	for _, u := range users {
		if !userNameRE.MatchString(u.Name) {
			return engine.Invalid("user name %q must match %s", u.Name, userNamePattern)
		}
		// "kc-" is the prefix of cluster ids, and the platform principals' certificates are named after them
		// (CN=kc-<id>-entity-user-operator): a tenant must never be able to obtain a certificate with such a name.
		if strings.HasPrefix(u.Name, "platform") || strings.HasPrefix(u.Name, "kc-") || seen[u.Name] {
			return engine.Invalid("user name %q is reserved or duplicated", u.Name)
		}
		switch u.Auth {
		case "", AuthSCRAM:
			if u.CSR != "" {
				return engine.Invalid("user %s: a certificate request needs auth \"tls\"", u.Name)
			}
		case AuthTLS:
			if err := validateCSR(u.CSR); err != nil {
				return engine.Invalid("user %s: %v", u.Name, err)
			}
		default:
			return engine.Invalid("user %s: auth must be \"scram\" or \"tls\"", u.Name)
		}
		seen[u.Name] = true
		if len(u.ACLs) > maxACLsPerUser {
			return engine.Invalid("user %s: at most %d ACLs", u.Name, maxACLsPerUser)
		}
		for _, a := range u.ACLs {
			if err := validateACL(a); err != nil {
				return engine.Invalid("user %s: %v", u.Name, err)
			}
		}
	}
	return nil
}

// validateCSR accepts a PEM PKCS#10 request with a proven RSA (2048-4096 bit) or ECDSA (P-256, P-384) key.
func validateCSR(p string) error {
	if len(p) > 8192 {
		return fmt.Errorf("the certificate request is too large")
	}
	blk, rest := pem.Decode([]byte(p))
	if blk == nil || blk.Type != "CERTIFICATE REQUEST" || len(strings.TrimSpace(string(rest))) != 0 {
		return fmt.Errorf("csr must be exactly one PEM \"CERTIFICATE REQUEST\"")
	}
	req, err := x509.ParseCertificateRequest(blk.Bytes)
	if err != nil {
		return fmt.Errorf("csr: %v", err)
	}
	if err := req.CheckSignature(); err != nil {
		return fmt.Errorf("csr signature does not verify: %v", err)
	}
	switch k := req.PublicKey.(type) {
	case *rsa.PublicKey:
		if k.N.BitLen() < 2048 || k.N.BitLen() > 4096 {
			return fmt.Errorf("RSA keys must be 2048 to 4096 bits")
		}
	case *ecdsa.PublicKey:
		if k.Curve != elliptic.P256() && k.Curve != elliptic.P384() {
			return fmt.Errorf("ECDSA keys must use P-256 or P-384")
		}
	default:
		return fmt.Errorf("the key must be RSA or ECDSA")
	}
	return nil
}

func validateACL(a ACL) error {
	ops, ok := allowedOps[a.Resource.Type]
	if !ok {
		return fmt.Errorf("resource type %q is not one of topic, group, transactionalId, cluster", a.Resource.Type)
	}
	if len(a.Operations) == 0 {
		return fmt.Errorf("an ACL needs at least one operation")
	}
	for _, op := range a.Operations {
		if !slices.Contains(ops, op) {
			return fmt.Errorf("operation %q is not allowed on a %s resource (allowed: %s)", op, a.Resource.Type, strings.Join(ops, ", "))
		}
	}
	if a.Resource.Type == "cluster" {
		if a.Resource.Name != "" || a.Resource.PatternType != "" {
			return fmt.Errorf("a cluster resource has no name or pattern type")
		}
		return nil
	}
	if !resourceNameRE.MatchString(a.Resource.Name) {
		return fmt.Errorf("resource name %q is invalid", a.Resource.Name)
	}
	if strings.HasPrefix(a.Resource.Name, reservedPrefix) {
		return fmt.Errorf("resource names starting with %q are reserved for the platform", reservedPrefix)
	}
	if pt := a.Resource.PatternType; pt != "" && pt != "literal" && pt != "prefix" {
		return fmt.Errorf("patternType must be literal or prefix")
	}
	if a.Resource.PatternType == "prefix" && a.Resource.Name == "*" {
		return fmt.Errorf("a prefix pattern cannot be \"*\"")
	}
	return nil
}

// ---------- engine.UserManager ----------

var _ engine.UserManager = (*Engine)(nil)

// AddUser implements engine.UserManager. req is {"name": "...", "admin": bool, "acls": [...]}.
func (e *Engine) AddUser(spec, req json.RawMessage) (json.RawMessage, string, error) {
	var u User
	if err := strictUnmarshal(req, &u); err != nil {
		return nil, "", engine.Invalid("invalid user: %v", err)
	}
	u.SecretVersion = 0 // set when the secret is created
	if u.Auth == "" {
		u.Auth = AuthSCRAM
	}
	s, err := parseSpecJSON(spec)
	if err != nil {
		return nil, "", err
	}
	for _, x := range s.Users {
		if x.Name == u.Name {
			return nil, "", engine.Invalid("user %q already exists", u.Name)
		}
	}
	s.Users = append(s.Users, u)
	if err := validateUsers(s.Users); err != nil {
		return nil, "", err
	}
	out, err := json.Marshal(s)
	return out, u.Name, err
}

// RemoveUser implements engine.UserManager.
func (e *Engine) RemoveUser(spec json.RawMessage, name string) (json.RawMessage, error) {
	s, err := parseSpecJSON(spec)
	if err != nil {
		return nil, err
	}
	i := slices.IndexFunc(s.Users, func(u User) bool { return u.Name == name })
	if i < 0 {
		return nil, notFoundUser(name)
	}
	s.Users = slices.Delete(s.Users, i, i+1)
	return json.Marshal(s)
}

// RotateUser implements engine.UserManager: the secret version in the spec is bumped by the
// workflow once the new secret exists, so here it only checks that the user exists.
func (e *Engine) RotateUser(spec json.RawMessage, name string) (json.RawMessage, error) {
	s, err := parseSpecJSON(spec)
	if err != nil {
		return nil, err
	}
	i := slices.IndexFunc(s.Users, func(u User) bool { return u.Name == name })
	if i < 0 || s.Users[i].isTLS() { // certificates are renewed with a new request, not rotated
		return nil, notFoundUser(name)
	}
	return json.Marshal(s)
}

// SetUserSecretVersion implements engine.UserManager.
func (e *Engine) SetUserSecretVersion(spec json.RawMessage, name string, version int) (json.RawMessage, error) {
	s, err := parseSpecJSON(spec)
	if err != nil {
		return nil, err
	}
	i := slices.IndexFunc(s.Users, func(u User) bool { return u.Name == name })
	if i < 0 {
		return nil, notFoundUser(name)
	}
	s.Users[i].SecretVersion = version
	return json.Marshal(s)
}

// SetUserCSR implements engine.UserManager: certificate renewal replaces the request, and the cell signs it.
func (e *Engine) SetUserCSR(spec json.RawMessage, name, csr string) (json.RawMessage, error) {
	s, err := parseSpecJSON(spec)
	if err != nil {
		return nil, err
	}
	i := slices.IndexFunc(s.Users, func(u User) bool { return u.Name == name })
	if i < 0 || !s.Users[i].isTLS() {
		return nil, notFoundUser(name)
	}
	if err := validateCSR(csr); err != nil {
		return nil, engine.Invalid("%v", err)
	}
	s.Users[i].CSR = csr
	return json.Marshal(s)
}

// ListUsers implements engine.UserManager.
func (e *Engine) ListUsers(spec json.RawMessage) (json.RawMessage, error) {
	s, err := parseSpecJSON(spec)
	if err != nil {
		return nil, err
	}
	type view struct {
		Name      string `json:"name"`
		Mechanism string `json:"mechanism"`
		Principal string `json:"principal"`
		Admin     bool   `json:"admin"`
		ACLs      []ACL  `json:"acls"`
	}
	out := []view{}
	for _, u := range s.Users {
		acls := u.ACLs
		if acls == nil {
			acls = []ACL{}
		}
		mech := "SCRAM-SHA-512"
		if u.isTLS() {
			mech = "mTLS"
		}
		out = append(out, view{Name: u.Name, Mechanism: mech, Principal: "User:" + u.principalName(), Admin: u.Admin, ACLs: acls})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return json.Marshal(out)
}

func notFoundUser(name string) error { return &engine.NotFoundError{What: "user " + name} }

func parseSpecJSON(spec json.RawMessage) (Spec, error) {
	var s Spec
	if err := strictUnmarshal(spec, &s); err != nil {
		return s, engine.Invalid("invalid kafka spec: %v", err)
	}
	return s, nil
}

// ---------- rendering ----------

func userSecretName(user string) string { return "user-" + user }

// renderUsers returns the KafkaUser objects, the secrets they need (SCRAM passwords) and the certificate
// requests the cell must sign (tls users).
func renderUsers(clusterID, cell string, s Spec, common map[string]any) (objs []map[string]any, mats []engine.SecretMaterial, certs []engine.CertRequest) {
	users := slices.Clone(s.Users)
	sort.Slice(users, func(i, j int) bool { return users[i].Name < users[j].Name })
	for _, u := range users {
		acls := u.ACLs
		if u.Admin {
			acls = append(adminACLs(), acls...)
		}
		aclObjs := []any{}
		for _, a := range acls {
			res := map[string]any{"type": a.Resource.Type}
			if a.Resource.Type != "cluster" {
				res["name"] = a.Resource.Name
				pt := a.Resource.PatternType
				if pt == "" {
					pt = "literal"
				}
				res["patternType"] = pt
			}
			ops := make([]any, len(a.Operations))
			for i, op := range a.Operations {
				ops[i] = op
			}
			aclObjs = append(aclObjs, map[string]any{"resource": res, "operations": ops})
		}
		if u.isTLS() {
			// tls-external: the User Operator only creates the ACLs; the certificate is issued by the cell agent
			// from the tenant's CSR, with CN=<name>, signed by the cluster's clients CA.
			objs = append(objs, map[string]any{
				"apiVersion": "kafka.strimzi.io/v1",
				"kind":       "KafkaUser",
				"metadata": map[string]any{
					"name": u.Name, "namespace": clusterID,
					"labels": merge(common, map[string]any{"strimzi.io/cluster": clusterID}),
				},
				"spec": map[string]any{
					"authentication": map[string]any{"type": "tls-external"},
					"authorization":  map[string]any{"type": "simple", "acls": aclObjs},
				},
			})
			certs = append(certs, engine.CertRequest{
				Name: u.Name, Namespace: clusterID, CommonName: u.Name, CSR: u.CSR,
				CACertSecret: clusterID + "-clients-ca-cert", CACertKey: "ca.crt",
				CAKeySecret: clusterID + "-clients-ca", CAKeyKey: "ca.key", ValidDays: 90,
			})
			continue
		}
		secretName := userSecretName(u.Name)
		objs = append(objs, map[string]any{
			"apiVersion": "kafka.strimzi.io/v1",
			"kind":       "KafkaUser",
			"metadata": map[string]any{
				"name":      u.Name,
				"namespace": clusterID,
				"labels":    merge(common, map[string]any{"strimzi.io/cluster": clusterID}),
				// A changed annotation makes the User Operator look at the user again after a rotation.
				"annotations": map[string]any{labels.Prefix + "password-version": fmt.Sprint(u.SecretVersion)},
			},
			"spec": map[string]any{
				"authentication": map[string]any{
					"type":     "scram-sha-512",
					"password": map[string]any{"valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": secretName, "key": "password"}}},
				},
				"authorization": map[string]any{"type": "simple", "acls": aclObjs},
			},
		})
		mats = append(mats, engine.SecretMaterial{
			Namespace: clusterID, Name: secretName, Labels: map[string]string{labels.Inventory: clusterID},
			Data: map[string]secrets.Ref{"password": {Path: secrets.UserPath(cell, clusterID, u.Name), Key: "password", Version: u.SecretVersion}},
		})
	}
	return objs, mats, certs
}
