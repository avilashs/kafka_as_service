package placement

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

type ruleFile struct {
	Groups []struct {
		Name  string `json:"name"`
		Rules []struct {
			Alert       string            `json:"alert"`
			Expr        string            `json:"expr"`
			For         string            `json:"for"`
			Labels      map[string]string `json:"labels"`
			Annotations map[string]string `json:"annotations"`
		} `json:"rules"`
	} `json:"groups"`
}

// Design §17: "a runbook linked to each alert". Fails the build if an alert has no runbook,
// the link does not resolve, or an alert refers to a rule the auditor does not define.
func TestEveryAlertHasAnExistingRunbook(t *testing.T) {
	root := filepath.Join("..", "..")
	raw, err := os.ReadFile(filepath.Join(root, "deploy", "alerts", "placement.rules.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var rf ruleFile
	if err := yaml.Unmarshal(raw, &rf); err != nil {
		t.Fatal(err)
	}
	n := 0
	known := map[string]bool{}
	for _, r := range AllRules {
		known[r] = true
	}
	for _, g := range rf.Groups {
		for _, r := range g.Rules {
			n++
			if r.Alert == "" || r.Expr == "" {
				t.Errorf("rule without alert/expr: %+v", r)
				continue
			}
			url := r.Annotations["runbook_url"]
			if url == "" {
				t.Errorf("alert %s has no runbook_url", r.Alert)
				continue
			}
			if _, err := os.Stat(filepath.Join(root, url)); err != nil {
				t.Errorf("alert %s: runbook %s does not exist", r.Alert, url)
			}
			if r.Labels["severity"] == "" {
				t.Errorf("alert %s has no severity", r.Alert)
			}
			// rule="..." selectors must name rules that exist
			for _, part := range strings.Split(r.Expr, `rule="`)[1:] {
				if name := strings.SplitN(part, `"`, 2)[0]; !known[name] {
					t.Errorf("alert %s refers to unknown placement rule %q", r.Alert, name)
				}
			}
		}
	}
	if n == 0 {
		t.Fatal("no alerts found")
	}
}

// Every rule the auditor can raise is documented in the violation runbook.
func TestViolationRunbookCoversEveryRule(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "runbooks", "placement-violation.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range AllRules {
		if !strings.Contains(string(raw), "`"+r+"`") {
			t.Errorf("docs/runbooks/placement-violation.md does not document rule %s", r)
		}
	}
}
