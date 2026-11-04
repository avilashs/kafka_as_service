//go:build faultinject

package workflows

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// faultPoint crashes the process right after an activity's side effects have
// committed but before Temporal records the activity as complete, the worst case
// for duplicate side effects. It is compiled only with -tags faultinject and fires
// once per point: STREAMCELL_FAULT_CRASH_AFTER lists comma-separated point names,
// and STREAMCELL_FAULT_DIR holds marker files so a restarted worker does not crash again.
func faultPoint(name string, err error) error {
	if err != nil {
		return err
	}
	points := os.Getenv("STREAMCELL_FAULT_CRASH_AFTER")
	dir := os.Getenv("STREAMCELL_FAULT_DIR")
	if points == "" || dir == "" {
		return nil
	}
	for _, p := range strings.Split(points, ",") {
		if strings.TrimSpace(p) != name {
			continue
		}
		marker := filepath.Join(dir, strings.NewReplacer("/", "_", ":", "_").Replace(name))
		f, ferr := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if ferr != nil {
			return nil // already fired
		}
		f.Close()
		fmt.Fprintf(os.Stderr, "faultinject: crashing after %s\n", name)
		os.Exit(137)
	}
	return nil
}
