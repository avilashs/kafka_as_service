//go:build !faultinject

package workflows

// faultPoint is a no-op in normal builds. See fault_on.go.
func faultPoint(_ string, err error) error { return err }
