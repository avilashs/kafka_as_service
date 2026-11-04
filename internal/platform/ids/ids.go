// Package ids generates resource identifiers.
package ids

import (
	"crypto/rand"
	"encoding/hex"

	"github.com/google/uuid"
)

// Cluster returns an ID like "kc-1a2b3c4d5e" (DNS-label safe; doubles as the
// namespace and Strimzi cluster name, so it is kept short).
func Cluster() string {
	b := make([]byte, 5)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return "kc-" + hex.EncodeToString(b)
}

// Operation returns an ID like "op-<uuid>".
func Operation() string { return "op-" + uuid.NewString() }
