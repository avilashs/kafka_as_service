// Package cloud holds the cloud-specific parts of the cell: how a dedicated node pool
// is obtained and returned. The cell agent talks to NodePoolProvider only.
package cloud

import (
	"context"

	"github.com/avilashs/kafka_as_service/internal/platform/engine"
)

// NodePoolProvider provisions the nodes of a dedicated resource.
//
// Implementations: internal/cloud/kind (local: claims pre-created spare nodes by
// labelling and tainting them) and internal/cloud/aws (Karpenter NodePool per resource).
type NodePoolProvider interface {
	// Ensure makes the pool exist with (at least) the steady-state nodes of spec, labelled
	// and tainted so that only the owner's pods can run there. Idempotent. A nil error
	// means the request is accepted; nodes may still be arriving (pods stay Pending).
	Ensure(ctx context.Context, spec engine.NodePoolSpec) error
	// Release returns the pool's nodes. It must not strip a node's taint or labels while
	// any non-system pod still runs on it. done is true when nothing remains. Idempotent.
	Release(ctx context.Context, spec engine.NodePoolSpec) (done bool, err error)
}
