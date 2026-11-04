# ADR-0016: KRaft controllers on the provider control pool in both tiers

**Status:** Proposed

## Decision

Each cluster has 3 controller pods in their own `KafkaNodePool`, on the control pool (taint `streamcell.dev/control`). No two controllers of one cluster share a node, and they are spread across 3 zones.

Controllers are Guaranteed pods. The shared tier uses `c.small` and the dedicated tier uses `c.standard`.

Dedicated controller nodes may be added later if M6 measurements show interference.

## Consequences

- A dedicated tenant's controllers share nodes with other tenants' controllers. They are off the produce and fetch path, so this is an accepted, bounded risk.
- The fixed overhead per cluster matters for small shared clusters (Q-7).
