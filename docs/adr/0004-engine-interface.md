# ADR-0004: A minimal `Engine` interface

**Status:** Proposed

## Context

A ClickHouse engine follows Kafka on the same platform. The platform core must contain nothing Kafka-specific, and there should be no abstraction beyond what these two engines need.

## Decision

The `Engine` interface has these methods:

| Method | Purpose |
|---|---|
| `Validate` | Validate a spec |
| `PlanPlacement` | Return the placement request |
| `Render` | Render the desired-state bundle |
| `PlanChange` | Plan scale, resize, upgrade and config changes |
| `PlanDelete` | Plan deletion |
| `ObserveStatus` | Derive status from observed state |

All are pure functions. A separate cell-side `Prober` interface handles read-only health checks.

A change is expressed as `[]Step{Bundle, WaitFor, Timeout, Undo}`, which a generic workflow executes.

Placement uses engine-neutral terms: `PodGroup` (replicas, pool, resources, zone spread, exclusivity). The full sketch is in design §8.

A `depguard` lint rule stops `platform/*` from importing `engines/*`.

## Consequences

- One generic workflow per operation kind.
- Engine-specific orchestration that needs live feedback is expressed as `WaitFor` conditions evaluated by `ObserveStatus` on probe results. An example is "wait for URP = 0".
- If ClickHouse needs something that does not fit, we extend the interface then, not now.
