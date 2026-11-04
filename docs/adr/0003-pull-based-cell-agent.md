# ADR-0003: A pull-based cell agent with safety rules

**Status:** Proposed

## Context

The control plane must not have inbound access to cells. Running clusters must survive a total control-plane outage, and a bug in the control plane must not be able to wipe out a cell.

## Decision

A `cell-agent` in each cell does all of the following:

- long-polls `GET /cell/v1/desired?since=<revision>` over mTLS;
- applies rendered bundles with server-side apply;
- runs read-only engine probes;
- pushes status, inventory and metering.

Rules:

1. **Absence never means delete.** Only an explicit tombstone with a higher generation deletes anything.
2. **Monotonic revisions.** The agent persists the last applied revision. If the control plane serves an older one, the agent pauses.
3. **Scoped pruning.** The agent prunes only within one resource's own inventory.
4. **Mass-change brake.** Large deletions are held for manual confirmation.
5. **Bounded local autonomy.** For example, storage auto-expansion up to `maxSizeGiB`.

Rendering happens in the control plane, which makes it deterministic and auditable. The agent stays engine-agnostic apart from its probes.

## Consequences

- Changes take effect within one poll interval, with a 30-second long-poll.
- Metrics and logs also flow outbound only (ADR-0013).
- The control plane sees cell state only as the agent reports it. The agent's identity is per cell and least-privilege.
