# ADR-0007: Four-layer placement enforcement

**Status:** Proposed

## Context

Placement rules must be enforced, not just configured: a bug in the agent must not be able to break them.

## Decision

Enforce placement in four layers:

1. **Rendered constraints.** Anti-affinity on hostname, using `namespaceSelector: {}` for dedicated pods. Zone spread. A dedicated taint and owner label per cluster.
2. **A `ValidatingAdmissionPolicy` on Pods and Nodes.** It checks the spec: the required affinity terms are present, the pod tolerates only its own dedicated taint, pool selection matches the tier, and security settings are correct. Only the provisioner may change node labels or taints.
3. **A webhook on `pods/binding`.** It checks actual node occupancy at bind time, which CEL cannot do. It is fail-closed and scoped to tenant namespaces.
4. **`placement-auditor`.** It reads real pod and node state and exposes `streamcell_placement_violations`. It ships as a Deployment and also as the `streamcell-audit` CLI that tests use.

## As built (M2)

- Placement is expressed with engine-neutral pod labels (`required-pool`, `exclusive`, `cluster`, `role`); the policy, guard and auditor contain nothing engine-specific.
- The rule logic exists once, in `internal/placement`, and is called by the guard (for a hypothetical bind) and the auditor (for the actual state).
- The policy also refuses `spec.nodeName` at creation, because a pod that names its node bypasses both the scheduler and the guard.
- Rule names are generic (`same_group_on_node`, `pod_off_pool`, ...); see design §10.2 for the mapping from the original names.
- Known weaknesses are listed in design §10.2 and `docs/milestones/m2.md`.

## Consequences

- If the binding webhook is unavailable, scheduling stops in tenant namespaces. It runs with 3 replicas and a PDB.
- Every rule has an M2 test that proves it, including a deliberately bad pod per rule.
