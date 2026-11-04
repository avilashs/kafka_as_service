# ADR-0008: A Karpenter `NodePool` per dedicated cluster

**Status:** Proposed

## Context

Each dedicated cluster needs its own nodes: node count equal to broker count, plus temporary surge. EKS managed node groups have a per-cluster quota that would cap the number of dedicated clusters in a cell. (The quota has a default of about 30; re-verify.)

## Decision

Karpenter 1.15.x provides the following:

| Pool | Configuration |
|---|---|
| Shared pool | One `NodePool`, autoscaled within the cell maximum |
| Control pool | One `NodePool` |
| Each dedicated cluster | One `NodePool`. Label `streamcell.dev/dedicated-owner=<id>`, taint `streamcell.dev/dedicated=<id>:NoSchedule`, `limits.nodes = brokers + surge`, `disruption.budgets: nodes: "1"`, consolidation only when nodes are empty, and a long `terminationGracePeriod`. |

NVMe brokers carry `karpenter.sh/do-not-disrupt`, and the Streamcell rotation controller handles them instead.

The local kind provider emulates node pools by labelling and tainting pre-created spare nodes.

## Consequences

- A `NodePool` is a cheap custom resource, so there is no per-cell cap on node groups.
- Node rotation uses Karpenter drift together with Drain Cleaner.
- The cloud-specific `NodePoolProvider` interface has two implementations: Karpenter on AWS, and labels on kind.

## As built (M2)

- `NodePoolProvider` (`internal/cloud`) has two implementations. **kind** claims pre-created spare nodes. **AWS** renders a Karpenter v1 `NodePool` per dedicated cluster; its manifest is unit-tested against the verified v1.15.1 field names but has **not** run against a real Karpenter yet (M6).
- Spare nodes in kind carry a `streamcell.dev/spare` taint so that only DaemonSets run on them; without it CoreDNS or the gateway can land on a spare node and the provider (rightly) refuses to claim a node that runs workloads. Claiming swaps that taint for the owner's taint in one update.
- Capacity: a dedicated pool is a per-instance-type, per-zone budget of whole nodes; a cluster reserves one node per broker and the pod must fit the node shape. Surge nodes are not yet reserved (they come from `limits.nodes` headroom); reserving them belongs with node rotation in M4.
