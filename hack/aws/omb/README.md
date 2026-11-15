# Noisy-neighbour and tier comparison (M6), OpenMessaging Benchmark

**STATUS: prepared, NOT RUN.** Nothing in this directory has been executed; the AWS runs were not approved yet
(see `docs/milestones/m6.md`). OpenMessaging Benchmark is Apache-2.0 and is used unmodified.

The brief asks for "the noisy-neighbour OpenMessaging Benchmark comparison, and dedicated versus shared". The
comparison is about what a tenant on the shared tier sees when a neighbour is loud, and whether the dedicated tier
removes it.

| Run | Cluster under test | Neighbour | Question |
|---|---|---|---|
| A. baseline | shared, 3 × `m.small`-class brokers | none | the reference latency and throughput |
| B. noisy | the same cluster | a second shared-tier cluster, **placed on the same nodes** (same instance, other tenant), driven with `workload-noisy.yaml` | how much of A is lost to a neighbour: p50/p99/p99.9 publish and end-to-end latency, throughput |
| C. dedicated | a dedicated-tier cluster with the same broker size | none can share its nodes; the neighbour of B runs next to it on the shared pool | whether the dedicated tier is unaffected |

How to run each (the victim workload is the same in all three):

1. Create the clusters through the platform API (shared tier for A/B, dedicated for C) and a SCRAM user.
2. For B, check with `kubectl get pods -o wide -A` that a broker of the neighbour shares a node with a broker of the victim; if the allocator spread them, reduce the shared pool to three nodes (`SHARED_MAX_NODES`) or add the noisy cluster until it does. The placement rules keep one broker of a cluster per node, not other tenants away: that is the point of the shared tier.
3. Run OMB from a separate node pool (workers on instances that are not part of the cell) so that the load generators do not become the bottleneck: `bin/benchmark --drivers driver-kafka.yaml --workers <w1>,<w2> workload-victim.yaml` (neighbour: `workload-noisy.yaml` with its own driver file and workers, started two minutes earlier).
4. Keep the OMB result JSON, the node and pod placement, and the cell's CPU/IO metrics of the window next to the write-up.

The CPU model matrix of ADR-0009 (request with CFS limit, request without limit, static CPU manager with Guaranteed integer CPUs) is a further axis of run B: it is the experiment that decides the shared-tier default, and it needs one run of B per model.

Rendering the driver file: `BOOTSTRAP=… USER=… PASSWORD=… CA_PATH=… envsubst < driver-kafka.yaml.tmpl > driver-kafka.yaml`.
