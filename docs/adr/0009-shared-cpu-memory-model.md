# ADR-0009: Shared-tier CPU and memory model

**Status:** Proposed, with the CPU part decided by the M6 benchmark

## Context

Memory is never overcommitted, but CPU may be overcommitted by a set ratio. The CPU manager's static policy needs Guaranteed QoS, which means requests equal limits, and that rules out CPU overcommit. CFS quota throttling hurts tail latency.

## Decision

- **Memory:** requests equal limits. A fixed heap (`-Xms = -Xmx`) per size. The remainder of the limit is for page cache and off-heap memory, which cgroup v2 charges to the pod.
- **CPU:** request = nominal ÷ ratio (default 2.0).

The M6 OpenMessaging Benchmark measures p99/p999 produce and end-to-end latency, with an aggressive neighbour, for three options:

| Option | CPU setting |
|---|---|
| (a) | `limit = nominal` |
| (b) | No CPU limit |
| (c) | Static CPU manager, no overcommit |

The provisional default is (a). This ADR will be updated with the measured choice.

- **Dedicated tier:** Guaranteed pods with integer CPUs. The static policy is evaluated there as well.
