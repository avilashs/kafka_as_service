# ADR-0006: Dedicated-tier networking: one NLB per cluster with a port per broker

**Status:** Proposed

## Context

The dedicated tier should share no data-path component with other tenants. Strimzi's `loadbalancer` listener creates one NLB per broker plus one for bootstrap.

## Decision

Use a Strimzi `cluster-ip` listener, with per-broker `advertisedHost` set to `b<N>.<id>.<cell>.<domain>` and `advertisedPort` set to `9001+N`. The platform manages one NLB per cluster:

- Port 9000 serves bootstrap and targets all brokers.
- Port `9001+N` targets broker N, using IP targets.

Private connectivity uses the same NLB, made internal, behind a VPC endpoint service (PrivateLink).

## Consequences

- At 3 brokers this costs about a quarter as much as one NLB per broker (design §14.4). The per-GB charges are identical.
- The NLB listener quota limits a cluster to 49 brokers.
- The platform owns NLB, listener and target-group reconciliation through the AWS Load Balancer Controller, using a `Service` per broker port or `TargetGroupBinding`s.
