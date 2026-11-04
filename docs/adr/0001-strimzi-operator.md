# ADR-0001: Strimzi as the Kafka operator

**Status:** Proposed

## Context

We need an operator that runs stock Apache Kafka in KRaft mode on Kubernetes. It must handle rolling operations safely, provide rebalancing, and scale to many clusters per Kubernetes cluster. The licence must be Apache-compatible.

## Decision

Use Strimzi 1.3.x (`kafka.strimzi.io/v1` only), with these resources and components:

- `Kafka`, plus separate `KafkaNodePool`s for controllers and brokers;
- `KafkaUser` through the User Operator;
- Cruise Control with `autoRebalance` for `add-brokers` and `remove-brokers`;
- Drain Cleaner.

Disable the Topic Operator, because tenants manage topics through the Kafka API.

Run Strimzi in cluster-wide mode, sharded by `STRIMZI_CUSTOM_RESOURCE_SELECTOR=streamcell.dev/operator-shard=<n>`. Each shard has its own Deployment and leader lease. Shards double as the canary for operator upgrades.

## Consequences

- No Phase 0 blocker was found.
- Strimzi rolls brokers with an ISR-aware restart check. Strimzi 1.2 added KIP-1066 cordoning on scale-down, and 1.1 added a native `tlsroute` listener.
- CRDs are cluster-scoped, so every operator shard in a cell must support the installed CRD version. CRDs are upgraded first.
- There is no published limit on clusters per operator. Spike S-2 measures it.
- Listeners are configured per `Kafka` resource, not per node pool. This complicates the tier migration (spike S-6).

## Alternatives

- **A custom operator.** Too much undifferentiated work.
- **Other Kafka operators.** Most target a vendor distribution, or fail the licence rules.
