# ADR-0010: The customer admin principal and broker plugins

**Status:** Proposed (spike S-7)

## Context

Tenants need the full Kafka API, including ACL management, but must not get super-user rights. In Kafka, `CreateAcls` requires `ALTER` on the `Cluster` resource. The same permission allows partition reassignment, SCRAM credential changes and other cluster-level operations.

The KRaft source also shows these limits for a partition limit:

- `CreateTopicPolicy` runs on controllers.
- `createPartitions` calls no policy.
- Policies cannot see cluster state.

## Decision

Ship a Java JAR, `streamcell-kafka-plugins`, added to a derived Strimzi Kafka image for both brokers and controllers. It contains three plugins:

- **`PlatformAuthorizer`** wraps `StandardAuthorizer`. It makes decisions using `AuthorizableRequestContext.requestType()` and the requested actions:
  - It denies tenant principals: broker or cluster `AlterConfigs`, `AlterPartitionReassignments`, `AlterUserScramCredentials`, `UnregisterBroker` and `AlterClientQuotas`.
  - It filters `CreateAcls` bindings that name platform principals, `__platform*` resources, or cluster-level operations.
  - It rejects `CreatePartitions` while the cluster is at its partition limit.
- **`PartitionLimitCreateTopicPolicy`** checks requested partitions plus a cached current total against the limit. It also enforces a replication factor of at least 3 by default.
- **`AllowlistAlterConfigPolicy`** enforces the topic-config allowlist and value bounds.

Bursts inside the cache window are bounded by the `controller_mutation_rate` quota.

## Consequences

- Tenants keep the full Kafka API. The partition limit can be exceeded by at most one racing `CreatePartitions` request, which is then blocked and alerted on (Q-6).
- These are stock plugin interfaces, not a fork.
- Each Kafka version requires a plugin compatibility test.
