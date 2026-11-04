# ADR-0010: The customer admin principal and broker plugins

**Status:** Accepted in M3 (spike S-7 done; evidence in [m3.md](../milestones/m3.md))

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

## What M3 found (spike S-7)

Checked in a real broker (`plugins/kafka/it/it.sh`, KRaft, Kafka 4.1.0 runtime locally and the Strimzi 4.3.1 image in CI; the jar is compiled against 4.3.1):

- `StandardAuthorizer` is a public, non-final class, and `createAcls`/`deleteAcls` are default methods of `ClusterMetadataAuthorizer`. The plugin therefore **subclasses** `StandardAuthorizer` and overrides `authorize`, `createAcls`, `deleteAcls`. No delegation of the whole `Authorizer` interface is needed.
- `AuthorizableRequestContext.requestType()` is reliable on controllers too, including for requests forwarded from brokers (the envelope is unwrapped before authorization). The rule "cluster `Alter` counts only for `CreateAcls`/`DeleteAcls`" holds for reassignment, leader election, feature updates and SCRAM changes (tested).
- **Cluster-wide `Create` bypasses per-topic checks.** `CreateTopics` authorizes `Create` on the cluster first and, when that passes, skips the per-topic check. A tenant admin holding cluster `Create` could create `__`-named topics. The first version of the plugin did not catch this; the integration test did. Rule added: cluster `Create` is always denied to tenants; topic `Create` comes with the topic ACLs.
- **Decision:** the admin profile is `Alter` (for ACL management), `Describe`, `DescribeConfigs`, `IdempotentWrite` on the cluster, and `All` on topics, groups and transactional ids. User-defined cluster ACLs may not contain `Alter`, `AlterConfigs`, `ClusterAction`, `Create` or `All`.
- A denial of `Describe` on a topic looks like a missing topic to the client (Kafka hides existence). Expected.
- `ElectLeaders` needs cluster `Alter` and is therefore denied to tenants (design §15.1 allowed it "for own topics"). Preferred-leader election is the platform's job (Cruise Control). Design table to be read accordingly.
- `AlterConfigPolicy` cannot see the principal and must not block the platform's own tooling (Cruise Control sets replication throttles as broker configs). It judges topic configs only; broker configs are kept from tenants by the authorizer.
- ACL deletion: a delete filter cannot be inspected for what it would match, so tenants must name the resource type (topic, group or transactionalId); filters of type `ANY` or `CLUSTER` are refused.
- Not done yet (M4/M5): the partition-count limit (cached count in `CreateTopicPolicy`, `CreatePartitions` gate in the authorizer). The policy bounds a single request only (`streamcell.topic.max.partitions`, default 1000).
- The User Operator manages ACLs of its `KafkaUser`s and re-applies them periodically; a tenant who deletes such an ACL through the Admin API will see it return. Tenant-created ACLs on other principals are untouched by the operator (to be confirmed on the real cluster in the kind run).
