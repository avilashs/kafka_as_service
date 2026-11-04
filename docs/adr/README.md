# Architecture decision records

Each record has a status: Proposed, Accepted, Superseded or Deprecated. Every ADR below is **Proposed** until Phase 0 is approved.

| ADR | Title |
|---|---|
| [0001](0001-strimzi-operator.md) | Strimzi as the Kafka operator |
| [0002](0002-temporal.md) | Temporal as the workflow engine |
| [0003](0003-pull-based-cell-agent.md) | A pull-based cell agent with safety rules |
| [0004](0004-engine-interface.md) | A minimal `Engine` interface |
| [0005](0005-shared-tier-networking.md) | Shared-tier networking: Strimzi `tlsroute` with Envoy Gateway, zone-local |
| [0006](0006-dedicated-tier-networking.md) | Dedicated-tier networking: one NLB per cluster with a port per broker |
| [0007](0007-placement-enforcement.md) | Four-layer placement enforcement |
| [0008](0008-karpenter-node-pools.md) | A Karpenter `NodePool` per dedicated cluster |
| [0009](0009-shared-cpu-memory-model.md) | Shared-tier CPU and memory model |
| [0010](0010-customer-admin-principal-and-plugins.md) | The customer admin principal and broker plugins |
| [0011](0011-storage.md) | Storage: gp3, auto-expansion within bounds, ext4 reserve as ballast |
| [0012](0012-secrets.md) | Secrets as references to a secret manager |
| [0013](0013-observability-stack.md) | An Apache-licensed observability stack |
| [0014](0014-opentofu.md) | OpenTofu for cell infrastructure |
| [0015](0015-languages.md) | Go for platform services, Java only for broker plugins |
| [0016](0016-controllers-on-control-pool.md) | KRaft controllers on the provider control pool in both tiers |
| [0017](0017-metering-idempotency.md) | Idempotent metering with deterministic record IDs |
| [0018](0018-mtls-client-certificates.md) | Certificate-authenticated clients: the cell signs a CSR, the key stays with the tenant |
