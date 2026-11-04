# Dependencies

The project licence is **Apache-2.0**. Every dependency must meet three rules:

1. Its licence is compatible with Apache-2.0.
2. It can be used to run a hosted service.
3. It is not BSL, SSPL, Elastic, Commons Clause or another source-available licence.

We also take no images from Bitnami: its free versioned images were withdrawn in Aug–Sep 2025.

Versions are the latest upstream tags as of 2026-11-04 (see [`upstream-check.md`](upstream-check.md)). Exact pins are set in M1 and recorded here.

Status values:
- *planned*: adopted in the design, not yet pinned.
- *eval*: being evaluated.
- *rejected*: considered and not used.
- *optional*: not shipped; operators can install it.

## Data plane (cells)

| Component | Version | Licence | Use | Status / notes |
|---|---|---|---|---|
| Apache Kafka | 4.3.x (4.4 when Strimzi supports it) | Apache-2.0 | Brokers and KRaft controllers, unmodified | planned |
| Strimzi Cluster/User Operator | 1.3.x | Apache-2.0 | Kafka lifecycle | planned. Images are based on Red Hat UBI 9, which is freely redistributable under the UBI EULA; review this once before GA. |
| Strimzi Kafka images | matching | Apache-2.0 (+ UBI base) | Base for the Streamcell broker image | planned. We add the plugin JAR in a derived image. |
| Cruise Control (Strimzi-bundled) | Strimzi-bundled | BSD-2-Clause | Partition rebalancing | planned |
| Strimzi Drain Cleaner | 1.8.x | Apache-2.0 | Safe node drains | planned |
| Kafka Exporter (danielqsj) | Strimzi-bundled | Apache-2.0 | Consumer-lag metrics | planned |
| Envoy Gateway / Envoy | ≥ 1.9 | Apache-2.0 | Shared-tier SNI passthrough (`TLSRoute v1`) | planned (spike S-3) |
| Gateway API CRDs | ≥ 1.5 Standard | Apache-2.0 | `Gateway`, `TLSRoute` | planned |
| Cilium | 1.20.x | Apache-2.0 (eBPF programs GPL-2.0/BSD-2-Clause dual) | CNI, NetworkPolicy, Bandwidth Manager | planned. The eBPF programs run in the kernel and are not linked into our code. |
| Karpenter (core + AWS provider) | 1.15.x | Apache-2.0 | Node provisioning, a `NodePool` per dedicated cluster | planned |
| AWS EBS CSI driver | current | Apache-2.0 | gp3 volumes, `VolumeAttributesClass` | planned |
| AWS Load Balancer Controller | current | Apache-2.0 | NLBs (cell gateway and dedicated) | planned |
| TopoLVM / sig-storage local-static-provisioner | current | Apache-2.0 | Local NVMe for dedicated | eval (spike S-8) |
| Prometheus, Alertmanager | current | Apache-2.0 | Cell metrics and alerts | planned |
| kube-state-metrics, node-exporter | current | Apache-2.0 | Metrics | planned |
| Fluent Bit | current | Apache-2.0 | Broker log shipping | planned |
| cert-manager | current | Apache-2.0 | Optional public ACME listener certificates | optional (Q-3) |

## Control plane

| Component | Version | Licence | Use | Status / notes |
|---|---|---|---|---|
| PostgreSQL | 17/18 (official images) | PostgreSQL License | Metadata store, Temporal persistence | planned |
| CloudNativePG | 1.30.x | Apache-2.0 | Self-hosted Postgres operator (RDS is an alternative on AWS) | planned |
| Temporal Server | 1.32.x | MIT | Durable workflows | planned (spike S-1). Deploy with external Postgres; **disable the Helm chart's bundled sub-charts** (Cassandra, Elasticsearch, Postgres). |
| Thanos (Receive/Query) | current | Apache-2.0 | Central tenant-metrics store | planned |
| prom-label-proxy | current | Apache-2.0 | Tenant label enforcement | planned |
| Perses | current | Apache-2.0 | Provider dashboards | planned |
| OpenBao | 2.7.x | MPL-2.0 | Secret manager for kind and self-hosted installs | planned |
| AWS Secrets Manager / KMS / Route 53 | service | n/a | Secrets, encryption keys, DNS on AWS | planned |

## Libraries (Go platform services)

| Library | Licence | Use |
|---|---|---|
| Go standard library (incl. `log/slog`, `net/http`) | BSD-3-Clause | — |
| `go.temporal.io/sdk` | MIT | Workflows |
| `github.com/jackc/pgx/v5` | MIT | Postgres driver |
| `sqlc` (codegen) | MIT | Typed queries |
| `github.com/pressly/goose/v3` | MIT | Migrations |
| `github.com/oapi-codegen/oapi-codegen` | Apache-2.0 | OpenAPI server and client codegen |
| `github.com/go-chi/chi/v5` | MIT | HTTP router |
| `k8s.io/client-go`, `sigs.k8s.io/controller-runtime` | Apache-2.0 | Agent, auditor, webhook |
| `github.com/twmb/franz-go` | BSD-3-Clause | Canary, probes, test verifier |
| `github.com/prometheus/client_golang` | Apache-2.0 | Metrics |
| `github.com/testcontainers/testcontainers-go` | MIT | Integration tests |
| AWS SDK for Go v2 | Apache-2.0 | Cloud provider implementation |

## Libraries (Java broker plugins)

| Library | Licence | Use |
|---|---|---|
| `org.apache.kafka:kafka-clients`, `kafka-server-common`, `kafka-metadata` (compile-only, provided) | Apache-2.0 | `CreateTopicPolicy`, `AlterConfigPolicy`, `Authorizer` interfaces |
| Gradle | Apache-2.0 | Build |
| JUnit 5 | EPL-2.0 | Tests only, not distributed |

## Tooling and tests

| Tool | Licence | Use |
|---|---|---|
| OpenTofu | MPL-2.0 | Cell infrastructure (**not** Terraform, which is BSL 1.1) |
| `hashicorp/aws` provider | MPL-2.0 | Used from OpenTofu |
| kind | Apache-2.0 | Local cells |
| Chainsaw (Kyverno) | Apache-2.0 | Kubernetes e2e tests |
| OpenMessaging Benchmark | Apache-2.0 | M6 benchmarks |
| cosign, syft | Apache-2.0 | Image signing, SBOM |
| golangci-lint | GPL-3.0 | Dev tool only; not distributed or linked |

## Rejected or avoided

| Component | Licence / reason |
|---|---|
| Terraform ≥ 1.6 | BSL 1.1 |
| HashiCorp Vault ≥ 1.15 | BSL 1.1. OpenBao is used instead. |
| Bitnami charts and images (Kafka, Postgres, sub-charts) | Free distribution withdrawn in 2025; `bitnamilegacy` is unpatched |
| ingress-nginx | Retired in March 2026 |
| Grafana, Loki, Mimir (as shipped dependencies) | AGPL-3.0. Allowed only as an optional, unmodified add-on that operators install themselves. |
| Redpanda, Confluent Server, and any proprietary or source-available Kafka distribution | Not stock Apache Kafka, and the licences fail the rules above |
| Kafka protocol proxies in the data path (e.g. Kroxylicious) | Ruled out by the brief: the data path must not parse Kafka requests |
