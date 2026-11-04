# Streamcell (working name)

Streamcell is an open-source, self-hostable platform for managed **Apache Kafka®** clusters, comparable to a provisioned managed Kafka service. Tenants request a cluster through a REST API and get their own stock Apache Kafka brokers and KRaft quorum, which they use with standard Kafka clients.

> **Status: M2 (placement) is implemented.** Create, describe, list and delete work for both the shared and the dedicated tier, with placement enforced by admission policy, a binding guard and a continuous audit. Reports: [M1](docs/milestones/m1.md), [M2](docs/milestones/m2.md). Clients cannot connect yet (M3).

## Quick start (local cell on kind, real Strimzi)

Requires docker, kind, kubectl, helm and Go 1.26. The cell has 10 kind nodes, so on Linux raise the inotify limits first: `sudo sysctl -w fs.inotify.max_user_watches=524288 fs.inotify.max_user_instances=512`.

```bash
make kind-up      # kind cell + Strimzi 1.3.0 + Envoy Gateway; Postgres, Temporal, api/worker/agent on the host
make kind-smoke   # dedicated scenario (placement, refusals, audit, node return), then shared (worker kill mid-create)
make kind-down
```

Create a cluster yourself (use `"tier":"dedicated"` with `"brokerType":"dev.ded"` for the dedicated tier):

```bash
curl -s -X POST localhost:8080/v1/clusters \
  -H 'Authorization: Bearer dev-tenant-token' -H 'Idempotency-Key: my-first-cluster' \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo","tier":"shared","spec":{"kafkaVersion":"4.3.1","brokerType":"dev.tiny","brokerCount":3,"storage":{"sizeGiB":1}}}'
curl -s localhost:8080/v1/operations/<operation-id> -H 'Authorization: Bearer dev-tenant-token'
```

Client connectivity through the gateway (DNS, certificates, SCRAM users) arrives in M3.

## Tests

```bash
make test         # unit tests
make e2e          # Postgres + Temporal + real kube-apiserver (envtest) with Strimzi CRDs
```

## Layout

| Path | Contents |
|---|---|
| `cmd/` | `streamcell-api`, `streamcell-worker`, `streamcell-agent`, `streamcell-admin`, `streamcell-admission`, `streamcell-audit` |
| `internal/platform/` | Engine-neutral core: API, store, capacity, workflows, outbox, cell API, `engine` interface |
| `internal/engines/kafka/` | Kafka engine (Strimzi v1 rendering) |
| `internal/agent/` | Cell agent |
| `api/openapi.yaml` | API spec |
| `deploy/kind/` | Local cell |
| `deploy/admission/` | Admission policies, binding guard and auditor manifests |
| `deploy/alerts/`, `docs/runbooks/` | Placement alert rules and their runbooks |
| `internal/placement/` | The placement rules, shared by the guard and the auditor; `streamcell-audit` |
| `third_party/strimzi/1.3.0/` | Vendored Strimzi install files and CRDs (Apache-2.0) |

## Documents

| Document | Contents |
|---|---|
| [`docs/design.md`](docs/design.md) | Architecture, data model, placement, sizing, networking costs, threat model, failure modes |
| [`docs/adr/`](docs/adr/) | Architecture decision records |
| [`docs/dependencies.md`](docs/dependencies.md) | Dependencies and their licences |
| [`docs/upstream-check.md`](docs/upstream-check.md) | Upstream versions and status as of 2026-11-04 |

Licence: Apache-2.0. Apache Kafka and Kafka are trademarks of the Apache Software Foundation. This project is not affiliated with or endorsed by the ASF.
