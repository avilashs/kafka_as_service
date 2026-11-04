# Streamcell (working name)

Streamcell is an open-source, self-hostable platform for managed **Apache Kafka®** clusters. It is comparable to a provisioned managed Kafka service:

- Tenants request a cluster through a REST API.
- Each tenant gets dedicated, stock Apache Kafka brokers and a KRaft quorum.
- Tenants connect with standard Kafka clients.

> **Status: Phase 0 (design), awaiting approval.** No implementation code exists yet.

| Document | Contents |
|---|---|
| [`docs/design.md`](docs/design.md) | Architecture, data model, placement, sizing, networking costs, threat model, failure modes, milestones, open questions |
| [`docs/adr/`](docs/adr/) | Architecture decision records |
| [`docs/dependencies.md`](docs/dependencies.md) | Dependencies and their licences |
| [`docs/upstream-check.md`](docs/upstream-check.md) | Upstream versions and status as of 2026-11-04 |

Licence: Apache-2.0 (see [LICENSE](LICENSE)).

Apache Kafka and Kafka are trademarks of the Apache Software Foundation. This project is not affiliated with or endorsed by the ASF.
