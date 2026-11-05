# ADR-0013: An Apache-licensed observability stack

**Status:** Partly built (M5): cell Prometheus + rules + runbooks, canary and health. Thanos, prom-label-proxy, Perses and Fluent Bit are not built.

## Decision

| Concern | Component |
|---|---|
| Cell metrics and alerting | Prometheus and Alertmanager |
| Central metrics store | Thanos Receive and Query, fed by outbound `remote_write` |
| Tenant metric queries | `prom-label-proxy`, behind the platform API |
| Provider dashboards | Perses |
| Log shipping | Fluent Bit to S3, with optional cross-account delivery to the tenant |

Grafana, Loki and Mimir (AGPL-3.0) are not shipped. Operators may add them unmodified.

## Consequences

- No inbound access to cells is needed for metrics.
- Tenant metric access is limited to a curated set of metric names.
