# ADR-0002: Temporal as the workflow engine

**Status:** Proposed (confirmed by spike S-1 in M1)

## Context

Every mutation is a long-running, multi-step process that must survive worker crashes. It needs retries that distinguish retryable from terminal errors, compensation, and a per-resource lock.

## Decision

Use Temporal Server 1.32.x (MIT) and the Go SDK, with Postgres persistence in a database separate from the metadata database. The workflow ID equals the operation ID, so a duplicate dispatch cannot start a second workflow.

Workflows are started through a transactional **outbox** written in the same Postgres transaction as the operation row. Activities follow the idempotency rules in design §6.

## Consequences

- We get durable timers, retries, heartbeats and visibility.
- Temporal is another stateful system to run. Kubernetes deployment uses the Helm chart with every bundled sub-chart disabled, because of the Bitnami withdrawal.
- Workflow code must be deterministic. We enforce this with Temporal's replay tests in CI.

## Alternatives

- **A Postgres-native job queue** (River, MIT). Simpler to run, but we would have to build durable timers, signals and saga tooling ourselves. This is the fallback if S-1 finds Temporal's operational cost too high.
- **Argo Workflows.** Runs in Kubernetes, which is the wrong failure domain for the control plane, and it is a poor fit for long-lived sagas.
