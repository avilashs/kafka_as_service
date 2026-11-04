# ADR-0012: Secrets as references to a secret manager

**Status:** Proposed

## Decision

Postgres stores only `secret_ref` values. On AWS they point to Secrets Manager (ARNs); on kind and self-hosted installs, to OpenBao paths.

- The workflow creates secrets with deterministic names.
- Each cell agent has read access to `streamcell/<cell>/*` only. It materializes Kubernetes Secrets that `KafkaUser.spec.authentication.password.valueFrom` references.
- Client certificates are issued by signing a tenant CSR with the per-cluster clients CA. The CA key never leaves the cell. The agent performs the signing and returns the certificate through status.
- Rotation creates a new secret version, and the agent rolls the reference.

## Consequences

- No secrets are kept in the database or in desired-state bundles.
- If the secret manager is unavailable, existing users keep working.
