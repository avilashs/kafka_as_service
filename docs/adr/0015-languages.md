# ADR-0015: Go for platform services, Java only for broker plugins

**Status:** Proposed

## Decision

Go is used for the API, workers, cell agent, admission webhook, auditor, canary and CLI. This fits the Kubernetes client ecosystem, the Temporal Go SDK, static binaries and franz-go.

Kafka broker plugins must run on the JVM. They are written in Java, kept in `plugins/kafka-java/`, and kept minimal.
