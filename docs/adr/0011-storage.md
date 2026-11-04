# ADR-0011: Storage: gp3, auto-expansion within bounds, ext4 reserve as ballast

**Status:** Proposed (spike S-5)

## Decision

- **Shared tier:** gp3 volumes with provisioned IOPS and throughput, set per size through `VolumeAttributesClass`.
- **Dedicated tier:** gp3, or local NVMe.
- **Auto-expansion:** done by the agent, using bounds carried in the desired-state bundle, so it keeps working while the control plane is down. It respects the EBS modification cooldown by expanding in large steps.
- **Ballast:** ext4 reserved blocks, 5 % by default. Only root can use them, and Kafka runs as non-root. The runbook releases them with `tune2fs`.

Strimzi does not allow custom init containers or sidecars, so a ballast file is impractical.

## Consequences

- Per-volume provisioning does not cap the instance's EBS bandwidth, so the allocator tracks the sum per node.
- NVMe storage cannot be expanded; growth means scaling out.
- Tiered storage (KIP-405, with Aiven's plugin) is a v2 evaluation. KIP-1150 is out of scope.
