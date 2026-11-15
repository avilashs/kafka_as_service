# ADR-0019: Azure for the cloud runs of M6

**Status:** Decided by the project owner (a change from the brief, which named AWS as the first cloud).

## Context

The brief names AWS as the first cloud and the design (sections 7, 10, 13, 14) is written for it: EKS, Karpenter, EBS, an NLB with per-zone names, PrivateLink. For M6, the cloud hardening milestone, the owner asked for Azure instead.

## Decision

M6 targets **Azure**: AKS with Azure CNI overlay and the Cilium data plane, Node Auto Provisioning (Karpenter) for dedicated pools, AKS node pools for the system, control and shared pools, Premium SSD managed disks with a customer-managed key, a Standard load balancer in front of Envoy Gateway, Azure DNS, NSG-based zone isolation for the zone-outage test.

What stays the same: the platform is cloud-neutral above the cell (the `Engine` interface, the cell agent, the placement rules, metering, the canary). The cloud-specific code is the OpenTofu module, the add-on script, the node class of the Karpenter provider, the fault helpers and the storage and gateway manifests. The AWS preparation of M6 (commit `9959a1d`) was removed; nothing of it had run.

## Consequences

- Cilium, which the design names, is the CNI (as a managed data plane), instead of the VPC CNI that the AWS preparation settled for.
- The design's per-zone load balancer names (ADR-0005) are not carried over; ADR-0006 (PrivateLink for the dedicated tier) needs an Azure Private Link design that does not exist yet.
- Spikes S-5, S-8, S-9 are re-read for Azure (ext4 on managed disks, `Lsv3` local NVMe, Private Link).
- Instance figures in the design (sections 11, 13) are AWS figures; the allocator's pool figures for Azure are in `deploy/azure/up.sh` and are unverified.
- AWS support is not abandoned by design, only not prepared: `internal/cloud/karpenter` keeps the AWS node class as its default.
