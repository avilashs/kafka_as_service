# Upstream version and status check

**Checked on 2026-11-04.**

Network access from the build session was limited to `github.com` and `raw.githubusercontent.com`. These hosts were unreachable: `kafka.apache.org`, `strimzi.io`, `cwiki.apache.org`, `aws.amazon.com`, `temporal.io`, `gateway.envoyproxy.io` and `karpenter.sh`.

I verified facts in three ways, ordered here by how far I trust them:

1. **Source:** read in the upstream repository (code, docs-as-code, CHANGELOG).
2. **Tag:** `git ls-remote --tags` on the upstream repository. This proves the tag exists, not that a release announcement was made.
3. **Search:** web search results, sometimes secondary sources. These are marked for re-checking.

| Item | Finding | Evidence | Confidence |
|---|---|---|---|
| Apache Kafka | Tags `4.3.0`, `4.3.1`, `4.3.2`, `4.4.0` exist. 4.2.0 was released Feb 2026 and 4.3.0 in May 2026 (Kafka blog, via search). ZooKeeper was removed in 4.0, so KRaft is the only mode. | Tag / Search | High |
| Strimzi | Latest GA tag `1.3.0`; `1.3.1-rc2` was tagged 2026-11-03. Main branch is `1.4.0-SNAPSHOT`, and its CHANGELOG adds Kafka 4.3.2 and 4.4.0 and removes 4.2.x. | Source (CHANGELOG) / Tag | High |
| Strimzi API | From 1.0.0 only `v1` is served; `v1beta2`, `v1beta1` and `v1alpha1` are removed. | Search (release notes) | High |
| Strimzi Kubernetes support | 1.3.0 supports Kubernetes ≥ 1.32. | Source (CHANGELOG) | High |
| Strimzi `tlsroute` listener | Added in 1.1.0. It generates Gateway API `v1` `TLSRoute`s per broker plus one for bootstrap, using `parentRefs`, `bootstrap.host` and `hostTemplate`. The `ingress` type is deprecated (since 0.51) because ingress-nginx was archived. | Source (docs, CHANGELOG) | High |
| Strimzi KIP-1066 cordoning | 1.2.0 cordons brokers during auto-rebalance on scale-down, for Kafka 4.3+. | Source (CHANGELOG) | High |
| Strimzi SA tokens | 1.2.0 stops auto-mounting SA tokens and mounts them through a volume instead. 1.3.0 supports projected SA tokens. | Source (CHANGELOG) | High. Which containers get the token is still unknown: spike S-4. |
| Strimzi Drain Cleaner | Tag `1.8.0`, bundled with Strimzi 1.4.0. The docs use PDB `maxUnavailable: 0` together with Drain Cleaner. | Tag / Source | High |
| Strimzi custom resource selector | `STRIMZI_CUSTOM_RESOURCE_SELECTOR` filters `Kafka`, `KafkaConnect`, `KafkaBridge` and `KafkaMirrorMaker2`. `KafkaRebalance` follows its `Kafka`. | Source (docs) | High |
| Node ID annotations | `strimzi.io/next-node-ids` and `strimzi.io/remove-node-ids` on `KafkaNodePool`. | Source (docs) | High |
| KRaft policy enforcement | `CreateTopicPolicy` is evaluated in `ReplicationControlManager` on the **controller**, and only for topic creation. `createPartitions` calls no policy. A controller mutation quota (KIP-599) applies to partition changes. | Source (`metadata/.../ReplicationControlManager.java`, trunk) | High for trunk. Re-check on the 4.3 branch at M3. |
| Cruise Control | Tag `3.1.0` upstream. Strimzi bundles its own build. Licence BSD-2-Clause. | Tag | High |
| KIP-1150 diskless topics | Accepted 2026-03-02 as a motivational KIP. Implementation KIPs 1163 and 1164 were still under discussion as of Aug 2026. | Search (Aiven blog, secondary) | Medium |
| KIP-405 tiered storage | GA since Kafka 3.9. Aiven's plugin is Apache-2.0 and supports S3, GCS and Azure. | Search | Medium. Re-verify when the v2 evaluation starts. |
| ingress-nginx | Retired March 2026. No further releases or security fixes. | Search (kubernetes.io blog) | High |
| Bitnami | Most free images moved to `bitnamilegacy` (unpatched) in Aug–Sep 2025. Only a small set of `latest`-tag dev images stayed free. | Search | High |
| Gateway API | Latest tag `v1.7.0`. `TLSRoute` reached Standard `v1` in v1.5 (Feb 2026). | Tag / Search | High |
| Envoy Gateway | Latest tag `v1.9.3`. Its `go.mod` uses `sigs.k8s.io/gateway-api v1.6.1` and its resource model uses `gwapiv1.TLSRoute`. | Tag / Source | High that v1 TLSRoute is supported. Spike S-3 runs it end to end. |
| Temporal | Latest tag `v1.32.2`. Licence MIT (checked in the LICENSE file at that tag). | Tag / Source | High |
| Kubernetes | Latest tag `v1.37.2`; 1.36 was released April 2026. | Tag / Search | High |
| Karpenter | `kubernetes-sigs/karpenter` v1.15.1 and `aws/karpenter-provider-aws` v1.15.1. `NodePool` supports `limits.nodes` and alpha static `replicas`. | Source (`pkg/apis/v1/nodepool.go`) | High |
| Cilium | Latest tag `v1.20.4`. The Bandwidth Manager docs at that tag describe egress as EDT and ingress as an eBPF token bucket; both annotations are supported. | Source (Documentation) | High |
| CloudNativePG | Tag `v1.30.1`, Apache-2.0. | Tag | High |
| OpenBao | Tag `v2.7.2`, MPL-2.0. | Tag | High |
| Terraform | BSL 1.1 since Aug 2023, so it is not used. OpenTofu is MPL-2.0. | Prior knowledge | High |
| AWS prices and limits | NLB, NLCU, IPv4, PrivateLink and cross-AZ prices; the EBS 6-hour modification cooldown; the 50-listener NLB limit; instance EBS and network baselines. | Prior knowledge only | **Low. Must be re-verified** (the AWS site was unreachable). |

## Items to re-verify once the docs hosts are reachable

1. AWS prices used in design §14.4, the EBS modification cooldown, the NLB listener quota, and per-instance EBS and network baselines.
2. The Envoy Gateway compatibility matrix: Gateway API version, Kubernetes range, and end-of-life dates.
3. Kafka 4.3 and 4.4 release notes: `metadata.version` values, share groups (KIP-932) status, and any config renames that affect the allowlist.
4. The final KIP-1150 status page and the status of KIP-1163 and KIP-1164.
5. Whether `strimzi-canary` is still maintained. Not used, but it is relevant prior art.
