# ADR-0014: OpenTofu for cell infrastructure

**Status:** Proposed

## Decision

Use OpenTofu (MPL-2.0) for cell infrastructure on AWS: VPC, EKS, Karpenter IAM, KMS and the Route 53 zone. Terraform has been under BSL 1.1 since 2023, which fails the licence rule.

Modules live in `infra/aws/` and are tested with `tofu plan` in CI.
