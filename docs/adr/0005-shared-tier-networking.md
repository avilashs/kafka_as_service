# ADR-0005: Shared-tier networking: Strimzi `tlsroute` with Envoy Gateway, zone-local

**Status:** Proposed (spike S-3)

## Context

Clients must reach each broker individually, without a per-tenant load balancer and without a proxy that parses Kafka. ingress-nginx is retired.

## Decision

Each `Kafka` resource gets an external listener with `type: tlsroute` and `tls: true`. Strimzi 1.1+ then creates a per-broker Service and a Gateway API `v1` `TLSRoute`, plus one for bootstrap. The routes attach to one Envoy Gateway (≥ 1.9) `Gateway` per cell, which has a TLS `Passthrough` listener on port 443.

The cell NLB has cross-zone load balancing disabled and IP targets on Envoy. Per-broker DNS names are CNAMEs to the NLB's zonal name for the broker's zone, so the platform adds no cross-zone hop. The bootstrap name uses the regional NLB name. Route 53 health checks fail zonal names over to the regional name.

## Consequences

- One NLB per cell. The marginal network cost per tenant is close to zero.
- Envoy is a shared data-path component, but it only reads SNI. Its capacity is part of the cell capacity model.
- Brokers see Envoy's IP address. Per-IP connection limits and ACL host rules therefore do not work. Connection limits are enforced in Envoy.

## Alternatives

- **Hand-written `TLSRoute`s on `cluster-ip` services.** Unnecessary now that Strimzi generates them.
- **Per-tenant NLBs.** Ruled out by the brief, and costly.
- **ingress-nginx.** Retired.
