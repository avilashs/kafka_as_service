# ADR-0018: Certificate-authenticated clients: the cell signs a CSR, the key stays with the tenant

**Status:** Accepted in M3

## Context

Tenants need mTLS besides SCRAM (Q-4: separate ports). Strimzi's `KafkaUser` of type `tls` generates the client key pair itself and stores the private key in a Kubernetes Secret in the cell. Returning it to the tenant would move a long-lived private key through the cell, the agent, the platform and the API.

## Decision

- A separate external listener `mtls` (port 9095, own hosts `mtls-bootstrap.…` and `m<N>.…`, `authentication: tls`), behind the same gateway and SNI routing, beside the SCRAM listener.
- The tenant generates the key and sends a PKCS#10 request (`POST /mtls-users`). The spec holds the request (public). The `KafkaUser` is `tls-external` (Strimzi only manages ACLs).
- The **cell agent signs** the request with the cluster's clients CA, whose key never leaves the cell. The agent, not the request, decides the subject (`CN=<user name>`), key usage (client authentication) and validity (90 days, capped by the CA). The result is a public certificate, reported as an observed output and kept in a Secret for stability across restarts; a request or CA change signs again. The CSR signature is verified (proof of possession); only RSA 2048–4096 and ECDSA P-256/P-384 are accepted.
- User names starting with `kc-` or `platform` are reserved. Platform principals are certificates named after the cluster id (`CN=kc-<id>-cruise-control`); with this rule no tenant can obtain one. `PlatformAuthorizer` additionally refuses ACLs for them.
- Renewal is a new request (`POST /renew`); there is no password to rotate.

## Consequences

- The agent gains a signing role: it needs `get` on the clients CA secrets in tenant namespaces, which it has as the cell's provisioner. A compromised agent can sign any certificate; it could already do everything else in the cell. Signing is not reachable from the platform except through a bundle.
- Certificates are not revocable before expiry except by deleting the user (ACLs vanish) or rolling the clients CA. CRLs are not supported by Kafka's default trust manager. 90-day validity bounds the exposure. Documented limit.
- Strimzi renews the clients CA on its own schedule; the agent signs with the current key, and previously issued certificates stay valid during the overlap Strimzi provides.
