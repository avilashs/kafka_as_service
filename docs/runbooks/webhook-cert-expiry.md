# Runbook: The placement webhook certificate is about to expire

**Alert:** `StreamcellWebhookCertExpiring`

The binding guard and the admission checks run as webhooks with `failurePolicy: Fail`: when their certificate expires, no pod can be bound in a tenant namespace, so no broker can be replaced.

1. `kubectl -n streamcell-system describe certificate streamcell-admission-tls`: the status says why issuing or renewing failed. `kubectl -n cert-manager logs deploy/cert-manager` has the details.
2. If cert-manager itself is down, restart it; it renews on start.
3. Forcing a renewal: `cmctl renew -n streamcell-system streamcell-admission-tls` (or delete the secret `streamcell-admission-tls`; the admission Deployment picks the new one up on its next rollout: `kubectl -n streamcell-system rollout restart deploy/streamcell-admission`).
4. If the certificate has already expired and binds fail: temporarily set the webhook `failurePolicy` to `Ignore` (`kubectl patch validatingwebhookconfiguration streamcell-binding-guard ...`), fix the certificate, then restore `Fail`. Placement rules are not enforced meanwhile: run the auditor (`placement-violation.md`) afterwards.
