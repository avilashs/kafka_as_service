# Runbook: StreamcellPlacementAuditStale / StreamcellPlacementAuditorMissing

**Meaning:** the placement auditor is not completing audits (stale), or no auditor reports at all
(missing). While this is firing, **placement violations would go unnoticed**. The three admission
layers still protect new pods; nothing is checking what already runs.

```bash
kubectl -n streamcell-system get deploy placement-auditor
kubectl -n streamcell-system logs deploy/placement-auditor --tail=50
```

| Log / state | Cause | Action |
|---|---|---|
| No pods / `CrashLoopBackOff` | Deployment unhealthy | `kubectl describe`; check the image and the control-pool capacity |
| `list pods: ... forbidden` | RBAC for the auditor's ServiceAccount was removed | Re-apply `deploy/admission/auditor.yaml` |
| `list pods: ... timeout` / `etcd` errors | The cell's API server is overloaded | Treat as a cell incident; the auditor resumes by itself |
| Prometheus has no target | Scrape config/annotations lost | Check the `prometheus.io/scrape` annotations on the Deployment |

Run `streamcell-audit` from a workstation with cell access as a stop-gap while the auditor is down.
