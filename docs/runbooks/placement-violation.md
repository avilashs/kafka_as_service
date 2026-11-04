# Runbook: StreamcellPlacementViolation

**Alert:** `StreamcellPlacementViolation` (page). **Meaning:** the placement auditor found a pod
or node that breaks one of the placement invariants. Placement is enforced at four layers
(rendered constraints, admission policy, binding guard, this audit); an audit finding means the
first three did not stop it, which is itself a bug to find.

## 1. See exactly what is wrong

```bash
streamcell-audit --output json | jq '.violations[]'
```

(or `kubectl -n streamcell-system logs deploy/placement-auditor | grep "placement violation"`).
Each violation names the `rule`, the resource (`cluster`), the `node` and the `pods`.

## 2. Rules and what to do

| Rule | What it means | First action |
|---|---|---|
| `same_group_on_node` | Two pods of one resource and role (e.g. two brokers of one cluster) share a node. Risk: one node failure takes out two replicas. | Delete **one** of the pods (`kubectl delete pod`); the operator recreates it and the scheduler must place it elsewhere. Never delete both. Check `kubectl describe pod` for the anti-affinity term. |
| `zone_skew` | A resource's pods are spread unevenly over zones (more than 1 apart). | Usually follows a zone outage or a node pool that cannot scale in a zone. Check capacity in the short zone; once it recovers, delete a pod in the heavy zone. |
| `dedicated_node_foreign_pod` | A pod that does not belong to the node's owner (or an unlabelled non-DaemonSet pod) runs on a dedicated node. | Delete the foreign pod. Find how it got there: the admission policy rejects wrong tolerations, so look for a pod in a namespace without the `streamcell.dev/inventory` label, or a hand-made Binding. |
| `dedicated_node_multi_exclusive` | Two whole-node pods (e.g. two brokers) on one dedicated node. | As `same_group_on_node`: delete one pod. |
| `dedicated_node_count_mismatch` | A resource holds more dedicated nodes than pods plus surge. See [dedicated-node-leak](dedicated-node-leak.md). | |
| `pod_off_pool` | A pod runs on a node of the wrong pool kind (e.g. a shared broker on a control or dedicated node, or a controller on a shared node). | Delete the pod. Check that the node labels are right (`kubectl get node -L streamcell.dev/pool`): a mislabelled node is a cause. |

## 3. Find the cause

The admission layers should have refused this. Check, in order:

```bash
kubectl get validatingadmissionpolicy,validatingadmissionpolicybinding | grep streamcell
kubectl get validatingwebhookconfiguration streamcell-binding-guard -o jsonpath='{.webhooks[0].failurePolicy}'
kubectl -n streamcell-system get deploy streamcell-admission placement-auditor
kubectl label namespace <ns> --list | grep inventory     # tenant namespaces must carry streamcell.dev/inventory
```

- Policy or binding missing, or the webhook set to `Ignore`: someone changed it. Restore from `deploy/admission/` and open an incident.
- Pod is in a namespace without the `streamcell.dev/inventory` label: it was outside policy scope.
  Tenant namespaces are created by the agent with that label; a namespace that lost it is a finding in itself.
- The pod pre-dates the policy: expected once, right after installing the policy on a running cell.

## 4. Verify

The alert clears one audit interval (30 s) after the violation is gone:

```bash
streamcell-audit && echo clean
```
