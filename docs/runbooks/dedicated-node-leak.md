# Runbook: StreamcellDedicatedNodeLeak

**Alert:** `StreamcellDedicatedNodeLeak` (ticket, after 30 minutes). **Meaning:** a dedicated resource
holds more nodes (label `streamcell.dev/dedicated-owner=<cluster>`) than it has whole-node pods plus the
allowed rotation surge. Idle dedicated nodes are billed. Transient during create (nodes arrive before pods)
and rotation, which is why the alert waits 30 minutes.

## Diagnose

```bash
C=<cluster id from the alert>
kubectl get nodes -l streamcell.dev/dedicated-owner=$C -L topology.kubernetes.io/zone
kubectl -n $C get pods -o wide                         # does the namespace still exist?
kubectl get kafka,kafkanodepool -n $C                  # does the resource still exist?
```

| Finding | Cause | Action |
|---|---|---|
| Namespace and `Kafka` gone, nodes remain | The delete finished but the agent could not release the pool (provider error, or a pod it could not see still on the node). | Check the agent log for `release node pool`. Fix the cause and wait one status tick; the agent retries. |
| Namespace exists, fewer pods than nodes after a scale-in | The scale-in removed brokers but the pool was not shrunk. | Check the operation in the control plane. Re-run the scale-in. |
| A rotation surge node never drained | Rotation workflow stuck. | See the rotation runbook (M4). Do **not** remove the node by hand while its broker's data is the last in-sync copy. |

## Manual release (last resort)

Only when the cluster is confirmed deleted in the control plane (`GET /v1/clusters/<id>` is 404) and
no pods remain on the nodes. Nodes carry a protection policy, so use the same identity as the agent:

```bash
for n in $(kubectl get nodes -l streamcell.dev/dedicated-owner=$C -o name); do
  kubectl label $n streamcell.dev/dedicated-owner- streamcell.dev/nodepool- streamcell.dev/pool=spare --overwrite
  kubectl taint $n streamcell.dev/dedicated-
done
```

On AWS delete the cluster's Karpenter `NodePool` (`kubectl delete nodepool ded-$C`) instead; Karpenter
drains and terminates the instances.
