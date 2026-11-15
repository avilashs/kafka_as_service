#!/usr/bin/env bash
# Brings an Azure cell to the state the kind cell is in, from the outputs of infra/azure/cell:
#   cluster add-ons (cert-manager, Envoy Gateway, Strimzi), the AKSNodeClass for Node Auto Provisioning, storage,
#   placement enforcement, the cell's Prometheus, and the cell registration file.
#
# STATUS: NOT RUN. Written against the documented interfaces of each component; nothing here has touched Azure
# (M6 was prepared offline, see docs/milestones/m6.md). Expect to fix it on the first real run. Every step is
# idempotent, so it can be re-run.
#
# Requires: az (logged in), kubectl, helm, envsubst, jq, and the tofu outputs in $OUTPUTS
# (tofu -chdir=infra/azure/cell output -json). Node Auto Provisioning and the network (Azure CNI overlay with the
# Cilium data plane) are set up by OpenTofu. The control plane (API, worker, Postgres, Temporal, OpenBao) is NOT part
# of the cell: run it where the tests run and point the agent at it.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUTPUTS="${OUTPUTS:?path to the output of: tofu -chdir=infra/azure/cell output -json}"
CELL_ID="${CELL_ID:?cell id (the cell_id given to tofu)}"
out() { jq -r ".$1.value" "$OUTPUTS"; }

ENVOY_GATEWAY_VERSION="${ENVOY_GATEWAY_VERSION:-v1.9.3}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.18.2}"
STRIMZI_DIR="$ROOT/third_party/strimzi/1.3.0"
OPNS=streamcell-operators

export DISK_ENCRYPTION_SET_ID DOMAIN CELL_ID LOCATION
DISK_ENCRYPTION_SET_ID=$(out disk_encryption_set_id); LOCATION=$(out location)
DOMAIN=${DOMAIN:-$(out dns_zone | sed "s/^$CELL_ID\.//; s/\.$//")}

az aks get-credentials --subscription "$(out subscription_id)" -g "$(out resource_group)" -n "$(out cluster_name)" --overwrite-existing >/dev/null
CTRL_TOLERATIONS='[{"key":"streamcell.dev/control","operator":"Exists","effect":"NoSchedule"}]'
CTRL_SELECTOR='{"streamcell.dev/pool":"control"}'

# --- cert-manager (webhook certificates) ---
helm upgrade --install cert-manager oci://quay.io/jetstack/charts/cert-manager --version "$CERT_MANAGER_VERSION" \
  -n cert-manager --create-namespace --set crds.enabled=true \
  --set-json "tolerations=$CTRL_TOLERATIONS" --set-json "nodeSelector=$CTRL_SELECTOR" \
  --set-json "webhook.tolerations=$CTRL_TOLERATIONS" --set-json "webhook.nodeSelector=$CTRL_SELECTOR" \
  --set-json "cainjector.tolerations=$CTRL_TOLERATIONS" --set-json "cainjector.nodeSelector=$CTRL_SELECTOR" --wait --timeout 5m

# --- node class for dedicated pools (Node Auto Provisioning) and storage ---
kubectl apply -f "$ROOT/deploy/azure/manifests/aksnodeclass.yaml"
envsubst < "$ROOT/deploy/azure/manifests/storage.yaml" | kubectl apply -f -

# --- Envoy Gateway (installs the Gateway API CRDs) and the cell gateway behind the Azure load balancer ---
helm upgrade --install eg oci://docker.io/envoyproxy/gateway-helm --version "$ENVOY_GATEWAY_VERSION" \
  -n envoy-gateway-system --create-namespace \
  --set-json "deployment.pod.tolerations=$CTRL_TOLERATIONS" --set-json "deployment.pod.nodeSelector=$CTRL_SELECTOR" --wait --timeout 5m
kubectl create namespace streamcell-gateway --dry-run=client -o yaml | kubectl apply -f -
envsubst < "$ROOT/deploy/azure/manifests/gateway.yaml" | kubectl apply -f -

# --- Strimzi 1.3.0 (as on the kind cell: cluster-wide, shard 0, its own network policies off) ---
kubectl create namespace "$OPNS" --dry-run=client -o yaml | kubectl apply -f -
for f in "$STRIMZI_DIR"/*.yaml; do sed "s/namespace: myproject/namespace: $OPNS/" "$f"; echo "---"; done | kubectl apply -n "$OPNS" --server-side -f - >/dev/null
for role in strimzi-cluster-operator-namespaced strimzi-cluster-operator-watched strimzi-entity-operator; do
  kubectl create clusterrolebinding "$role-all" --clusterrole="$role" --serviceaccount="$OPNS:strimzi-cluster-operator" --dry-run=client -o yaml | kubectl apply -f -
done
kubectl -n "$OPNS" set env deploy/strimzi-cluster-operator STRIMZI_NAMESPACE-
kubectl -n "$OPNS" set env deploy/strimzi-cluster-operator STRIMZI_NAMESPACE='*' \
  STRIMZI_CUSTOM_RESOURCE_SELECTOR='streamcell.dev/operator-shard=0' STRIMZI_NETWORK_POLICY_GENERATION=false
kubectl -n "$OPNS" patch deploy strimzi-cluster-operator --type merge -p \
  "{\"spec\":{\"template\":{\"spec\":{\"nodeSelector\":$CTRL_SELECTOR,\"tolerations\":$CTRL_TOLERATIONS}}}}"
kubectl -n "$OPNS" rollout status deploy/strimzi-cluster-operator --timeout 5m

# --- placement enforcement; the serving certificate comes from cert-manager instead of the certgen job ---
kubectl create namespace streamcell-system --dry-run=client -o yaml | kubectl apply -f -
# The agent identity on Azure is the Kubernetes user of its credentials: set it in the policy as the provisioner.
PROVISIONER="${PROVISIONER:?the Kubernetes username of the agent identity, see docs/milestones/m6.md}"
for f in "$ROOT"/deploy/admission/policies/*.yaml; do sed "s#^  provisioners: \".*\"#  provisioners: \"$PROVISIONER\"#" "$f" | kubectl apply -f -; done
kubectl apply -f "$ROOT/deploy/common/webhook-certificates.yaml"
kubectl -n streamcell-system wait --for=condition=Ready certificate/streamcell-admission-tls --timeout=3m
kubectl apply -f "$ROOT/deploy/admission/workloads/rbac.yaml" -f "$ROOT/deploy/admission/workloads/auditor.yaml"
# admission.yaml without its certgen Job (a document that mentions the job's name is dropped)
awk 'BEGIN{RS="\n---\n"; ORS="\n---\n"} !/streamcell-admission-certgen/' "$ROOT/deploy/admission/workloads/admission.yaml" | kubectl apply -f -
kubectl apply -f "$ROOT/deploy/admission/workloads/webhook-config.yaml"
kubectl annotate validatingwebhookconfiguration streamcell-binding-guard cert-manager.io/inject-ca-from=streamcell-system/streamcell-admission-tls --overwrite
kubectl -n streamcell-system rollout status deploy/streamcell-admission --timeout=3m

# --- the cell's Prometheus with the rule files ---
kubectl apply -f "$ROOT/deploy/kind/monitoring.yaml" >/dev/null 2>&1 || true
kubectl -n streamcell-monitoring create configmap streamcell-rules $(for f in "$ROOT"/deploy/alerts/*.rules.yaml; do printf -- '--from-file=%s ' "$f"; done) \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl apply -f "$ROOT/deploy/kind/monitoring.yaml" >/dev/null
kubectl -n streamcell-monitoring rollout status deploy/prometheus --timeout=3m

# --- the cell registration (pool figures: re-verify against the Azure VM size documentation before trusting the allocator) ---
export APISERVER_CIDRS_JSON ZONES_JSON ENGINE_IMAGES_JSON CONTROL_VM_SIZE SHARED_VM_SIZE DEDICATED_VM_SIZE SHARED_MAX_NODES DEDICATED_MAX_NODES
export DEDICATED_CPU_MILLI DEDICATED_MEM_MIB CONTROL_ZONES_JSON SHARED_ZONES_JSON DEDICATED_ZONES_JSON
APISERVER_CIDRS_JSON=$(kubectl get endpoints kubernetes -o json | jq -c '[.subsets[].addresses[].ip + "/32"]')
# AKS labels nodes with topology.kubernetes.io/zone=<location>-<zone>
ZONES_JSON=$(jq -c --arg loc "$LOCATION" '[.zones.value[] | $loc + "-" + .]' "$OUTPUTS")
ENGINE_IMAGES_JSON="${ENGINE_IMAGES_JSON:?JSON map of Kafka version to the platform image in your registry}"
CONTROL_VM_SIZE=$(out control_vm_size); SHARED_VM_SIZE="${SHARED_VM_SIZE:-Standard_D8ds_v5}"; DEDICATED_VM_SIZE="${DEDICATED_VM_SIZE:-Standard_D4ds_v5}"
SHARED_MAX_NODES="${SHARED_MAX_NODES:-9}"; DEDICATED_MAX_NODES="${DEDICATED_MAX_NODES:-9}"
DEDICATED_CPU_MILLI="${DEDICATED_CPU_MILLI:-3600}"; DEDICATED_MEM_MIB="${DEDICATED_MEM_MIB:-13500}"
zones_json() { # per-node figures, nodes per zone
  jq -nc --argjson az "$ZONES_JSON" --argjson cpu "$1" --argjson mem "$2" --argjson mbps "$3" --argjson att "$4" --argjson n "$5" \
    '[$az[] | {zone: ., cpuMilli: $cpu, memoryMiB: $mem, ebsMBps: $mbps, attach: $att, nodes: $n}]'
}
CONTROL_ZONES_JSON=$(zones_json 1600 5500 100 4 1)        # Standard_D2ds_v5: 2 vCPU, 8 GiB, 4 data disks
SHARED_ZONES_JSON=$(zones_json 7100 27000 190 16 1)       # Standard_D8ds_v5: 8 vCPU, 32 GiB, 16 data disks; the autoscaler adds nodes
DEDICATED_ZONES_JSON=$(zones_json 0 0 0 0 0)
envsubst < "$ROOT/deploy/azure/cell.json.tmpl" > "${OUT_DIR:-.}/cell-$CELL_ID.json"
echo "Cell add-ons are installed. Registration file: ${OUT_DIR:-.}/cell-$CELL_ID.json"
echo "Next: streamcell-admin register-cell <that file>; start the agent with NODEPOOL_PROVIDER=karpenter KARPENTER_NODECLASS=streamcell KARPENTER_NODECLASS_GROUP=karpenter.azure.com KARPENTER_NODECLASS_KIND=AKSNodeClass."
