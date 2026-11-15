#!/usr/bin/env bash
# Brings an AWS cell to the state the kind cell is in, from the outputs of infra/aws/cell:
#   cluster add-ons (Karpenter, AWS Load Balancer Controller, cert-manager, Envoy Gateway, Strimzi),
#   placement enforcement, the cell's Prometheus, and the cell registration.
#
# STATUS: NOT RUN. Written against the documented interfaces of each component; nothing here has touched AWS
# (M6 was prepared offline, see docs/milestones/m6.md). Expect to fix it on the first real run. Every step is
# idempotent, so it can be re-run.
#
# Requires: aws, kubectl, helm, envsubst, jq, tofu outputs in $OUTPUTS (tofu -chdir=infra/aws/cell output -json).
# The control plane (API, worker, Postgres, Temporal, OpenBao) is NOT part of the cell: run it where the tests
# run (hack/aws/control-plane.sh) and point the agent at it.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUTPUTS="${OUTPUTS:?path to the output of: tofu -chdir=infra/aws/cell output -json}"
CELL_ID="${CELL_ID:?cell id (the cell_id given to tofu)}"
out() { jq -r ".$1.value" "$OUTPUTS"; }

# Pinned like the kind cell where it shares a component; the others are the versions the design names. Re-verify.
ENVOY_GATEWAY_VERSION="${ENVOY_GATEWAY_VERSION:-v1.9.3}"
KARPENTER_VERSION="${KARPENTER_VERSION:-1.15.1}"
LBC_CHART_VERSION="${LBC_CHART_VERSION:-1.14.0}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.18.2}"
STRIMZI_DIR="$ROOT/third_party/strimzi/1.3.0"
OPNS=streamcell-operators

export CLUSTER_NAME REGION DOMAIN KMS_KEY_ARN NODE_ROLE_NAME CELL_ID
CLUSTER_NAME=$(out cluster_name); REGION=$(out region); DOMAIN=${DOMAIN:-$(out domain | sed "s/^$CELL_ID\.//; s/\.$//")}
KMS_KEY_ARN=$(out kms_key_arn); NODE_ROLE_NAME=$(out karpenter_node_role_name)
export SHARED_INSTANCE_TYPE="${SHARED_INSTANCE_TYPE:-m7i.2xlarge}" SHARED_MAX_NODES="${SHARED_MAX_NODES:-9}"

aws eks update-kubeconfig --region "$REGION" --name "$CLUSTER_NAME" >/dev/null
CTRL_TOLERATIONS='[{"key":"streamcell.dev/control","operator":"Exists","effect":"NoSchedule"}]'
CTRL_SELECTOR='{"streamcell.dev/pool":"control"}'

# --- cert-manager (webhook certificates) ---
helm upgrade --install cert-manager oci://quay.io/jetstack/charts/cert-manager --version "$CERT_MANAGER_VERSION" \
  -n cert-manager --create-namespace --set crds.enabled=true \
  --set-json "tolerations=$CTRL_TOLERATIONS" --set-json "nodeSelector=$CTRL_SELECTOR" \
  --set-json "webhook.tolerations=$CTRL_TOLERATIONS" --set-json "webhook.nodeSelector=$CTRL_SELECTOR" \
  --set-json "cainjector.tolerations=$CTRL_TOLERATIONS" --set-json "cainjector.nodeSelector=$CTRL_SELECTOR" --wait --timeout 5m

# --- Karpenter: the controller runs on the control pool, the NodePools and the EC2NodeClass come from the repo ---
helm upgrade --install karpenter oci://public.ecr.aws/karpenter/karpenter --version "$KARPENTER_VERSION" -n kube-system \
  --set "settings.clusterName=$CLUSTER_NAME" --set "settings.interruptionQueue=$(out karpenter_interruption_queue)" \
  --set-json "tolerations=$CTRL_TOLERATIONS" --set-json "nodeSelector=$CTRL_SELECTOR" --set replicas=2 --wait --timeout 5m
envsubst < "$ROOT/deploy/aws/manifests/ec2nodeclass.yaml" | kubectl apply -f -
envsubst < "$ROOT/deploy/aws/manifests/nodepool-shared.yaml" | kubectl apply -f -
envsubst < "$ROOT/deploy/aws/manifests/storage.yaml" | kubectl apply -f -

# --- AWS Load Balancer Controller (the NLB of the cell gateway) ---
helm repo add eks https://aws.github.io/eks-charts >/dev/null 2>&1 || true
helm repo update eks >/dev/null
helm upgrade --install aws-load-balancer-controller eks/aws-load-balancer-controller --version "$LBC_CHART_VERSION" -n kube-system \
  --set "clusterName=$CLUSTER_NAME" --set "region=$REGION" --set "vpcId=$(out vpc_id)" \
  --set serviceAccount.name=aws-load-balancer-controller \
  --set-json "tolerations=$CTRL_TOLERATIONS" --set-json "nodeSelector=$CTRL_SELECTOR" --wait --timeout 5m

# --- Envoy Gateway (installs the Gateway API CRDs) and the cell gateway behind the NLB ---
helm upgrade --install eg oci://docker.io/envoyproxy/gateway-helm --version "$ENVOY_GATEWAY_VERSION" \
  -n envoy-gateway-system --create-namespace \
  --set-json "deployment.pod.tolerations=$CTRL_TOLERATIONS" --set-json "deployment.pod.nodeSelector=$CTRL_SELECTOR" --wait --timeout 5m
kubectl create namespace streamcell-gateway --dry-run=client -o yaml | kubectl apply -f -
envsubst < "$ROOT/deploy/aws/manifests/gateway.yaml" | kubectl apply -f -

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
# The agent's identity on AWS is its IAM role mapped to a Kubernetes user: set it in the policy as the provisioner.
PROVISIONER="${PROVISIONER:?the Kubernetes username of the agent identity, see docs/milestones/m6.md}"
for f in "$ROOT"/deploy/admission/policies/*.yaml; do sed "s#^  provisioners: \".*\"#  provisioners: \"$PROVISIONER\"#" "$f" | kubectl apply -f -; done
kubectl apply -f "$ROOT/deploy/aws/manifests/webhook-certificates.yaml"
kubectl -n streamcell-system wait --for=condition=Ready certificate/streamcell-admission-tls --timeout=3m
kubectl apply -f "$ROOT/deploy/admission/workloads/rbac.yaml" -f "$ROOT/deploy/admission/workloads/auditor.yaml"
# admission.yaml without its certgen Job (a document that mentions the job's name is dropped)
awk 'BEGIN{RS="\n---\n"; ORS="\n---\n"} !/streamcell-admission-certgen/' "$ROOT/deploy/admission/workloads/admission.yaml" | kubectl apply -f -
kubectl apply -f "$ROOT/deploy/admission/workloads/webhook-config.yaml"
for w in streamcell-binding-guard; do
  kubectl annotate validatingwebhookconfiguration "$w" cert-manager.io/inject-ca-from=streamcell-system/streamcell-admission-tls --overwrite
done
kubectl -n streamcell-system rollout status deploy/streamcell-admission --timeout=3m

# --- the cell's Prometheus with the rule files ---
kubectl apply -f "$ROOT/deploy/kind/monitoring.yaml" >/dev/null 2>&1 || true
kubectl -n streamcell-monitoring create configmap streamcell-rules $(for f in "$ROOT"/deploy/alerts/*.rules.yaml; do printf -- '--from-file=%s ' "$f"; done) \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl apply -f "$ROOT/deploy/kind/monitoring.yaml" >/dev/null
kubectl -n streamcell-monitoring rollout status deploy/prometheus --timeout=3m

# --- the cell registration (pool figures: re-verify against the EC2 documentation before trusting the allocator) ---
APISERVER_IPS=$(kubectl get endpoints kubernetes -o json | jq -c '[.subsets[].addresses[].ip + "/32"]')
export APISERVER_CIDRS_JSON="$APISERVER_IPS"
export ZONES_JSON; ZONES_JSON=$(jq -c '.azs.value' "$OUTPUTS")
export ENGINE_IMAGES_JSON="${ENGINE_IMAGES_JSON:?JSON map of Kafka version to the platform image in your registry}"
export CONTROL_INSTANCE_TYPE; CONTROL_INSTANCE_TYPE=$(jq -r '.control_instance_type.value // "m7i.large"' "$OUTPUTS")
export DEDICATED_INSTANCE_TYPE="${DEDICATED_INSTANCE_TYPE:-m7i.xlarge}" DEDICATED_MAX_NODES="${DEDICATED_MAX_NODES:-9}"
export DEDICATED_CPU_MILLI="${DEDICATED_CPU_MILLI:-3800}" DEDICATED_MEM_MIB="${DEDICATED_MEM_MIB:-14500}"
zones_json() { # per-node figures, nodes per zone
  jq -nc --argjson az "$ZONES_JSON" --argjson cpu "$1" --argjson mem "$2" --argjson mbps "$3" --argjson att "$4" --argjson n "$5" \
    '[$az[] | {zone: ., cpuMilli: $cpu, memoryMiB: $mem, ebsMBps: $mbps, attach: $att, nodes: $n}]'
}
export CONTROL_ZONES_JSON SHARED_ZONES_JSON DEDICATED_ZONES_JSON
CONTROL_ZONES_JSON=$(zones_json 1900 7000 150 24 1)       # m7i.large: 2 vCPU, 8 GiB
SHARED_ZONES_JSON=$(zones_json 7600 29000 150 24 0)        # m7i.2xlarge: 8 vCPU, 32 GiB; Karpenter adds the nodes
DEDICATED_ZONES_JSON=$(zones_json 0 0 0 0 0)
envsubst < "$ROOT/deploy/aws/cell.json.tmpl" > "${OUT_DIR:-.}/cell-$CELL_ID.json"
echo "Cell add-ons are installed. Registration file: ${OUT_DIR:-.}/cell-$CELL_ID.json"
echo "Next: streamcell-admin register-cell <that file>, start the agent with NODEPOOL_PROVIDER=karpenter KARPENTER_NODECLASS=streamcell."
