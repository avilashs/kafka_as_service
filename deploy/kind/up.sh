#!/usr/bin/env bash
# Brings up a local cell on kind plus a local control plane:
#   kind cluster (node pools as labels/taints) + Strimzi 1.3.0 (cluster-wide, shard 0)
#   + Envoy Gateway; Postgres + Temporal dev server; streamcell-api, -worker, -agent.
# Also installs the placement enforcement (admission policies, pods/binding guard, auditor).
# Requires: docker, kind, kubectl, helm, go. Logs and pids go to .cache/kind/.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
STATE="$ROOT/.cache/kind"
mkdir -p "$STATE" "$ROOT/bin"
ENVOY_GATEWAY_VERSION="${ENVOY_GATEWAY_VERSION:-v1.9.3}"
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-kindest/node:v1.34.0}"
CELL_TOKEN="${CELL_TOKEN:-dev-cell-token}"
TENANT_TOKEN="${TENANT_TOKEN:-dev-tenant-token}"
TENANT_B_TOKEN="${TENANT_B_TOKEN:-dev-tenant-b-token}"
STRIMZI_DIR="$ROOT/third_party/strimzi/1.3.0"
OPNS=streamcell-operators

if ! kind get clusters | grep -qx streamcell; then
  kind create cluster --config "$ROOT/deploy/kind/cluster.yaml" --image "$KIND_NODE_IMAGE" --wait 120s
fi
kubectl config use-context kind-streamcell >/dev/null

# --- Envoy Gateway (installs Gateway API CRDs incl. TLSRoute v1) ---
helm upgrade --install eg oci://docker.io/envoyproxy/gateway-helm --version "$ENVOY_GATEWAY_VERSION" \
  -n envoy-gateway-system --create-namespace --wait --timeout 5m
kubectl create namespace streamcell-gateway --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f "$ROOT/deploy/kind/gateway.yaml"

# --- Strimzi 1.3.0, watching all namespaces, operator shard 0 ---
kubectl create namespace "$OPNS" --dry-run=client -o yaml | kubectl apply -f -
for f in "$STRIMZI_DIR"/*.yaml; do
  sed "s/namespace: myproject/namespace: $OPNS/" "$f"
  echo "---"
done | kubectl apply -n "$OPNS" --server-side -f - >/dev/null
# Cluster-wide watching needs ClusterRoleBindings for the namespaced roles (Strimzi docs).
for role in strimzi-cluster-operator-namespaced strimzi-cluster-operator-watched strimzi-entity-operator; do
  kubectl create clusterrolebinding "$role-all" --clusterrole="$role" \
    --serviceaccount="$OPNS:strimzi-cluster-operator" --dry-run=client -o yaml | kubectl apply -f -
done
kubectl -n "$OPNS" set env deploy/strimzi-cluster-operator STRIMZI_NAMESPACE- >/dev/null
kubectl -n "$OPNS" set env deploy/strimzi-cluster-operator STRIMZI_NAMESPACE='*' \
  STRIMZI_CUSTOM_RESOURCE_SELECTOR='streamcell.dev/operator-shard=0' >/dev/null
# The operator itself runs on the control pool.
kubectl -n "$OPNS" patch deploy strimzi-cluster-operator --type merge -p \
  '{"spec":{"template":{"spec":{"nodeSelector":{"streamcell.dev/pool":"control"},"tolerations":[{"key":"streamcell.dev/control","operator":"Exists","effect":"NoSchedule"}]}}}}'
kubectl -n "$OPNS" rollout status deploy/strimzi-cluster-operator --timeout 5m
kubectl create namespace streamcell-system --dry-run=client -o yaml | kubectl apply -f -

# --- placement enforcement (design §10.2 rules 5 and 6) ---
# The agent runs on the host with the kind admin kubeconfig, so that identity is the "provisioner"
# allowed to change node pool labels and taints. In a real cell it is the agent's ServiceAccount.
for f in "$ROOT"/deploy/admission/policies/*.yaml; do
  sed 's#^  provisioners: ".*"#  provisioners: "kubernetes-admin"#' "$f" | kubectl apply -f -
done
(cd "$ROOT" && make images kafka-image kafka-image-4.3.0)
kind load docker-image streamcell/cell-tools:dev --name streamcell
# The platform Kafka image (stock Strimzi image + the authorizer/policy plugins) must be on every node.
kind load docker-image streamcell/kafka:4.3.1-dev streamcell/kafka:4.3.0-dev --name streamcell
kubectl apply -f "$ROOT/deploy/admission/workloads/rbac.yaml" -f "$ROOT/deploy/admission/workloads/webhook-config.yaml" \
  -f "$ROOT/deploy/admission/workloads/admission.yaml" -f "$ROOT/deploy/admission/workloads/auditor.yaml"
kubectl -n streamcell-system wait --for=condition=complete job/streamcell-admission-certgen --timeout=3m
kubectl -n streamcell-system rollout status deploy/streamcell-admission --timeout=3m
kubectl -n streamcell-system rollout status deploy/placement-auditor --timeout=3m

# --- control-plane dependencies ---
eval "$("$ROOT/hack/e2e-deps.sh" up)"
DB_URL="${STREAMCELL_E2E_DATABASE_URL}"
docker exec streamcell-e2e-pg psql -U postgres -tc "SELECT 1 FROM pg_database WHERE datname='streamcell'" | grep -q 1 || \
  docker exec streamcell-e2e-pg psql -U postgres -c "CREATE DATABASE streamcell" >/dev/null
DB_URL="${DB_URL%/postgres?sslmode=disable}/streamcell?sslmode=disable"

(cd "$ROOT" && go build -o bin/ ./cmd/...)

# --- register the cell and a dev tenant ---
CP_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' streamcell-control-plane)"
sed "s#__APISERVER_IP__#${CP_IP}#" "$ROOT/deploy/kind/cell.json" > "$STATE/cell.json"
DATABASE_URL="$DB_URL" CELL_TOKEN="$CELL_TOKEN" "$ROOT/bin/streamcell-admin" register-cell "$STATE/cell.json"
DATABASE_URL="$DB_URL" "$ROOT/bin/streamcell-admin" register-tenant dev-tenant "Dev tenant"
DATABASE_URL="$DB_URL" "$ROOT/bin/streamcell-admin" register-tenant tenant-b "Second dev tenant (isolation tests)"

# --- run control plane + agent ---
# Secrets: the dev OpenBao started by hack/e2e-deps.sh. The agent reads user passwords from it; the
# database never holds them.
SECRETS_ENV=(SECRETS_BACKEND=openbao OPENBAO_ADDR="$STREAMCELL_E2E_OPENBAO_ADDR" OPENBAO_TOKEN="$STREAMCELL_E2E_OPENBAO_TOKEN")
start() { # name, then command with env
  local name=$1; shift
  if [[ -f "$STATE/$name.pid" ]] && kill -0 "$(cat "$STATE/$name.pid")" 2>/dev/null; then return; fi
  nohup env "$@" >"$STATE/$name.log" 2>&1 &
  echo $! >"$STATE/$name.pid"
}
start api DATABASE_URL="$DB_URL" TEMPORAL_ADDRESS="$STREAMCELL_E2E_TEMPORAL" \
  STREAMCELL_DEV_TOKENS="$TENANT_TOKEN:dev-tenant,$TENANT_B_TOKEN:tenant-b" "${SECRETS_ENV[@]}" LISTEN=127.0.0.1:8080 CELL_LISTEN=127.0.0.1:8081 "$ROOT/bin/streamcell-api"
start worker DATABASE_URL="$DB_URL" TEMPORAL_ADDRESS="$STREAMCELL_E2E_TEMPORAL" "${SECRETS_ENV[@]}" "$ROOT/bin/streamcell-worker"
start agent CONTROL_PLANE_URL=http://127.0.0.1:8081 CELL_TOKEN="$CELL_TOKEN" STATE_NAMESPACE=streamcell-system NODEPOOL_PROVIDER=kind \
  KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}" STATUS_EVERY=5s "${SECRETS_ENV[@]}" "$ROOT/bin/streamcell-agent"
for _ in $(seq 1 30); do curl -sf http://127.0.0.1:8080/healthz && break; sleep 1; done
echo "Local cell is up. API: http://127.0.0.1:8080 (Authorization: Bearer $TENANT_TOKEN). Logs: $STATE"
