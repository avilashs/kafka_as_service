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
  STRIMZI_CUSTOM_RESOURCE_SELECTOR='streamcell.dev/operator-shard=0' \
  STRIMZI_NETWORK_POLICY_GENERATION=false >/dev/null
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

# --- the cell's Prometheus with the alert rules ---
kubectl apply -f "$ROOT/deploy/kind/monitoring.yaml" >/dev/null 2>&1 || true   # namespace first (the rules ConfigMap lives in it)
kubectl -n streamcell-monitoring create configmap streamcell-rules $(for f in "$ROOT"/deploy/alerts/*.rules.yaml; do printf -- '--from-file=%s ' "$f"; done) \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl apply -f "$ROOT/deploy/kind/monitoring.yaml" >/dev/null
kubectl -n streamcell-monitoring rollout status deploy/prometheus --timeout=3m

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
# Health thresholds are shortened for the local cell (production defaults: 5 min window, 5 min broker down, 2 min under min ISR).
start api DATABASE_URL="$DB_URL" TEMPORAL_ADDRESS="$STREAMCELL_E2E_TEMPORAL" \
  STREAMCELL_DEV_TOKENS="$TENANT_TOKEN:dev-tenant,$TENANT_B_TOKEN:tenant-b" "${SECRETS_ENV[@]}" LISTEN=127.0.0.1:8080 CELL_LISTEN=127.0.0.1:8081 \
  HEALTH_EVERY=10s HEALTH_WINDOW=90s HEALTH_MIN_AVAILABILITY=0.95 HEALTH_BROKER_DOWN_FOR=60s HEALTH_UNDER_MIN_ISR_FOR=60s HEALTH_STALE_AFTER=2m "$ROOT/bin/streamcell-api"
start worker DATABASE_URL="$DB_URL" TEMPORAL_ADDRESS="$STREAMCELL_E2E_TEMPORAL" "${SECRETS_ENV[@]}" "$ROOT/bin/streamcell-worker"
start agent CONTROL_PLANE_URL=http://127.0.0.1:8081 CELL_TOKEN="$CELL_TOKEN" STATE_NAMESPACE=streamcell-system NODEPOOL_PROVIDER=kind \
  KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}" STATUS_EVERY=5s "${SECRETS_ENV[@]}" \
  CELL_ID=kind-1 METERING_DIR="$STATE/metering" METERING_SAMPLE_EVERY=10s METERING_INTERVAL=5m "$ROOT/bin/streamcell-agent"

# The public path from the host: Envoy's service through a port-forward, relayed to port 443 (Strimzi advertises
# the tlsroute hosts on 443). The canary and the scenarios use it; names are not resolved, connections are dialled here.
start_relay() {
  local svc
  for _ in $(seq 1 60); do
    svc=$(kubectl -n envoy-gateway-system get svc -l gateway.envoyproxy.io/owning-gateway-name=streamcell-gateway -o name 2>/dev/null | head -1)
    [[ -n "$svc" ]] && break; sleep 2
  done
  [[ -n "$svc" ]] || { echo "no Envoy data-plane service for the gateway" >&2; return 1; }
  # kubectl port-forward exits when its pod connection breaks ("lost connection to pod", seen when the observe
  # scenario cut the gateway's network path); a supervisor restarts it, otherwise the public path stays dead.
  nohup bash -c 'while true; do kubectl -n envoy-gateway-system port-forward --address 127.0.0.1 "$0" 8443:443; sleep 1; done' "$svc" >"$STATE/gw-forward.log" 2>&1 &
  echo $! >"$STATE/gw-forward.pid"
  docker rm -f streamcell-gw-relay >/dev/null 2>&1 || true
  docker run -d --name streamcell-gw-relay --network host alpine/socat tcp-listen:443,fork,reuseaddr tcp:127.0.0.1:8443 >/dev/null
}
start_relay
start canary CONTROL_PLANE_URL=http://127.0.0.1:8081 CELL_TOKEN="$CELL_TOKEN" CELL_ID=kind-1 "${SECRETS_ENV[@]}" \
  CANARY_DIAL=127.0.0.1:443 CANARY_ROUND_EVERY=2s CANARY_REPORT_EVERY=10s CANARY_WARMUP=30s CANARY_RECONNECT_EVERY=20s "$ROOT/bin/streamcell-canary"
for _ in $(seq 1 30); do curl -sf http://127.0.0.1:8080/healthz && break; sleep 1; done
echo "Local cell is up. API: http://127.0.0.1:8080 (Authorization: Bearer $TENANT_TOKEN). Logs: $STATE"
