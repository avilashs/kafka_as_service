#!/usr/bin/env bash
# Acceptance on a real cell (kind + real Strimzi + the real scheduler, admission policy, binding
# guard and auditor). Two scenarios, run in sequence (memory is the constraint, not logic):
#
#   dedicated  M2: a dedicated cluster gets exactly one node per broker; nothing else runs there;
#              violating pods are refused or detected; delete returns the nodes.
#   shared     M1: create a shared cluster, SIGKILL the workflow worker mid-create, verify no
#              duplicates and placement, delete.
#
# usage: smoke.sh [dedicated|shared|all]
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
STATE="$ROOT/.cache/kind"
API=http://127.0.0.1:8080
AUTH="Authorization: Bearer ${TENANT_TOKEN:-dev-tenant-token}"
PSQL=(docker exec streamcell-e2e-pg psql -U postgres -d streamcell -tAc)
fail() { echo "FAIL: $*" >&2; exit 1; }

op_field() { curl -sf -H "$AUTH" "$API/v1/operations/$1" | jq -r ".$2 // empty"; }
wait_op() { # op-id, timeout seconds
  local deadline=$((SECONDS + $2)) state step last=""
  while (( SECONDS < deadline )); do
    state=$(op_field "$1" state); step=$(op_field "$1" step)
    [[ "$state/$step" != "$last" ]] && echo "  $1: $state ($step)" && last="$state/$step"
    case "$state" in
      SUCCEEDED) return 0 ;;
      FAILED) curl -s -H "$AUTH" "$API/v1/operations/$1" | jq . >&2; fail "operation $1 failed" ;;
    esac
    sleep 5
  done
  fail "operation $1 did not finish in $2s"
}

create_cluster() { # json body -> "op-id cluster-id"
  local resp
  resp=$(curl -sf -X POST "$API/v1/clusters" -H "$AUTH" -H 'Content-Type: application/json' \
    -H "Idempotency-Key: smoke-$RANDOM-$(date +%s)" -d "$1") || fail "create request failed"
  echo "$(jq -r .id <<<"$resp") $(jq -r .clusterId <<<"$resp")"
}

# While a delete runs, print what is holding the namespace every 20 s. Delete time was an open
# finding from M1 (373 s) and M2 (dedicated 171 s, shared 427 s); this shows what the time is spent on.
delete_timeline() { # cluster-id start-seconds
  while true; do
    sleep 20
    echo "  [delete +$((SECONDS - $2))s] namespace: $(kubectl get ns "$1" -o json 2>/dev/null | jq -c '{phase: .status.phase, deletionTimestamp: .metadata.deletionTimestamp, finalizers: .spec.finalizers, conditions: [.status.conditions[]? | select(.status=="True") | .type + ": " + (.message // "")]}')"
    kubectl -n "$1" get pods -o json 2>/dev/null | jq -r '.items[] | "    pod " + .metadata.name + " gracePeriod=" + ((.metadata.deletionGracePeriodSeconds // "none") | tostring) + " deleting=" + ((.metadata.deletionTimestamp != null) | tostring) + " owner=" + ((.metadata.ownerReferences[0].kind) // "none")' 2>/dev/null || true
    kubectl -n "$1" get pvc,strimzipodset --no-headers 2>/dev/null | sed 's/^/    /' || true
  done
}

delete_cluster() { # cluster-id -> waits, prints how long it took
  local start=$SECONDS dop sampler
  dop=$(curl -sf -X DELETE "$API/v1/clusters/$1" -H "$AUTH" -H "Idempotency-Key: smoke-del-$1" | jq -r .id)
  delete_timeline "$1" "$start" &
  sampler=$!
  wait_op "$dop" 900
  kill "$sampler" 2>/dev/null || true
  wait "$sampler" 2>/dev/null || true
  echo "  deleted in $((SECONDS - start))s"
  ! kubectl get namespace "$1" >/dev/null 2>&1 || fail "namespace $1 still exists after delete"
}

node_label() { kubectl get node "$1" -o jsonpath="{.metadata.labels.${2//./\\.}}"; }
pods_on() { # node -> non-DaemonSet, non-terminal pods as namespace/name
  kubectl get pods -A --field-selector "spec.nodeName=$1" -o json | jq -r \
    '.items[] | select(.status.phase != "Succeeded" and .status.phase != "Failed")
       | select((.metadata.ownerReferences // []) | map(.kind) | index("DaemonSet") | not)
       | .metadata.namespace + "/" + .metadata.name'
}
broker_nodes() { kubectl -n "$1" get pods -l strimzi.io/broker-role=true -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}'; }
ctrl_nodes()   { kubectl -n "$1" get pods -l strimzi.io/controller-role=true -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}'; }

expect_refused() { # description, expected substring, command...
  local what=$1 want=$2 out; shift 2
  if out=$("$@" 2>&1); then fail "$what: was ACCEPTED, expected a refusal containing: $want"; fi
  grep -qF -- "$want" <<<"$out" || fail "$what: refused, but not by '$want'. Output: $out"
  echo "  refused as expected: $what"
}

audit_clean() { "$ROOT/bin/streamcell-audit" --surge "${1:-0}" 2>&1 || fail "placement audit reports violations"; }

dump_security_context() { # evidence for spike S-4: what Strimzi puts on a broker pod
  echo "  broker pod security evidence ($1/$1-brokers-0):"
  kubectl -n "$1" get pod "$1-brokers-0" -o json | jq -c '{
    automountServiceAccountToken: .spec.automountServiceAccountToken,
    serviceAccountName: .spec.serviceAccountName,
    podSecurityContext: .spec.securityContext,
    projectedTokenVolumes: [.spec.volumes[]? | select(.projected != null) | .name],
    containers: [(.spec.initContainers // []) + .spec.containers | .[] | {name, securityContext}]}' || true
}

check_common_placement() { # cluster-id
  local cid=$1 bn cn
  bn=$(broker_nodes "$cid"); cn=$(ctrl_nodes "$cid")
  [[ $(wc -l <<<"$bn") -eq 3 && $(sort -u <<<"$bn" | wc -l) -eq 3 ]] || fail "brokers not on 3 distinct nodes: $bn"
  [[ $(sort -u <<<"$cn" | wc -l) -eq 3 ]] || fail "controllers not on 3 distinct nodes: $cn"
  for n in $cn; do [[ "$(node_label "$n" streamcell.dev/pool)" == control ]] || fail "controller on non-control node $n"; done
  local zones; zones=$(for n in $bn; do node_label "$n" topology.kubernetes.io/zone; echo; done | sort -u | grep -c .)
  [[ "$zones" -eq 3 ]] || fail "brokers span $zones zones, want 3"
  echo "  brokers: 3 distinct nodes in 3 zones; controllers: 3 distinct control nodes"
}

# ---------------------------------------------------------------------------------------------
scenario_dedicated() {
  echo "=================== dedicated tier ==================="
  echo "== create (dedicated, 3 brokers)"
  read -r OP CID < <(create_cluster '{"name":"ded","tier":"dedicated","spec":{"kafkaVersion":"4.3.1","brokerType":"dev.ded","brokerCount":3,"storage":{"sizeGiB":1}}}')
  [[ -n "${OP:-}" && -n "${CID:-}" ]] || fail "cluster creation request failed"
  echo "  operation $OP cluster $CID"
  wait_op "$OP" 1500
  [[ "$(curl -sf -H "$AUTH" "$API/v1/clusters/$CID" | jq -r .state)" == ACTIVE ]] || fail "cluster not ACTIVE"
  kubectl -n "$CID" get pods -o wide

  echo "== rule 3: one node per broker, owned and tainted for this cluster alone"
  check_common_placement "$CID"
  local bn; bn=$(broker_nodes "$CID")
  for n in $bn; do
    [[ "$(node_label "$n" streamcell.dev/pool)" == dedicated ]] || fail "broker node $n is not in the dedicated pool"
    [[ "$(node_label "$n" streamcell.dev/dedicated-owner)" == "$CID" ]] || fail "node $n is not owned by $CID"
    kubectl get node "$n" -o json | jq -e --arg c "$CID" \
      '[.spec.taints[]? | select(.key=="streamcell.dev/dedicated" and .value==$c and .effect=="NoSchedule")] | length == 1' >/dev/null \
      || fail "node $n lacks the dedicated taint for $CID"
    kubectl get node "$n" -o json | jq -e '[.spec.taints[]? | select(.key=="streamcell.dev/spare")] | length == 0' >/dev/null \
      || fail "node $n still has the spare taint"
    local others; others=$(pods_on "$n" | grep -v "^$CID/" || true)
    [[ -z "$others" ]] || fail "node $n runs pods that are not $CID's: $others"
    [[ $(pods_on "$n" | grep -c .) -eq 1 ]] || fail "node $n should run exactly one pod (the broker): $(pods_on "$n")"
  done
  [[ $(kubectl get nodes -l "streamcell.dev/dedicated-owner=$CID" --no-headers | wc -l) -eq 3 ]] || fail "pool is not exactly 3 nodes"
  [[ $(kubectl get nodes -l streamcell.dev/pool=spare --no-headers | wc -l) -eq 0 ]] || fail "spare nodes remain although the cluster claimed 3"
  echo "  3 nodes claimed (node count == broker count); each runs exactly one pod, the broker"
  dump_security_context "$CID"

  echo "== rule 6: the audit is clean for a correct cell"
  audit_clean 0

  echo "== rule 4: other workloads cannot reach the dedicated nodes (real scheduler)"
  local node1; node1=$(head -1 <<<"$bn")
  kubectl delete pod intruder --ignore-not-found >/dev/null
  kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata: { name: intruder, namespace: default }
spec:
  nodeSelector: { streamcell.dev/dedicated-owner: "$CID" }
  containers: [{ name: c, image: registry.k8s.io/pause:3.10, imagePullPolicy: IfNotPresent }]
EOF
  sleep 20
  [[ -z "$(kubectl get pod intruder -o jsonpath='{.spec.nodeName}')" ]] || fail "intruder without a toleration was scheduled onto a dedicated node"
  kubectl get events --field-selector involvedObject.name=intruder -o json | jq -e '[.items[] | select(.reason=="FailedScheduling" and (.message | contains("untolerated taint")))] | length > 0' >/dev/null \
    || fail "intruder is Pending, but not because of the taint"
  echo "  pod without toleration: Pending, 'untolerated taint' (the taint keeps it out)"
  kubectl delete pod intruder --wait=false >/dev/null

  echo "== rule 5: the admission policy refuses bad pods in a tenant namespace"
  expect_refused "pod without placement labels" "[labels]" kubectl apply -n "$CID" -f - <<EOF
apiVersion: v1
kind: Pod
metadata: { name: nolabels }
spec: { containers: [{ name: c, image: registry.k8s.io/pause:3.10 }] }
EOF
  expect_refused "pod tolerating the dedicated taint of this cluster, claiming to be shared" "[tolerations]" kubectl apply -n "$CID" -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: sneaky
  labels: { streamcell.dev/cluster: "$CID", streamcell.dev/role: sneaky, streamcell.dev/required-pool: shared }
spec:
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms: [{ matchExpressions: [{ key: streamcell.dev/pool, operator: In, values: [shared] }] }]
  tolerations: [{ key: streamcell.dev/dedicated, operator: Equal, value: "$CID", effect: NoSchedule }]
  containers: [{ name: c, image: registry.k8s.io/pause:3.10 }]
EOF
  expect_refused "pod that sets nodeName itself" "[node-name]" kubectl apply -n "$CID" -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: direct
  labels: { streamcell.dev/cluster: "$CID", streamcell.dev/role: direct, streamcell.dev/required-pool: shared }
spec:
  nodeName: "$node1"
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms: [{ matchExpressions: [{ key: streamcell.dev/pool, operator: In, values: [shared] }] }]
  containers: [{ name: c, image: registry.k8s.io/pause:3.10 }]
EOF

  echo "== rule 5: the pods/binding guard refuses a hand-made binding onto a dedicated node"
  kubectl delete pod probe -n "$CID" --ignore-not-found >/dev/null
  kubectl apply -n "$CID" -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: probe
  labels: { streamcell.dev/cluster: "$CID", streamcell.dev/role: probe, streamcell.dev/required-pool: shared }
spec:
  schedulerName: nobody   # stays Pending: nothing schedules it, so the only binding is ours
  securityContext: { runAsNonRoot: true, seccompProfile: { type: RuntimeDefault } }
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms: [{ matchExpressions: [{ key: streamcell.dev/pool, operator: In, values: [shared] }] }]
  containers:
    - name: c
      image: registry.k8s.io/pause:3.10
      securityContext: { allowPrivilegeEscalation: false, capabilities: { drop: [ALL] } }
EOF
  expect_refused "binding a shared-pool pod to a dedicated node" "streamcell placement guard" \
    kubectl create --raw "/api/v1/namespaces/$CID/pods/probe/binding" -f - <<EOF
{"apiVersion":"v1","kind":"Binding","metadata":{"name":"probe"},"target":{"apiVersion":"v1","kind":"Node","name":"$node1"}}
EOF
  kubectl delete pod probe -n "$CID" --wait=false >/dev/null

  echo "== rule 3a: only the provisioner may change pool labels and taints"
  expect_refused "another identity re-labelling a dedicated node" "node protection" \
    kubectl --as=someone-else --as-group=system:masters label node "$node1" streamcell.dev/dedicated-owner=kc-other --overwrite
  expect_refused "another identity removing the dedicated taint" "node protection" \
    kubectl --as=someone-else --as-group=system:masters taint node "$node1" streamcell.dev/dedicated-

  echo "== rule 6: the audit finds what admission cannot see (a pod outside tenant namespaces)"
  kubectl delete pod trespasser --ignore-not-found >/dev/null
  kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata: { name: trespasser, namespace: default }
spec:
  nodeSelector: { streamcell.dev/dedicated-owner: "$CID" }
  tolerations: [{ key: streamcell.dev/dedicated, operator: Equal, value: "$CID", effect: NoSchedule }]
  containers: [{ name: c, image: registry.k8s.io/pause:3.10, imagePullPolicy: IfNotPresent }]
EOF
  for _ in $(seq 1 30); do [[ -n "$(kubectl get pod trespasser -o jsonpath='{.spec.nodeName}')" ]] && break; sleep 2; done
  [[ -n "$(kubectl get pod trespasser -o jsonpath='{.spec.nodeName}')" ]] || fail "trespasser was never scheduled (test setup problem)"
  if out=$("$ROOT/bin/streamcell-audit" --surge 0 2>&1); then fail "audit did not detect the trespasser: $out"; fi
  grep -q dedicated_node_foreign_pod <<<"$out" || fail "audit failed, but not with dedicated_node_foreign_pod: $out"
  echo "  streamcell-audit exits non-zero: dedicated_node_foreign_pod"
  local found=""
  for _ in $(seq 1 24); do # in-cluster auditor, 30s interval
    found=$(kubectl get --raw "/api/v1/namespaces/streamcell-system/services/placement-auditor:9102/proxy/metrics" 2>/dev/null \
      | grep '^streamcell_placement_violations{' | grep dedicated_node_foreign_pod || true)
    [[ -n "$found" ]] && break
    sleep 5
  done
  [[ -n "$found" ]] || fail "the in-cluster auditor never exported the violation metric"
  echo "  in-cluster auditor metric: $found"
  kubectl delete pod trespasser --wait=true >/dev/null
  audit_clean 0
  echo "  audit clean again after removing it"

  echo "== delete: nodes go back to the spare pool"
  delete_cluster "$CID"
  for _ in $(seq 1 30); do
    [[ $(kubectl get nodes -l "streamcell.dev/dedicated-owner=$CID" --no-headers 2>/dev/null | wc -l) -eq 0 ]] && break
    sleep 2
  done
  [[ $(kubectl get nodes -l "streamcell.dev/dedicated-owner=$CID" --no-headers | wc -l) -eq 0 ]] || fail "nodes still owned by $CID after delete"
  [[ $(kubectl get nodes -l streamcell.dev/pool=spare --no-headers | wc -l) -eq 3 ]] || fail "expected 3 spare nodes after delete"
  for n in $bn; do
    kubectl get node "$n" -o json | jq -e '
      ([.spec.taints[]? | select(.key=="streamcell.dev/dedicated")] | length == 0) and
      ([.spec.taints[]? | select(.key=="streamcell.dev/spare")] | length == 1) and
      (.metadata.labels["streamcell.dev/pool"] == "spare") and
      (.metadata.labels | has("streamcell.dev/dedicated-owner") | not)' >/dev/null \
      || fail "node $n not returned cleanly: $(kubectl get node "$n" -o jsonpath='{.metadata.labels} {.spec.taints}')"
  done
  [[ "$("${PSQL[@]}" "SELECT count(*) FROM reservations WHERE cluster_id='$CID' AND status<>'RELEASED'")" == 0 ]] || fail "reservations still held"
  audit_clean 0
  echo "  dedicated scenario PASS"
}

# ---------------------------------------------------------------------------------------------
scenario_shared() {
  echo "=================== shared tier ==================="
  echo "== create"
  read -r OP CID < <(create_cluster '{"name":"smoke","tier":"shared","spec":{"kafkaVersion":"4.3.1","brokerType":"dev.tiny","brokerCount":3,"storage":{"sizeGiB":1}}}')
  [[ -n "${OP:-}" && -n "${CID:-}" ]] || fail "cluster creation request failed"
  echo "  operation $OP cluster $CID"

  echo "== kill worker while waiting for Ready"
  for _ in $(seq 1 120); do [[ "$(op_field "$OP" step)" == wait-ready ]] && break; sleep 1; done
  [[ "$(op_field "$OP" step)" == wait-ready ]] || fail "never reached wait-ready"
  kill -9 "$(cat "$STATE/worker.pid")"
  sleep 5
  eval "$("$ROOT/hack/e2e-deps.sh" up)"
  nohup env DATABASE_URL="${STREAMCELL_E2E_DATABASE_URL%/postgres?sslmode=disable}/streamcell?sslmode=disable" \
    TEMPORAL_ADDRESS="$STREAMCELL_E2E_TEMPORAL" "$ROOT/bin/streamcell-worker" >"$STATE/worker2.log" 2>&1 &
  echo $! >"$STATE/worker.pid"
  wait_op "$OP" 1500

  echo "== verify"
  [[ "$(curl -sf -H "$AUTH" "$API/v1/clusters/$CID" | jq -r .state)" == ACTIVE ]] || fail "cluster not ACTIVE"
  for check in \
    "SELECT count(*) FROM reservations WHERE cluster_id='$CID'=1" \
    "SELECT count(*) FROM desired_states WHERE cluster_id='$CID'=1" \
    "SELECT count(*) FROM outbox WHERE payload->>'operationId'='$OP'=1" \
    "SELECT count(*) FROM operations WHERE cluster_id='$CID'=1"; do
    q="${check%=*}"; want="${check##*=}"; got=$("${PSQL[@]}" "$q")
    [[ "$got" == "$want" ]] || fail "$q: got $got want $want"
  done
  echo "  no duplicate rows"
  kubectl -n "$CID" get pods -o wide
  check_common_placement "$CID"
  for n in $(broker_nodes "$CID"); do [[ "$(node_label "$n" streamcell.dev/pool)" == shared ]] || fail "broker on non-shared node $n"; done
  [[ $(kubectl get nodes -l streamcell.dev/pool=spare --no-headers | wc -l) -eq 3 ]] || fail "a shared cluster must not touch the spare (dedicated) nodes"
  for n in $(kubectl get nodes -l streamcell.dev/pool=spare -o name | cut -d/ -f2); do
    [[ -z "$(pods_on "$n")" ]] || fail "workload on spare node $n: $(pods_on "$n")"
  done
  echo "  shared brokers on 3 shared nodes; spare/dedicated nodes untouched"
  dump_security_context "$CID"
  audit_clean 1
  curl -sf -H "$AUTH" "$API/v1/clusters/$CID/bootstrap-brokers" | jq .

  echo "== delete"
  delete_cluster "$CID"
  echo "  shared scenario PASS"
}

case "${1:-all}" in
  dedicated) scenario_dedicated ;;
  shared)    scenario_shared ;;
  all)       scenario_dedicated; scenario_shared ;;
  *) echo "usage: $0 [dedicated|shared|all]" >&2; exit 2 ;;
esac
echo PASS
