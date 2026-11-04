#!/usr/bin/env bash
# M1 acceptance on a real cell (kind + real Strimzi): create a shared cluster via the
# API, SIGKILL the workflow worker while it waits for Ready, restart it, verify the
# create completes with no duplicate side effects and correct placement, then delete.
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

echo "== create"
resp=$(curl -sf -X POST "$API/v1/clusters" -H "$AUTH" -H 'Content-Type: application/json' -H "Idempotency-Key: smoke-$(date +%s)" \
  -d '{"name":"smoke","tier":"shared","spec":{"kafkaVersion":"4.3.1","brokerType":"dev.tiny","brokerCount":3,"storage":{"sizeGiB":1}}}')
OP=$(jq -r .id <<<"$resp"); CID=$(jq -r .clusterId <<<"$resp")
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
wait_op "$OP" 1200

echo "== verify"
state=$(curl -sf -H "$AUTH" "$API/v1/clusters/$CID" | jq -r .state)
[[ "$state" == ACTIVE ]] || fail "cluster state $state"
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
brokers=$(kubectl -n "$CID" get pods -l strimzi.io/broker-role=true -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}')
ctrls=$(kubectl -n "$CID" get pods -l strimzi.io/controller-role=true -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}')
[[ $(wc -l <<<"$brokers") -eq 3 && $(sort -u <<<"$brokers" | wc -l) -eq 3 ]] || fail "brokers not on 3 distinct nodes: $brokers"
[[ $(sort -u <<<"$ctrls" | wc -l) -eq 3 ]] || fail "controllers not on 3 distinct nodes: $ctrls"
for n in $brokers; do [[ "$(kubectl get node "$n" -o jsonpath='{.metadata.labels.streamcell\.dev/pool}')" == shared ]] || fail "broker on non-shared node $n"; done
for n in $ctrls; do [[ "$(kubectl get node "$n" -o jsonpath='{.metadata.labels.streamcell\.dev/pool}')" == control ]] || fail "controller on non-control node $n"; done
zones=$(for n in $brokers; do kubectl get node "$n" -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}{"\n"}'; done | sort -u | wc -l)
[[ "$zones" -eq 3 ]] || fail "brokers span $zones zones"
echo "  placement ok: brokers on 3 shared nodes in 3 zones, controllers on 3 control nodes"
curl -sf -H "$AUTH" "$API/v1/clusters/$CID/bootstrap-brokers" | jq .

echo "== delete"
DOP=$(curl -sf -X DELETE "$API/v1/clusters/$CID" -H "$AUTH" -H "Idempotency-Key: smoke-del-$CID" | jq -r .id)
wait_op "$DOP" 900
kubectl get namespace "$CID" >/dev/null 2>&1 && fail "namespace $CID still exists"
echo "PASS"
