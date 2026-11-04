#!/usr/bin/env bash
# M4 acceptance on a real cell: every lifecycle operation runs while producers write with acks=all and
# idempotence through the public endpoint; afterwards a verifier reads the whole topic and proves that every
# acknowledged record is present, intact and not duplicated (design §16). Sourced by smoke.sh after connect.sh.
#
# Operations: config change, resize (rolling restart), version upgrade (rolling restart), scale out, scale in.
# Not run here: storage expansion (kind's local-path StorageClass cannot resize volumes; tested against the
# API and the cell objects in envtest only) and node rotation.

LOADTEST="$ROOT/bin/streamcell-loadtest"
LOAD_PID=""
declare -a LIFE_RESULTS=()
LIFE_FAILED=0

life_record() { # PASS|FAIL description
  LIFE_RESULTS+=("$1  $2"); echo "  [$1] $2"
  [[ "$1" == FAIL ]] && LIFE_FAILED=1
  return 0
}

stop_load() {
  if [[ -n "$LOAD_PID" ]] && kill -0 "$LOAD_PID" 2>/dev/null; then kill "$LOAD_PID" 2>/dev/null || true; fi
  LOAD_PID=""
}

# start_load <n>: producers write until $WORK/stop-<n> exists
start_load() {
  rm -f "$WORK/stop-$1" "$WORK/ledger-$1"
  STREAMCELL_LOADTEST_PASSWORD="$LIFE_PW" "$LOADTEST" produce -bootstrap "$LIFE_BOOT" -user admin1 -ca "$WORK/ca-$LIFE_CID.pem" \
    -dial 127.0.0.1:443 -topic load -ledger "$WORK/ledger-$1" -rate 150 -producers 2 -size 128 -stop-file "$WORK/stop-$1" \
    >"$WORK/produce-$1.log" 2>&1 &
  LOAD_PID=$!
}

# finish_load <n> <description>: stop the producers, verify the ledger against the topic
finish_load() {
  touch "$WORK/stop-$1"
  wait "$LOAD_PID" 2>/dev/null || true
  LOAD_PID=""
  local produced verdict
  produced=$(tail -1 "$WORK/produce-$1.log")
  verdict=$(STREAMCELL_LOADTEST_PASSWORD="$LIFE_PW" "$LOADTEST" verify -bootstrap "$LIFE_BOOT" -user admin1 -ca "$WORK/ca-$LIFE_CID.pem" \
    -dial 127.0.0.1:443 -topic load -ledger "$WORK/ledger-$1" 2>&1)
  local rc=$?
  echo "    $produced"
  echo "    verify: $(head -1 <<<"$verdict")"
  if [[ $rc -eq 0 ]] && grep -q 'lost=0 duplicated=0 corrupt=0 reordered=0' <<<"$verdict" && grep -qE 'acked=[1-9][0-9]*' <<<"$verdict"; then
    life_record PASS "$2: 0 lost, 0 duplicated, 0 corrupt, 0 reordered ($(head -1 <<<"$verdict" | sed 's/ (present.*//'); $produced)"
  else
    life_record FAIL "$2: $(head -1 <<<"$verdict")"; sed 's/^/      | /' <<<"$verdict" | head -15
  fi
}

# run_op <n> <description> <patch json> [extra check function]
run_op() {
  local n=$1 what=$2 patch=$3 check=${4:-} op started
  echo "== $what"
  start_load "$n"
  sleep 15   # steady state before the change
  started=$SECONDS
  op=$(api_as "$TENANT_TOKEN" PATCH "/v1/clusters/$LIFE_CID" "$patch" | jq -r .id)
  [[ -n "$op" && "$op" != null ]] || { life_record FAIL "$what: the PATCH was refused"; stop_load; return; }
  with_token "$TENANT_TOKEN" wait_op "$op" 1500
  echo "    operation took $((SECONDS - started))s"
  sleep 15   # the change is done; keep writing for a while
  [[ -n "$check" ]] && $check
  finish_load "$n" "$what"
  local st; st=$(api_as "$TENANT_TOKEN" GET "/v1/clusters/$LIFE_CID" | jq -r .state)
  [[ "$st" == ACTIVE ]] && life_record PASS "$what: cluster is ACTIVE afterwards" || life_record FAIL "$what: cluster state is $st"
  local urp; urp=$(kafka kafka-topics --bootstrap-server "$LIFE_BOOT" --command-config "$LIFE_ADM" --describe --under-replicated-partitions 2>/dev/null | grep -c Topic: || true)
  [[ "$urp" == 0 ]] && life_record PASS "$what: no under-replicated partitions afterwards" || life_record FAIL "$what: $urp under-replicated partitions afterwards"
}

check_config() {
  local v; v=$(kubectl -n "$LIFE_CID" get kafka "$LIFE_CID" -o jsonpath='{.spec.kafka.config.log\.retention\.hours}')
  [[ "$v" == 48 ]] && life_record PASS "config: log.retention.hours=48 is in the cell" || life_record FAIL "config: log.retention.hours in the cell is '$v'"
  local live; live=$(kafka kafka-configs --bootstrap-server "$LIFE_BOOT" --command-config "$LIFE_ADM" --describe --entity-type brokers --entity-name 3 --all 2>&1 | grep -E 'log.retention.hours' | head -1)
  echo "    broker config as seen through the Admin API: ${live:-<not reported>}"
}
check_size() {
  local lim; lim=$(kubectl -n "$LIFE_CID" get pods -l strimzi.io/broker-role=true -o jsonpath='{range .items[*]}{.spec.containers[0].resources.limits.memory}{"\n"}{end}' | sort -u | tr '\n' ' ')
  [[ "$lim" == "1Gi " || "$lim" == "1024Mi " ]] && life_record PASS "resize: every broker pod has the new memory limit ($lim)" || life_record FAIL "resize: broker memory limits are '$lim'"
}
check_version() {
  local imgs v; imgs=$(kubectl -n "$LIFE_CID" get pods -l strimzi.io/broker-role=true -o jsonpath='{range .items[*]}{.spec.containers[0].image}{"\n"}{end}' | sort -u | tr '\n' ' ')
  v=$(kubectl -n "$LIFE_CID" get kafka "$LIFE_CID" -o jsonpath='{.status.kafkaVersion}')
  [[ "$imgs" == "streamcell/kafka:4.3.1-dev " && "$v" == 4.3.1 ]] && life_record PASS "version: every broker runs 4.3.1 (status.kafkaVersion=$v)" || life_record FAIL "version: images '$imgs', status.kafkaVersion '$v'"
}
broker_ids_in_use() { kafka kafka-topics --bootstrap-server "$LIFE_BOOT" --command-config "$LIFE_ADM" --describe --topic load 2>/dev/null | grep -o 'Replicas: [0-9,]*' | sed 's/Replicas: //' | tr ',' '\n' | sort -un | wc -l; }
check_scale_out() {
  local pods nodes used
  pods=$(kubectl -n "$LIFE_CID" get pods -l strimzi.io/broker-role=true --no-headers | grep -c Running)
  nodes=$(broker_nodes "$LIFE_CID" | sort -u | wc -l)
  used=$(broker_ids_in_use)
  [[ "$pods" == 6 && "$nodes" == 6 ]] && life_record PASS "scale out: 6 broker pods on 6 distinct nodes" || life_record FAIL "scale out: $pods running broker pods on $nodes nodes"
  [[ "$used" -gt 3 ]] && life_record PASS "scale out: partitions were moved onto the new brokers ($used brokers hold replicas of the load topic)" || life_record FAIL "scale out: only $used brokers hold replicas"
}
check_scale_in() {
  local pods used
  pods=$(kubectl -n "$LIFE_CID" get pods -l strimzi.io/broker-role=true --no-headers | wc -l)
  used=$(broker_ids_in_use)
  [[ "$pods" == 3 && "$used" == 3 ]] && life_record PASS "scale in: 3 broker pods, replicas of the load topic on 3 brokers" || life_record FAIL "scale in: $pods broker pods, $used brokers hold replicas"
}

scenario_lifecycle() {
  echo "=================== lifecycle operations under acks=all load (M4) ==================="
  WORK=$(mktemp -d); chmod 755 "$WORK"
  trap 'stop_load; stop_gateway_forward' EXIT
  [[ -x "$LOADTEST" ]] || fail "$LOADTEST is missing (make build)"

  local resp op
  resp=$(api_as "$TENANT_TOKEN" POST /v1/clusters '{"name":"life","tier":"shared","spec":{"kafkaVersion":"4.3.0","brokerType":"dev.tiny","brokerCount":3,"storage":{"sizeGiB":1}}}') || fail "create failed"
  op=$(jq -r .id <<<"$resp"); LIFE_CID=$(jq -r .clusterId <<<"$resp")
  echo "  cluster $LIFE_CID (Kafka 4.3.0, dev.tiny, 3 brokers), operation $op"
  wait_op "$op" 1500
  curl -sf -H "$AUTH" "$API/v1/clusters/$LIFE_CID/ca-certificate" >"$WORK/ca-$LIFE_CID.pem" || fail "no CA"
  chmod 644 "$WORK"/*.pem
  mk_user "$TENANT_TOKEN" "$LIFE_CID" '{"name":"admin1","admin":true}'
  set_hosts "$LIFE_CID"
  LIFE_BOOT=$(bootstrap_of "$TENANT_TOKEN" "$LIFE_CID")
  LIFE_ADM=$(props "$TENANT_TOKEN" "$LIFE_CID" admin1)
  LIFE_PW=$(api_as "$TENANT_TOKEN" GET "/v1/clusters/$LIFE_CID/scram-users/admin1/credentials" | jq -r .password)
  start_gateway_forward
  retry_ok "life: first connection through the public endpoint" 12 kafka kafka-topics --bootstrap-server "$LIFE_BOOT" --command-config "$LIFE_ADM" --list
  kafka kafka-topics --bootstrap-server "$LIFE_BOOT" --command-config "$LIFE_ADM" --create --topic load --partitions 6 --replication-factor 3 \
    --config min.insync.replicas=2 || fail "cannot create the load topic"

  # a baseline run: the verifier must pass when nothing is happening, or later results mean nothing
  echo "== baseline (no operation)"
  start_load 0; sleep 20; finish_load 0 "baseline"

  run_op 1 "config change (retention)"  '{"spec":{"config":{"log.retention.hours":"48"}}}' check_config
  run_op 2 "resize dev.tiny -> dev.small (rolling restart)" '{"spec":{"brokerType":"dev.small"}}' check_size
  run_op 3 "version upgrade 4.3.0 -> 4.3.1 (rolling restart)" '{"spec":{"kafkaVersion":"4.3.1"}}' check_version
  run_op 4 "scale out 3 -> 6 brokers" '{"spec":{"brokerCount":6}}' check_scale_out
  run_op 5 "scale in 6 -> 3 brokers" '{"spec":{"brokerCount":3}}' check_scale_in

  echo "== refusals"
  local code
  code=$(curl -s -o /dev/null -w '%{http_code}' -X PATCH "$API/v1/clusters/$LIFE_CID" -H "$AUTH" -H 'Content-Type: application/json' -H "Idempotency-Key: r-$RANDOM" -d '{"spec":{"kafkaVersion":"4.3.0"}}')
  [[ "$code" == 422 ]] && life_record PASS "downgrade is refused (422)" || life_record FAIL "downgrade returned $code"
  code=$(curl -s -o /dev/null -w '%{http_code}' -X PATCH "$API/v1/clusters/$LIFE_CID" -H "$AUTH" -H 'Content-Type: application/json' -H "Idempotency-Key: r-$RANDOM" -d '{"spec":{"storage":{"sizeGiB":0}}}')
  [[ "$code" == 422 ]] && life_record PASS "storage shrink is refused (422)" || life_record FAIL "storage shrink returned $code"

  echo "== delete"
  stop_gateway_forward; trap - EXIT
  delete_cluster "$LIFE_CID"

  echo; echo "== lifecycle results"; printf '  %s\n' "${LIFE_RESULTS[@]}"
  [[ $LIFE_FAILED -eq 0 ]] || fail "lifecycle scenario has failing checks (listed above)"
  echo "  lifecycle scenario PASS"
}
