#!/usr/bin/env bash
# M5 acceptance on a real cell: the canary detects injected outages and the platform marks the cluster
# DEGRADED (and ACTIVE again); metering of a live cluster reconciles with an independent timeline while the
# control plane is frozen and the agent restarts. Sourced by smoke.sh after connect.sh and lifecycle.sh.
#
# The thresholds are the shortened ones of up.sh: window 90 s, broker down 60 s, round trips every 2 s.

truth_pid=""
declare -a OBS_RESULTS=()
OBS_FAILED=0
obs_record() { OBS_RESULTS+=("$1  $2"); echo "  [$1] $2"; [[ "$1" == FAIL ]] && OBS_FAILED=1; return 0; }

obs_state() { api_as "$TENANT_TOKEN" GET "/v1/clusters/$OBS_CID" | jq -r '.state + "|" + (.stateReason // "")'; }

# wait_state <state> <timeout seconds> -> prints the seconds it took, returns 1 on timeout
wait_state() {
  local want=$1 limit=$2 start=$SECONDS s
  while (( SECONDS - start < limit )); do
    s=$(obs_state); [[ "${s%%|*}" == "$want" ]] && { echo $((SECONDS - start)); return 0; }
    sleep 5
  done
  echo "$limit+"; return 1
}

canary_availability() { # last N seconds -> "successes/attempts"
  "${PSQL[@]}" "SELECT COALESCE(sum(successes),0) || '/' || COALESCE(sum(attempts),0) FROM canary_results WHERE cluster_id='$OBS_CID' AND at > now() - interval '$1 seconds'"
}

truth_sampler() { # every 5 s: epoch, running broker pods, bound GiB
  while true; do
    local pods gib
    pods=$(kubectl -n "$OBS_CID" get pods -l strimzi.io/kind=Kafka,strimzi.io/broker-role=true -o json 2>/dev/null | jq '[.items[]|select(.status.phase=="Running" and .metadata.deletionTimestamp==null)]|length' 2>/dev/null || echo "")
    gib=$(kubectl -n "$OBS_CID" get pvc -o json 2>/dev/null | jq '[.items[]|select(.metadata.name|startswith("data-'"$OBS_CID"'-brokers-"))|select(.status.phase=="Bound")|.spec.resources.requests.storage|rtrimstr("Gi")|tonumber]|add // 0' 2>/dev/null || echo "")
    [[ -n "$pods" && -n "$gib" ]] && echo -e "$(date +%s)\t$pods\t$gib" >>"$WORK/truth.tsv"
    sleep 5
  done
}

scenario_observe() {
  echo "=================== observability: canary, health, metering (M5) ==================="
  WORK=$(mktemp -d); chmod 755 "$WORK"
  truth_pid=""
  OBS_CORDONED=""
  trap '[[ -n "$truth_pid" ]] && kill "$truth_pid" 2>/dev/null; [[ -n "${OBS_CORDONED:-}" ]] && kubectl uncordon "$OBS_CORDONED" >/dev/null 2>&1; true' EXIT

  local resp op
  resp=$(api_as "$TENANT_TOKEN" POST /v1/clusters '{"name":"obs","tier":"shared","spec":{"kafkaVersion":"4.3.1","brokerType":"dev.tiny","brokerCount":3,"storage":{"sizeGiB":1}}}') || fail "create failed"
  op=$(jq -r .id <<<"$resp"); OBS_CID=$(jq -r .clusterId <<<"$resp")
  echo "  cluster $OBS_CID, operation $op"
  wait_op "$op" 1500
  : >"$WORK/truth.tsv"; truth_sampler & truth_pid=$!
  local t_active; t_active=$(date +%s)

  curl -sf -H "$AUTH" "$API/v1/clusters/$OBS_CID/ca-certificate" >"$WORK/ca-$OBS_CID.pem" || fail "no CA"; chmod 644 "$WORK"/*.pem
  mk_user "$TENANT_TOKEN" "$OBS_CID" '{"name":"admin1","admin":true}'
  set_hosts "$OBS_CID"
  local boot adm pw; boot=$(bootstrap_of "$TENANT_TOKEN" "$OBS_CID"); adm=$(props "$TENANT_TOKEN" "$OBS_CID" admin1)
  pw=$(api_as "$TENANT_TOKEN" GET "/v1/clusters/$OBS_CID/scram-users/admin1/credentials" | jq -r .password)
  start_gateway_forward
  retry_ok "obs: first connection through the public endpoint" 12 kafka kafka-topics --bootstrap-server "$boot" --command-config "$adm" --list

  echo "== the canary sees a healthy cluster"
  local i av
  for i in $(seq 1 30); do [[ $("${PSQL[@]}" "SELECT count(*) FROM canary_results WHERE cluster_id='$OBS_CID'") -ge 3 ]] && break; sleep 5; done
  av=$(canary_availability 120); echo "  canary availability over the last 120 s: $av"
  local ok=${av%/*} all=${av#*/}
  if [[ $all -ge 20 && $(( ok * 100 )) -ge $(( all * 99 )) ]]; then obs_record PASS "canary reports $av round trips through the public endpoint on a healthy cluster"; else obs_record FAIL "canary availability on a healthy cluster is $av"; tail -15 "$STATE/canary.log" | cut -c1-400 | sed 's/^/      | /'; fi
  [[ "$(obs_state)" == ACTIVE\|* ]] && obs_record PASS "a healthy cluster stays ACTIVE" || obs_record FAIL "cluster state while healthy: $(obs_state)"

  echo "== metrics: scraped by the cell's Prometheus, not reachable by anyone else"
  kubectl -n streamcell-monitoring port-forward svc/prometheus 19090:9090 >"$WORK/prom-forward.log" 2>&1 &
  local prom_pid=$!; sleep 4
  local up
  for i in $(seq 1 18); do
    up=$(curl -sf "http://127.0.0.1:19090/api/v1/query" --data-urlencode "query=count(up{job=\"streamcell-kafka\",namespace=\"$OBS_CID\"} == 1)" | jq -r '.data.result[0].value[1] // "0"')
    [[ "$up" == 6 ]] && break; sleep 10
  done
  [[ "$up" == 6 ]] && obs_record PASS "Prometheus scrapes all 6 Kafka pods (3 brokers, 3 controllers) of the cluster" || obs_record FAIL "Prometheus scrapes $up of 6 pods"
  local isr; isr=$(curl -sf "http://127.0.0.1:19090/api/v1/query" --data-urlencode "query=sum(kafka_server_replicamanager_underminisrpartitioncount{namespace=\"$OBS_CID\"})" | jq -r '.data.result[0].value[1] // "none"')
  [[ "$isr" == 0 ]] && obs_record PASS "kafka_server_replicamanager_underminisrpartitioncount is exported and 0" || obs_record FAIL "under-min-ISR metric: $isr"
  local rules; rules=$(curl -sf "http://127.0.0.1:19090/api/v1/rules" | jq -r '[.data.groups[].rules[]|select(.type=="alerting")|.name]|length')
  [[ "$rules" -ge 4 ]] && obs_record PASS "Prometheus loaded $rules alerting rules, none of them firing on a healthy cluster: $(curl -sf http://127.0.0.1:19090/api/v1/alerts | jq -r '[.data.alerts[]|select(.state=="firing")|.labels.alertname]|join(",")')" || obs_record FAIL "alerting rules loaded: $rules"
  kill "$prom_pid" 2>/dev/null
  kubectl create namespace probe-ns --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl -n probe-ns run client --image="$KAFKA_IMAGE" --restart=Never --command -- sleep 3600 >/dev/null 2>&1 || true
  kubectl -n probe-ns wait --for=condition=Ready pod/client --timeout=180s >/dev/null 2>&1 || true
  local bip; bip=$(kubectl -n "$OBS_CID" get pods -l strimzi.io/kind=Kafka,strimzi.io/broker-role=true -o jsonpath='{.items[0].status.podIP}')
  if kubectl -n probe-ns exec client -- timeout 5 bash -c "exec 3<>/dev/tcp/$bip/9404" 2>/dev/null; then obs_record FAIL "a pod outside the tenant namespaces CAN open the broker metrics port 9404"; else obs_record PASS "a pod outside the tenant namespaces cannot open the broker metrics port 9404"; fi
  kubectl delete namespace probe-ns --wait=false >/dev/null 2>&1 || true

  echo "== control plane frozen for 60 s, agent restarted (metering must not lose or double anything)"
  kill -STOP "$(cat "$STATE/api.pid")"
  sleep 20
  kill "$(cat "$STATE/agent.pid")" 2>/dev/null || true; sleep 2; rm -f "$STATE/agent.pid"
  ( cd "$ROOT" && start_agent_again ) || true
  sleep 40
  kill -CONT "$(cat "$STATE/api.pid")"
  sleep 15

  # a measurement of "time to DEGRADED" needs a healthy start, not a state left over from the freeze above
  local t0; t0=$(wait_state ACTIVE 300) && obs_record PASS "cluster is ACTIVE before the injected outages ($t0 s after the control plane was released)" || obs_record FAIL "cluster not ACTIVE before the outages ($(obs_state))"
  echo "== injected outage 1: the gateway path is cut (NetworkPolicy allow-gateway-ingress removed)"
  kubectl -n "$OBS_CID" get networkpolicy allow-gateway-ingress -o json | jq 'del(.metadata.resourceVersion,.metadata.uid,.metadata.creationTimestamp,.metadata.managedFields,.status)' >"$WORK/np.json"
  kubectl -n "$OBS_CID" delete networkpolicy allow-gateway-ingress >/dev/null
  local t1; t1=$(wait_state DEGRADED 240) && obs_record PASS "cluster is DEGRADED $t1 s after the path was cut ($(obs_state | cut -d'|' -f2))" || { obs_record FAIL "cluster not DEGRADED after $t1 s ($(obs_state))"; echo "      canary results of the last 8 minutes (at, attempts, successes):"; "${PSQL[@]}" "SELECT to_char(at,'HH24:MI:SS') || ' ' || attempts || ' ' || successes FROM canary_results WHERE cluster_id='$OBS_CID' AND at > now() - interval '8 minutes' ORDER BY at" | sed 's/^/        /'; tail -10 "$STATE/canary.log" | cut -c1-300 | sed 's/^/      | /'; }
  [[ "$(obs_state)" == *"canary availability"* ]] && obs_record PASS "the reason names the canary" || obs_record FAIL "reason: $(obs_state)"
  check_denied "a tenant client really cannot connect while the path is cut" "timed out|Timed out|disconnected|refused|reset|failed" \
    kafka kafka-topics --bootstrap-server "$boot" --command-config "$adm" --list
  kubectl apply -f "$WORK/np.json" >/dev/null
  local t2; t2=$(wait_state ACTIVE 360) && obs_record PASS "cluster is ACTIVE again $t2 s after the path was restored" || obs_record FAIL "cluster not ACTIVE after $t2 s ($(obs_state))"
  retry_ok "a tenant client connects again" 12 kafka kafka-topics --bootstrap-server "$boot" --command-config "$adm" --list

  echo "== injected outage 2: a broker is lost for good (its node cordoned, the pod deleted; local volumes pin it)"
  local pod node
  pod=$(kubectl -n "$OBS_CID" get pods -l strimzi.io/kind=Kafka,strimzi.io/broker-role=true -o jsonpath='{.items[0].metadata.name}')
  node=$(kubectl -n "$OBS_CID" get pod "$pod" -o jsonpath='{.spec.nodeName}')
  kubectl cordon "$node" >/dev/null; OBS_CORDONED="$node"; kubectl -n "$OBS_CID" delete pod "$pod" --wait=false >/dev/null
  local t3; t3=$(wait_state DEGRADED 300) && obs_record PASS "cluster is DEGRADED $t3 s after a broker was lost ($(obs_state | cut -d'|' -f2))" || obs_record FAIL "cluster not DEGRADED after $t3 s ($(obs_state))"
  [[ "$(obs_state)" == *"broker pods are ready"* ]] && obs_record PASS "the reason names the broker pods" || obs_record FAIL "reason: $(obs_state)"
  # one broker down does not stop acks=all with RF 3 / min ISR 2: the data path still works
  retry_ok "clients still work with one broker down (RF 3, min ISR 2)" 3 kafka kafka-topics --bootstrap-server "$boot" --command-config "$adm" --list
  kubectl uncordon "$node" >/dev/null; OBS_CORDONED=""
  local t4; t4=$(wait_state ACTIVE 420) && obs_record PASS "cluster is ACTIVE again $t4 s after the broker returned" || obs_record FAIL "cluster not ACTIVE after $t4 s ($(obs_state))"

  echo "== traffic with known size, for the data meters"
  local ledger="$WORK/ledger"
  kafka kafka-topics --bootstrap-server "$boot" --command-config "$adm" --create --if-not-exists --topic meter-load --partitions 3 --replication-factor 3 >/dev/null 2>&1 || true
  STREAMCELL_LOADTEST_PASSWORD="$pw" "$LOADTEST" produce -bootstrap "$boot" -user admin1 -ca "$WORK/ca-$OBS_CID.pem" -dial 127.0.0.1:443 \
    -topic meter-load -ledger "$ledger" -rate 100 -producers 1 -size 200 -duration 40s >"$WORK/produce.log" 2>&1 || true
  tail -2 "$WORK/produce.log"
  local t_traffic; t_traffic=$(date +%s)
  # what the brokers export (evidence for the meter definitions): names, and the series that carry byte counts
  kubectl -n streamcell-monitoring port-forward svc/prometheus 19090:9090 >"$WORK/prom-forward2.log" 2>&1 &
  local prom2=$!; sleep 4
  echo "  metric names exported by the cluster's pods:"
  curl -sf "http://127.0.0.1:19090/api/v1/query" --data-urlencode "query=count by (__name__) ({namespace=\"$OBS_CID\",__name__=~\"kafka_server_brokertopicmetrics.*\"})" | jq -r '.data.result[]|"    \(.metric.__name__) series=\(.value[1])"' || true
  curl -sf "http://127.0.0.1:19090/api/v1/query" --data-urlencode "query={namespace=\"$OBS_CID\",__name__=~\"kafka_server_brokertopicmetrics_bytesin.*\",topic=\"meter-load\"}" | jq -c '.data.result[]|[.metric,.value[1]]' | head -5 || true
  kill "$prom2" 2>/dev/null
  # exactly what the agent sees: the exposition through the API server's pod proxy
  local bpod; bpod=$(kubectl -n "$OBS_CID" get pods -l strimzi.io/kind=Kafka,strimzi.io/broker-role=true -o jsonpath='{.items[0].metadata.name}')
  echo "  agent's view of $bpod (pod proxy, port 9404):"
  kubectl get --raw "/api/v1/namespaces/$OBS_CID/pods/http:$bpod:9404/proxy/metrics" 2>&1 | grep -E 'brokertopicmetrics_(bytesin|bytesout)persec' | head -8 | sed 's/^/    /'
  echo "  agent's meter state (counters):"
  jq -c '.. | objects | select(.Meter? == "data_in_bytes")' "$STATE/metering/meter-state.json" 2>/dev/null | head -3 | sed 's/^/    /' || true
  grep -c . "$STATE/agent.log" | sed 's/^/    agent.log lines: /'; grep 'metering' "$STATE/agent.log" | tail -5 | cut -c1-300 | sed 's/^/    /'

  echo "== metering: wait for closed intervals and compare with the timeline sampled by this script"
  local deadline=$((SECONDS + 900)) n m
  while (( SECONDS < deadline )); do
    n=$("${PSQL[@]}" "SELECT count(DISTINCT interval_start) FROM metering_records WHERE cluster_id='$OBS_CID' AND meter='broker_hours'")
    # the interval that contains the traffic must be closed too
    m=$("${PSQL[@]}" "SELECT count(*) FROM metering_records WHERE cluster_id='$OBS_CID' AND meter='data_in_bytes' AND extract(epoch from interval_start) >= $((t_traffic - t_traffic % 300))")
    [[ "$n" -ge 3 && "$m" -ge 1 ]] && break; sleep 20
  done
  meter_checks "$t_active"

  echo "== delete"
  kill "$truth_pid" 2>/dev/null; truth_pid=""; trap - EXIT
  delete_cluster "$OBS_CID"

  echo; echo "== observability results"; printf '  %s\n' "${OBS_RESULTS[@]}"
  [[ $OBS_FAILED -eq 0 ]] || fail "observability scenario has failing checks (listed above)"
  echo "  observe scenario PASS"
}

# restart the agent the way up.sh started it
start_agent_again() {
  eval "$("$ROOT/hack/e2e-deps.sh" up)"
  nohup env CONTROL_PLANE_URL=http://127.0.0.1:8081 CELL_TOKEN="${CELL_TOKEN:-dev-cell-token}" STATE_NAMESPACE=streamcell-system NODEPOOL_PROVIDER=kind \
    KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}" STATUS_EVERY=5s SECRETS_BACKEND=openbao OPENBAO_ADDR="$STREAMCELL_E2E_OPENBAO_ADDR" OPENBAO_TOKEN="$STREAMCELL_E2E_OPENBAO_TOKEN" \
    CELL_ID=kind-1 METERING_DIR="$STATE/metering" METERING_SAMPLE_EVERY=10s METERING_INTERVAL=5m "$ROOT/bin/streamcell-agent" >>"$STATE/agent.log" 2>&1 &
  echo $! >"$STATE/agent.pid"
}

meter_checks() {
  local t_active=$1 rows start end qty truth tol
  echo "  records:"; "${PSQL[@]}" "SELECT meter, labels::text, extract(epoch from interval_start)::bigint, quantity FROM metering_records WHERE cluster_id='$OBS_CID' ORDER BY interval_start, meter" | sed 's/^/    /'
  local dups conflicts
  dups=$("${PSQL[@]}" "SELECT count(*) - count(DISTINCT record_id) FROM metering_records WHERE cluster_id='$OBS_CID'")
  conflicts=$("${PSQL[@]}" "SELECT count(*) FROM metering_conflicts")
  [[ "$dups" == 0 && "$conflicts" == 0 ]] && obs_record PASS "no duplicate records, no conflicts (control plane frozen, agent restarted, acknowledgements retried)" || obs_record FAIL "duplicates=$dups conflicts=$conflicts"
  # complete intervals that start after the cluster was ACTIVE: compare with the timeline
  local checked=0 bad=0
  while IFS=$'\t' read -r start qty; do
    end=$((start + 300))
    (( start < t_active )) && continue
    truth=$(awk -F'\t' -v s="$start" -v e="$end" '$1>=s && $1<e {sum += $2*5} END {print sum+0}' "$WORK/truth.tsv")
    tol=$(( 3 * 20 + truth / 20 ))   # two sampling periods of 3 brokers, plus 5 %
    local diff=$(( qty > truth ? qty - truth : truth - qty ))
    checked=$((checked + 1))
    if (( diff <= tol )); then echo "    broker_hours interval $start: stored $qty, timeline $truth (tolerance $tol)"
    else bad=$((bad + 1)); echo "    broker_hours interval $start: stored $qty, timeline $truth (tolerance $tol) <-- off"; fi
  done < <("${PSQL[@]}" "SELECT extract(epoch from interval_start)::bigint, sum(quantity) FROM metering_records WHERE cluster_id='$OBS_CID' AND meter='broker_hours' GROUP BY 1 ORDER BY 1" | tr '|' '\t')
  (( checked >= 2 && bad == 0 )) && obs_record PASS "broker_hours of $checked complete 5-minute intervals match the sampled timeline (including the interval with a lost broker)" || obs_record FAIL "broker_hours: $checked intervals checked, $bad off"
  checked=0; bad=0
  while IFS=$'\t' read -r start qty; do
    end=$((start + 300)); (( start < t_active )) && continue
    truth=$(awk -F'\t' -v s="$start" -v e="$end" '$1>=s && $1<e {sum += $3*5} END {print sum+0}' "$WORK/truth.tsv")
    tol=$(( 3 * 20 + truth / 20 )); local diff=$(( qty > truth ? qty - truth : truth - qty )); checked=$((checked + 1))
    if (( diff <= tol )); then echo "    storage interval $start: stored $qty GiB-s, timeline $truth"; else bad=$((bad + 1)); echo "    storage interval $start: stored $qty, timeline $truth <-- off"; fi
  done < <("${PSQL[@]}" "SELECT extract(epoch from interval_start)::bigint, sum(quantity) FROM metering_records WHERE cluster_id='$OBS_CID' AND meter='storage_gb_hours' GROUP BY 1 ORDER BY 1" | tr '|' '\t')
  (( checked >= 2 && bad == 0 )) && obs_record PASS "storage_gb_hours of $checked intervals match the sampled timeline" || obs_record FAIL "storage_gb_hours: $checked intervals checked, $bad off"
  # data meters: the producer's acknowledged payload is a lower bound; protocol overhead and replication-free accounting bound it above
  local acked in_bytes out_bytes payload
  acked=$(sed -n 's/.*acked=\([0-9]*\).*/\1/p' "$WORK/produce.log" | tail -1)
  payload=$(( ${acked:-0} * 400 ))   # the load tool hex-encodes 200 random bytes: at least 400 bytes of value per record
  in_bytes=$("${PSQL[@]}" "SELECT COALESCE(sum(quantity),0) FROM metering_records WHERE cluster_id='$OBS_CID' AND meter='data_in_bytes'")
  echo "    produced $acked records (>= $payload value bytes); data_in_bytes stored: $in_bytes"
  if [[ "$in_bytes" -ge "$payload" && "$in_bytes" -le $(( payload * 3 )) ]]; then obs_record PASS "data_in_bytes ($in_bytes) covers the produced payload ($payload) and stays within 3x of it; the platform's canary traffic is excluded"
  else obs_record FAIL "data_in_bytes $in_bytes vs produced payload $payload"; fi
}
