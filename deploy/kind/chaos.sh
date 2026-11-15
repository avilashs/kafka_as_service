#!/usr/bin/env bash
# M6 fault injection on a real cell (kind, real Strimzi): each fault is injected while producers write with
# acks=all and idempotence through the public endpoint; a verifier then proves that every acknowledged record is
# present, intact and not duplicated, and the script measures how long the cell took to recover. Sourced by
# smoke.sh after lifecycle.sh (it uses its load helpers).
#
# These are EMULATIONS of the AWS tests the brief asks for, they are not the AWS tests:
#   broker kill            -> same fault (SIGKILL of the broker pod through the API server)
#   node termination       -> the node's container is stopped (kind volumes are local to the node: the broker cannot
#                             be rescheduled, which on AWS the EBS volume allows: that part is NOT exercised)
#   zone outage            -> every node of one zone is stopped (no NACLs: nothing is dropped on the network, the
#                             machines just stop; the gateway is a single Envoy here, so the zone without it is chosen)
#   disk full              -> the broker's data volume is filled by a file; same fault, 1 GiB volume
#   control plane down     -> API, worker, Temporal and Postgres are frozen while a broker is killed
# Not run here: the noisy-neighbour benchmark and dedicated versus shared (they need real instances).

CH_DATA=""    # the broker data directory as mounted in the pod (data or data-<id>, found at run time)
CH_FILLER=""

ch_cid() { echo "$LIFE_CID"; }
cluster_state() { api_as "$TENANT_TOKEN" GET "/v1/clusters/$LIFE_CID" 2>/dev/null | jq -r '.state + "|" + (.stateReason // "")' 2>/dev/null || echo "unreachable|"; }

brokers_ready() { # prints the number of broker pods that are Ready and not terminating
  kubectl -n "$LIFE_CID" get pods -l strimzi.io/broker-role=true -o json 2>/dev/null |
    jq '[.items[]|select(.metadata.deletionTimestamp==null)|select(.status.conditions // [] | map(select(.type=="Ready" and .status=="True")) | length > 0)]|length' 2>/dev/null || echo 0
}

wait_brokers_ready() { # want, timeout seconds -> prints seconds taken, returns 1 on timeout
  local want=$1 limit=$2 start=$SECONDS
  while (( SECONDS - start < limit )); do
    [[ "$(brokers_ready)" == "$want" ]] && { echo $((SECONDS - start)); return 0; }
    sleep 3
  done
  echo "$limit+"; return 1
}

wait_urp_zero() { # timeout seconds -> prints seconds taken
  local limit=$1 start=$SECONDS urp=-1
  while (( SECONDS - start < limit )); do
    urp=$(kafka kafka-topics --bootstrap-server "$LIFE_BOOT" --command-config "$LIFE_ADM" --describe --under-replicated-partitions 2>/dev/null | grep -c Topic: || true)
    [[ "$urp" == 0 ]] && { echo $((SECONDS - start)); return 0; }
    sleep 8
  done
  echo "$limit+ ($urp left)"; return 1
}

wait_cluster_state() { # state, timeout -> prints seconds
  local want=$1 limit=$2 start=$SECONDS s
  while (( SECONDS - start < limit )); do
    s=$(cluster_state); [[ "${s%%|*}" == "$want" ]] && { echo $((SECONDS - start)); return 0; }
    sleep 5
  done
  echo "$limit+"; return 1
}

# envoy_node / envoy_zone: where the (single) gateway proxy runs in this cell
envoy_node() { kubectl -n envoy-gateway-system get pods -l gateway.envoyproxy.io/owning-gateway-name=streamcell-gateway -o jsonpath='{.items[0].spec.nodeName}'; }
zone_of() { node_label "$1" topology.kubernetes.io/zone; }

# run_fault <n> <description> <inject fn> <recover fn> <expect DEGRADED yes|no>
run_fault() {
  local n=$1 what=$2 inject=$3 recover=$4 degraded=${5:-no} t_inject t_rec t_urp t_active
  echo "== $what"
  start_load "$n"
  sleep 20                                  # steady state
  t_inject=$SECONDS
  $inject
  if [[ "$degraded" == yes ]]; then
    local td; td=$(wait_cluster_state DEGRADED 240) && life_record PASS "$what: cluster is DEGRADED $td s after the fault ($(cluster_state | cut -d'|' -f2))" || life_record FAIL "$what: never DEGRADED (state $(cluster_state))"
  fi
  $recover
  t_rec=$(wait_brokers_ready 3 600) && echo "    brokers Ready again $((SECONDS - t_inject)) s after the fault" || life_record FAIL "$what: brokers not all Ready after 600 s ($(brokers_ready) of 3)"
  t_urp=$(wait_urp_zero 300) && echo "    no under-replicated partitions $t_urp s later" || life_record FAIL "$what: under-replicated partitions remain ($t_urp)"
  sleep 15
  finish_load "$n" "$what"
  t_active=$(wait_cluster_state ACTIVE 300) && life_record PASS "$what: cluster is ACTIVE again ($t_active s after recovery, $((SECONDS - t_inject)) s after the fault)" || life_record FAIL "$what: cluster state is $(cluster_state)"
}

# ---- faults ----
inj_broker_kill() { kubectl -n "$LIFE_CID" delete pod "$LIFE_CID-brokers-0" --grace-period=0 --force >/dev/null 2>&1; echo "    killed $LIFE_CID-brokers-0 (SIGKILL)"; }
rec_none() { :; }

CH_NODE=""
inj_node_stop() {
  local en cand p
  CH_NODE=""
  en=$(envoy_node)
  for p in 0 1 2; do
    cand=$(kubectl -n "$LIFE_CID" get pod "$LIFE_CID-brokers-$p" -o jsonpath='{.spec.nodeName}')
    [[ "$cand" != "$en" ]] && { CH_NODE=$cand; CH_POD="$LIFE_CID-brokers-$p"; break; }
  done
  [[ -n "$CH_NODE" ]] || { life_record FAIL "node stop: every broker shares a node with the gateway"; return; }
  if [[ "${CHAOS_PROVIDER:-kind}" == aws ]]; then
    "$ROOT/hack/aws/chaos.sh" node-terminate "$CH_NODE" && echo "    terminated the instance of node $CH_NODE (hosts $CH_POD)"
  else
    docker stop "$CH_NODE" >/dev/null && echo "    stopped node $CH_NODE (hosts $CH_POD)"
  fi
}
rec_node_stop() {
  sleep 30
  if [[ "${CHAOS_PROVIDER:-kind}" == aws ]]; then
    echo "    nothing to restart: the pod is rescheduled onto a new node and its EBS volume follows it"
    return
  fi
  [[ -n "$CH_NODE" ]] && docker start "$CH_NODE" >/dev/null && echo "    started node $CH_NODE again"
  for _ in $(seq 1 60); do [[ "$(kubectl get node "$CH_NODE" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)" == True ]] && break; sleep 5; done
}

CH_ZONE_NODES=""
inj_zone_stop() {
  local ez z
  ez=$(zone_of "$(envoy_node)")
  for z in zone-a zone-b zone-c; do [[ "$z" != "$ez" ]] && { CH_ZONE=$z; break; }; done
  if [[ "${CHAOS_PROVIDER:-kind}" == aws ]]; then
    CH_ZONE=$(kubectl get node "$(kubectl -n "$LIFE_CID" get pod "$LIFE_CID-brokers-0" -o jsonpath='{.spec.nodeName}')" -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}')
    echo "    isolating $CH_ZONE (network ACL deny rules)"
    "$ROOT/hack/aws/chaos.sh" zone-isolate "$CH_ZONE"
    return
  fi
  CH_ZONE_NODES=$(kubectl get nodes -l "topology.kubernetes.io/zone=$CH_ZONE" -o jsonpath='{.items[*].metadata.name}')
  echo "    gateway is in $ez; stopping every node of $CH_ZONE: $CH_ZONE_NODES"
  docker stop $CH_ZONE_NODES >/dev/null
}
rec_zone_stop() {
  sleep 30
  if [[ "${CHAOS_PROVIDER:-kind}" == aws ]]; then "$ROOT/hack/aws/chaos.sh" zone-restore "$CH_ZONE"; return; fi
  docker start $CH_ZONE_NODES >/dev/null && echo "    started the nodes of $CH_ZONE again"
  for _ in $(seq 1 60); do
    [[ "$(kubectl get nodes -l "topology.kubernetes.io/zone=$CH_ZONE" --no-headers 2>/dev/null | grep -c ' Ready')" == "$(wc -w <<<"$CH_ZONE_NODES")" ]] && break; sleep 5
  done
}

# disk_full_possible: the broker's data volume must be small. The fault writes until the volume is full; on a volume
# without a limit that would be the disk of the machine (a first attempt on kind took down the CI runner, exit 137).
disk_full_possible() {
  local d free max=${DISK_FULL_MAX_KB:-20971520}
  d=$(kubectl -n "$LIFE_CID" exec -c kafka "$LIFE_CID-brokers-2" -- sh -c 'ls -d /var/lib/kafka/data* 2>/dev/null | head -1' 2>/dev/null | tr -d '\r')
  [[ -n "$d" ]] || return 1
  free=$(kubectl -n "$LIFE_CID" exec -c kafka "$LIFE_CID-brokers-2" -- sh -c 'df -Pk '"$d"' | awk "NR==2{print \$4}"' 2>/dev/null | tr -d '\r')
  echo "    data directory $d has ${free:-?} KiB free (limit for this fault: $max KiB)"
  [[ -n "$free" && "$free" -le "$max" ]]
}

inj_disk_full() {
  CH_POD="$LIFE_CID-brokers-2"
  CH_DATA=$(kubectl -n "$LIFE_CID" exec -c kafka "$CH_POD" -- sh -c 'ls -d /var/lib/kafka/data* 2>/dev/null | head -1' 2>/dev/null | tr -d '\r')
  if [[ -z "$CH_DATA" ]]; then life_record FAIL "disk full: no data directory found in $CH_POD (/var/lib/kafka)"; return; fi
  CH_FILLER="$CH_DATA/streamcell-filler"
  local out free
  out=$(kubectl -n "$LIFE_CID" exec -c kafka "$CH_POD" -- sh -c 'd='"$CH_DATA"'; a=$(df -Pk "$d" | awk "NR==2{print \$4}"); echo "    free before: ${a} KiB in $d"; head -c $(( (a - 1024) * 1024 )) /dev/zero > '"$CH_FILLER"'; df -Pk "$d" | tail -1 | sed "s/^/    after: /"' 2>&1 | tail -3)
  echo "$out"
  free=$(awk '/after:/{print $5}' <<<"$out")
  if [[ -n "$free" && "$free" -lt 4096 ]]; then life_record PASS "disk full: the data volume of $CH_POD is full ($free KiB free)"; else life_record FAIL "disk full: could not fill the volume (free: '${free:-unknown}' KiB)"; fi
}
rec_disk_full() {
  local r0 r i
  sleep 60
  r0=$(kubectl -n "$LIFE_CID" get pod "$CH_POD" -o jsonpath='{.status.containerStatuses[?(@.name=="kafka")].restartCount}' 2>/dev/null)
  echo "    $CH_POD after 60 s of a full disk: ready=$(kubectl -n "$LIFE_CID" get pod "$CH_POD" -o jsonpath='{.status.containerStatuses[?(@.name=="kafka")].ready}'), restarts=$r0"
  kubectl -n "$LIFE_CID" logs -c kafka "$CH_POD" --tail=400 2>/dev/null | grep -iE 'No space left|log dir|LogDirFailure|offline|shutting down' | tail -4 | cut -c1-240 | sed 's/^/    log: /' || true
  # the runbook's first step: free the space (the container may be restarting; try until an exec gets through)
  for i in $(seq 1 150); do
    [[ -n "$CH_FILLER" ]] || return
    if kubectl -n "$LIFE_CID" exec -c kafka "$CH_POD" -- rm -f "$CH_FILLER" >/dev/null 2>&1; then echo "    filler removed after $i attempt(s)"; return; fi
    sleep 2
  done
  life_record FAIL "disk full: the filler could not be removed (the broker never came up long enough to exec into)"
}

CH_FROZEN=""
inj_control_plane_down() {
  for p in api worker; do kill -STOP "$(cat "$STATE/$p.pid")" && CH_FROZEN="$CH_FROZEN $p"; done
  docker pause streamcell-e2e-pg >/dev/null
  pkill -STOP -f 'temporal server start-dev' || true
  echo "    API, worker, Temporal and Postgres frozen"
  if curl -s -m 5 -o /dev/null -H "$AUTH" "$API/v1/clusters/$LIFE_CID"; then life_record FAIL "control plane down: the tenant API still answers"; else life_record PASS "control plane down: the tenant API does not answer (as injected)"; fi
  inj_broker_kill   # the data plane must repair itself without the control plane
}
rec_control_plane_down() {
  local t0=$SECONDS
  # with the control plane frozen the broker has to come back by itself
  wait_brokers_ready 3 400 >/dev/null && life_record PASS "control plane down: the killed broker came back $((SECONDS - t0)) s later with the control plane still frozen" || life_record FAIL "control plane down: the broker did not come back by itself"
  sleep 30
  pkill -CONT -f 'temporal server start-dev' || true
  docker unpause streamcell-e2e-pg >/dev/null
  for p in api worker; do kill -CONT "$(cat "$STATE/$p.pid")"; done
  echo "    control plane released"
  sleep 20
}

scenario_chaos() {
  echo "=================== fault injection under acks=all load (M6, kind emulation) ==================="
  WORK=$(mktemp -d); chmod 755 "$WORK"
  trap 'stop_load; stop_gateway_forward' EXIT
  [[ -x "$LOADTEST" ]] || fail "$LOADTEST is missing (make build)"
  local resp op
  resp=$(api_as "$TENANT_TOKEN" POST /v1/clusters '{"name":"chaos","tier":"shared","spec":{"kafkaVersion":"4.3.1","brokerType":"dev.tiny","brokerCount":3,"storage":{"sizeGiB":1}}}') || fail "create failed"
  op=$(jq -r .id <<<"$resp"); LIFE_CID=$(jq -r .clusterId <<<"$resp")
  echo "  cluster $LIFE_CID, operation $op"
  wait_op "$op" 1500
  curl -sf -H "$AUTH" "$API/v1/clusters/$LIFE_CID/ca-certificate" >"$WORK/ca-$LIFE_CID.pem" || fail "no CA"
  chmod 644 "$WORK"/*.pem
  mk_user "$TENANT_TOKEN" "$LIFE_CID" '{"name":"admin1","admin":true}'
  set_hosts "$LIFE_CID"
  LIFE_BOOT=$(bootstrap_of "$TENANT_TOKEN" "$LIFE_CID")
  LIFE_ADM=$(props "$TENANT_TOKEN" "$LIFE_CID" admin1)
  LIFE_PW=$(api_as "$TENANT_TOKEN" GET "/v1/clusters/$LIFE_CID/scram-users/admin1/credentials" | jq -r .password)
  start_gateway_forward
  retry_ok "chaos: first connection through the public endpoint" 12 kafka kafka-topics --bootstrap-server "$LIFE_BOOT" --command-config "$LIFE_ADM" --list
  kafka kafka-topics --bootstrap-server "$LIFE_BOOT" --command-config "$LIFE_ADM" --create --topic load --partitions 6 --replication-factor 3 \
    --config min.insync.replicas=2 || fail "cannot create the load topic"

  echo "== baseline (no fault)"
  start_load 0; sleep 20; finish_load 0 "baseline"

  run_fault 1 "broker kill (SIGKILL of one broker pod)" inj_broker_kill rec_none no
  run_fault 2 "node stop (the node of one broker; emulates node termination)" inj_node_stop rec_node_stop yes
  run_fault 3 "zone stop (every node of one zone; emulates a zone outage)" inj_zone_stop rec_zone_stop yes
  if disk_full_possible; then
    run_fault 4 "disk full (one broker's data volume filled)" inj_disk_full rec_disk_full no
  else
    echo "== disk full: SKIPPED. The data volume has no size limit here (kind's local-path volumes live on the node's disk, which is far larger than any test volume): filling it would fill the host."
    LIFE_RESULTS+=("SKIP  disk full: needs a size-limited volume (EBS); not emulable on kind's local-path volumes")
  fi
  run_fault 5 "control plane down (API, worker, Temporal, Postgres frozen) with a broker kill meanwhile" inj_control_plane_down rec_control_plane_down no

  echo "== delete"
  stop_gateway_forward; trap - EXIT
  delete_cluster "$LIFE_CID"

  echo; echo "== fault injection results"; printf '  %s\n' "${LIFE_RESULTS[@]}"
  [[ $LIFE_FAILED -eq 0 ]] || fail "chaos scenario has failing checks (listed above)"
  echo "  chaos scenario PASS"
}
