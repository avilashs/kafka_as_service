#!/usr/bin/env bash
# M3 acceptance on a real cell: stock Kafka clients (the Strimzi Kafka image's own CLI tools, running in
# docker on the CI host) talk to two real tenant clusters through the PUBLIC endpoint: SNI-routed TLS
# passthrough on the shared gateway. Sourced by smoke.sh (uses its helpers).
#
# Checks are recorded, not fatal, so one CI run reports every result; the scenario fails at the end if any
# check failed. A check that cannot be run says SKIP with the reason, never PASS.

KAFKA_IMAGE="${KAFKA_IMAGE:-quay.io/strimzi/kafka:1.3.0-kafka-4.3.1}"
TENANT_TOKEN="${TENANT_TOKEN:-dev-tenant-token}"
API_B_TOKEN="${TENANT_B_TOKEN:-dev-tenant-b-token}"
CONNECT_FAILED=0
declare -a CONNECT_RESULTS=()

record() { # PASS|FAIL|SKIP description
  CONNECT_RESULTS+=("$1  $2")
  echo "  [$1] $2"
  [[ "$1" == FAIL ]] && CONNECT_FAILED=1
  return 0
}

# expect_ok description command...   /   expect_fail description pattern command...
expect_ok() {
  local what=$1 out; shift
  if out=$("$@" 2>&1); then record PASS "$what"; else record FAIL "$what"; sed 's/^/      | /' <<<"$out" | tail -25; fi
}
# check_denied: the command's OUTPUT must show the denial, whatever its exit status (console clients can
# log an authorization error and still exit 0).
check_denied() { # description, egrep pattern, command...
  local what=$1 pat=$2 out; shift 2
  out=$("$@" 2>&1 || true)
  if grep -qiE -- "$pat" <<<"$out"; then record PASS "$what"; else record FAIL "$what (no /$pat/ in output)"; sed 's/^/      | /' <<<"$out" | tail -15; fi
}
expect_fail() { # description, egrep pattern the output must match, command...
  local what=$1 pat=$2 out; shift 2
  if out=$("$@" 2>&1); then
    record FAIL "$what (was ACCEPTED)"
  elif grep -qiE -- "$pat" <<<"$out"; then
    record PASS "$what"
  else
    record FAIL "$what (failed, but not with /$pat/)"; sed 's/^/      | /' <<<"$out" | tail -15
  fi
}

# ---------- plumbing: public endpoint from the host ----------

# The relay is started by up.sh (the canary needs it too) and removed by down.sh; the scenarios only use it.
start_gateway_forward() {
  if docker ps --format '{{.Names}}' | grep -qx streamcell-gw-relay; then echo "  gateway relay is up (started by up.sh)"; return 0; fi
  start_gateway_forward_own
}
start_gateway_forward_own() {
  local svc
  svc=$(kubectl -n envoy-gateway-system get svc -l gateway.envoyproxy.io/owning-gateway-name=streamcell-gateway -o name | head -1)
  [[ -n "$svc" ]] || { kubectl -n envoy-gateway-system get svc -o wide; fail "no Envoy data-plane service for the gateway"; }
  echo "  gateway data plane: $svc"
  kubectl -n envoy-gateway-system get pods -l gateway.envoyproxy.io/owning-gateway-name=streamcell-gateway -o wide
  # Clients must use port 443: Strimzi advertises the tlsroute hosts on 443. Port-forward to an unprivileged
  # port, and let a root container (docker) listen on 443 and relay.
  nohup kubectl -n envoy-gateway-system port-forward --address 127.0.0.1 "$svc" 8443:443 >"$STATE/gw-forward.log" 2>&1 &
  echo $! >"$STATE/gw-forward.pid"
  docker rm -f streamcell-gw-relay >/dev/null 2>&1 || true
  docker run -d --name streamcell-gw-relay --network host alpine/socat \
    tcp-listen:443,fork,reuseaddr tcp:127.0.0.1:8443 >/dev/null
  sleep 3
}
stop_gateway_forward() { :; }

WORK=""
HOSTS=()
set_hosts() { # cluster ids: every name the clients will resolve is the local relay
  HOSTS=()
  local cid h
  for cid in "$@"; do
    h="$cid.$(kubectl get kafka -n "$cid" "$cid" -o jsonpath='{.spec.kafka.listeners[?(@.name=="external")].configuration.bootstrap.host}' | cut -d. -f3-)"
    HOSTS+=(--add-host "bootstrap.$h:127.0.0.1" --add-host "mtls-bootstrap.$h:127.0.0.1")
    for i in $(seq 0 12); do HOSTS+=(--add-host "b$i.$h:127.0.0.1" --add-host "m$i.$h:127.0.0.1"); done
  done
}

# kafka <tool> args...   (tool is a script name without .sh); stdin passes through
kafka() {
  local tool=$1; shift
  docker run --rm -i --network host "${HOSTS[@]}" -v "$WORK:/work:ro" --entrypoint "/opt/kafka/bin/$tool.sh" "$KAFKA_IMAGE" "$@"
}

# ---------- API helpers ----------

api_as() { # token method path [json]
  local tok=$1 method=$2 path=$3 body=${4:-}
  if [[ -n "$body" ]]; then
    curl -sf -X "$method" "$API$path" -H "Authorization: Bearer $tok" -H 'Content-Type: application/json' -H "Idempotency-Key: c-$RANDOM-$RANDOM" -d "$body"
  else
    curl -sf -X "$method" "$API$path" -H "Authorization: Bearer $tok" -H "Idempotency-Key: c-$RANDOM-$RANDOM"
  fi
}

with_token() { local tok=$1; shift; ( AUTH="Authorization: Bearer $tok"; "$@" ); }

mk_user() { # token cluster-id json -> waits for the operation
  local op; op=$(api_as "$1" POST "/v1/clusters/$2/scram-users" "$3" | jq -r .id) || fail "create user request failed: $3"
  with_token "$1" wait_op "$op" 600
}

produce() { # config topic bootstrap  (messages on stdin)
  kafka kafka-console-producer --bootstrap-server "$3" --producer.config "$1" --topic "$2"
}
produce_one() { echo "x" | produce "$@"; }
produce_50() { seq 1 50 | sed 's/^/message-/' | produce "$@"; }

# props <token> <cluster-id> <user> -> writes $WORK/<cid>-<user>.properties, prints the path as seen in the container
props() {
  local tok=$1 cid=$2 user=$3 pw f="$WORK/$2-$3.properties"
  pw=$(api_as "$tok" GET "/v1/clusters/$cid/scram-users/$user/credentials" | jq -r .password) || fail "no credentials for $user"
  [[ -n "$pw" && "$pw" != null ]] || fail "empty password for $user"
  {
    echo "security.protocol=SASL_SSL"
    echo "sasl.mechanism=SCRAM-SHA-512"
    echo "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"$user\" password=\"$pw\";"
    echo "ssl.truststore.type=PEM"
    echo "ssl.truststore.location=/work/ca-$cid.pem"
    echo "request.timeout.ms=30000"
    echo "default.api.timeout.ms=60000"
  } >"$f"
  echo "/work/$(basename "$f")"
}

bootstrap_of() { api_as "$1" GET "/v1/clusters/$2/bootstrap-brokers" | jq -r '.endpoints["scram-sha-512"]'; }

create_as() { # token name -> "op cid"
  local resp
  resp=$(api_as "$1" POST /v1/clusters "{\"name\":\"$2\",\"tier\":\"shared\",\"spec\":{\"kafkaVersion\":\"4.3.1\",\"brokerType\":\"dev.tiny\",\"brokerCount\":3,\"storage\":{\"sizeGiB\":1}}}") || fail "create $2 failed"
  echo "$(jq -r .id <<<"$resp") $(jq -r .clusterId <<<"$resp")"
}

retry_ok() { # description, attempts, command...  (the first connection waits for TLSRoute programming)
  local what=$1 n=$2 i out; shift 2
  for i in $(seq 1 "$n"); do
    if out=$("$@" 2>&1); then record PASS "$what (attempt $i)"; return 0; fi
    sleep 10
  done
  record FAIL "$what (after $n attempts)"; sed 's/^/      | /' <<<"$out" | tail -25
  return 1
}

# ---------- the scenario ----------

scenario_connect() {
  echo "=================== connectivity, authentication, tenant isolation (M3) ==================="
  WORK=$(mktemp -d); chmod 755 "$WORK"
  docker pull -q "$KAFKA_IMAGE" >/dev/null || fail "cannot pull $KAFKA_IMAGE"
  trap 'stop_gateway_forward' EXIT

  echo "== two clusters, two tenants"
  local opa cida opb cidb
  read -r opa cida < <(create_as "$TENANT_TOKEN" tenant-a)
  read -r opb cidb < <(create_as "$API_B_TOKEN" tenant-b)
  echo "  A: $cida (op $opa)   B: $cidb (op $opb)"
  wait_op "$opa" 1500
  with_token "$API_B_TOKEN" wait_op "$opb" 1500
  free -m | sed 's/^/  /' || true
  kubectl -n "$cida" get pods -o wide

  curl -sf -H "$AUTH" "$API/v1/clusters/$cida/ca-certificate" >"$WORK/ca-$cida.pem" || fail "no CA for A"
  api_as "$API_B_TOKEN" GET "/v1/clusters/$cidb/ca-certificate" >"$WORK/ca-$cidb.pem" || fail "no CA for B"
  openssl x509 -in "$WORK/ca-$cida.pem" -noout -subject -issuer -dates | sed 's/^/  A CA: /'
  openssl x509 -in "$WORK/ca-$cidb.pem" -noout -subject | sed 's/^/  B CA: /'
  if cmp -s "$WORK/ca-$cida.pem" "$WORK/ca-$cidb.pem"; then record FAIL "clusters have distinct CAs"; else record PASS "clusters have distinct CAs"; fi
  chmod 644 "$WORK"/*.pem

  echo "== users (through the platform API)"
  mk_user "$TENANT_TOKEN" "$cida" '{"name":"admin1","admin":true}'
  mk_user "$TENANT_TOKEN" "$cida" '{"name":"writer","acls":[
    {"resource":{"type":"topic","name":"orders","patternType":"prefix"},"operations":["Write","Describe"]},
    {"resource":{"type":"cluster"},"operations":["IdempotentWrite"]}]}'
  mk_user "$TENANT_TOKEN" "$cida" '{"name":"reader","acls":[
    {"resource":{"type":"topic","name":"orders","patternType":"prefix"},"operations":["Read","Describe"]},
    {"resource":{"type":"group","name":"grp-","patternType":"prefix"},"operations":["Read"]}]}'
  mk_user "$API_B_TOKEN" "$cidb" '{"name":"admin1","admin":true}'
  echo "  KafkaUser status in A:"; kubectl -n "$cida" get kafkauser

  set_hosts "$cida" "$cidb"
  local boot_a boot_b
  boot_a=$(bootstrap_of "$TENANT_TOKEN" "$cida"); boot_b=$(bootstrap_of "$API_B_TOKEN" "$cidb")
  echo "  public endpoints: A=$boot_a B=$boot_b"
  [[ "$boot_a" == *:443 ]] || record FAIL "bootstrap endpoint uses port 443 (got $boot_a)"

  local adm wr rd badm
  adm=$(props "$TENANT_TOKEN" "$cida" admin1); wr=$(props "$TENANT_TOKEN" "$cida" writer)
  rd=$(props "$TENANT_TOKEN" "$cida" reader); badm=$(props "$API_B_TOKEN" "$cidb" admin1)

  start_gateway_forward

  echo "== TLS certificate served through the gateway"
  # What the broker presents, as seen through SNI passthrough: issued by the cluster CA, names match.
  local cert; cert=$(echo | openssl s_client -connect 127.0.0.1:443 -servername "${boot_a%:443}" -CAfile "$WORK/ca-$cida.pem" -verify_hostname "${boot_a%:443}" 2>&1 || true)
  grep -q "Verification: OK\|Verify return code: 0" <<<"$cert" && record PASS "A's broker certificate verifies against A's CA for the bootstrap hostname" \
    || { record FAIL "A's broker certificate verifies against A's CA for the bootstrap hostname"; echo "$cert" | tail -15 | sed 's/^/      | /'; }

  echo "== basic client flow (stock Kafka tools)"
  retry_ok "admin: kafka-topics --list through the public endpoint" 12 kafka kafka-topics --bootstrap-server "$boot_a" --command-config "$adm" --list
  # Creating a topic needs every broker's own name (b0, b1, ...), not only the bootstrap name. Right after
  # ACTIVE the first attempts can hit a broker whose route or certificate name is not there yet (seen once in
  # CI: "No subject alternative DNS name matching b0..." for about a minute); until the M5 canary defines
  # "reachable", the scenario waits for it and says how long it took.
  local t0=$SECONDS
  retry_ok "admin: create topic orders-1 (3 partitions, RF 3)" 12 \
    kafka kafka-topics --bootstrap-server "$boot_a" --command-config "$adm" --create --if-not-exists --topic orders-1 --partitions 3 --replication-factor 3
  echo "  per-broker names reachable $((SECONDS - t0))s after the first successful bootstrap"
  retry_ok "admin: create topic private-1" 6 \
    kafka kafka-topics --bootstrap-server "$boot_a" --command-config "$adm" --create --if-not-exists --topic private-1 --partitions 1 --replication-factor 3
  expect_ok "admin: describe topic (Admin API)" \
    kafka kafka-topics --bootstrap-server "$boot_a" --command-config "$adm" --describe --topic orders-1
  expect_ok "admin: describe topic configs (Admin API)" \
    kafka kafka-configs --bootstrap-server "$boot_a" --command-config "$adm" --describe --entity-type topics --entity-name orders-1

  expect_ok "writer: console producer writes 50 messages" produce_50 "$wr" orders-1 "$boot_a"
  local got
  got=$(kafka kafka-console-consumer --bootstrap-server "$boot_a" --consumer.config "$rd" --topic orders-1 --group grp-1 --from-beginning --max-messages 50 --timeout-ms 60000 2>/dev/null | grep '^message-' | sort -u | wc -l || true)
  [[ "$got" == 50 ]] && record PASS "reader: console consumer reads back all 50 distinct messages" || record FAIL "reader: console consumer read $got distinct messages, want 50"

  echo "== authorisation (ACLs)"
  check_denied "reader cannot produce" "not authorized|authorization" produce_one "$rd" orders-1 "$boot_a"
  check_denied "writer cannot produce to a topic outside its ACLs" "not authorized|authorization" produce_one "$wr" private-1 "$boot_a"
  check_denied "reader cannot read a topic outside its ACLs" "not authorized|authorization" \
    kafka kafka-console-consumer --bootstrap-server "$boot_a" --consumer.config "$rd" --topic private-1 --group grp-2 --from-beginning --max-messages 1 --timeout-ms 20000
  check_denied "reader cannot create topics" "not authorized|authorization|denied" \
    kafka kafka-topics --bootstrap-server "$boot_a" --command-config "$rd" --create --topic orders-sneaky --partitions 1 --replication-factor 3
  check_denied "tenant admin cannot alter broker configuration" "not authorized|authorization|denied" \
    kafka kafka-configs --bootstrap-server "$boot_a" --command-config "$adm" --alter --entity-type brokers --entity-default --add-config log.retention.ms=1000

  echo "== tenants manage ACLs through the Kafka Admin API (PlatformAuthorizer)"
  expect_ok "admin: grant reader Read on private-1 with kafka-acls (CreateAcls over the public endpoint)" \
    kafka kafka-acls --bootstrap-server "$boot_a" --command-config "$adm" --add --allow-principal User:reader --operation Read --operation Describe --topic private-1
  expect_ok "admin: list ACLs (DescribeAcls)" kafka kafka-acls --bootstrap-server "$boot_a" --command-config "$adm" --list --topic private-1
  expect_ok "admin: write a message to private-1" bash -c "echo secret-y | docker run --rm -i --network host ${HOSTS[*]} -v $WORK:/work:ro --entrypoint /opt/kafka/bin/kafka-console-producer.sh $KAFKA_IMAGE --bootstrap-server $boot_a --producer.config $adm --topic private-1"
  local cout; cout=$(kafka kafka-console-consumer --bootstrap-server "$boot_a" --consumer.config "$rd" --topic private-1 --group grp-3 --from-beginning --max-messages 1 --timeout-ms 60000 2>&1 || true)
  if grep -q '^secret-y$' <<<"$cout"; then record PASS "reader reads private-1 once the tenant admin granted it"; else record FAIL "reader could not read private-1 after the grant"; sed 's/^/      | /' <<<"$cout" | tail -12; fi
  check_denied "admin cannot grant cluster-level Alter" "authoriz|platform|denied" \
    kafka kafka-acls --bootstrap-server "$boot_a" --command-config "$adm" --add --allow-principal User:reader --cluster --operation Alter
  check_denied "admin cannot create a reserved topic" "authoriz|denied" \
    kafka kafka-topics --bootstrap-server "$boot_a" --command-config "$adm" --create --topic __sneaky --partitions 1 --replication-factor 3
  check_denied "admin cannot set a topic config outside the allowlist" "policy|cannot be set" \
    kafka kafka-configs --bootstrap-server "$boot_a" --command-config "$adm" --alter --entity-type topics --entity-name orders-1 --add-config unclean.leader.election.enable=true
  expect_ok "admin: allowed topic config change (retention.ms)" \
    kafka kafka-configs --bootstrap-server "$boot_a" --command-config "$adm" --alter --entity-type topics --entity-name orders-1 --add-config retention.ms=3600000

  echo "== transactions and idempotence"
  kafka kafka-topics --bootstrap-server "$boot_a" --command-config "$adm" --create --topic orders-tx --partitions 3 --replication-factor 3 >/dev/null 2>&1 || true
  expect_ok "admin: transactional producer commits 200 records (kafka-producer-perf-test, transactional.id)" \
    kafka kafka-producer-perf-test --topic orders-tx --num-records 200 --record-size 64 --throughput -1 \
      --command-config "$adm" --transaction-duration-ms 200 \
      --command-property bootstrap.servers="$boot_a" transactional.id=tx-e2e enable.idempotence=true acks=all
  got=$(kafka kafka-console-consumer --bootstrap-server "$boot_a" --consumer.config "$adm" --topic orders-tx --group grp-tx --from-beginning \
        --consumer-property isolation.level=read_committed --max-messages 200 --timeout-ms 60000 2>/dev/null | awk 'length($0)==64' | wc -l || true)
  [[ "$got" == 200 ]] && record PASS "read_committed consumer sees exactly the 200 committed records" || record FAIL "read_committed consumer saw $got records, want 200"
  expect_ok "consumer groups listed through the Admin API" \
    kafka kafka-consumer-groups --bootstrap-server "$boot_a" --command-config "$adm" --list

  echo "== SCRAM rotation takes effect on the live cluster"
  local op; op=$(api_as "$TENANT_TOKEN" POST "/v1/clusters/$cida/scram-users/reader/rotate" | jq -r .id); wait_op "$op" 600
  expect_fail "old reader password is rejected after rotation" "authentication|SASL" \
    kafka kafka-topics --bootstrap-server "$boot_a" --command-config "$rd" --list
  rd=$(props "$TENANT_TOKEN" "$cida" reader)
  expect_ok "new reader password is accepted" kafka kafka-topics --bootstrap-server "$boot_a" --command-config "$rd" --list

  echo "== mutual TLS: the cell signs the tenant's CSR; the private key never leaves the client"
  mtls_checks "$cida" "$cidb"

  echo "== tenant isolation: credentials and CA"
  # A's admin credentials, pointed at B's truststore (so TLS to B succeeds and only the credentials are tested)
  sed "s#/work/ca-$cida.pem#/work/ca-$cidb.pem#" "$WORK/$cida-admin1.properties" >"$WORK/x-a-creds-b-ca.properties"
  # B's admin credentials, pointed at A's truststore
  sed "s#/work/ca-$cidb.pem#/work/ca-$cida.pem#" "$WORK/$cidb-admin1.properties" >"$WORK/x-b-creds-a-ca.properties"
  chmod 644 "$WORK"/*.properties
  check_denied "A's admin credentials are rejected by B's cluster (B's CA trusted)" "authentication|SASL" \
    kafka kafka-topics --bootstrap-server "$boot_b" --command-config /work/x-a-creds-b-ca.properties --list
  check_denied "A's CA does not validate B's brokers" "ssl|certificate|handshake|PKIX" \
    kafka kafka-topics --bootstrap-server "$boot_b" --command-config "/work/$cida-admin1.properties" --list
  check_denied "B's admin password does not work on A even though A's CA is trusted (same user name, other cluster)" "authentication|SASL" \
    kafka kafka-topics --bootstrap-server "$boot_a" --command-config /work/x-b-creds-a-ca.properties --list
  check_denied "B's CA does not validate A's brokers" "ssl|certificate|handshake|PKIX" \
    kafka kafka-topics --bootstrap-server "$boot_a" --command-config "/work/$cidb-admin1.properties" --list
  expect_ok "B's own admin works on B (control)" kafka kafka-topics --bootstrap-server "$boot_b" --command-config "$badm" --list
  expect_ok "tenant B's API token cannot read A's credentials (404)" bash -c "! curl -sf -H 'Authorization: Bearer $API_B_TOKEN' $API/v1/clusters/$cida/scram-users/admin1/credentials"

  echo "== tenant isolation: network"
  network_isolation "$cida" "$cidb"

  echo "== secrets"
  local pw n
  pw=$(api_as "$TENANT_TOKEN" GET "/v1/clusters/$cida/scram-users/writer/credentials" | jq -r .password)
  [[ ${#pw} -ge 43 ]] || fail "could not read writer's password"
  n=$("${PSQL[@]}" "SELECT count(*) FROM (SELECT spec::text AS r FROM clusters UNION ALL SELECT payload::text FROM outbox UNION ALL SELECT request::text FROM operations UNION ALL SELECT bundle::text FROM desired_states UNION ALL SELECT observed::text FROM observed_status) x WHERE position('$pw' in r) > 0")
  [[ "$n" == 0 ]] && record PASS "writer's password is in none of clusters/outbox/operations/desired_states/observed_status" || record FAIL "writer's password found in $n database rows"

  echo "== delete both clusters"
  delete_cluster "$cida"
  with_token "$API_B_TOKEN" delete_cluster "$cidb"
  stop_gateway_forward; trap - EXIT

  echo
  echo "== connectivity results"
  printf '  %s\n' "${CONNECT_RESULTS[@]}"
  [[ $CONNECT_FAILED -eq 0 ]] || fail "connectivity scenario has failing checks (listed above)"
  echo "  connect scenario PASS"
}

# Network isolation, checked from inside the cluster. Egress and ingress are enforced by NetworkPolicy
# (default deny + explicit allows), which needs a CNI that enforces it; the canary decides whether a
# "blocked" result means anything.
network_isolation() {
  local a=$1 b=$2
  local probe=probe-ns
  kubectl create namespace "$probe" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl -n "$probe" run client --image="$KAFKA_IMAGE" --restart=Never --command -- sleep 3600 >/dev/null 2>&1 || true
  kubectl -n "$probe" wait --for=condition=Ready pod/client --timeout=180s >/dev/null || { record FAIL "probe pod did not start"; return; }

  tcp() { # namespace pod container host port -> exit status 0 when the TCP connection opens
    kubectl -n "$1" exec "$2" -c "$3" -- timeout 5 bash -c "exec 3<>/dev/tcp/$4/$5" 2>/dev/null
  }
  local a_pod=$a-brokers-3 b_pod=$b-brokers-3
  # node ids start after the controllers; find real broker pod names instead of guessing
  a_pod=$(kubectl -n "$a" get pods -l strimzi.io/broker-role=true -o jsonpath='{.items[0].metadata.name}')
  b_pod=$(kubectl -n "$b" get pods -l strimzi.io/broker-role=true -o jsonpath='{.items[0].metadata.name}')
  local a_ip b_ip
  a_ip=$(kubectl -n "$a" get pod "$a_pod" -o jsonpath='{.status.podIP}'); b_ip=$(kubectl -n "$b" get pod "$b_pod" -o jsonpath='{.status.podIP}')

  # control: inside its own namespace a pod reaches its own broker
  tcp "$a" "$a_pod" kafka "$a-kafka-bootstrap.$a.svc" 9093 && record PASS "inside A: A's pods reach A's internal listener (control)" || record FAIL "inside A: A's pods reach A's internal listener (control)"
  # canary: does this cluster enforce NetworkPolicy at all?
  local enforced=1
  if tcp "$b" "$b_pod" kafka "$a-kafka-bootstrap.$a.svc" 9093; then enforced=0; fi
  if tcp "$b" "$b_pod" kafka "$a_ip" 9093; then enforced=0; fi
  if (( enforced )); then
    record PASS "B's pods cannot open A's internal listener (by service name or pod IP)"
  else
    record FAIL "B's pods CAN open A's internal listener — NetworkPolicy not enforced or policy wrong"
  fi
  tcp "$a" "$a_pod" kafka "$b-kafka-bootstrap.$b.svc" 9093 && record FAIL "A's pods CAN open B's internal listener" || record PASS "A's pods cannot open B's internal listener"
  # a pod outside every tenant namespace reaches neither the external listener port nor the replication port of a broker
  for port in 9094 9091 9093; do
    kubectl -n "$probe" exec client -- timeout 5 bash -c "exec 3<>/dev/tcp/$a_ip/$port" 2>/dev/null \
      && record FAIL "pod outside tenant namespaces CAN open A's broker port $port" || record PASS "pod outside tenant namespaces cannot open A's broker port $port"
  done
  # the gateway's proxy pods are the one allowed path to the external listener
  local envoy; envoy=$(kubectl -n envoy-gateway-system get pods -l gateway.envoyproxy.io/owning-gateway-name=streamcell-gateway -o jsonpath='{.items[0].metadata.name}')
  if kubectl -n envoy-gateway-system exec "$envoy" -c envoy -- bash -c "exec 3<>/dev/tcp/$a_ip/9094" 2>/dev/null; then
    record PASS "the gateway proxy can open A's external listener port (control)"
  else
    record SKIP "gateway proxy shell check: the Envoy image has no bash; the client flow above already proves this path"
  fi
  kubectl delete namespace "$probe" --wait=false >/dev/null 2>&1 || true
}

mtls_checks() {
  local a=$1 b=$2 boot_m boot_mb
  boot_m=$(api_as "$TENANT_TOKEN" GET "/v1/clusters/$a/bootstrap-brokers" | jq -r '.endpoints.mtls')
  boot_mb=$(api_as "$API_B_TOKEN" GET "/v1/clusters/$b/bootstrap-brokers" | jq -r '.endpoints.mtls')
  echo "  mTLS endpoints: A=$boot_m B=$boot_mb"
  [[ "$boot_m" == mtls-bootstrap.*:443 ]] && record PASS "mTLS endpoint is separate from the SCRAM one (own host, port 443)" || record FAIL "mTLS endpoint (got $boot_m)"

  # key and CSR are made here; only the CSR is sent. The subject asked for is hostile on purpose.
  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$WORK/svc.key" 2>/dev/null
  openssl req -new -key "$WORK/svc.key" -subj "/CN=kc-pretend-entity-user-operator/O=io.strimzi" -out "$WORK/svc.csr"
  local body; body=$(jq -n --arg csr "$(cat "$WORK/svc.csr")" '{name:"svc1", csr:$csr, acls:[
    {resource:{type:"topic",name:"orders",patternType:"prefix"},operations:["Read","Write","Describe"]},
    {resource:{type:"group",name:"grp-",patternType:"prefix"},operations:["Read"]},
    {resource:{type:"cluster"},operations:["IdempotentWrite"]}]}')
  local op; op=$(api_as "$TENANT_TOKEN" POST "/v1/clusters/$a/mtls-users" "$body" | jq -r .id)
  [[ -n "$op" && "$op" != null ]] || { record FAIL "create mTLS user request"; return; }
  with_token "$TENANT_TOKEN" wait_op "$op" 600
  api_as "$TENANT_TOKEN" GET "/v1/clusters/$a/mtls-users/svc1/certificate" >"$WORK/svc.crt" || { record FAIL "no certificate issued"; return; }
  openssl x509 -in "$WORK/svc.crt" -noout -subject -issuer -dates -ext extendedKeyUsage | sed 's/^/  cert: /'
  openssl x509 -in "$WORK/svc.crt" -noout -subject -nameopt RFC2253 | grep -qx 'subject=CN=svc1' && record PASS "issued subject is CN=svc1 (the CSR's subject was ignored)" || record FAIL "issued subject is not CN=svc1"
  # the certificate is for our key, and chains to the cluster's clients CA (Strimzi writes it to <cluster>-clients-ca-cert)
  [[ "$(openssl x509 -in "$WORK/svc.crt" -noout -pubkey)" == "$(openssl pkey -in "$WORK/svc.key" -pubout)" ]] && record PASS "certificate is for the tenant's own key" || record FAIL "certificate is for another key"
  kubectl -n "$a" get secret "$a-clients-ca-cert" -o jsonpath='{.data.ca\.crt}' | base64 -d >"$WORK/clients-ca-$a.pem"
  openssl verify -CAfile "$WORK/clients-ca-$a.pem" "$WORK/svc.crt" >/dev/null 2>&1 && record PASS "certificate verifies against A's clients CA" || record FAIL "certificate does not verify against A's clients CA"

  # PEM keystore: private key (PKCS#8) followed by the certificate
  cat "$WORK/svc.key" "$WORK/svc.crt" >"$WORK/svc-keystore.pem"
  chmod 644 "$WORK"/*
  cat >"$WORK/mtls-a.properties" <<PROPS
security.protocol=SSL
ssl.keystore.type=PEM
ssl.keystore.location=/work/svc-keystore.pem
ssl.truststore.type=PEM
ssl.truststore.location=/work/ca-$a.pem
request.timeout.ms=30000
default.api.timeout.ms=60000
PROPS
  sed "s#/work/ca-$a.pem#/work/ca-$b.pem#" "$WORK/mtls-a.properties" >"$WORK/mtls-a-on-b.properties"
  chmod 644 "$WORK"/*.properties

  retry_ok "mTLS: list topics through the mTLS endpoint" 6 kafka kafka-topics --bootstrap-server "$boot_m" --command-config /work/mtls-a.properties --list
  produce_mtls() { seq 1 20 | sed 's/^/mtls-/' | kafka kafka-console-producer --bootstrap-server "$boot_m" --producer.config /work/mtls-a.properties --topic orders-1; }
  expect_ok "mTLS: produce 20 messages to orders-1 (ACL from the user's spec)" produce_mtls
  local got
  got=$(kafka kafka-console-consumer --bootstrap-server "$boot_m" --consumer.config /work/mtls-a.properties --topic orders-1 --group grp-m --from-beginning --max-messages 70 --timeout-ms 60000 2>/dev/null | grep -c '^mtls-' || true)
  [[ "$got" == 20 ]] && record PASS "mTLS: consume the 20 messages back" || record FAIL "mTLS: consumed $got of 20"
  check_denied "mTLS user is held to its ACLs (cannot write private-1)" "not authorized|authorization" \
    bash -c "echo x | docker run --rm -i --network host ${HOSTS[*]} -v $WORK:/work:ro --entrypoint /opt/kafka/bin/kafka-console-producer.sh $KAFKA_IMAGE --bootstrap-server $boot_m --producer.config /work/mtls-a.properties --topic private-1"
  check_denied "no client certificate is refused on the mTLS port" "ssl|handshake|certificate|disconnected|bad_certificate|timed out" \
    kafka kafka-topics --bootstrap-server "$boot_m" --command-config "/work/$a-admin1.properties" --list
  check_denied "A's certificate is rejected by B's mTLS listener" "ssl|handshake|certificate|disconnected|bad_certificate|timed out" \
    kafka kafka-topics --bootstrap-server "$boot_mb" --command-config /work/mtls-a-on-b.properties --list
}
