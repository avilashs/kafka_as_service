#!/usr/bin/env bash
# Integration test of the plugins against a REAL Kafka broker (KRaft, one node, SASL/PLAIN so no
# external services are needed). The authorizer logic does not depend on the authentication mechanism.
#
#   KAFKA_IMAGE=apache/kafka:4.1.0 plugins/kafka/it/it.sh        (needs docker and the built jar)
#
# Every check is recorded; the script exits non-zero if any failed.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
JAR="${JAR:-$HERE/../target/streamcell-kafka-plugins.jar}"
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:4.1.0}"
NAME=streamcell-plugin-it
TMP="$(mktemp -d)"; chmod 755 "$TMP"
FAILED=0
cleanup() { docker rm -f "$NAME" >/dev/null 2>&1; rm -rf "$TMP"; }
trap cleanup EXIT
[[ -f "$JAR" ]] || { echo "build the jar first: (cd plugins/kafka && mvn package)"; exit 2; }

cat >"$TMP/server.properties" <<PROPS
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:19093
listeners=SASL://:19092,CONTROLLER://:19093
advertised.listeners=SASL://localhost:19092
listener.security.protocol.map=SASL:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=SASL
controller.listener.names=CONTROLLER
sasl.enabled.mechanisms=PLAIN
sasl.mechanism.inter.broker.protocol=PLAIN
listener.name.sasl.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="broker" password="bp" user_broker="bp" user_admin1="a1" user_app="ap" user_other="ot";
authorizer.class.name=dev.streamcell.kafka.PlatformAuthorizer
super.users=User:broker;User:ANONYMOUS
create.topic.policy.class.name=dev.streamcell.kafka.PlatformCreateTopicPolicy
alter.config.policy.class.name=dev.streamcell.kafka.PlatformAlterConfigPolicy
streamcell.topic.min.replication.factor=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
log.dirs=/tmp/kraft
PROPS
for u in broker:bp admin1:a1 app:ap other:ot; do
  cat >"$TMP/${u%%:*}.properties" <<PROPS
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="${u%%:*}" password="${u##*:}";
PROPS
done
chmod 644 "$TMP"/*

docker rm -f "$NAME" >/dev/null 2>&1
docker run -d --name "$NAME" --network host -v "$TMP:/it:ro" -v "$JAR:/opt/kafka/libs/streamcell-kafka-plugins.jar:ro" \
  --entrypoint bash "$KAFKA_IMAGE" -c '
    cp /it/server.properties /tmp/server.properties &&
    /opt/kafka/bin/kafka-storage.sh format -t "$(/opt/kafka/bin/kafka-storage.sh random-uuid)" -c /tmp/server.properties --standalone &&
    exec /opt/kafka/bin/kafka-server-start.sh /tmp/server.properties' >/dev/null

K() { docker exec -i "$NAME" /opt/kafka/bin/"$1".sh "${@:2}"; }
cfg() { docker exec "$NAME" true; echo "--command-config /it/$1.properties"; }
BS="--bootstrap-server localhost:19092"

for i in $(seq 1 60); do
  K kafka-topics $BS --command-config /it/broker.properties --list >/dev/null 2>&1 && break
  sleep 2
  [[ $i == 60 ]] && { echo "broker did not start"; docker logs "$NAME" | tail -40; exit 1; }
done
docker logs "$NAME" 2>&1 | grep -q "PlatformAuthorizer" && echo "(broker log mentions PlatformAuthorizer)"

pass() { echo "  [PASS] $1"; }
bad()  { echo "  [FAIL] $1"; FAILED=1; }
# ok <desc> <cmd...>: must succeed.   denied <desc> <egrep> <cmd...>: output must match (exit status ignored).
ok() { local d=$1; shift; local o; if o=$("$@" 2>&1); then pass "$d"; else bad "$d"; echo "$o" | tail -8 | sed 's/^/      | /'; fi; }
denied() { local d=$1 p=$2; shift 2; local o; o=$("$@" 2>&1 || true); if grep -qiE -- "$p" <<<"$o"; then pass "$d"; else bad "$d"; echo "$o" | tail -8 | sed 's/^/      | /'; fi; }

ADM="--command-config /it/admin1.properties"
APP="--command-config /it/app.properties"
SU="--command-config /it/broker.properties"

echo "== setup as super user: admin1 gets every cluster permission a tenant admin could ever be given"
ok "grant admin1 topic/group/tx access" K kafka-acls $BS $SU --add --allow-principal User:admin1 --operation All --topic '*' --group '*' --transactional-id '*'
# Even with these ACLs in place, the hard rules must hold.
ok "grant admin1 cluster Alter, AlterConfigs, ClusterAction, Describe, Create" \
  K kafka-acls $BS $SU --add --allow-principal User:admin1 --cluster --operation Alter --operation AlterConfigs --operation ClusterAction --operation Describe --operation DescribeConfigs --operation Create

echo "== tenant admin: allowed"
ok "create topic" K kafka-topics $BS $ADM --create --topic orders --partitions 3 --replication-factor 1
ok "create ACL for another user on own topic (CreateAcls)" K kafka-acls $BS $ADM --add --allow-principal User:app --operation Read --operation Describe --topic orders --group 'g-' --resource-pattern-type prefixed
ok "list ACLs (DescribeAcls)" K kafka-acls $BS $ADM --list --topic orders
ok "delete an ACL on a topic (DeleteAcls)" K kafka-acls $BS $ADM --remove --allow-principal User:app --operation Describe --topic orders --force
ok "re-add it" K kafka-acls $BS $ADM --add --allow-principal User:app --operation Describe --topic orders
ok "alter an allowed topic config" K kafka-configs $BS $ADM --alter --entity-type topics --entity-name orders --add-config retention.ms=3600000
ok "describe topic configs" K kafka-configs $BS $ADM --describe --entity-type topics --entity-name orders
echo "x" | K kafka-console-producer $BS --producer.config /it/admin1.properties --topic orders >/dev/null 2>&1 && pass "admin produces" || bad "admin produces"
ok "app (ACL created by the tenant admin) can read" bash -c "docker exec -i $NAME /opt/kafka/bin/kafka-console-consumer.sh $BS --consumer.config /it/app.properties --topic orders --group g-1 --from-beginning --max-messages 1 --timeout-ms 20000 | grep -q x"

echo "== tenant admin: denied although ACLs allow it"
denied "grant a cluster permission (Alter) to someone else" "authoriz|platform|managed" K kafka-acls $BS $ADM --add --allow-principal User:app --cluster --operation Alter
denied "grant ClusterAction" "authoriz|platform|managed" K kafka-acls $BS $ADM --add --allow-principal User:app --cluster --operation ClusterAction
denied "grant on a reserved topic" "authoriz|reserved" K kafka-acls $BS $ADM --add --allow-principal User:app --operation Read --topic __consumer_offsets
denied "grant to a platform principal (a super user)" "authoriz|platform" K kafka-acls $BS $ADM --add --allow-principal User:broker --operation Read --topic orders
denied "grant with a host restriction" "authoriz|host" K kafka-acls $BS $ADM --add --allow-principal User:app --allow-host 10.0.0.1 --operation Read --topic orders
denied "delete filter that could match cluster ACLs" "authoriz|resource type" K kafka-acls $BS $ADM --remove --allow-principal User:admin1 --force
denied "delete filter on the cluster resource" "authoriz|resource type" K kafka-acls $BS $ADM --remove --allow-principal User:admin1 --cluster --operation Alter --force
denied "alter broker configuration" "authoriz|denied" K kafka-configs $BS $ADM --alter --entity-type brokers --entity-default --add-config log.retention.ms=1000
denied "alter client quotas" "authoriz|denied" K kafka-configs $BS $ADM --alter --entity-type users --entity-name app --add-config producer_byte_rate=1
denied "alter SCRAM credentials" "authoriz|denied" K kafka-configs $BS $ADM --alter --entity-type users --entity-name app --add-config 'SCRAM-SHA-512=[password=x]'
denied "preferred leader election (needs cluster Alter)" "authoriz|denied|needs" K kafka-leader-election $BS --admin.config /it/admin1.properties --election-type preferred --all-topic-partitions
denied "create a reserved topic, although admin1 holds cluster Create" "authoriz|denied|not authorized" K kafka-topics $BS $ADM --create --topic __sneaky --partitions 1 --replication-factor 1
denied "describe the consumer offsets topic (a denial looks like a missing topic)" "authoriz|denied|does not exist" K kafka-topics $BS $ADM --describe --topic __consumer_offsets
denied "unregister a broker" "authoriz|denied|needs" K kafka-cluster unregister $BS --config /it/admin1.properties --id 1
denied "update features (needs cluster Alter)" "authoriz|denied|needs" K kafka-features $BS $ADM upgrade --feature transaction.version=1

echo "== policies"
denied "topic config outside the allowlist" "policy|cannot be set" K kafka-configs $BS $ADM --alter --entity-type topics --entity-name orders --add-config unclean.leader.election.enable=true
denied "min.insync.replicas below the floor" "policy|at least" K kafka-configs $BS $ADM --alter --entity-type topics --entity-name orders --add-config min.insync.replicas=1
denied "too many partitions in one topic" "policy|at most" K kafka-topics $BS $ADM --create --topic huge --partitions 1001 --replication-factor 1
denied "create topic with a forbidden config" "policy|cannot be set" K kafka-topics $BS $ADM --create --topic bad --partitions 1 --replication-factor 1 --config flush.messages=1
ok "super user may do all of it (the platform's own tools keep working)" K kafka-configs $BS $SU --alter --entity-type brokers --entity-default --add-config log.retention.ms=604800000

echo "== an ordinary user"
denied "app cannot create ACLs" "authoriz|denied" K kafka-acls $BS $APP --add --allow-principal User:app --operation Write --topic orders
denied "other (no ACLs) cannot produce" "authoriz" bash -c "echo x | docker exec -i $NAME /opt/kafka/bin/kafka-console-producer.sh $BS --producer.config /it/other.properties --topic orders"

echo
[[ $FAILED -eq 0 ]] && echo "PLUGIN INTEGRATION: PASS" || { echo "PLUGIN INTEGRATION: FAIL"; docker logs "$NAME" 2>&1 | grep -iE "error|exception" | tail -15; exit 1; }
