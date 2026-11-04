package dev.streamcell.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.policy.AlterConfigPolicy;
import org.apache.kafka.server.policy.CreateTopicPolicy;
import org.junit.jupiter.api.Test;

class PlatformAuthorizerTest {
    private static Action act(AclOperation op, ResourceType t, String name) {
        return new Action(op, new ResourcePattern(t, name, PatternType.LITERAL), 1, true, true);
    }

    private static boolean denied(int api, AclOperation op, ResourceType t, String name) {
        return PlatformAuthorizer.denied(api, act(op, t, name));
    }

    @Test
    void clusterAlterIsOnlyForAclManagement() {
        assertFalse(denied(ApiKeys.CREATE_ACLS.id, AclOperation.ALTER, ResourceType.CLUSTER, "kafka-cluster"));
        assertFalse(denied(ApiKeys.DELETE_ACLS.id, AclOperation.ALTER, ResourceType.CLUSTER, "kafka-cluster"));
        for (ApiKeys k : new ApiKeys[] {ApiKeys.ALTER_PARTITION_REASSIGNMENTS, ApiKeys.ELECT_LEADERS, ApiKeys.UPDATE_FEATURES,
                ApiKeys.ALTER_REPLICA_LOG_DIRS, ApiKeys.ALTER_USER_SCRAM_CREDENTIALS, ApiKeys.UNREGISTER_BROKER}) {
            assertTrue(denied(k.id, AclOperation.ALTER, ResourceType.CLUSTER, "kafka-cluster"), k.name);
        }
    }

    @Test
    void brokerConfigurationAndBrokerActionsAreDeniedForEveryRequest() {
        for (ApiKeys k : new ApiKeys[] {ApiKeys.CREATE_ACLS, ApiKeys.INCREMENTAL_ALTER_CONFIGS, ApiKeys.ALTER_CONFIGS, ApiKeys.ALTER_CLIENT_QUOTAS}) {
            assertTrue(denied(k.id, AclOperation.ALTER_CONFIGS, ResourceType.CLUSTER, "kafka-cluster"));
            assertTrue(denied(k.id, AclOperation.CLUSTER_ACTION, ResourceType.CLUSTER, "kafka-cluster"));
        }
    }

    @Test
    void clusterWideCreateIsDeniedBecauseItBypassesPerTopicChecks() {
        assertTrue(denied(ApiKeys.CREATE_TOPICS.id, AclOperation.CREATE, ResourceType.CLUSTER, "kafka-cluster"));
        assertFalse(denied(ApiKeys.CREATE_TOPICS.id, AclOperation.CREATE, ResourceType.TOPIC, "orders"));
        assertTrue(denied(ApiKeys.CREATE_TOPICS.id, AclOperation.CREATE, ResourceType.TOPIC, "__sneaky"));
    }

    @Test
    void everydayClusterOperationsAreLeftToAcls() {
        int api = ApiKeys.CREATE_TOPICS.id;
        for (AclOperation op : new AclOperation[] {AclOperation.DESCRIBE, AclOperation.DESCRIBE_CONFIGS, AclOperation.IDEMPOTENT_WRITE}) {
            assertFalse(denied(api, op, ResourceType.CLUSTER, "kafka-cluster"), op.name());
        }
    }

    @Test
    void reservedAndUnsupportedResources() {
        int api = ApiKeys.PRODUCE.id;
        for (ResourceType t : new ResourceType[] {ResourceType.TOPIC, ResourceType.GROUP, ResourceType.TRANSACTIONAL_ID}) {
            assertTrue(denied(api, AclOperation.READ, t, "__consumer_offsets"));
            assertTrue(denied(api, AclOperation.DESCRIBE, t, "__platform-canary"));
            assertFalse(denied(api, AclOperation.READ, t, "orders"));
            assertFalse(denied(api, AclOperation.READ, t, "a__b"));
        }
        assertTrue(denied(api, AclOperation.ALTER, ResourceType.USER, "alice"));
        assertTrue(denied(api, AclOperation.DESCRIBE, ResourceType.DELEGATION_TOKEN, "t"));
    }

    private static PlatformAuthorizer authorizerWithSuperUsers(String supers) {
        PlatformAuthorizer a = new PlatformAuthorizer();
        // Only the part of configure() that does not need a running broker.
        a.superUsersForTest(supers);
        return a;
    }

    private static AclBinding binding(ResourceType t, String name, PatternType pt, String principal, String host, AclOperation op) {
        return new AclBinding(new ResourcePattern(t, name, pt), new AccessControlEntry(principal, host, op, AclPermissionType.ALLOW));
    }

    @Test
    void tenantsMayGrantOnlyTheirOwnKindsOfAccess() {
        PlatformAuthorizer a = authorizerWithSuperUsers("User:CN=kc-1-entity-user-operator;User:broker");
        assertNull(a.rejectBinding(binding(ResourceType.TOPIC, "orders", PatternType.LITERAL, "User:app", "*", AclOperation.READ)));
        assertNull(a.rejectBinding(binding(ResourceType.TOPIC, "orders-", PatternType.PREFIXED, "User:app", "*", AclOperation.ALL)));
        assertNull(a.rejectBinding(binding(ResourceType.GROUP, "g", PatternType.LITERAL, "User:*", "*", AclOperation.READ)));
        assertNull(a.rejectBinding(binding(ResourceType.TRANSACTIONAL_ID, "tx", PatternType.LITERAL, "User:app", "*", AclOperation.WRITE)));
        // cluster-level, delegation tokens, users
        assertNotNull(a.rejectBinding(binding(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL, "User:app", "*", AclOperation.ALTER)));
        assertNotNull(a.rejectBinding(binding(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL, "User:app", "*", AclOperation.CLUSTER_ACTION)));
        assertNotNull(a.rejectBinding(binding(ResourceType.DELEGATION_TOKEN, "t", PatternType.LITERAL, "User:app", "*", AclOperation.ALL)));
        // reserved names
        assertNotNull(a.rejectBinding(binding(ResourceType.TOPIC, "__consumer_offsets", PatternType.LITERAL, "User:app", "*", AclOperation.READ)));
        assertNotNull(a.rejectBinding(binding(ResourceType.TOPIC, "__", PatternType.PREFIXED, "User:app", "*", AclOperation.READ)));
        assertNotNull(a.rejectBinding(binding(ResourceType.TOPIC, "", PatternType.PREFIXED, "User:app", "*", AclOperation.READ)));
        // platform principals
        assertNotNull(a.rejectBinding(binding(ResourceType.TOPIC, "orders", PatternType.LITERAL, "User:broker", "*", AclOperation.READ)));
        assertNotNull(a.rejectBinding(binding(ResourceType.TOPIC, "orders", PatternType.LITERAL, "User:CN=kc-1-entity-user-operator", "*", AclOperation.READ)));
        assertNotNull(a.rejectBinding(binding(ResourceType.TOPIC, "orders", PatternType.LITERAL, "User:platform-canary", "*", AclOperation.READ)));
        assertNotNull(a.rejectBinding(binding(ResourceType.TOPIC, "orders", PatternType.LITERAL, "Group:x", "*", AclOperation.READ)));
        // host restrictions are meaningless behind the gateway
        assertNotNull(a.rejectBinding(binding(ResourceType.TOPIC, "orders", PatternType.LITERAL, "User:app", "10.0.0.1", AclOperation.READ)));
    }

    // ---------- policies ----------

    private static CreateTopicPolicy.RequestMetadata create(String topic, Integer parts, Short rf, Map<String, String> cfg) {
        return new CreateTopicPolicy.RequestMetadata(topic, parts, rf, Map.of(), cfg);
    }

    @Test
    void createTopicPolicy() {
        PlatformCreateTopicPolicy p = new PlatformCreateTopicPolicy();
        p.configure(Map.of());
        p.validate(create("orders", 12, (short) 3, Map.of("retention.ms", "86400000", "min.insync.replicas", "2")));
        p.validate(create("__CruiseControlMetrics", 32, (short) 1, Map.of("anything", "goes"))); // platform topics are exempt
        assertThrows(PolicyViolationException.class, () -> p.validate(create("t", 1, (short) 2, Map.of())));
        assertThrows(PolicyViolationException.class, () -> p.validate(create("t", 1001, (short) 3, Map.of())));
        assertThrows(PolicyViolationException.class, () -> p.validate(create("t", 1, (short) 3, Map.of("unclean.leader.election.enable", "true"))));
        assertThrows(PolicyViolationException.class, () -> p.validate(create("t", 1, (short) 3, Map.of("min.insync.replicas", "1"))));
        assertThrows(PolicyViolationException.class, () -> p.validate(create("t", 1, (short) 3, Map.of("min.insync.replicas", "4"))));
        assertThrows(PolicyViolationException.class, () -> p.validate(create("t", 1, (short) 3, Map.of("flush.messages", "1"))));
        assertThrows(PolicyViolationException.class, () -> p.validate(create("t", 1, (short) 3, Map.of("max.message.bytes", "999999999"))));
        assertThrows(PolicyViolationException.class, () -> p.validate(create("t", 1, (short) 3, Map.of("segment.bytes", "10"))));
        assertThrows(PolicyViolationException.class, () -> p.validate(create("t", 1, (short) 3, Map.of("retention.ms", "soon"))));
    }

    @Test
    void createTopicPolicyHonoursBrokerSettings() {
        PlatformCreateTopicPolicy p = new PlatformCreateTopicPolicy();
        p.configure(Map.of("streamcell.topic.max.partitions", "10", "streamcell.topic.min.replication.factor", "1"));
        p.validate(create("t", 10, (short) 1, Map.of()));
        assertThrows(PolicyViolationException.class, () -> p.validate(create("t", 11, (short) 1, Map.of())));
    }

    @Test
    void alterConfigPolicy() {
        PlatformAlterConfigPolicy p = new PlatformAlterConfigPolicy();
        ConfigResource topic = new ConfigResource(ConfigResource.Type.TOPIC, "orders");
        p.validate(new AlterConfigPolicy.RequestMetadata(topic, Map.of("retention.ms", "60000")));
        assertThrows(PolicyViolationException.class, () -> p.validate(new AlterConfigPolicy.RequestMetadata(topic, Map.of("unclean.leader.election.enable", "true"))));
        assertThrows(PolicyViolationException.class, () -> p.validate(new AlterConfigPolicy.RequestMetadata(topic, Map.of("min.insync.replicas", "1"))));
        // broker configs are not this policy's business (the authorizer keeps tenants away; Strimzi and Cruise Control need them)
        p.validate(new AlterConfigPolicy.RequestMetadata(new ConfigResource(ConfigResource.Type.BROKER, "1"), Map.of("leader.replication.throttled.rate", "1000")));
        // platform topics
        p.validate(new AlterConfigPolicy.RequestMetadata(new ConfigResource(ConfigResource.Type.TOPIC, "__CruiseControlMetrics"), Map.of("retention.ms", "1")));
        assertEquals(1, 1);
    }
}
