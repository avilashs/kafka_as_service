package dev.streamcell.kafka;

import java.util.Map;

import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy;

/**
 * Bounds what a tenant can ask for when creating a topic. It runs on the controllers (KRaft), so the jar
 * must be in the controller image too. Topics whose name starts with "__" are the platform's own (Cruise
 * Control metrics, canaries) and are not restricted.
 *
 * <p>Broker settings read: {@code streamcell.topic.min.replication.factor} (default 3),
 * {@code streamcell.topic.max.partitions} (default 1000).
 *
 * <p>Limit on the total number of partitions of a cluster is a separate control (design section 12): this policy
 * cannot see cluster state, so it bounds only the single request.
 */
public class PlatformCreateTopicPolicy implements CreateTopicPolicy {
    private short minReplicationFactor = 3;
    private int maxPartitions = 1000;

    @Override
    public void configure(Map<String, ?> configs) {
        Object rf = configs.get("streamcell.topic.min.replication.factor");
        if (rf != null) minReplicationFactor = Short.parseShort(rf.toString());
        Object mp = configs.get("streamcell.topic.max.partitions");
        if (mp != null) maxPartitions = Integer.parseInt(mp.toString());
    }

    @Override
    public void validate(RequestMetadata r) throws PolicyViolationException {
        if (r.topic() != null && r.topic().startsWith(PlatformAuthorizer.RESERVED_PREFIX)) return;
        int partitions = r.numPartitions() != null ? r.numPartitions() : r.replicasAssignments().size();
        if (partitions > maxPartitions) {
            throw new PolicyViolationException("a topic may have at most " + maxPartitions + " partitions, requested " + partitions);
        }
        int rf = r.replicationFactor() != null ? r.replicationFactor()
                : r.replicasAssignments().values().stream().mapToInt(java.util.List::size).min().orElse(0);
        if (rf < minReplicationFactor) {
            throw new PolicyViolationException("replication factor must be at least " + minReplicationFactor + ", requested " + rf);
        }
        TopicConfigRules.validate(r.configs(), rf);
    }

    @Override
    public void close() {}
}
