package dev.streamcell.kafka;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.errors.PolicyViolationException;

/**
 * The topic-level configuration a tenant may set, with bounds. Anything not listed is refused: the list
 * is explicit (design section 15.1) so a new Kafka version cannot silently add a way around it.
 */
final class TopicConfigRules {
    private TopicConfigRules() {}

    /** Settable by tenants, any value that parses. */
    private static final Set<String> FREE = Set.of(
            "cleanup.policy", "compression.type", "delete.retention.ms", "max.compaction.lag.ms",
            "min.compaction.lag.ms", "message.timestamp.type", "message.timestamp.before.max.ms",
            "message.timestamp.after.max.ms", "retention.ms", "segment.ms", "segment.jitter.ms",
            "min.cleanable.dirty.ratio", "preallocate", "message.downconversion.enable",
            "max.message.bytes", "segment.bytes", "retention.bytes", "file.delete.delay.ms",
            "index.interval.bytes", "segment.index.bytes", "flush.messages", "flush.ms", "min.insync.replicas");

    /** The free settings whose value is a plain number. */
    private static final Set<String> NUMERIC = Set.of(
            "delete.retention.ms", "max.compaction.lag.ms", "min.compaction.lag.ms", "message.timestamp.before.max.ms",
            "message.timestamp.after.max.ms", "retention.ms", "segment.ms", "segment.jitter.ms", "retention.bytes",
            "file.delete.delay.ms", "index.interval.bytes", "segment.index.bytes");

    static final long MAX_MESSAGE_BYTES = 8L * 1024 * 1024;
    static final long MIN_SEGMENT_BYTES = 1024 * 1024;

    static void validate(Map<String, String> configs, int replicationFactor) {
        for (Map.Entry<String, String> e : configs.entrySet()) {
            String k = e.getKey();
            if (!FREE.contains(k)) {
                throw new PolicyViolationException("topic configuration \"" + k + "\" cannot be set");
            }
            String v = e.getValue();
            if (v == null) continue; // a delete of an override
            try {
                if (NUMERIC.contains(k)) Long.parseLong(v);
                switch (k) {
                    case "min.insync.replicas": {
                        long n = Long.parseLong(v);
                        // replicationFactor 0 means "unknown" (an alter on an existing topic): apply the floor of 2
                        if (n < (replicationFactor == 0 ? 2 : Math.min(2, replicationFactor))) {
                            throw new PolicyViolationException("min.insync.replicas must be at least 2");
                        }
                        if (replicationFactor > 0 && n > replicationFactor) {
                            throw new PolicyViolationException("min.insync.replicas cannot exceed the replication factor");
                        }
                        break;
                    }
                    case "max.message.bytes":
                        if (Long.parseLong(v) > MAX_MESSAGE_BYTES) {
                            throw new PolicyViolationException("max.message.bytes may be at most " + MAX_MESSAGE_BYTES);
                        }
                        break;
                    case "segment.bytes":
                        if (Long.parseLong(v) < MIN_SEGMENT_BYTES) {
                            throw new PolicyViolationException("segment.bytes must be at least " + MIN_SEGMENT_BYTES);
                        }
                        break;
                    case "flush.messages":
                    case "flush.ms": // forcing fsync per message would turn one tenant into a disk-bound neighbour
                        if (Long.parseLong(v) < 1000) {
                            throw new PolicyViolationException(k + " must be at least 1000 (frequent fsync hurts every tenant on the broker)");
                        }
                        break;
                    default:
                        break;
                }
            } catch (NumberFormatException ex) {
                throw new PolicyViolationException("value of " + k + " must be a number");
            }
        }
    }
}
