package dev.streamcell.kafka;

import java.util.Map;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.AlterConfigPolicy;

/**
 * Applies {@link TopicConfigRules} to topic configuration changes. Broker configuration is deliberately not
 * judged here: tenants cannot change it at all ({@link PlatformAuthorizer} denies cluster AlterConfigs), and
 * the platform's own tooling (Strimzi, Cruise Control throttles) must be able to.
 */
public class PlatformAlterConfigPolicy implements AlterConfigPolicy {
    @Override
    public void configure(Map<String, ?> configs) {}

    @Override
    public void validate(RequestMetadata r) throws PolicyViolationException {
        if (r.resource().type() != ConfigResource.Type.TOPIC) return;
        String topic = r.resource().name();
        if (topic != null && PlatformAuthorizer.reserved(topic)) return;
        TopicConfigRules.validate(r.configs(), 0);
    }

    @Override
    public void close() {}
}
