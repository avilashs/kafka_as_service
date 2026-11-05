# Runbook: Partitions are below min.insync.replicas

**Alert:** `StreamcellUnderMinIsr`

Producers with acks=all get NOT_ENOUGH_REPLICAS for the affected partitions. Find the brokers that are not in sync: `kubectl -n <cluster> exec <broker> -c kafka -- bin/kafka-topics.sh --bootstrap-server localhost:9091 --describe --under-min-isr-partitions` (Strimzi's internal listener needs the cluster's own certificates; use the platform tooling where available). Usually a roll or a node loss is in progress: wait for Strimzi. If two brokers are down at once, find out why before touching anything; never lower min.insync.replicas to make the alert go away.
