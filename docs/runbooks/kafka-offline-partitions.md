# Runbook: Partitions have no leader

**Alert:** `StreamcellOfflinePartitions`

All replicas of some partition are unavailable. Treat as an outage. Check which brokers are down (kafka-broker-down) and bring them back; do not run unclean leader election unless the tenant has agreed to losing the unreplicated tail of the log. The platform sets the cluster DEGRADED; the tenant sees it in the API.
