# Runbook: A broker pod does not answer its metrics endpoint

**Alert:** `StreamcellBrokerMetricsDown`

Find which pod: `kubectl -n <cluster> get pods -l strimzi.io/kind=Kafka`. A pod that is Pending usually has no node it may run on (placement rules, see the placement runbooks); CrashLoopBackOff: read its log. If the pod is Running and Ready but the alert fires, the metrics NetworkPolicy of the namespace or the metrics reporter is broken: `kubectl -n <cluster> get networkpolicy`. One broker down does not stop clients (replication factor 3, min ISR 2); two brokers down does, see kafka-under-min-isr.
