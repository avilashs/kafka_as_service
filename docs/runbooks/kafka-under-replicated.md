# Runbook: Partitions have been under-replicated for ten minutes

**Alert:** `StreamcellUnderReplicated`

Normal during a roll or a scale operation. Check for a running operation on the cluster (`GET /v1/clusters/<id>` shows currentOperationId). If none: a broker is slow or its disk is full (see RB-disk-full), or a network path between brokers is impaired. Check broker logs for replica fetch errors.
