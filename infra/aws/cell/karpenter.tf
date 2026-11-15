# Karpenter's AWS side (ADR-0008). The controller policies are the ones of the upstream CloudFormation template
# (vendored under infra/aws/policies, rendered with this cell's values). The controller itself, the EC2NodeClass
# and the NodePools are installed by deploy/aws/bootstrap.sh from the outputs below.

resource "aws_iam_role" "karpenter_node" {
  name               = "${local.name}-karpenter-node"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

resource "aws_iam_role_policy_attachment" "karpenter_node" {
  for_each = toset([
    "AmazonEKSWorkerNodePolicy",
    "AmazonEKS_CNI_Policy",
    "AmazonEC2ContainerRegistryPullOnly",
    "AmazonSSMManagedInstanceCore",
  ])

  role       = aws_iam_role.karpenter_node.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/${each.key}"
}

# Nodes created by Karpenter join through an access entry (the cluster uses API authentication mode).
resource "aws_eks_access_entry" "karpenter_node" {
  cluster_name  = aws_eks_cluster.this.name
  principal_arn = aws_iam_role.karpenter_node.arn
  type          = "EC2_LINUX"
}

resource "aws_iam_role" "karpenter" {
  name               = "${local.name}-karpenter"
  assume_role_policy = data.aws_iam_policy_document.pods_assume.json
}

locals {
  karpenter_policies = toset([
    "NodeLifecyclePolicy",
    "IAMIntegrationPolicy",
    "EKSIntegrationPolicy",
    "InterruptionPolicy",
    "ZonalShiftPolicy",
    "ResourceDiscoveryPolicy",
  ])
}

resource "aws_iam_policy" "karpenter" {
  for_each = local.karpenter_policies

  name = "${local.name}-karpenter-${each.key}"
  policy = templatefile("${path.module}/../policies/karpenter-${each.key}.json.tftpl", {
    partition              = local.partition
    region                 = var.region
    account_id             = local.account_id
    url_suffix             = "amazonaws.com"
    cluster_name           = local.name
    node_role_arn          = aws_iam_role.karpenter_node.arn
    interruption_queue_arn = aws_sqs_queue.karpenter.arn
  })
}

resource "aws_iam_role_policy_attachment" "karpenter" {
  for_each = local.karpenter_policies

  role       = aws_iam_role.karpenter.name
  policy_arn = aws_iam_policy.karpenter[each.key].arn
}

resource "aws_eks_pod_identity_association" "karpenter" {
  cluster_name    = aws_eks_cluster.this.name
  namespace       = "kube-system"
  service_account = "karpenter"
  role_arn        = aws_iam_role.karpenter.arn

  depends_on = [aws_eks_addon.pod_identity]
}

# ---- interruption handling: EC2 events reach Karpenter through an SQS queue ----

resource "aws_sqs_queue" "karpenter" {
  name                      = local.name
  message_retention_seconds = 300
  sqs_managed_sse_enabled   = true
}

data "aws_iam_policy_document" "karpenter_queue" {
  statement {
    sid       = "AllowEventsAndSqs"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.karpenter.arn]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com", "sqs.amazonaws.com"]
    }
  }

  statement {
    sid       = "DenyHTTP"
    effect    = "Deny"
    actions   = ["sqs:*"]
    resources = [aws_sqs_queue.karpenter.arn]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_sqs_queue_policy" "karpenter" {
  queue_url = aws_sqs_queue.karpenter.id
  policy    = data.aws_iam_policy_document.karpenter_queue.json
}

locals {
  interruption_events = {
    scheduled-change  = { source = "aws.health", detail_type = "AWS Health Event" }
    spot-interruption = { source = "aws.ec2", detail_type = "EC2 Spot Instance Interruption Warning" }
    rebalance         = { source = "aws.ec2", detail_type = "EC2 Instance Rebalance Recommendation" }
    instance-state    = { source = "aws.ec2", detail_type = "EC2 Instance State-change Notification" }
    capacity-reserved = { source = "aws.ec2", detail_type = "EC2 Capacity Reservation Instance Interruption Warning" }
  }
}

resource "aws_cloudwatch_event_rule" "karpenter" {
  for_each = local.interruption_events

  name = "${local.name}-${each.key}"
  event_pattern = jsonencode({
    source        = [each.value.source]
    "detail-type" = [each.value.detail_type]
  })
}

resource "aws_cloudwatch_event_target" "karpenter" {
  for_each = local.interruption_events

  rule = aws_cloudwatch_event_rule.karpenter[each.key].name
  arn  = aws_sqs_queue.karpenter.arn
}
