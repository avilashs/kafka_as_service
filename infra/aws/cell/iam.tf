# Pod Identity (no OIDC provider): a role is bound to a service account of the cluster.
data "aws_iam_policy_document" "pods_assume" {
  statement {
    actions = ["sts:AssumeRole", "sts:TagSession"]
    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }
  }
}

# ---- EBS CSI driver ----

resource "aws_iam_role" "ebs_csi" {
  name               = "${local.name}-ebs-csi"
  assume_role_policy = data.aws_iam_policy_document.pods_assume.json
}

resource "aws_iam_role_policy_attachment" "ebs_csi" {
  role       = aws_iam_role.ebs_csi.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}

# Encrypted volumes with the cell key: the driver needs to create grants for it.
data "aws_iam_policy_document" "ebs_csi_kms" {
  statement {
    actions   = ["kms:Decrypt", "kms:GenerateDataKeyWithoutPlaintext", "kms:CreateGrant", "kms:DescribeKey", "kms:ReEncrypt*"]
    resources = [aws_kms_key.cell.arn]
  }
}

resource "aws_iam_role_policy" "ebs_csi_kms" {
  name   = "cell-key"
  role   = aws_iam_role.ebs_csi.id
  policy = data.aws_iam_policy_document.ebs_csi_kms.json
}

resource "aws_eks_addon" "ebs_csi" {
  cluster_name                = aws_eks_cluster.this.name
  addon_name                  = "aws-ebs-csi-driver"
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"
  configuration_values = jsonencode({
    controller = {
      tolerations  = [{ key = "streamcell.dev/control", operator = "Exists", effect = "NoSchedule" }]
      nodeSelector = { "streamcell.dev/pool" = "control" }
    }
  })

  depends_on = [aws_eks_node_group.control, aws_eks_addon.pod_identity]
}

resource "aws_eks_pod_identity_association" "ebs_csi" {
  cluster_name    = aws_eks_cluster.this.name
  namespace       = "kube-system"
  service_account = "ebs-csi-controller-sa"
  role_arn        = aws_iam_role.ebs_csi.arn

  depends_on = [aws_eks_addon.pod_identity]
}

# ---- AWS Load Balancer Controller (the cell's NLB) ----

resource "aws_iam_role" "lbc" {
  name               = "${local.name}-lbc"
  assume_role_policy = data.aws_iam_policy_document.pods_assume.json
}

# The policy is the one published with the controller (vendored, Apache-2.0, see docs/dependencies.md).
resource "aws_iam_policy" "lbc" {
  name   = "${local.name}-lbc"
  policy = file("${path.module}/../policies/aws-load-balancer-controller.json")
}

resource "aws_iam_role_policy_attachment" "lbc" {
  role       = aws_iam_role.lbc.name
  policy_arn = aws_iam_policy.lbc.arn
}

resource "aws_eks_pod_identity_association" "lbc" {
  cluster_name    = aws_eks_cluster.this.name
  namespace       = "kube-system"
  service_account = "aws-load-balancer-controller"
  role_arn        = aws_iam_role.lbc.arn

  depends_on = [aws_eks_addon.pod_identity]
}
