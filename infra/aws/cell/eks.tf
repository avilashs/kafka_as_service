data "aws_iam_policy_document" "eks_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "cluster" {
  name               = "${local.name}-cluster"
  assume_role_policy = data.aws_iam_policy_document.eks_assume.json
}

resource "aws_iam_role_policy_attachment" "cluster" {
  role       = aws_iam_role.cluster.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/AmazonEKSClusterPolicy"
}

resource "aws_eks_cluster" "this" {
  name     = local.name
  role_arn = aws_iam_role.cluster.arn
  version  = var.kubernetes_version

  vpc_config {
    subnet_ids              = concat([for s in aws_subnet.private : s.id], [for s in aws_subnet.public : s.id])
    endpoint_private_access = true
    endpoint_public_access  = true
    public_access_cidrs     = var.public_access_cidrs
  }

  access_config {
    authentication_mode                         = "API"
    bootstrap_cluster_creator_admin_permissions = true
  }

  encryption_config {
    resources = ["secrets"]
    provider {
      key_arn = aws_kms_key.cell.arn
    }
  }

  enabled_cluster_log_types = ["api", "audit", "authenticator"]

  depends_on = [aws_iam_role_policy_attachment.cluster]
}

# ---- control pool: a managed node group (Karpenter cannot run on nodes it creates) ----

data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "control_node" {
  name               = "${local.name}-control-node"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

resource "aws_iam_role_policy_attachment" "control_node" {
  for_each = toset([
    "AmazonEKSWorkerNodePolicy",
    "AmazonEKS_CNI_Policy",
    "AmazonEC2ContainerRegistryPullOnly",
    "AmazonSSMManagedInstanceCore",
  ])

  role       = aws_iam_role.control_node.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/${each.key}"
}

resource "aws_launch_template" "control" {
  name_prefix = "${local.name}-control-"

  # IMDSv2 only, hop limit 1: a pod cannot reach the instance role through the metadata service.
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_size           = 50
      volume_type           = "gp3"
      encrypted             = true
      delete_on_termination = true
    }
  }
}

resource "aws_eks_node_group" "control" {
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "control"
  node_role_arn   = aws_iam_role.control_node.arn
  subnet_ids      = [for s in aws_subnet.private : s.id]
  ami_type        = "AL2023_x86_64_STANDARD"
  instance_types  = [var.control_instance_type]

  scaling_config {
    desired_size = var.control_node_count
    min_size     = var.control_node_count
    max_size     = var.control_node_count + 3
  }

  update_config {
    max_unavailable = 1
  }

  launch_template {
    id      = aws_launch_template.control.id
    version = aws_launch_template.control.latest_version
  }

  labels = {
    "streamcell.dev/pool" = "control"
  }

  taint {
    key    = "streamcell.dev/control"
    value  = "true"
    effect = "NO_SCHEDULE"
  }

  depends_on = [aws_iam_role_policy_attachment.control_node]

  lifecycle {
    ignore_changes = [scaling_config[0].desired_size]
  }
}

# ---- add-ons ----
# EKS installs vpc-cni, kube-proxy and CoreDNS itself; taking them over as add-ons needs OVERWRITE.
# NetworkPolicy enforcement is the VPC CNI's own (every tenant namespace is default-deny). Cilium, which the
# design names for the Bandwidth Manager, is NOT used here: see docs/milestones/m6.md.

resource "aws_eks_addon" "vpc_cni" {
  cluster_name                = aws_eks_cluster.this.name
  addon_name                  = "vpc-cni"
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"
  configuration_values        = jsonencode({ enableNetworkPolicy = "true" })

  depends_on = [aws_eks_node_group.control]
}

resource "aws_eks_addon" "kube_proxy" {
  cluster_name                = aws_eks_cluster.this.name
  addon_name                  = "kube-proxy"
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"

  depends_on = [aws_eks_node_group.control]
}

resource "aws_eks_addon" "coredns" {
  cluster_name                = aws_eks_cluster.this.name
  addon_name                  = "coredns"
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"
  configuration_values = jsonencode({
    tolerations  = [{ key = "streamcell.dev/control", operator = "Exists", effect = "NoSchedule" }]
    nodeSelector = { "streamcell.dev/pool" = "control" }
  })

  depends_on = [aws_eks_node_group.control]
}

resource "aws_eks_addon" "pod_identity" {
  cluster_name                = aws_eks_cluster.this.name
  addon_name                  = "eks-pod-identity-agent"
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"

  depends_on = [aws_eks_node_group.control]
}
