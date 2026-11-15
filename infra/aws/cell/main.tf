provider "aws" {
  region = var.region

  default_tags {
    tags = merge(var.tags, {
      "streamcell.dev/cell" = var.cell_id
      "managed-by"          = "opentofu"
    })
  }
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  name       = "streamcell-${var.cell_id}"
  partition  = data.aws_partition.current.partition
  account_id = data.aws_caller_identity.current.account_id
  az_index   = { for i, az in var.azs : az => i }
}

# One key for EKS secrets and for the data volumes of the brokers (the EBS CSI StorageClass refers to it).
# Root volumes of nodes use the default EBS key: a customer key there needs grants for the Auto Scaling service
# linked role, a pitfall that is not worth its risk for the node disk.
resource "aws_kms_key" "cell" {
  description             = "Streamcell cell ${var.cell_id}: EKS secrets and broker volumes"
  enable_key_rotation     = true
  deletion_window_in_days = 14
}

resource "aws_kms_alias" "cell" {
  name          = "alias/${local.name}"
  target_key_id = aws_kms_key.cell.key_id
}
