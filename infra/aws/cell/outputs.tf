output "cluster_name" {
  value = aws_eks_cluster.this.name
}

output "cluster_endpoint" {
  value = aws_eks_cluster.this.endpoint
}

output "region" {
  value = var.region
}

output "azs" {
  value = var.azs
}

output "vpc_id" {
  value = aws_vpc.this.id
}

output "private_subnet_ids" {
  value = { for az, s in aws_subnet.private : az => s.id }
}

output "public_subnet_ids" {
  value = { for az, s in aws_subnet.public : az => s.id }
}

output "private_nacl_ids" {
  description = "Network ACL of each zone's private subnet; the zone-outage test adds deny rules to one of them."
  value       = { for az, n in aws_network_acl.private : az => n.id }
}

output "karpenter_node_role_name" {
  value = aws_iam_role.karpenter_node.name
}

output "karpenter_interruption_queue" {
  value = aws_sqs_queue.karpenter.name
}

output "kms_key_arn" {
  value = aws_kms_key.cell.arn
}

output "route53_zone_id" {
  value = aws_route53_zone.cell.zone_id
}

output "route53_name_servers" {
  value = aws_route53_zone.cell.name_servers
}

output "domain" {
  value = aws_route53_zone.cell.name
}
