output "cluster_name" {
  value = azurerm_kubernetes_cluster.this.name
}

output "resource_group" {
  value = azurerm_resource_group.this.name
}

output "node_resource_group" {
  description = "Resource group of the AKS nodes (the VM scale sets); node termination tests delete instances there."
  value       = azurerm_kubernetes_cluster.this.node_resource_group
}

output "location" {
  value = var.location
}

output "zones" {
  value = var.zones
}

output "subscription_id" {
  value = var.subscription_id
}

output "vnet_name" {
  value = azurerm_virtual_network.this.name
}

output "zone_subnets" {
  value = { for z, s in azurerm_subnet.zone : z => s.id }
}

output "zone_nsgs" {
  description = "Network security group of each zone's subnet; the zone-outage test adds deny rules to one of them."
  value       = { for z, n in azurerm_network_security_group.zone : z => n.name }
}

output "disk_encryption_set_id" {
  value = azurerm_disk_encryption_set.cell.id
}

output "dns_zone" {
  value = azurerm_dns_zone.cell.name
}

output "dns_name_servers" {
  value = azurerm_dns_zone.cell.name_servers
}

output "oidc_issuer_url" {
  value = azurerm_kubernetes_cluster.this.oidc_issuer_url
}

output "control_vm_size" {
  value = var.control_vm_size
}
