# AKS with Azure CNI in overlay mode and the Cilium data plane (the design names Cilium): NetworkPolicy is enforced
# by Cilium, every tenant namespace is default-deny. Node Auto Provisioning (Karpenter) creates the nodes of
# dedicated clusters (ADR-0008); the system, control and shared pools are AKS node pools.

resource "azurerm_kubernetes_cluster" "this" {
  name                = local.name
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  dns_prefix          = local.name
  kubernetes_version  = var.kubernetes_version
  sku_tier            = "Standard" # financially backed API server SLA, uptime SLA across zones

  disk_encryption_set_id            = azurerm_disk_encryption_set.cell.id
  oidc_issuer_enabled               = true
  workload_identity_enabled         = true
  local_account_disabled            = false
  role_based_access_control_enabled = true

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.cluster.id]
  }

  default_node_pool {
    name                         = "system"
    vm_size                      = var.system_vm_size
    zones                        = var.zones
    vnet_subnet_id               = azurerm_subnet.system.id
    auto_scaling_enabled         = true
    min_count                    = 3
    max_count                    = 6
    only_critical_addons_enabled = true
    host_encryption_enabled      = false
    os_disk_type                 = "Managed"
    os_disk_size_gb              = 64
    temporary_name_for_rotation  = "systemtmp"

    upgrade_settings {
      max_surge = "33%"
    }
  }

  network_profile {
    network_plugin      = "azure"
    network_plugin_mode = "overlay"
    network_data_plane  = "cilium"
    network_policy      = "cilium"
    load_balancer_sku   = "standard"
    outbound_type       = "userAssignedNATGateway"
  }

  api_server_access_profile {
    authorized_ip_ranges = var.authorized_ip_ranges
  }

  node_provisioning_profile {
    mode               = "Auto"
    default_node_pools = "None"
  }

  tags = local.tags

  depends_on = [
    azurerm_role_assignment.cluster_network,
    azurerm_role_assignment.cluster_des,
    azurerm_role_assignment.des_key,
    azurerm_subnet_nat_gateway_association.system,
  ]

  lifecycle {
    ignore_changes = [default_node_pool[0].node_count]
  }
}

# ---- control pool: one tainted node per zone (operators, gateway, controllers, platform components) ----

resource "azurerm_kubernetes_cluster_node_pool" "control" {
  for_each = local.zone_index

  name                  = "ctrl${each.key}"
  kubernetes_cluster_id = azurerm_kubernetes_cluster.this.id
  mode                  = "User"
  vm_size               = var.control_vm_size
  zones                 = [each.key]
  vnet_subnet_id        = azurerm_subnet.zone[each.key].id
  node_count            = 1
  os_disk_type          = "Managed"
  os_disk_size_gb       = 64

  node_labels = {
    "streamcell.dev/pool" = "control"
  }
  node_taints = ["streamcell.dev/control=true:NoSchedule"]
  tags        = local.tags

  upgrade_settings {
    max_surge = "1"
  }

  depends_on = [azurerm_subnet_nat_gateway_association.zone, azurerm_subnet_network_security_group_association.zone]
}

# ---- shared pool: one autoscaled pool per zone, so that a zone's nodes sit in that zone's subnet ----

resource "azurerm_kubernetes_cluster_node_pool" "shared" {
  for_each = local.zone_index

  name                  = "shared${each.key}"
  kubernetes_cluster_id = azurerm_kubernetes_cluster.this.id
  mode                  = "User"
  vm_size               = var.shared_vm_size
  zones                 = [each.key]
  vnet_subnet_id        = azurerm_subnet.zone[each.key].id
  auto_scaling_enabled  = true
  min_count             = var.shared_min_nodes_per_zone
  max_count             = var.shared_max_nodes_per_zone
  os_disk_type          = "Managed"
  os_disk_size_gb       = 128

  node_labels = {
    "streamcell.dev/pool" = "shared"
  }
  tags = local.tags

  upgrade_settings {
    max_surge = "1"
  }

  lifecycle {
    ignore_changes = [node_count]
  }

  depends_on = [azurerm_subnet_nat_gateway_association.zone, azurerm_subnet_network_security_group_association.zone]
}
