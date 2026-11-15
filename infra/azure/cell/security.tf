# Identity of the cluster (it needs rights on the network before the cluster exists), a key for the data volumes
# of the brokers, and a disk encryption set that refers to it (the StorageClass of the cell names the set).

resource "azurerm_user_assigned_identity" "cluster" {
  name                = "${local.name}-cluster"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  tags                = local.tags
}

resource "azurerm_role_assignment" "cluster_network" {
  scope                = azurerm_virtual_network.this.id
  role_definition_name = "Network Contributor"
  principal_id         = azurerm_user_assigned_identity.cluster.principal_id
}

resource "azurerm_key_vault" "cell" {
  name                       = substr(replace("sc${var.cell_id}${substr(sha1(azurerm_resource_group.this.id), 0, 6)}", "-", ""), 0, 24)
  location                   = azurerm_resource_group.this.location
  resource_group_name        = azurerm_resource_group.this.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  rbac_authorization_enabled = true
  purge_protection_enabled   = true # required by disk encryption sets
  soft_delete_retention_days = 7
  tags                       = local.tags
}

resource "azurerm_role_assignment" "deployer_keys" {
  scope                = azurerm_key_vault.cell.id
  role_definition_name = "Key Vault Crypto Officer"
  principal_id         = data.azurerm_client_config.current.object_id
}

resource "azurerm_key_vault_key" "disks" {
  name         = "disks"
  key_vault_id = azurerm_key_vault.cell.id
  key_type     = "RSA"
  key_size     = 3072
  key_opts     = ["decrypt", "encrypt", "sign", "unwrapKey", "verify", "wrapKey"]

  depends_on = [azurerm_role_assignment.deployer_keys]
}

resource "azurerm_disk_encryption_set" "cell" {
  name                = local.name
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  key_vault_key_id    = azurerm_key_vault_key.disks.versionless_id
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "des_key" {
  scope                = azurerm_key_vault.cell.id
  role_definition_name = "Key Vault Crypto Service Encryption User"
  principal_id         = azurerm_disk_encryption_set.cell.identity[0].principal_id
}

# The cluster identity creates disks that use the set.
resource "azurerm_role_assignment" "cluster_des" {
  scope                = azurerm_disk_encryption_set.cell.id
  role_definition_name = "Contributor"
  principal_id         = azurerm_user_assigned_identity.cluster.principal_id
}
