provider "azurerm" {
  subscription_id = var.subscription_id

  features {
    key_vault {
      # A destroyed test cell must not leave a soft-deleted vault behind that blocks the name.
      purge_soft_delete_on_destroy = true
    }
  }
}

data "azurerm_client_config" "current" {}

locals {
  name = "streamcell-${var.cell_id}"
  tags = merge(var.tags, {
    "streamcell.dev/cell" = var.cell_id
    "managed-by"          = "opentofu"
  })
  zone_index = { for i, z in var.zones : z => i }
}

resource "azurerm_resource_group" "this" {
  name     = local.name
  location = var.location
  tags     = local.tags
}
