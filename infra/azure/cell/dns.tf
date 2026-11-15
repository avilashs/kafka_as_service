# Per-broker and bootstrap names of every cluster of the cell live in this zone (ADR-0005). The records are
# created by the cell's DNS controller; the name servers must be delegated from the parent domain by the operator.
resource "azurerm_dns_zone" "cell" {
  name                = "${var.cell_id}.${var.domain}"
  resource_group_name = azurerm_resource_group.this.name
  tags                = local.tags
}
