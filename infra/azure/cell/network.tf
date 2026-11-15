# One subnet per zone for the control and shared pools, each with its own network security group and NAT gateway,
# plus a subnet for the system pool. The zone-outage test inserts deny rules (priority 100) into one zone's NSG
# with the Azure CLI and removes them afterwards (hack/azure/chaos.sh): the module creates the NSGs without custom
# rules and never manages rules, so nothing here fights with the test.

resource "azurerm_virtual_network" "this" {
  name                = local.name
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  address_space       = [var.vnet_cidr]
  tags                = local.tags
}

resource "azurerm_subnet" "system" {
  name                 = "system"
  resource_group_name  = azurerm_resource_group.this.name
  virtual_network_name = azurerm_virtual_network.this.name
  address_prefixes     = [cidrsubnet(var.vnet_cidr, 8, 0)]
}

resource "azurerm_subnet" "zone" {
  for_each = local.zone_index

  name                 = "zone-${each.key}"
  resource_group_name  = azurerm_resource_group.this.name
  virtual_network_name = azurerm_virtual_network.this.name
  address_prefixes     = [cidrsubnet(var.vnet_cidr, 4, each.value + 1)]
}

resource "azurerm_network_security_group" "zone" {
  for_each = local.zone_index

  name                = "${local.name}-zone-${each.key}"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  tags                = local.tags
}

resource "azurerm_subnet_network_security_group_association" "zone" {
  for_each = local.zone_index

  subnet_id                 = azurerm_subnet.zone[each.key].id
  network_security_group_id = azurerm_network_security_group.zone[each.key].id
}

# Outbound traffic of every zone leaves through that zone's own NAT gateway: a lost zone does not take the egress
# of the others with it.
resource "azurerm_public_ip" "nat" {
  for_each = local.zone_index

  name                = "${local.name}-nat-${each.key}"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  allocation_method   = "Static"
  sku                 = "Standard"
  zones               = [each.key]
  tags                = local.tags
}

resource "azurerm_nat_gateway" "zone" {
  for_each = local.zone_index

  name                = "${local.name}-${each.key}"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  sku_name            = "Standard"
  zones               = [each.key]
  tags                = local.tags
}

resource "azurerm_nat_gateway_public_ip_association" "zone" {
  for_each = local.zone_index

  nat_gateway_id       = azurerm_nat_gateway.zone[each.key].id
  public_ip_address_id = azurerm_public_ip.nat[each.key].id
}

resource "azurerm_subnet_nat_gateway_association" "zone" {
  for_each = local.zone_index

  subnet_id      = azurerm_subnet.zone[each.key].id
  nat_gateway_id = azurerm_nat_gateway.zone[each.key].id
}

# The system subnet needs outbound access too (image pulls of the add-ons); a regional NAT gateway serves it.
resource "azurerm_public_ip" "system" {
  name                = "${local.name}-nat-system"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  allocation_method   = "Static"
  sku                 = "Standard"
  tags                = local.tags
}

resource "azurerm_nat_gateway" "system" {
  name                = "${local.name}-system"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  sku_name            = "Standard"
  tags                = local.tags
}

resource "azurerm_nat_gateway_public_ip_association" "system" {
  nat_gateway_id       = azurerm_nat_gateway.system.id
  public_ip_address_id = azurerm_public_ip.system.id
}

resource "azurerm_subnet_nat_gateway_association" "system" {
  subnet_id      = azurerm_subnet.system.id
  nat_gateway_id = azurerm_nat_gateway.system.id
}
