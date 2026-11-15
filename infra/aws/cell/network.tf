# Three zones, each with a public subnet (NLB, NAT gateway) and a large private subnet (nodes and pods).
# Every private subnet has its own network ACL with allow-all rules numbered 100. The zone-outage test inserts
# deny rules with a lower number (50) with the AWS CLI and removes them afterwards (hack/aws/zone-outage.sh),
# which is why the rules are separate resources and the ACL has no inline rules.

resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags                 = { Name = local.name }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = local.name }
}

resource "aws_subnet" "public" {
  for_each = local.az_index

  vpc_id                  = aws_vpc.this.id
  availability_zone       = each.key
  cidr_block              = cidrsubnet(var.vpc_cidr, 8, each.value)
  map_public_ip_on_launch = false

  tags = {
    Name                     = "${local.name}-public-${each.key}"
    "kubernetes.io/role/elb" = "1"
  }
}

resource "aws_subnet" "private" {
  for_each = local.az_index

  vpc_id            = aws_vpc.this.id
  availability_zone = each.key
  cidr_block        = cidrsubnet(var.vpc_cidr, 4, each.value + 1)

  tags = {
    Name                              = "${local.name}-private-${each.key}"
    "kubernetes.io/role/internal-elb" = "1"
    "karpenter.sh/discovery"          = local.name
  }
}

resource "aws_eip" "nat" {
  for_each = local.az_index

  domain = "vpc"
  tags   = { Name = "${local.name}-nat-${each.key}" }
}

# One NAT gateway per zone: a zone that is lost does not take the egress of the others with it.
resource "aws_nat_gateway" "this" {
  for_each = local.az_index

  allocation_id = aws_eip.nat[each.key].id
  subnet_id     = aws_subnet.public[each.key].id
  tags          = { Name = "${local.name}-${each.key}" }

  depends_on = [aws_internet_gateway.this]
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${local.name}-public" }

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }
}

resource "aws_route_table_association" "public" {
  for_each = local.az_index

  subnet_id      = aws_subnet.public[each.key].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table" "private" {
  for_each = local.az_index

  vpc_id = aws_vpc.this.id
  tags   = { Name = "${local.name}-private-${each.key}" }

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.this[each.key].id
  }
}

resource "aws_route_table_association" "private" {
  for_each = local.az_index

  subnet_id      = aws_subnet.private[each.key].id
  route_table_id = aws_route_table.private[each.key].id
}

# S3 without NAT charges (image layers, tiered storage later).
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [for rt in aws_route_table.private : rt.id]
  tags              = { Name = "${local.name}-s3" }
}

resource "aws_network_acl" "private" {
  for_each = local.az_index

  vpc_id     = aws_vpc.this.id
  subnet_ids = [aws_subnet.private[each.key].id]
  tags       = { Name = "${local.name}-private-${each.key}" }
}

resource "aws_network_acl_rule" "private_in" {
  for_each = local.az_index

  network_acl_id = aws_network_acl.private[each.key].id
  rule_number    = 100
  egress         = false
  protocol       = "-1"
  rule_action    = "allow"
  cidr_block     = "0.0.0.0/0"
}

resource "aws_network_acl_rule" "private_out" {
  for_each = local.az_index

  network_acl_id = aws_network_acl.private[each.key].id
  rule_number    = 100
  egress         = true
  protocol       = "-1"
  rule_action    = "allow"
  cidr_block     = "0.0.0.0/0"
}
