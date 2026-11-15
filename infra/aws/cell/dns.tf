# Per-broker and bootstrap names of every cluster of the cell live in this zone (ADR-0005). The records are
# created by the cell's DNS controller; the NS records must be delegated from the parent domain by the operator.
resource "aws_route53_zone" "cell" {
  name = "${var.cell_id}.${var.domain}"
}
