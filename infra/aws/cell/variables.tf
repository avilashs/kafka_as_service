variable "region" {
  description = "AWS region of the cell."
  type        = string
}

variable "cell_id" {
  description = "Cell identifier (DNS-safe, at most 20 characters). Names every resource."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,19}$", var.cell_id))
    error_message = "cell_id must be lowercase letters, digits and hyphens, start with a letter, 2-20 characters."
  }
}

variable "azs" {
  description = "Exactly three availability zones of the region. A cell spans three zones (design section 7.1)."
  type        = list(string)

  validation {
    condition     = length(var.azs) == 3
    error_message = "A cell uses exactly three availability zones."
  }
}

variable "vpc_cidr" {
  description = "CIDR of the cell VPC. Pods use VPC addresses (VPC CNI), so the private subnets are large."
  type        = string
  default     = "10.0.0.0/16"
}

variable "kubernetes_version" {
  description = "EKS control plane version."
  type        = string
  default     = "1.34"
}

variable "control_instance_type" {
  description = "Instance type of the control pool (operators, Karpenter, gateway, controllers, platform components)."
  type        = string
  default     = "m7i.large"
}

variable "control_node_count" {
  description = "Nodes in the control pool (one per zone at least)."
  type        = number
  default     = 3
}

variable "public_access_cidrs" {
  description = "CIDRs allowed to reach the public EKS API endpoint. Narrow this; the default is open."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "domain" {
  description = "Parent DNS domain. The cell owns the hosted zone <cell_id>.<domain>, in which per-broker names are created."
  type        = string
}

variable "tags" {
  description = "Tags added to every resource."
  type        = map(string)
  default     = {}
}
