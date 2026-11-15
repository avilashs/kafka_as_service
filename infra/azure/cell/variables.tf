variable "subscription_id" {
  description = "Azure subscription that holds the cell."
  type        = string
}

variable "location" {
  description = "Azure region of the cell (it must offer availability zones)."
  type        = string
}

variable "cell_id" {
  description = "Cell identifier (DNS-safe, at most 16 characters). Names every resource."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,15}$", var.cell_id))
    error_message = "cell_id must be lowercase letters, digits and hyphens, start with a letter, 2-16 characters."
  }
}

variable "zones" {
  description = "The three availability zones of the region that the cell spans (design section 7.1)."
  type        = list(string)
  default     = ["1", "2", "3"]

  validation {
    condition     = length(var.zones) == 3
    error_message = "A cell uses exactly three availability zones."
  }
}

variable "vnet_cidr" {
  description = "Address space of the cell network. Pods use an overlay, so the node subnets can be modest."
  type        = string
  default     = "10.10.0.0/16"
}

variable "kubernetes_version" {
  description = "AKS version. Leave null for the region's default."
  type        = string
  default     = null
}

variable "system_vm_size" {
  description = "VM size of the system pool (CoreDNS, metrics-server, CNI components)."
  type        = string
  default     = "Standard_D2ds_v5"
}

variable "control_vm_size" {
  description = "VM size of the control pool: one node per zone (operators, gateway, controllers, platform components)."
  type        = string
  default     = "Standard_D2ds_v5"
}

variable "shared_vm_size" {
  description = "VM size of the shared pool (tenant brokers of the shared tier)."
  type        = string
  default     = "Standard_D8ds_v5"
}

variable "shared_min_nodes_per_zone" {
  type    = number
  default = 1
}

variable "shared_max_nodes_per_zone" {
  type    = number
  default = 3
}

variable "authorized_ip_ranges" {
  description = "CIDRs allowed to reach the Kubernetes API server. Narrow this; the default is open."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "domain" {
  description = "Parent DNS domain. The cell owns the DNS zone <cell_id>.<domain>, in which per-broker names are created."
  type        = string
}

variable "tags" {
  description = "Tags added to every resource."
  type        = map(string)
  default     = {}
}
