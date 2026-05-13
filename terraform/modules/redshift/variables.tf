variable "cluster_identifier" {
  description = "Redshift cluster identifier"
  type        = string
}

variable "database_name" {
  description = "Redshift database name"
  type        = string
}

variable "master_username" {
  description = "Redshift master username"
  type        = string
}

variable "master_password" {
  description = "Redshift master password"
  type        = string
  sensitive   = true
}

variable "node_type" {
  description = "Redshift node type"
  type        = string
  default     = "ra3.xlplus"
}

variable "number_of_nodes" {
  description = "Number of Redshift nodes"
  type        = number
  default     = 1
}

variable "vpc_security_group_ids" {
  description = "VPC security group IDs for Redshift"
  type        = list(string)
}

variable "subnet_group_name" {
  description = "Redshift subnet group name (set to null to create one)"
  type        = string
  default     = null
}

variable "subnet_ids" {
  description = "Subnet IDs for Redshift subnet group"
  type        = list(string)
  default     = []
}

variable "enable_encryption" {
  description = "Enable encryption at rest"
  type        = bool
  default     = false
}

variable "kms_key_id" {
  description = "KMS key ID for encryption (null = AWS-managed key)"
  type        = string
  default     = null
}

variable "backup_retention_days" {
  description = "Automated snapshot retention days"
  type        = number
  default     = 1
}

variable "maintenance_window" {
  description = "Preferred maintenance window"
  type        = string
  default     = "sun:23:00-mon:01:00"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "logging_bucket_name" {
  description = "S3 bucket for Redshift logs"
  type        = string
}

variable "enable_cloudwatch_logs" {
  description = "Enable CloudWatch log group"
  type        = bool
  default     = false
}

variable "log_retention_days" {
  description = "CloudWatch log retention days"
  type        = number
  default     = 7
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}
