variable "bucket_names" {
  description = "Map of bucket names for different data layers"
  type = object({
    raw_data       = string
    processed_data = string
    curated_data   = string
    terraform_state = string
  })
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "kms_key_id" {
  description = "KMS key ID for encryption (optional)"
  type        = string
  default     = null
}

variable "enable_encryption" {
  description = "Enable encryption for S3 buckets"
  type        = bool
  default     = true
}

variable "tags" {
  description = "Additional tags for all resources"
  type        = map(string)
  default     = {}
} 