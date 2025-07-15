output "raw_bucket_name" {
  description = "Raw data bucket name"
  value       = aws_s3_bucket.raw_data.bucket
}

output "processed_bucket_name" {
  description = "Processed data bucket name"
  value       = aws_s3_bucket.processed_data.bucket
}

output "curated_bucket_name" {
  description = "Curated data bucket name"
  value       = aws_s3_bucket.curated_data.bucket
}

output "terraform_state_bucket_name" {
  description = "Terraform state bucket name"
  value       = aws_s3_bucket.terraform_state.bucket
}

output "raw_bucket_arn" {
  description = "Raw data bucket ARN"
  value       = aws_s3_bucket.raw_data.arn
}

output "processed_bucket_arn" {
  description = "Processed data bucket ARN"
  value       = aws_s3_bucket.processed_data.arn
}

output "curated_bucket_arn" {
  description = "Curated data bucket ARN"
  value       = aws_s3_bucket.curated_data.arn
}

output "terraform_state_bucket_arn" {
  description = "Terraform state bucket ARN"
  value       = aws_s3_bucket.terraform_state.arn
}

output "kms_key_arn" {
  description = "KMS key ARN for S3 encryption"
  value       = var.enable_encryption ? aws_kms_key.s3[0].arn : null
}

output "kms_key_id" {
  description = "KMS key ID for S3 encryption"
  value       = var.enable_encryption ? aws_kms_key.s3[0].key_id : null
} 