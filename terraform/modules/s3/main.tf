# S3 Module for Retail Insights Platform
# Creates S3 buckets for data lake with encryption and lifecycle policies

# KMS Key for S3 encryption
resource "aws_kms_key" "s3" {
  count                   = var.enable_encryption ? 1 : 0
  description             = "KMS key for S3 bucket encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Enable IAM User Permissions"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      },
      {
        Sid    = "Allow S3 to use the key"
        Effect = "Allow"
        Principal = {
          Service = "s3.amazonaws.com"
        }
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ]
        Resource = "*"
      }
    ]
  })
  
  tags = merge(var.tags, {
    Name = "${var.environment}-s3-encryption-key"
  })
}

resource "aws_kms_alias" "s3" {
  count         = var.enable_encryption ? 1 : 0
  name          = "alias/${var.environment}-s3-encryption-key"
  target_key_id = aws_kms_key.s3[0].key_id
}

# Raw Data Bucket
resource "aws_s3_bucket" "raw_data" {
  bucket = var.bucket_names.raw_data
  
  tags = merge(var.tags, {
    Name = "Raw Data Bucket"
    Layer = "raw"
  })
}

resource "aws_s3_bucket_versioning" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw_data" {
  count  = var.enable_encryption ? 1 : 0
  bucket = aws_s3_bucket.raw_data.id
  
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = var.kms_key_id != null ? var.kms_key_id : aws_kms_key.s3[0].arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id
  
  rule {
    id     = "raw_data_lifecycle"
    status = "Enabled"
    
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    
    transition {
      days          = 365
      storage_class = "DEEP_ARCHIVE"
    }
    
    expiration {
      days = 2555  # 7 years
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id
  
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Processed Data Bucket
resource "aws_s3_bucket" "processed_data" {
  bucket = var.bucket_names.processed_data
  
  tags = merge(var.tags, {
    Name = "Processed Data Bucket"
    Layer = "processed"
  })
}

resource "aws_s3_bucket_versioning" "processed_data" {
  bucket = aws_s3_bucket.processed_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processed_data" {
  count  = var.enable_encryption ? 1 : 0
  bucket = aws_s3_bucket.processed_data.id
  
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = var.kms_key_id != null ? var.kms_key_id : aws_kms_key.s3[0].arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "processed_data" {
  bucket = aws_s3_bucket.processed_data.id
  
  rule {
    id     = "processed_data_lifecycle"
    status = "Enabled"
    
    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
    
    transition {
      days          = 365
      storage_class = "GLACIER"
    }
    
    expiration {
      days = 1825  # 5 years
    }
  }
}

resource "aws_s3_bucket_public_access_block" "processed_data" {
  bucket = aws_s3_bucket.processed_data.id
  
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Curated Data Bucket
resource "aws_s3_bucket" "curated_data" {
  bucket = var.bucket_names.curated_data
  
  tags = merge(var.tags, {
    Name = "Curated Data Bucket"
    Layer = "curated"
  })
}

resource "aws_s3_bucket_versioning" "curated_data" {
  bucket = aws_s3_bucket.curated_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "curated_data" {
  count  = var.enable_encryption ? 1 : 0
  bucket = aws_s3_bucket.curated_data.id
  
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = var.kms_key_id != null ? var.kms_key_id : aws_kms_key.s3[0].arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "curated_data" {
  bucket = aws_s3_bucket.curated_data.id
  
  rule {
    id     = "curated_data_lifecycle"
    status = "Enabled"
    
    transition {
      days          = 180
      storage_class = "STANDARD_IA"
    }
    
    expiration {
      days = 1095  # 3 years
    }
  }
}

resource "aws_s3_bucket_public_access_block" "curated_data" {
  bucket = aws_s3_bucket.curated_data.id
  
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Terraform State Bucket
resource "aws_s3_bucket" "terraform_state" {
  bucket = var.bucket_names.terraform_state
  
  tags = merge(var.tags, {
    Name = "Terraform State Bucket"
    Purpose = "terraform-state"
  })
}

resource "aws_s3_bucket_versioning" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "terraform_state" {
  count  = var.enable_encryption ? 1 : 0
  bucket = aws_s3_bucket.terraform_state.id
  
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = var.kms_key_id != null ? var.kms_key_id : aws_kms_key.s3[0].arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id
  
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Data sources
data "aws_caller_identity" "current" {} 