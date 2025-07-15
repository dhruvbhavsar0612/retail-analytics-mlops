# Redshift Module for Retail Insights Platform
# Creates Redshift cluster with encryption and monitoring

# Redshift Cluster
resource "aws_redshift_cluster" "main" {
  cluster_identifier        = var.cluster_identifier
  database_name            = var.database_name
  master_username          = var.master_username
  master_password          = var.master_password
  node_type                = var.node_type
  number_of_nodes          = var.number_of_nodes
  cluster_type             = var.number_of_nodes > 1 ? "multi-node" : "single-node"
  
  # Networking
  vpc_security_group_ids = var.vpc_security_group_ids
  cluster_subnet_group_name = var.subnet_group_name
  
  # Encryption
  encrypted = var.enable_encryption
  kms_key_id = var.enable_encryption ? var.kms_key_id : null
  
  # Backup and maintenance
  automated_snapshot_retention_period = var.backup_retention_days
  preferred_maintenance_window        = var.maintenance_window
  skip_final_snapshot                 = var.environment == "dev"
  final_snapshot_identifier           = var.environment != "dev" ? "${var.cluster_identifier}-final-snapshot" : null
  
  # Monitoring
  cluster_parameter_group_name = aws_redshift_parameter_group.main.name
  enhanced_vpc_routing         = true
  
  # Logging
  logging {
    enable        = true
    bucket_name   = var.logging_bucket_name
    s3_key_prefix = "redshift-logs/"
  }
  
  tags = merge(var.tags, {
    Name = var.cluster_identifier
  })
}

# Parameter Group
resource "aws_redshift_parameter_group" "main" {
  name   = "${var.cluster_identifier}-parameter-group"
  family = "redshift-1.0"
  
  parameter {
    name  = "enable_user_activity_logging"
    value = "true"
  }
  
  parameter {
    name  = "log_connections"
    value = "true"
  }
  
  parameter {
    name  = "log_disconnections"
    value = "true"
  }
  
  parameter {
    name  = "log_statement"
    value = "all"
  }
  
  parameter {
    name  = "log_duration"
    value = "true"
  }
  
  parameter {
    name  = "log_min_duration_statement"
    value = "1000"
  }
  
  tags = merge(var.tags, {
    Name = "${var.cluster_identifier}-parameter-group"
  })
}

# Subnet Group (if not provided)
resource "aws_redshift_subnet_group" "main" {
  count      = var.subnet_group_name == null ? 1 : 0
  name       = "${var.cluster_identifier}-subnet-group"
  subnet_ids = var.subnet_ids
  
  tags = merge(var.tags, {
    Name = "${var.cluster_identifier}-subnet-group"
  })
}

# CloudWatch Log Group for Redshift
resource "aws_cloudwatch_log_group" "redshift" {
  count             = var.enable_cloudwatch_logs ? 1 : 0
  name              = "/aws/redshift/${var.cluster_identifier}"
  retention_in_days = var.log_retention_days
  
  tags = merge(var.tags, {
    Name = "${var.cluster_identifier}-log-group"
  })
}

# IAM Role for Redshift
resource "aws_iam_role" "redshift" {
  name = "${var.cluster_identifier}-redshift-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "redshift.amazonaws.com"
        }
      }
    ]
  })
  
  tags = merge(var.tags, {
    Name = "${var.cluster_identifier}-redshift-role"
  })
}

# IAM Policy for Redshift
resource "aws_iam_role_policy" "redshift" {
  name = "${var.cluster_identifier}-redshift-policy"
  role = aws_iam_role.redshift.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Resource = [
          "arn:aws:s3:::${var.logging_bucket_name}",
          "arn:aws:s3:::${var.logging_bucket_name}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData",
          "cloudwatch:GetMetricData",
          "cloudwatch:ListMetrics"
        ]
        Resource = "*"
      }
    ]
  })
}

# Attach IAM role to Redshift cluster
resource "aws_redshift_cluster_iam_roles" "main" {
  cluster_identifier = aws_redshift_cluster.main.cluster_identifier
  iam_role_arns      = [aws_iam_role.redshift.arn]
} 