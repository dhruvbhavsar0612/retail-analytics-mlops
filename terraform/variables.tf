# Real-Time Retail Insights Platform - Variables
# Input variables for Terraform configuration

variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
  
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "redshift_username" {
  description = "Redshift master username"
  type        = string
  default     = "admin"
  sensitive   = true
}

variable "redshift_password" {
  description = "Redshift master password"
  type        = string
  sensitive   = true
  
  validation {
    condition     = length(var.redshift_password) >= 8
    error_message = "Redshift password must be at least 8 characters long."
  }
}

variable "kafka_cluster_size" {
  description = "Number of Kafka broker instances"
  type        = number
  default     = 3
  
  validation {
    condition     = var.kafka_cluster_size >= 1 && var.kafka_cluster_size <= 5
    error_message = "Kafka cluster size must be between 1 and 5."
  }
}

variable "airflow_instance_type" {
  description = "EC2 instance type for Airflow"
  type        = string
  default     = "t3.large"
}

variable "kafka_instance_type" {
  description = "EC2 instance type for Kafka brokers"
  type        = string
  default     = "t3.medium"
}

variable "redshift_node_type" {
  description = "Redshift node type"
  type        = string
  default     = "dc2.large"
}

variable "redshift_nodes" {
  description = "Number of Redshift nodes"
  type        = number
  default     = 2
  
  validation {
    condition     = var.redshift_nodes >= 1 && var.redshift_nodes <= 10
    error_message = "Redshift nodes must be between 1 and 10."
  }
}

variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets"
  type        = list(string)
  default     = ["10.0.10.0/24", "10.0.11.0/24"]
}

variable "enable_encryption" {
  description = "Enable encryption at rest for all resources"
  type        = bool
  default     = true
}

variable "enable_backup" {
  description = "Enable automated backups"
  type        = bool
  default     = true
}

variable "backup_retention_days" {
  description = "Number of days to retain backups"
  type        = number
  default     = 7
  
  validation {
    condition     = var.backup_retention_days >= 1 && var.backup_retention_days <= 35
    error_message = "Backup retention days must be between 1 and 35."
  }
}

variable "monitoring_interval" {
  description = "CloudWatch monitoring interval in seconds"
  type        = number
  default     = 60
  
  validation {
    condition     = contains([60, 300], var.monitoring_interval)
    error_message = "Monitoring interval must be either 60 or 300 seconds."
  }
}

variable "tags" {
  description = "Additional tags for all resources"
  type        = map(string)
  default = {
    Project     = "retail-insights"
    Owner       = "data-engineering-team"
    CostCenter  = "data-platform"
    Compliance  = "pci-dss"
  }
}

variable "databricks_workspace_url" {
  description = "Databricks workspace URL"
  type        = string
  default     = ""
}

variable "databricks_token" {
  description = "Databricks access token"
  type        = string
  default     = ""
  sensitive   = true
}

variable "enable_auto_scaling" {
  description = "Enable auto-scaling for EC2 instances"
  type        = bool
  default     = false
}

variable "min_instances" {
  description = "Minimum number of instances for auto-scaling"
  type        = number
  default     = 1
}

variable "max_instances" {
  description = "Maximum number of instances for auto-scaling"
  type        = number
  default     = 5
}

variable "scale_up_threshold" {
  description = "CPU threshold for scaling up"
  type        = number
  default     = 70
}

variable "scale_down_threshold" {
  description = "CPU threshold for scaling down"
  type        = number
  default     = 30
}

variable "enable_cloudwatch_logs" {
  description = "Enable CloudWatch logs for all services"
  type        = bool
  default     = true
}

variable "log_retention_days" {
  description = "Number of days to retain CloudWatch logs"
  type        = number
  default     = 30
  
  validation {
    condition     = var.log_retention_days >= 1 && var.log_retention_days <= 365
    error_message = "Log retention days must be between 1 and 365."
  }
}

variable "enable_sns_notifications" {
  description = "Enable SNS notifications for alerts"
  type        = bool
  default     = true
}

variable "notification_email" {
  description = "Email address for SNS notifications"
  type        = string
  default     = ""
  
  validation {
    condition     = var.notification_email == "" || can(regex("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$", var.notification_email))
    error_message = "Notification email must be a valid email address."
  }
} 