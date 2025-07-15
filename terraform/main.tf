# Real-Time Retail Insights Platform - Infrastructure as Code
# Main Terraform configuration for AWS resources

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  
  backend "s3" {
    bucket = "retail-insights-terraform-state"
    key    = "terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = {
      Project     = "retail-insights"
      Environment = var.environment
      ManagedBy   = "terraform"
      Owner       = "data-engineering-team"
    }
  }
}

# Data sources
data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "current" {}

# VPC and Networking
module "vpc" {
  source = "./modules/vpc"
  
  vpc_name             = "retail-insights-vpc"
  vpc_cidr             = "10.0.0.0/16"
  availability_zones   = slice(data.aws_availability_zones.available.names, 0, 2)
  public_subnet_cidrs  = ["10.0.1.0/24", "10.0.2.0/24"]
  private_subnet_cidrs = ["10.0.10.0/24", "10.0.11.0/24"]
  environment          = var.environment
}

# S3 Buckets for Data Lake
module "s3" {
  source = "./modules/s3"
  
  bucket_names = {
    raw_data      = "retail-insights-raw-${var.environment}"
    processed_data = "retail-insights-processed-${var.environment}"
    curated_data   = "retail-insights-curated-${var.environment}"
    terraform_state = "retail-insights-terraform-state"
  }
  
  environment = var.environment
  kms_key_id  = module.kms.kms_key_id
}

# KMS for Encryption
module "kms" {
  source = "./modules/kms"
  
  key_name        = "retail-insights-encryption-key"
  key_description = "KMS key for Retail Insights Platform encryption"
  environment     = var.environment
}

# IAM Roles and Policies
module "iam" {
  source = "./modules/iam"
  
  environment = var.environment
  vpc_id      = module.vpc.vpc_id
  s3_bucket_names = {
    raw_data      = module.s3.raw_bucket_name
    processed_data = module.s3.processed_bucket_name
    curated_data   = module.s3.curated_bucket_name
  }
}

# Redshift Cluster
module "redshift" {
  source = "./modules/redshift"
  
  cluster_identifier = "retail-insights-${var.environment}"
  node_type          = "dc2.large"
  number_of_nodes    = 2
  database_name      = "retail_analytics"
  master_username    = var.redshift_username
  master_password    = var.redshift_password
  
  vpc_security_group_ids = [module.vpc.redshift_security_group_id]
  subnet_group_name      = module.vpc.redshift_subnet_group_name
  
  kms_key_id = module.kms.kms_key_id
  environment = var.environment
}

# EC2 Instances for Kafka and Airflow
module "ec2" {
  source = "./modules/ec2"
  
  environment = var.environment
  vpc_id      = module.vpc.vpc_id
  subnet_ids  = module.vpc.private_subnet_ids
  
  kafka_instance_type = "t3.medium"
  airflow_instance_type = "t3.large"
  
  key_pair_name = module.key_pair.key_name
  security_group_ids = [
    module.vpc.kafka_security_group_id,
    module.vpc.airflow_security_group_id
  ]
  
  iam_instance_profile = module.iam.ec2_instance_profile_name
}

# Key Pair for EC2 instances
module "key_pair" {
  source = "./modules/key_pair"
  
  key_name = "retail-insights-${var.environment}"
}

# CloudWatch Log Groups
module "cloudwatch" {
  source = "./modules/cloudwatch"
  
  log_group_names = [
    "/aws/retail-insights/kafka",
    "/aws/retail-insights/airflow",
    "/aws/retail-insights/databricks",
    "/aws/retail-insights/redshift"
  ]
  
  environment = var.environment
}

# SNS Topics for Alerts
module "sns" {
  source = "./modules/sns"
  
  topic_names = [
    "retail-insights-alerts-${var.environment}",
    "retail-insights-notifications-${var.environment}"
  ]
  
  environment = var.environment
}

# CloudWatch Alarms
module "alarms" {
  source = "./modules/alarms"
  
  environment = var.environment
  sns_topic_arn = module.sns.alert_topic_arn
  
  redshift_cluster_id = module.redshift.cluster_id
  ec2_instance_ids = module.ec2.instance_ids
}

# Outputs
output "vpc_id" {
  description = "VPC ID"
  value       = module.vpc.vpc_id
}

output "redshift_endpoint" {
  description = "Redshift cluster endpoint"
  value       = module.redshift.cluster_endpoint
  sensitive   = true
}

output "s3_bucket_names" {
  description = "S3 bucket names"
  value = {
    raw_data      = module.s3.raw_bucket_name
    processed_data = module.s3.processed_bucket_name
    curated_data   = module.s3.curated_bucket_name
  }
}

output "kafka_broker_endpoints" {
  description = "Kafka broker endpoints"
  value       = module.ec2.kafka_endpoints
}

output "airflow_endpoint" {
  description = "Airflow web UI endpoint"
  value       = module.ec2.airflow_endpoint
}

output "kms_key_id" {
  description = "KMS key ID for encryption"
  value       = module.kms.kms_key_id
} 