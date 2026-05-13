# Real-Time Retail Insights Platform — Infrastructure as Code
# Cost-conscious deployment: VPC + S3 + Redshift (single-node)
# Kafka and Airflow run locally via Docker

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
  backend "local" {}
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "retail-insights"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "current" {}

resource "random_string" "bucket_suffix" {
  length  = 8
  lower   = true
  upper   = false
  numeric = true
  special = false
}

locals {
  raw_bucket       = "retail-insights-raw-${random_string.bucket_suffix.result}"
  processed_bucket = "retail-insights-processed-${random_string.bucket_suffix.result}"
  curated_bucket   = "retail-insights-curated-${random_string.bucket_suffix.result}"
}

module "vpc" {
  source = "./modules/vpc"

  vpc_name             = "retail-insights-vpc"
  vpc_cidr             = "10.0.0.0/16"
  availability_zones   = slice(data.aws_availability_zones.available.names, 0, 2)
  public_subnet_cidrs  = ["10.0.1.0/24", "10.0.2.0/24"]
  private_subnet_cidrs = ["10.0.10.0/24", "10.0.11.0/24"]
  environment          = var.environment
}

module "s3" {
  source = "./modules/s3"

  bucket_names = {
    raw_data        = local.raw_bucket
    processed_data  = local.processed_bucket
    curated_data    = local.curated_bucket
    terraform_state = local.raw_bucket
  }

  environment      = var.environment
  enable_encryption = false
}

module "redshift" {
  source = "./modules/redshift"

  cluster_identifier = "retail-insights-${var.environment}"
  database_name      = "retail_analytics"
  master_username    = var.redshift_username
  master_password    = var.redshift_password
  node_type          = "ra3.xlplus"
  number_of_nodes    = 1

  vpc_security_group_ids = [module.vpc.redshift_security_group_id]
  subnet_group_name      = module.vpc.redshift_subnet_group_name

  logging_bucket_name = local.processed_bucket
  enable_encryption   = false
  environment         = var.environment
}

output "vpc_id" {
  description = "VPC ID"
  value       = module.vpc.vpc_id
}

output "redshift_endpoint" {
  description = "Redshift cluster endpoint"
  value       = module.redshift.cluster_endpoint
  sensitive   = true
}

output "redshift_port" {
  description = "Redshift cluster port"
  value       = module.redshift.cluster_port
}

output "s3_bucket_names" {
  description = "S3 bucket names"
  value = {
    raw_data      = module.s3.raw_bucket_name
    processed_data = module.s3.processed_bucket_name
    curated_data   = module.s3.curated_bucket_name
  }
}

output "availability_zones" {
  description = "AZs used"
  value       = slice(data.aws_availability_zones.available.names, 0, 2)
}
