# Redshift Module for Retail Insights Platform

resource "aws_redshift_cluster" "main" {
  cluster_identifier        = var.cluster_identifier
  database_name            = var.database_name
  master_username          = var.master_username
  master_password          = var.master_password
  node_type                = var.node_type
  number_of_nodes          = var.number_of_nodes
  cluster_type             = var.number_of_nodes > 1 ? "multi-node" : "single-node"

  vpc_security_group_ids   = var.vpc_security_group_ids
  cluster_subnet_group_name = var.subnet_group_name

  encrypted                = true

  automated_snapshot_retention_period = var.backup_retention_days
  skip_final_snapshot                 = var.environment == "dev"

  availability_zone_relocation_enabled = true
  port                                 = 5439

  tags = merge(var.tags, {
    Name = var.cluster_identifier
  })
}

resource "aws_redshift_subnet_group" "main" {
  count      = var.subnet_group_name == null ? 1 : 0
  name       = "${var.cluster_identifier}-subnet-group"
  subnet_ids = var.subnet_ids

  tags = merge(var.tags, {
    Name = "${var.cluster_identifier}-subnet-group"
  })
}
