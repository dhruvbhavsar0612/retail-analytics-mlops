output "cluster_endpoint" {
  description = "Redshift cluster endpoint"
  value       = aws_redshift_cluster.main.endpoint
  sensitive   = true
}

output "cluster_id" {
  description = "Redshift cluster identifier"
  value       = aws_redshift_cluster.main.cluster_identifier
}

output "cluster_port" {
  description = "Redshift cluster port"
  value       = aws_redshift_cluster.main.port
}
