output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "vpc_cidr" {
  description = "VPC CIDR block"
  value       = aws_vpc.main.cidr_block
}

output "public_subnet_ids" {
  description = "List of public subnet IDs"
  value       = aws_subnet.public[*].id
}

output "private_subnet_ids" {
  description = "List of private subnet IDs"
  value       = aws_subnet.private[*].id
}

output "kafka_security_group_id" {
  description = "Kafka security group ID"
  value       = aws_security_group.kafka.id
}

output "airflow_security_group_id" {
  description = "Airflow security group ID"
  value       = aws_security_group.airflow.id
}

output "redshift_security_group_id" {
  description = "Redshift security group ID"
  value       = aws_security_group.redshift.id
}

output "monitoring_security_group_id" {
  description = "Monitoring security group ID"
  value       = aws_security_group.monitoring.id
}

output "redshift_subnet_group_name" {
  description = "Redshift subnet group name"
  value       = aws_redshift_subnet_group.main.name
}

output "nat_gateway_id" {
  description = "NAT Gateway ID"
  value       = aws_nat_gateway.main.id
}

output "internet_gateway_id" {
  description = "Internet Gateway ID"
  value       = aws_internet_gateway.main.id
} 