# EC2 Module for Retail Insights Platform
# Creates EC2 instances for Kafka brokers and Airflow

# Data source for latest Amazon Linux 2 AMI
data "aws_ami" "amazon_linux_2" {
  most_recent = true
  owners      = ["amazon"]
  
  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*-x86_64-gp2"]
  }
  
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# Kafka Broker Instances
resource "aws_instance" "kafka_brokers" {
  count = var.kafka_cluster_size
  
  ami                    = data.aws_ami.amazon_linux_2.id
  instance_type          = var.kafka_instance_type
  key_name               = var.key_pair_name
  vpc_security_group_ids = [var.kafka_security_group_id]
  subnet_id              = var.subnet_ids[count.index % length(var.subnet_ids)]
  iam_instance_profile   = var.iam_instance_profile
  
  root_block_device {
    volume_size = 100
    volume_type = "gp3"
    encrypted   = true
  }
  
  user_data = base64encode(templatefile("${path.module}/templates/kafka_user_data.sh", {
    broker_id = count.index
    cluster_size = var.kafka_cluster_size
    zookeeper_connect = join(",", [for i in range(var.kafka_cluster_size) : "kafka-${i}:2181"])
    kafka_version = var.kafka_version
    java_heap_size = var.kafka_java_heap_size
  }))
  
  tags = merge(var.tags, {
    Name = "kafka-broker-${count.index}"
    Role = "kafka"
    BrokerId = count.index
  })
  
  depends_on = [aws_instance.zookeeper]
}

# Zookeeper Instances
resource "aws_instance" "zookeeper" {
  count = var.kafka_cluster_size
  
  ami                    = data.aws_ami.amazon_linux_2.id
  instance_type          = var.zookeeper_instance_type
  key_name               = var.key_pair_name
  vpc_security_group_ids = [var.kafka_security_group_id]
  subnet_id              = var.subnet_ids[count.index % length(var.subnet_ids)]
  iam_instance_profile   = var.iam_instance_profile
  
  root_block_device {
    volume_size = 50
    volume_type = "gp3"
    encrypted   = true
  }
  
  user_data = base64encode(templatefile("${path.module}/templates/zookeeper_user_data.sh", {
    zookeeper_id = count.index
    cluster_size = var.kafka_cluster_size
    zookeeper_version = var.zookeeper_version
    java_heap_size = var.zookeeper_java_heap_size
  }))
  
  tags = merge(var.tags, {
    Name = "zookeeper-${count.index}"
    Role = "zookeeper"
    ZookeeperId = count.index
  })
}

# Airflow Instance
resource "aws_instance" "airflow" {
  ami                    = data.aws_ami.amazon_linux_2.id
  instance_type          = var.airflow_instance_type
  key_name               = var.key_pair_name
  vpc_security_group_ids = [var.airflow_security_group_id]
  subnet_id              = var.subnet_ids[0]
  iam_instance_profile   = var.iam_instance_profile
  
  root_block_device {
    volume_size = 100
    volume_type = "gp3"
    encrypted   = true
  }
  
  user_data = base64encode(templatefile("${path.module}/templates/airflow_user_data.sh", {
    airflow_version = var.airflow_version
    postgres_password = var.airflow_postgres_password
    airflow_admin_password = var.airflow_admin_password
    databricks_host = var.databricks_host
    databricks_token = var.databricks_token
    redshift_host = var.redshift_host
    redshift_database = var.redshift_database
    redshift_username = var.redshift_username
    redshift_password = var.redshift_password
  }))
  
  tags = merge(var.tags, {
    Name = "airflow-server"
    Role = "airflow"
  })
}

# Monitoring Instance (Prometheus + Grafana)
resource "aws_instance" "monitoring" {
  ami                    = data.aws_ami.amazon_linux_2.id
  instance_type          = var.monitoring_instance_type
  key_name               = var.key_pair_name
  vpc_security_group_ids = [var.monitoring_security_group_id]
  subnet_id              = var.subnet_ids[0]
  iam_instance_profile   = var.iam_instance_profile
  
  root_block_device {
    volume_size = 50
    volume_type = "gp3"
    encrypted   = true
  }
  
  user_data = base64encode(templatefile("${path.module}/templates/monitoring_user_data.sh", {
    prometheus_version = var.prometheus_version
    grafana_version = var.grafana_version
    kafka_brokers = join(",", aws_instance.kafka_brokers[*].private_ip)
    airflow_host = aws_instance.airflow.private_ip
    redshift_host = var.redshift_host
  }))
  
  tags = merge(var.tags, {
    Name = "monitoring-server"
    Role = "monitoring"
  })
}

# Elastic IP for Airflow
resource "aws_eip" "airflow" {
  instance = aws_instance.airflow.id
  domain   = "vpc"
  
  tags = merge(var.tags, {
    Name = "airflow-eip"
  })
}

# Elastic IP for Monitoring
resource "aws_eip" "monitoring" {
  instance = aws_instance.monitoring.id
  domain   = "vpc"
  
  tags = merge(var.tags, {
    Name = "monitoring-eip"
  })
}

# CloudWatch Log Groups
resource "aws_cloudwatch_log_group" "kafka" {
  count             = var.enable_cloudwatch_logs ? 1 : 0
  name              = "/aws/ec2/kafka"
  retention_in_days = var.log_retention_days
  
  tags = merge(var.tags, {
    Name = "kafka-log-group"
  })
}

resource "aws_cloudwatch_log_group" "airflow" {
  count             = var.enable_cloudwatch_logs ? 1 : 0
  name              = "/aws/ec2/airflow"
  retention_in_days = var.log_retention_days
  
  tags = merge(var.tags, {
    Name = "airflow-log-group"
  })
}

resource "aws_cloudwatch_log_group" "monitoring" {
  count             = var.enable_cloudwatch_logs ? 1 : 0
  name              = "/aws/ec2/monitoring"
  retention_in_days = var.log_retention_days
  
  tags = merge(var.tags, {
    Name = "monitoring-log-group"
  })
} 