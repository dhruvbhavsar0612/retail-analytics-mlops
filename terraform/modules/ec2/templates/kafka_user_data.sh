#!/bin/bash
# User data script for Kafka broker instances

# Update system
yum update -y
yum install -y java-11-amazon-corretto-headless wget unzip

# Create kafka user
useradd -r -s /bin/false kafka

# Download and install Kafka
cd /opt
wget https://downloads.apache.org/kafka/${kafka_version}/kafka_2.13-${kafka_version}.tgz
tar -xzf kafka_2.13-${kafka_version}.tgz
ln -s kafka_2.13-${kafka_version} kafka
chown -R kafka:kafka kafka

# Create Kafka configuration
cat > /opt/kafka/config/server.properties << EOF
# Broker ID
broker.id=${broker_id}

# Network settings
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://$(curl -s http://169.254.169.254/latest/meta-data/local-ipv4):9092

# Zookeeper connection
zookeeper.connect=${zookeeper_connect}

# Log settings
log.dirs=/var/lib/kafka-logs
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000

# Performance settings
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# Replication settings
default.replication.factor=3
min.insync.replicas=2

# Delete topic enable
delete.topic.enable=true

# Auto create topics
auto.create.topics.enable=true
EOF

# Create systemd service for Kafka
cat > /etc/systemd/system/kafka.service << EOF
[Unit]
Description=Apache Kafka Server
Documentation=http://kafka.apache.org/documentation.html
Requires=network.target remote-fs.target
After=network.target remote-fs.target

[Service]
Type=simple
User=kafka
Group=kafka
Environment="JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto"
Environment="KAFKA_HEAP_OPTS=-Xmx${java_heap_size} -Xms${java_heap_size}"
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Create log directory
mkdir -p /var/lib/kafka-logs
chown -R kafka:kafka /var/lib/kafka-logs

# Start Kafka service
systemctl daemon-reload
systemctl enable kafka
systemctl start kafka

# Create topics
sleep 30
/opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 3 --partitions 6 --topic retail_clickstream
/opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 3 --partitions 6 --topic retail_transactions
/opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 3 --partitions 3 --topic retail_inventory

# Install CloudWatch agent
yum install -y amazon-cloudwatch-agent

# Configure CloudWatch agent
cat > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json << EOF
{
  "agent": {
    "metrics_collection_interval": 60,
    "run_as_user": "cwagent"
  },
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/opt/kafka/logs/server.log",
            "log_group_name": "/aws/ec2/kafka",
            "log_stream_name": "{instance_id}",
            "timezone": "UTC"
          }
        ]
      }
    }
  },
  "metrics": {
    "metrics_collected": {
      "disk": {
        "measurement": ["used_percent"],
        "metrics_collection_interval": 60,
        "resources": ["*"]
      },
      "mem": {
        "measurement": ["mem_used_percent"],
        "metrics_collection_interval": 60
      }
    }
  }
}
EOF

# Start CloudWatch agent
systemctl enable amazon-cloudwatch-agent
systemctl start amazon-cloudwatch-agent

# Output instance info
echo "Kafka broker ${broker_id} setup complete"
echo "Instance ID: $(curl -s http://169.254.169.254/latest/meta-data/instance-id)"
echo "Private IP: $(curl -s http://169.254.169.254/latest/meta-data/local-ipv4)" 