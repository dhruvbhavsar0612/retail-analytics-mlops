#!/usr/bin/env python3
"""
Databricks Kafka Consumer
Consumes data from Kafka topics and forwards to Databricks for real-time processing
"""

import json
import logging
import time
from typing import Dict, Any, List
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import requests
import argparse
import os
from datetime import datetime
import threading
from queue import Queue
import signal
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabricksKafkaConsumer:
    """Consumes Kafka messages and forwards to Databricks"""
    
    def __init__(self, 
                 bootstrap_servers: str,
                 topics: List[str],
                 databricks_host: str,
                 databricks_token: str,
                 batch_size: int = 100,
                 batch_timeout: int = 30):
        
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics
        self.databricks_host = databricks_host.rstrip('/')
        self.databricks_token = databricks_token
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        
        self.consumer = None
        self.running = False
        self.message_queue = Queue()
        self.stats = {
            'messages_consumed': 0,
            'messages_sent': 0,
            'errors': 0,
            'start_time': None
        }
        
        # Databricks API endpoints
        self.databricks_api_base = f"{self.databricks_host}/api/2.0"
        self.headers = {
            'Authorization': f'Bearer {self.databricks_token}',
            'Content-Type': 'application/json'
        }
    
    def connect(self):
        """Connect to Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='databricks-consumer-group',
                max_poll_records=self.batch_size,
                session_timeout_ms=30000,
                heartbeat_interval_ms=3000
            )
            logger.info(f"Connected to Kafka topics: {self.topics}")
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def _send_to_databricks(self, messages: List[Dict[str, Any]], topic: str):
        """Send batch of messages to Databricks"""
        try:
            # Prepare payload for Databricks
            payload = {
                "topic": topic,
                "messages": messages,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "batch_size": len(messages)
            }
            
            # Send to Databricks REST API
            response = requests.post(
                f"{self.databricks_api_base}/jobs/run-now",
                headers=self.headers,
                json={
                    "job_id": self._get_job_id_for_topic(topic),
                    "notebook_params": {
                        "data": json.dumps(payload),
                        "topic": topic,
                        "batch_size": str(len(messages))
                    }
                },
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"Successfully sent {len(messages)} messages to Databricks for topic {topic}")
                self.stats['messages_sent'] += len(messages)
                return True
            else:
                logger.error(f"Failed to send to Databricks: {response.status_code} - {response.text}")
                self.stats['errors'] += 1
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error sending to Databricks: {e}")
            self.stats['errors'] += 1
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending to Databricks: {e}")
            self.stats['errors'] += 1
            return False
    
    def _get_job_id_for_topic(self, topic: str) -> str:
        """Get Databricks job ID for specific topic"""
        # This would typically be configured or retrieved from Databricks
        job_mapping = {
            'retail_clickstream': 'clickstream-processing-job',
            'retail_transactions': 'transaction-processing-job',
            'retail_inventory': 'inventory-processing-job'
        }
        return job_mapping.get(topic, 'default-processing-job')
    
    def _process_message(self, message):
        """Process individual Kafka message"""
        try:
            # Extract message data
            topic = message.topic
            key = message.key
            value = message.value
            offset = message.offset
            partition = message.partition
            
            # Add metadata
            enriched_message = {
                **value,
                "kafka_metadata": {
                    "topic": topic,
                    "partition": partition,
                    "offset": offset,
                    "key": key,
                    "timestamp": message.timestamp
                }
            }
            
            # Add to queue for batch processing
            self.message_queue.put((topic, enriched_message))
            self.stats['messages_consumed'] += 1
            
            logger.debug(f"Processed message from {topic}:{partition}:{offset}")
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            self.stats['errors'] += 1
    
    def _batch_processor(self):
        """Process messages in batches"""
        batches = {}
        last_batch_time = {}
        
        while self.running:
            try:
                # Get message from queue with timeout
                try:
                    topic, message = self.message_queue.get(timeout=1)
                except:
                    continue
                
                # Initialize batch for topic if needed
                if topic not in batches:
                    batches[topic] = []
                    last_batch_time[topic] = time.time()
                
                # Add message to batch
                batches[topic].append(message)
                
                # Check if batch is ready to send
                current_time = time.time()
                batch_ready = (
                    len(batches[topic]) >= self.batch_size or
                    (current_time - last_batch_time[topic]) >= self.batch_timeout
                )
                
                if batch_ready and batches[topic]:
                    # Send batch to Databricks
                    success = self._send_to_databricks(batches[topic], topic)
                    
                    if success:
                        # Clear batch on success
                        batches[topic] = []
                        last_batch_time[topic] = current_time
                    else:
                        # Keep messages in batch for retry (implement retry logic)
                        logger.warning(f"Failed to send batch for topic {topic}, will retry")
                
            except Exception as e:
                logger.error(f"Error in batch processor: {e}")
                self.stats['errors'] += 1
    
    def _stats_reporter(self):
        """Report statistics periodically"""
        while self.running:
            time.sleep(60)  # Report every minute
            
            if self.stats['start_time']:
                elapsed = time.time() - self.stats['start_time']
                rate = self.stats['messages_consumed'] / elapsed if elapsed > 0 else 0
                
                logger.info(
                    f"Stats - Consumed: {self.stats['messages_consumed']}, "
                    f"Sent: {self.stats['messages_sent']}, "
                    f"Errors: {self.stats['errors']}, "
                    f"Rate: {rate:.2f} msg/sec"
                )
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info("Received shutdown signal, stopping consumer...")
        self.stop()
    
    def start(self):
        """Start the consumer"""
        logger.info("Starting Databricks Kafka consumer...")
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.running = True
        self.stats['start_time'] = time.time()
        
        # Start batch processor thread
        batch_thread = threading.Thread(target=self._batch_processor, daemon=True)
        batch_thread.start()
        
        # Start stats reporter thread
        stats_thread = threading.Thread(target=self._stats_reporter, daemon=True)
        stats_thread.start()
        
        try:
            # Main consumer loop
            for message in self.consumer:
                if not self.running:
                    break
                
                self._process_message(message)
                
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
            raise
        finally:
            self.stop()
    
    def stop(self):
        """Stop the consumer"""
        logger.info("Stopping consumer...")
        self.running = False
        
        if self.consumer:
            self.consumer.close()
        
        # Wait for queue to be processed
        while not self.message_queue.empty():
            time.sleep(0.1)
        
        logger.info("Consumer stopped")

def main():
    parser = argparse.ArgumentParser(description='Databricks Kafka Consumer')
    parser.add_argument('--bootstrap-servers', default='localhost:9092',
                       help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--topics', nargs='+', 
                       default=['retail_clickstream', 'retail_transactions', 'retail_inventory'],
                       help='Kafka topics to consume (default: retail_clickstream retail_transactions retail_inventory)')
    parser.add_argument('--databricks-host', required=True,
                       help='Databricks workspace URL')
    parser.add_argument('--databricks-token', required=True,
                       help='Databricks access token')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Batch size for sending to Databricks (default: 100)')
    parser.add_argument('--batch-timeout', type=int, default=30,
                       help='Batch timeout in seconds (default: 30)')
    
    args = parser.parse_args()
    
    # Create consumer
    consumer = DatabricksKafkaConsumer(
        bootstrap_servers=args.bootstrap_servers,
        topics=args.topics,
        databricks_host=args.databricks_host,
        databricks_token=args.databricks_token,
        batch_size=args.batch_size,
        batch_timeout=args.batch_timeout
    )
    
    try:
        consumer.connect()
        consumer.start()
    except Exception as e:
        logger.error(f"Failed to run consumer: {e}")
        exit(1)

if __name__ == "__main__":
    main() 