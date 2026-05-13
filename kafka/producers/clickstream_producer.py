#!/usr/bin/env python3
"""
Retail Clickstream Data Producer
Simulates realistic retail clickstream data for the Real-Time Retail Insights Platform
"""

import json
import random
import time
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging
from kafka import KafkaProducer  # type: ignore[attr-defined]
from kafka.errors import KafkaError
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class RetailClickstreamProducer:
    """Produces realistic retail clickstream data to Kafka"""

    def __init__(self, bootstrap_servers: str, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer: Optional[KafkaProducer] = None
        self.session_id: Optional[str] = None
        self.user_id: Optional[str] = None

        # Product catalog for realistic data
        self.products = self._load_product_catalog()
        self.categories = list(set(product["category"] for product in self.products))

        # User behavior patterns
        self.user_sessions: Dict[str, Any] = {}
        self.page_views = 0

    def _load_product_catalog(self) -> List[Dict[str, Any]]:
        """Load realistic product catalog"""
        return [
            {
                "id": "P001",
                "name": "Wireless Bluetooth Headphones",
                "category": "Electronics",
                "price": 89.99,
                "brand": "TechSound",
            },
            {
                "id": "P002",
                "name": "Smart Fitness Watch",
                "category": "Electronics",
                "price": 199.99,
                "brand": "FitTech",
            },
            {
                "id": "P003",
                "name": "Organic Cotton T-Shirt",
                "category": "Clothing",
                "price": 24.99,
                "brand": "EcoWear",
            },
            {
                "id": "P004",
                "name": "Running Shoes",
                "category": "Footwear",
                "price": 129.99,
                "brand": "RunFast",
            },
            {
                "id": "P005",
                "name": "Coffee Maker",
                "category": "Home & Garden",
                "price": 79.99,
                "brand": "BrewMaster",
            },
            {
                "id": "P006",
                "name": "Yoga Mat",
                "category": "Sports",
                "price": 34.99,
                "brand": "FlexFit",
            },
            {
                "id": "P007",
                "name": "Laptop Backpack",
                "category": "Accessories",
                "price": 59.99,
                "brand": "TravelPro",
            },
            {
                "id": "P008",
                "name": "Wireless Mouse",
                "category": "Electronics",
                "price": 29.99,
                "brand": "TechSound",
            },
            {
                "id": "P009",
                "name": "Desk Lamp",
                "category": "Home & Garden",
                "price": 44.99,
                "brand": "LightPro",
            },
            {
                "id": "P010",
                "name": "Water Bottle",
                "category": "Sports",
                "price": 19.99,
                "brand": "HydrateWell",
            },
            {
                "id": "P011",
                "name": "Denim Jeans",
                "category": "Clothing",
                "price": 69.99,
                "brand": "EcoWear",
            },
            {
                "id": "P012",
                "name": "Gaming Headset",
                "category": "Electronics",
                "price": 149.99,
                "brand": "GameTech",
            },
            {
                "id": "P013",
                "name": "Plant Pot",
                "category": "Home & Garden",
                "price": 14.99,
                "brand": "GreenThumb",
            },
            {
                "id": "P014",
                "name": "Resistance Bands",
                "category": "Sports",
                "price": 24.99,
                "brand": "FlexFit",
            },
            {
                "id": "P015",
                "name": "Phone Case",
                "category": "Accessories",
                "price": 19.99,
                "brand": "ProtectPro",
            },
        ]

    def connect(self):
        """Connect to Kafka broker"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                batch_size=16384,
                linger_ms=10,
                buffer_memory=33554432,
            )
            logger.info(f"Connected to Kafka broker: {self.bootstrap_servers}")
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def _generate_user_id(self) -> str:
        """Generate a realistic user ID"""
        return f"user_{random.randint(10000, 99999)}"

    def _generate_session_id(self) -> str:
        """Generate a session ID"""
        return str(uuid.uuid4())

    def _generate_device_info(self) -> Dict[str, str]:
        """Generate realistic device information"""
        devices = [
            {"type": "desktop", "os": "Windows", "browser": "Chrome"},
            {"type": "desktop", "os": "macOS", "browser": "Safari"},
            {"type": "mobile", "os": "iOS", "browser": "Safari"},
            {"type": "mobile", "os": "Android", "browser": "Chrome"},
            {"type": "tablet", "os": "iOS", "browser": "Safari"},
        ]
        return random.choice(devices)

    def _generate_location(self) -> Dict[str, Any]:
        """Generate realistic location data"""
        # Major US cities with coordinates
        cities = [
            {"city": "New York", "lat": 40.7128, "lng": -74.0060},
            {"city": "Los Angeles", "lat": 34.0522, "lng": -118.2437},
            {"city": "Chicago", "lat": 41.8781, "lng": -87.6298},
            {"city": "Houston", "lat": 29.7604, "lng": -95.3698},
            {"city": "Phoenix", "lat": 33.4484, "lng": -112.0740},
            {"city": "Philadelphia", "lat": 39.9526, "lng": -75.1652},
            {"city": "San Antonio", "lat": 29.4241, "lng": -98.4936},
            {"city": "San Diego", "lat": 32.7157, "lng": -117.1611},
            {"city": "Dallas", "lat": 32.7767, "lng": -96.7970},
            {"city": "San Jose", "lat": 37.3382, "lng": -121.8863},
        ]
        city = random.choice(cities)
        lat = float(city["lat"])  # type: ignore[arg-type]
        lng = float(city["lng"])  # type: ignore[arg-type]
        # Add some randomness to coordinates
        return {
            "latitude": lat + random.uniform(-0.1, 0.1),
            "longitude": lng + random.uniform(-0.1, 0.1),
            "city": str(city["city"]),
        }

    def _generate_page_view_event(self) -> Dict[str, Any]:
        """Generate a page view event"""
        product = random.choice(self.products)

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "page_view",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "user_id": self.user_id,
            "session_id": self.session_id,
            "page_url": f"/product/{product['id']}",
            "page_title": f"{product['name']} - {product['brand']}",
            "referrer": random.choice(
                [
                    "https://www.google.com/search",
                    "https://www.facebook.com",
                    "https://www.instagram.com",
                    "https://www.twitter.com",
                    "https://www.pinterest.com",
                    "direct",
                ]
            ),
            "product_id": product["id"],
            "product_name": product["name"],
            "product_category": product["category"],
            "product_price": product["price"],
            "product_brand": product["brand"],
            "device_info": self._generate_device_info(),
            "location": self._generate_location(),
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "ip_address": f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
        }

        return event

    def _generate_add_to_cart_event(self) -> Dict[str, Any]:
        """Generate an add to cart event"""
        product = random.choice(self.products)
        quantity = random.randint(1, 3)

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "add_to_cart",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "user_id": self.user_id,
            "session_id": self.session_id,
            "product_id": product["id"],
            "product_name": product["name"],
            "product_category": product["category"],
            "product_price": product["price"],
            "product_brand": product["brand"],
            "quantity": quantity,
            "total_value": product["price"] * quantity,
            "device_info": self._generate_device_info(),
            "location": self._generate_location(),
        }

        return event

    def _generate_purchase_event(self) -> Dict[str, Any]:
        """Generate a purchase event"""
        # Simulate cart with multiple items
        cart_items = random.sample(self.products, random.randint(1, 4))
        total_value = sum(item["price"] * random.randint(1, 2) for item in cart_items)

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "purchase",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "user_id": self.user_id,
            "session_id": self.session_id,
            "order_id": f"ORD_{random.randint(100000, 999999)}",
            "cart_items": [
                {
                    "product_id": item["id"],
                    "product_name": item["name"],
                    "product_category": item["category"],
                    "product_price": item["price"],
                    "quantity": random.randint(1, 2),
                }
                for item in cart_items
            ],
            "total_value": total_value,
            "shipping_method": random.choice(["standard", "express", "overnight"]),
            "payment_method": random.choice(["credit_card", "paypal", "apple_pay", "google_pay"]),
            "device_info": self._generate_device_info(),
            "location": self._generate_location(),
        }

        return event

    def _generate_search_event(self) -> Dict[str, Any]:
        """Generate a search event"""
        search_terms = [
            "wireless headphones",
            "running shoes",
            "coffee maker",
            "yoga mat",
            "laptop backpack",
            "smart watch",
            "organic clothing",
            "fitness equipment",
            "home decor",
            "tech accessories",
            "sports gear",
            "kitchen appliances",
        ]

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "search",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "user_id": self.user_id,
            "session_id": self.session_id,
            "search_query": random.choice(search_terms),
            "search_results_count": random.randint(10, 100),
            "device_info": self._generate_device_info(),
            "location": self._generate_location(),
        }

        return event

    def _should_start_new_session(self) -> bool:
        """Determine if we should start a new session"""
        if not self.session_id:
            return True

        # 30% chance to start new session after 5+ page views
        if self.page_views > 5 and random.random() < 0.3:
            return True

        # 10% chance to start new session randomly
        if random.random() < 0.1:
            return True

        return False

    def _should_generate_purchase(self) -> bool:
        """Determine if we should generate a purchase event"""
        # 5% chance of purchase per session
        return random.random() < 0.05

    def _should_generate_add_to_cart(self) -> bool:
        """Determine if we should generate an add to cart event"""
        # 15% chance of add to cart per session
        return random.random() < 0.15

    def _should_generate_search(self) -> bool:
        """Determine if we should generate a search event"""
        # 20% chance of search per session
        return random.random() < 0.20

    def generate_event(self) -> Dict[str, Any]:
        """Generate a realistic clickstream event"""
        # Start new session if needed
        if self._should_start_new_session():
            self.session_id = self._generate_session_id()
            self.user_id = self._generate_user_id()
            self.page_views = 0
            logger.info(f"Started new session: {self.session_id} for user: {self.user_id}")

        # Determine event type based on probabilities
        if self._should_generate_purchase():
            event = self._generate_purchase_event()
        elif self._should_generate_add_to_cart():
            event = self._generate_add_to_cart_event()
        elif self._should_generate_search():
            event = self._generate_search_event()
        else:
            event = self._generate_page_view_event()
            self.page_views += 1

        return event

    def send_event(self, event: Dict[str, Any]):
        """Send event to Kafka topic"""
        assert self.producer is not None, "Producer not connected"
        try:
            # Use user_id as key for partitioning
            future = self.producer.send(self.topic, key=event["user_id"], value=event)

            # Wait for the send to complete
            record_metadata = future.get(timeout=10)

            logger.info(
                f"Event sent successfully - "
                f"Topic: {record_metadata.topic}, "
                f"Partition: {record_metadata.partition}, "
                f"Offset: {record_metadata.offset}, "
                f"Event Type: {event['event_type']}"
            )

        except KafkaError as e:
            logger.error(f"Failed to send event: {e}")
            raise

    def run(self, events_per_second: int = 10, duration_minutes: Optional[int] = None):
        """Run the producer"""
        logger.info(f"Starting clickstream producer - {events_per_second} events/sec")

        start_time = time.time()
        event_count = 0

        try:
            while True:
                # Check if we should stop
                if duration_minutes and (time.time() - start_time) > (duration_minutes * 60):
                    logger.info(f"Duration reached, stopping producer. Total events: {event_count}")
                    break

                # Generate and send event
                event = self.generate_event()
                self.send_event(event)
                event_count += 1

                # Sleep to maintain rate
                time.sleep(1.0 / events_per_second)

                # Log progress every 100 events
                if event_count % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = event_count / elapsed
                    logger.info(f"Sent {event_count} events in {elapsed:.1f}s (rate: {rate:.1f} events/sec)")

        except KeyboardInterrupt:
            logger.info(f"Producer stopped by user. Total events: {event_count}")
        except Exception as e:
            logger.error(f"Producer error: {e}")
            raise
        finally:
            if self.producer is not None:
                self.producer.flush()
                self.producer.close()


def main():
    parser = argparse.ArgumentParser(description="Retail Clickstream Data Producer")
    parser.add_argument(
        "--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers (default: localhost:9092)"
    )
    parser.add_argument("--topic", default="retail_clickstream", help="Kafka topic name (default: retail_clickstream)")
    parser.add_argument("--events-per-second", type=int, default=10, help="Events per second to generate (default: 10)")
    parser.add_argument(
        "--duration-minutes", type=int, default=None, help="Duration to run in minutes (default: run indefinitely)"
    )

    args = parser.parse_args()

    # Create producer
    producer = RetailClickstreamProducer(args.bootstrap_servers, args.topic)

    try:
        producer.connect()
        producer.run(args.events_per_second, args.duration_minutes)
    except Exception as e:
        logger.error(f"Failed to run producer: {e}")
        exit(1)


if __name__ == "__main__":
    main()
