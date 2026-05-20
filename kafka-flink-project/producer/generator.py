#!/usr/bin/env python3
"""
Kafka Producer - Generates simulated e-commerce order data
Publishes to Kafka topic "orders"
Supports continuous infinite loop with configurable delay and graceful shutdown
"""

import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable, KafkaConnectionError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class OrderGenerator:
    """Generates simulated e-commerce order data"""
    
    PRODUCTS = [
        {"product_id": "PROD-001", "name": "Laptop", "base_price": 999.99},
        {"product_id": "PROD-002", "name": "Smartphone", "base_price": 699.99},
        {"product_id": "PROD-003", "name": "Tablet", "base_price": 449.99},
        {"product_id": "PROD-004", "name": "Headphones", "base_price": 199.99},
        {"product_id": "PROD-005", "name": "Smart Watch", "base_price": 299.99},
        {"product_id": "PROD-006", "name": "Camera", "base_price": 799.99},
        {"product_id": "PROD-007", "name": "Keyboard", "base_price": 79.99},
        {"product_id": "PROD-008", "name": "Mouse", "price": 39.99},
        {"product_id": "PROD-009", "name": "Monitor", "price": 349.99},
        {"product_id": "PROD-010", "name": "Speaker", "price": 149.99},
    ]
    
    CUSTOMERS = [f"CUST-{str(i).zfill(4)}" for i in range(1, 101)]
    
    STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled"]
    
    def __init__(self):
        self.order_id_prefix = "ORD"
        self.shipping_cost = 9.99
        self.tax_rate = 0.08  # 8% tax
    
    def generate_order(self) -> Dict[str, Any]:
        """Generate a single random order"""
        # Random product with some price variation (±15%)
        product = random.choice(self.PRODUCTS)
        base_price = product["base_price"] if "base_price" in product else product["price"]
        price_variation = random.uniform(-0.15, 0.15)
        final_price = round(base_price * (1 + price_variation), 2)
        
        # Random quantity (1-5)
        quantity = random.randint(1, 5)
        
        # Random customer
        customer_id = random.choice(self.CUSTOMERS)
        
        # Random timestamp within last 24 hours
        hours_ago = random.uniform(0, 24)
        order_timestamp = datetime.now() - timedelta(hours=hours_ago)
        
        # Random status (weighted - more pending/processing)
        status_weights = [0.3, 0.25, 0.2, 0.15, 0.1]  # pending, processing, shipped, delivered, cancelled
        status = random.choices(self.STATUSES, weights=status_weights)[0]
        
        order = {
            "order_id": f"{self.order_id_prefix}-{uuid.uuid4().hex[:8].upper()}",
            "customer_id": customer_id,
            "product_id": product["product_id"],
            "quantity": quantity,
            "price": final_price,
            "order_timestamp": order_timestamp.isoformat(),
            "status": status
        }
        
        return order
    
    def generate_batch(self, num_orders: int = 10) -> list:
        """Generate a batch of orders"""
        return [self.generate_order() for _ in range(num_orders)]


class OrderProducer:
    """Kafka producer for publishing orders with retry logic and graceful shutdown"""
    
    def __init__(
        self, 
        bootstrap_servers: str = "localhost:9092",
        max_retries: int = 5,
        retry_backoff_base: float = 2.0,
        initial_backoff: float = 1.0
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = "orders"
        self.producer: Optional[KafkaProducer] = None
        self.generator = OrderGenerator()
        
        # Retry configuration
        self.max_retries = max_retries
        self.retry_backoff_base = retry_backoff_base
        self.initial_backoff = initial_backoff
        
        # Shutdown flag
        self._shutdown_requested = False
        self._total_published = 0
        
        # Register signal handlers
        self._register_signal_handlers()
    
    def _register_signal_handlers(self):
        """Register handlers for graceful shutdown"""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        self._shutdown_requested = True
    
    def connect(self, retries: Optional[int] = None) -> bool:
        """
        Connect to Kafka with exponential backoff retry logic
        """
        retry_count = retries if retries is not None else self.max_retries
        backoff = self.initial_backoff
        
        for attempt in range(1, retry_count + 1):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=3,
                    max_in_flight_requests_per_connection=1,
                    # Additional reliability settings
                    request_timeout_ms=30000,
                    reconnect_backoff_ms=1000,
                    reconnect_backoff_max_ms=10000,
                )
                logger.info(f"Successfully connected to Kafka at {self.bootstrap_servers}")
                return True
                
            except NoBrokersAvailable as e:
                logger.warning(
                    f"Attempt {attempt}/{retry_count} - No brokers available. "
                    f"Retrying in {backoff:.1f}s... (Error: {e})"
                )
            except KafkaConnectionError as e:
                logger.warning(
                    f"Attempt {attempt}/{retry_count} - Connection error. "
                    f"Retrying in {backoff:.1f}s... (Error: {e})"
                )
            except Exception as e:
                logger.error(f"Unexpected error connecting to Kafka: {e}")
                break
            
            if not self._shutdown_requested and attempt < retry_count:
                time.sleep(backoff)
                backoff = min(backoff * self.retry_backoff_base, 60.0)  # Cap at 60 seconds
        
        logger.error(f"Failed to connect to Kafka after {retry_count} attempts")
        return False
    
    def reconnect_if_needed(self) -> bool:
        """Attempt to reconnect if producer is not connected"""
        if self.producer is None:
            logger.info("Producer not connected. Attempting to reconnect...")
            return self.connect()
        return True
    
    def publish_order(self, order: Dict[str, Any]) -> bool:
        """Publish a single order to Kafka with error handling"""
        try:
            if not self.reconnect_if_needed():
                return False
            
            future = self.producer.send(
                self.topic,
                key=order["order_id"],
                value=order
            )
            # Wait for send to complete (with timeout)
            record_metadata = future.get(timeout=10)
            
            logger.debug(
                f"Published order {order['order_id']} to "
                f"{record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}"
            )
            self._total_published += 1
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to publish order {order.get('order_id', 'unknown')}: {e}")
            # Mark producer as disconnected so we attempt to reconnect
            self.producer = None
            return False
        except Exception as e:
            logger.error(f"Unexpected error publishing order: {e}")
            return False
    
    def publish_batch(self, orders: list, delay: float = 0.5):
        """Publish a batch of orders with delay between each"""
        for order in orders:
            success = self.publish_order(order)
            if success:
                logger.info(
                    f"Published: {order['order_id']} - {order['product_id']} "
                    f"x{order['quantity']} - ${order['price']} - {order['status']}"
                )
            else:
                logger.error(f"Failed: {order['order_id']}")
            time.sleep(delay)
    
    def close(self):
        """Close the producer gracefully"""
        if self.producer:
            try:
                logger.info("Flushing pending messages...")
                self.producer.flush()
                logger.info("Flushed all pending messages")
            except Exception as e:
                logger.warning(f"Error during flush: {e}")
            finally:
                self.producer.close()
                self.producer = None
                logger.info("Producer closed")
    
    @property
    def total_published(self) -> int:
        """Return total number of orders published"""
        return self._total_published
    
    def is_shutdown_requested(self) -> bool:
        """Check if shutdown was requested"""
        return self._shutdown_requested


def main():
    """Main function to run the continuous producer"""
    # Get configuration from environment variables
    kafka_server = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    delay_min = float(os.environ.get("DELAY_MIN", "1.0"))  # Minimum delay in seconds
    delay_max = float(os.environ.get("DELAY_MAX", "2.0"))  # Maximum delay in seconds
    batch_mode = os.environ.get("BATCH_MODE", "false").lower() == "true"
    batch_size = int(os.environ.get("BATCH_SIZE", "5"))
    max_retries = int(os.environ.get("MAX_RETRIES", "5"))
    
    logger.info("=" * 60)
    logger.info("Starting Order Producer - Continuous Mode")
    logger.info("=" * 60)
    logger.info(f"Kafka Server: {kafka_server}")
    logger.info(f"Topic: orders")
    logger.info(f"Delay Range: {delay_min}s - {delay_max}s")
    logger.info(f"Batch Mode: {batch_mode} (batch_size={batch_size})")
    logger.info(f"Max Retries: {max_retries}")
    logger.info("=" * 60)
    logger.info("Press Ctrl+C or send SIGTERM for graceful shutdown")
    logger.info("=" * 60)
    
    producer = OrderProducer(
        bootstrap_servers=kafka_server,
        max_retries=max_retries
    )
    
    # Initial connection with retries
    if not producer.connect():
        logger.error("Exiting due to connection failure")
        sys.exit(1)
    
    try:
        while not producer.is_shutdown_requested():
            try:
                if batch_mode:
                    # Generate and publish a batch
                    orders = producer.generator.generate_batch(batch_size)
                    for order in orders:
                        if producer.is_shutdown_requested():
                            break
                        success = producer.publish_order(order)
                        if success:
                            logger.info(
                                f"Published: {order['order_id']} - {order['product_id']} "
                                f"x{order['quantity']} - ${order['price']} - {order['status']}"
                            )
                        else:
                            logger.warning(f"Failed to publish: {order['order_id']}")
                else:
                    # Generate and publish a single order
                    order = producer.generator.generate_order()
                    success = producer.publish_order(order)
                    
                    if success:
                        logger.info(
                            f"Published: {order['order_id']} - {order['product_id']} "
                            f"x{order['quantity']} - ${order['price']} - {order['status']}"
                        )
                    else:
                        logger.warning(f"Failed to publish: {order['order_id']}")
                        # Wait before retrying connection
                        time.sleep(min(delay_max * 2, 5.0))
                        continue
                
                # Calculate random delay between min and max
                delay = random.uniform(delay_min, delay_max)
                logger.debug(f"Sleeping for {delay:.2f}s before next order")
                time.sleep(delay)
                
            except Exception as e:
                logger.error(f"Error in production loop: {e}")
                # Brief pause before continuing
                time.sleep(1)
                
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received")
    
    finally:
        logger.info(f"\nShutting down gracefully...")
        logger.info(f"Total orders published: {producer.total_published}")
        producer.close()
        logger.info("Producer shutdown complete")


if __name__ == "__main__":
    main()
