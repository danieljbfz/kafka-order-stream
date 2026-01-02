import json
import socket
import random
import time
from uuid import uuid4
import logging
from log_utils import EmojiFormatter
from confluent_kafka import Producer

logger = logging.getLogger("OrderProducer")
handler = logging.StreamHandler()
handler.setFormatter(EmojiFormatter('%(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

class OrderFactory:
    """Generates random order data for the stream"""
    
    PRODUCTS = [
        ('laptop', 1200.00), ('mouse', 25.00), ('monitor', 300.00),
        ('keyboard', 75.00), ('webcam', 50.00), ('headset', 120.00)
    ]

    USERS = ["daniel", "sofia", "marcus", "claire"]

    @classmethod
    def create_random_order(cls):
        items = []
        # Add 1 to 3 random items to the order
        for _ in range(random.randint(1, 3)):
            prod_name, price = random.choice(cls.PRODUCTS)
            items.append({
                'productId': prod_name,
                'quantity': random.randint(1, 2),
                'price': price
            })
            
        return {
            'orderId': str(uuid4()),
            'userId': random.choice(cls.USERS),
            'items': items,
            'timestamp': time.time()
        }

class KafkaProducerWrapper:
    """Connects to Kafka and produces messages"""
    
    def __init__(self, bootstrap_servers='localhost:9092'):
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': socket.gethostname(),
            # Batch messages to improve performance
            'linger.ms': 10, 
            'compression.type': 'snappy'
        }
        self.producer = Producer(conf)

    @staticmethod
    def delivery_report(err, msg):
        """Checks if the message actually made it to Kafka"""
        if err is not None:
            logger.error(f"Delivery failed: {err}")
        else:
            logger.info(f"Delivered {msg.value().decode('utf-8')} to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def produce(self, topic, key, value):
        try:
            self.producer.produce(
                topic=topic,
                key=key,
                value=json.dumps(value).encode('utf-8'),
                on_delivery=self.delivery_report
            )
            # Poll handles delivery callbacks
            self.producer.poll(0)
        except BufferError:
            logger.warning("Local queue is full. Flushing...")
            self.producer.poll(1)

    def close(self):
        logger.warning("Flushing the remaining messages...")
        self.producer.flush()
        logger.info("Disconnected successfully.")

def main():
    topic_name = 'orders'
    kafka_manager = KafkaProducerWrapper()
    
    logger.info("Starting the stream... Press Ctrl+C to stop.")
    
    try:
        while True:
            # Generate a new order
            new_order = OrderFactory.create_random_order()
            
            # Send the new order to Kafka
            kafka_manager.produce(topic_name, new_order['orderId'], new_order)
            
            # Wait a bit before sending the next one
            time.sleep(random.uniform(1.0, 4.0))
            
    except KeyboardInterrupt:
        logger.warning("Shutting down...")
    finally:
        kafka_manager.close()

if __name__ == "__main__":
    main()