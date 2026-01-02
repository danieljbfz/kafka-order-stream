import json
import signal
import logging
from log_utils import EmojiFormatter
from rich.live import Live
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.console import Group
from rich.columns import Columns
from rich import box
from confluent_kafka import Consumer, KafkaError, KafkaException

logger = logging.getLogger("OrderConsumer")
handler = logging.StreamHandler()
handler.setFormatter(EmojiFormatter('%(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

class OrderProcessor:
    """Updates global revenue metrics and stores a summary of the latest 5 orders"""
    def __init__(self):
        self.total_revenue = 0.0
        self.order_count = 0
        self.last_orders = []

    def process(self, order_data):
        order_total = sum(item['price'] * item['quantity'] for item in order_data['items'])
        self.total_revenue += order_total
        self.order_count += 1
        
        user = order_data.get('userId', 'Unknown')
        self.last_orders.insert(0, (user, f"${order_total:,.2f}"))
        self.last_orders = self.last_orders[:5]
    
    def generate_ui(self):
        LEFT_W = 34
        RIGHT_W = 34
        COL_W = 14
        GAP = 4

        stats = Table(show_header=False, box=None, pad_edge=False)
        stats.add_column(justify="left", width=LEFT_W - COL_W)
        stats.add_column(justify="right", width=COL_W, no_wrap=True)

        stats.add_row("Total Orders", f"[bold cyan]{self.order_count}[/]")
        stats.add_row("Total Revenue", f"[bold green]${self.total_revenue:,.2f}[/]")

        activity = Table(
            title="Recent Activity",
            box=box.SIMPLE_HEAVY,
            pad_edge=False,
            expand=False
        )
        activity.add_column("User", style="magenta", no_wrap=True, width=COL_W)
        activity.add_column("Amount", justify="right", style="green", no_wrap=True, width=COL_W)

        for user, amount in self.last_orders:
            activity.add_row(user, amount)

        left_panel = Panel(stats, box=box.ROUNDED, padding=(0, 1), expand=False)
        right_panel = Panel(activity, box=box.ROUNDED, padding=(0, 1), expand=False)

        layout = Table.grid(expand=False)
        layout.add_column(width=LEFT_W)
        layout.add_column(width=RIGHT_W)
        layout.add_row(left_panel, right_panel)

        outer_width = LEFT_W + RIGHT_W + GAP
        outer = Panel(
            Group(layout),
            title=" Kafka Order Monitor ",
            border_style="blue",
            box=box.ROUNDED,
            padding=(0, 1),
            expand=False,
            width=outer_width
        )
        return outer

class KafkaConsumerWrapper:
    """Connects to Kafka and listens for new messages"""
    def __init__(self, topic, group_id, bootstrap_servers='localhost:9092'):
        self.conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        }
        self.consumer = Consumer(self.conf)
        self.topic = topic
        self.consumer.subscribe([topic])

    def poll(self, timeout=1.0):
        msg = self.consumer.poll(timeout)

        if msg is None: 
            return None
        
        if msg.error():
            # Hit the end of the partition, keep going
            if msg.error().code() == KafkaError._PARTITION_EOF: 
                logger.debug("Hit the end of the partition")
                return None

            # Found a real problem
            logger.error(f"Kafka consumer error: {msg.error()}")
            raise KafkaException(msg.error())

        try:
            return json.loads(msg.value().decode("utf-8"))
        except Exception as e:
            logger.error(f"Failed to decode message: {e}")
            return None

    def close(self):
        logger.warning("Disconnecting from Kafka...")
        self.consumer.close()
        logger.info("Disconnected successfully.")

def main():
    topic_name = 'orders'
    processor = OrderProcessor()
    kafka_consumer = KafkaConsumerWrapper(topic=topic_name, group_id='order_analytics_v1')

    logger.info(f"Listening for {topic_name}... Press Ctrl+C to stop.")

    try:
        with Live(processor.generate_ui(), refresh_per_second=4, screen=False) as live:
            while True:
                # Poll for new messages
                order = kafka_consumer.poll()
                
                if order:
                    # Process the new order
                    processor.process(order)

                    # Render the updated metrics
                    live.update(processor.generate_ui())

    except KeyboardInterrupt:
        logger.warning("Shutting down...")
    finally:
        kafka_consumer.close()


if __name__ == "__main__":
    main()