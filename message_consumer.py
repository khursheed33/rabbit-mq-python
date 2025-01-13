#!/usr/bin/env python3
import time
from threading import Thread
from rabbit_mq_manager import RabbitMQManager
from datetime import datetime, UTC
import json
from typing import Dict, List


class DataConsumer:
    def __init__(self, rabbitmq: RabbitMQManager, topics: List[str], user_id: str = None):
        """
        Initialize consumer with RabbitMQ manager and topics
        
        Args:
            rabbitmq: RabbitMQManager instance
            topics: List of topics to consume messages from
            user_id: Optional user ID for subscribed consumer
        """
        self.rabbitmq = rabbitmq
        self.topics = topics
        self.user_id = user_id

    def process_message(self, message: Dict):
        """Process received message"""
        try:
            content = message.get('content', message)  # Handle both wrapped and unwrapped messages
            timestamp = content.get('timestamp', datetime.now(UTC).isoformat())
            
            print(f"\nReceived Message:")
            print(f"Timestamp: {timestamp}")
            print(f"Content: {json.dumps(content, indent=2)}")
            if self.user_id:
                print(f"Consumed by subscriber: {self.user_id}")
            print("-" * 50)
        except Exception as e:
            print(f"Error processing message: {str(e)}")
            print(f"Raw message: {message}")

    def start_consuming(self):
        """Start consuming messages from all topics"""
        print(f"Starting consumer{' (Subscriber: ' + self.user_id + ')' if self.user_id else ''}")
        print(f"Listening to topics: {', '.join(self.topics)}")
        
        for topic in self.topics:
            try:
                self.rabbitmq.consume_messages(
                    queue_id=topic,
                    callback=self.process_message,
                    user_id=self.user_id
                )
            except Exception as e:
                print(f"Error consuming from {topic}: {str(e)}")


def create_consumer(rabbitmq: RabbitMQManager, topics: List[str], user_id: str, delay: int = 0):
    """
    Create a consumer and start it after an optional delay.

    Args:
        rabbitmq: RabbitMQManager instance
        topics: List of topics to consume messages from
        user_id: User ID for the consumer
        delay: Delay before starting the consumer, in seconds
    """
    if delay > 0:
        time.sleep(delay)
    consumer = DataConsumer(rabbitmq, topics, user_id)
    consumer.start_consuming()


if __name__ == '__main__':
    # Initialize RabbitMQ manager
    rabbitmq = RabbitMQManager(
        host='localhost',
        port=5672,
        username='admin',
        password='admin'
    )

    # Define consumers and their configurations
    consumers_config = [
        {"topics": ["orders"], "user_id": "consumer1", "delay": 0},  # Consumer 1 listens on "orders"
        {"topics": ["orders"], "user_id": "consumer2", "delay": 20},  # Consumer 2 listens on "orders" after 20 seconds
        {"topics": ["metrics"], "user_id": "consumer3", "delay": 0},  # Consumer 3 listens on "metrics"
    ]

    # Create and start consumers in separate threads
    threads = []
    for config in consumers_config:
        thread = Thread(
            target=create_consumer,
            args=(rabbitmq, config["topics"], config["user_id"], config["delay"])
        )
        thread.start()
        threads.append(thread)

    # Wait for all threads to complete
    for thread in threads:
        thread.join()
