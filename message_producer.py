#!/usr/bin/env python3
from rabbit_mq_manager import RabbitMQManager, QueueConfig
import time
import random
import json
from datetime import datetime, UTC
import asyncio
from typing import List, Dict


class DataProducer:
    def __init__(self, rabbitmq: RabbitMQManager, topics: List[str]):
        """
        Initialize producer with RabbitMQ manager and topics
        
        Args:
            rabbitmq: RabbitMQManager instance
            topics: List of topics to produce messages for
        """
        self.rabbitmq = rabbitmq
        self.topics = topics
        self._setup_queues()
        
    def _setup_queues(self):
        """Set up queues for all topics"""
        for topic in self.topics:
            config = QueueConfig(
                queue_id=topic,
                durable=True
            )
            self.rabbitmq.create_queue(config)
            
    def generate_message(self, topic: str) -> Dict:
        """Generate a sample message for given topic"""
        message_types = {
            'orders': {
                'order_id': f"ORD-{random.randint(1000, 9999)}",
                'customer_id': f"CUST-{random.randint(100, 999)}",
                'amount': round(random.uniform(10.0, 1000.0), 2),
                'items': random.randint(1, 10)
            },
            'users': {
                'user_id': f"USR-{random.randint(1000, 9999)}",
                'action': random.choice(['login', 'logout', 'update_profile', 'purchase']),
                'platform': random.choice(['web', 'mobile', 'desktop']),
                'region': random.choice(['NA', 'EU', 'ASIA', 'SA'])
            },
            'metrics': {
                'service': random.choice(['auth', 'payment', 'inventory', 'shipping']),
                'cpu_usage': round(random.uniform(0, 100), 2),
                'memory_usage': round(random.uniform(0, 100), 2),
                'response_time': round(random.uniform(10, 500), 2)
            }
        }
        
        base_message = message_types.get(topic, {'default': 'message'})
        base_message.update({
            'timestamp': datetime.now(UTC).isoformat(),
            'topic': topic
        })
        return base_message

    async def produce_messages(self, interval: float = 5.0):
        """
        Continuously produce messages for all topics
        
        Args:
            interval: Time interval between messages in seconds
        """
        while True:
            for topic in self.topics:
                message = self.generate_message(topic)
                success = self.rabbitmq.publish_message(topic, message)
                if success:
                    print(f"Published to {topic}: {json.dumps(message, indent=2)}")
                else:
                    print(f"Failed to publish to {topic}")
            await asyncio.sleep(interval)


if __name__ == "__main__":
    # Initialize RabbitMQ manager with Docker credentials
    rabbitmq = RabbitMQManager(
        host='localhost',
        port=5672,
        username='admin',  # Updated to match Docker configuration
        password='admin'   # Updated to match Docker configuration
    )
    
    # Define topics
    topics = ['orders', 'users', 'metrics']
    
    # Run producer
    producer = DataProducer(rabbitmq, topics)
    asyncio.run(producer.produce_messages())