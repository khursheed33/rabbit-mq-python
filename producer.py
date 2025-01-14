import time
import json
from datetime import datetime

from rabbit_mq_manager import QueueConfig, RabbitMQManager

class MessageProducer:
    def __init__(self, rabbitmq_manager: RabbitMQManager, queue_id: str):
        self.rabbitmq_manager = rabbitmq_manager
        self.queue_id = queue_id
        self.message_index = 1

    def produce_message(self):
        """Produce a message every 10 seconds with an incrementing index"""
        while True:
            message = {
                'index': self.message_index,
                'content': f'Message #{self.message_index}',
                'timestamp': datetime.utcnow().isoformat()
            }
            success = self.rabbitmq_manager.publish_message(self.queue_id, message)
            if success:
                print(f"Produced: {json.dumps(message)}")
            else:
                print("Failed to produce message")
            self.message_index += 1
            time.sleep(10)  # Wait for 10 seconds before producing the next message


if __name__ == "__main__":
    # RabbitMQ configuration
    queue_id = "test_queue"
    rabbitmq_manager = RabbitMQManager('172.52.20.37')

    # Ensure queue exists
    queue_config = QueueConfig(queue_id=queue_id)
    rabbitmq_manager.delete_queue(queue_id=queue_id)
    rabbitmq_manager.create_queue(queue_config)

    # Create and run the producer
    producer = MessageProducer(rabbitmq_manager, queue_id)
    producer.produce_message()
