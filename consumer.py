import json
from time import sleep

from rabbit_mq_manager import QueueConfig, RabbitMQManager

class MessageConsumer:
    def __init__(self, rabbitmq_manager: RabbitMQManager, queue_id: str, user_id: str):
        self.rabbitmq_manager = rabbitmq_manager
        self.queue_id = queue_id
        self.user_id = user_id

    def consume_messages(self):
        """Consume messages from the queue"""
        def callback(message):
            """Process the message"""
            print(f"Consumer {self.user_id} received: {json.dumps(message)}")

        # Start consuming messages
        print(f"Consumer {self.user_id} started consuming from queue {self.queue_id}")
        self.rabbitmq_manager.consume_messages(self.queue_id, callback, user_id=self.user_id)


if __name__ == "__main__":
    # RabbitMQ configuration
    queue_id = "test_queue"
    rabbitmq_manager = RabbitMQManager()

    # Ensure queue exists
    queue_config = QueueConfig(queue_id=queue_id)
    rabbitmq_manager.create_queue(queue_config)

    # Create and run the consumers
    # Run the consumer in different terminals with different user_ids to test
    consumer_1 = MessageConsumer(rabbitmq_manager, queue_id, user_id="user_1")

    # Run consumers in separate threads or processes if you want them to run concurrently
    # For simplicity, we'll just run them sequentially here
    consumer_1.consume_messages()
    # To test multiple consumers, you can run the following in another terminal
    # consumer_2.consume_messages()
