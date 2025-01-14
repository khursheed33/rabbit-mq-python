import json
from uuid import uuid4
from rabbit_mq_manager import QueueConfig, RabbitMQManager


class MessageConsumer:
    def __init__(self, rabbitmq_manager: RabbitMQManager, queue_id: str, user_id: str, session_id: str = None):
        self.rabbitmq_manager = rabbitmq_manager
        self.queue_id = queue_id
        self.user_id = user_id
        self.session_id =  str(uuid4())  # Generate a unique session ID if not provided

        # Subscribe this session to the queue
        self.rabbitmq_manager.subscribe_to_queue(self.queue_id, self.user_id, self.session_id)

    def consume_messages(self):
        """Consume messages from the session-specific queue."""
        def callback(message):
            """Process the message."""
            print(f"Consumer {self.user_id} [Session {self.session_id}] received: {json.dumps(message)}")

        # Start consuming messages
        print(f"Consumer {self.user_id} [Session {self.session_id}] started consuming from queue {self.queue_id}")
        self.rabbitmq_manager.consume_messages(self.queue_id, callback, user_id=self.user_id, session_id=self.session_id)

if __name__ == "__main__":
    # RabbitMQ configuration
    queue_id = "test_queue"
    rabbitmq_manager = RabbitMQManager('172.52.20.37')

    # Ensure queue exists
    queue_config = QueueConfig(queue_id=queue_id)
    rabbitmq_manager.create_queue(queue_config)

    # Create and run the consumer
    # Provide a unique session_id if you want to distinguish between different instances explicitly
    consumer = MessageConsumer(rabbitmq_manager, queue_id, user_id="user_1")

    # Consume messages
    consumer.consume_messages()
    
