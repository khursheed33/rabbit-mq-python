import pika
import json
import time
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
from dataclasses import dataclass

@dataclass
class QueueConfig:
    """Configuration for a RabbitMQ queue"""
    queue_id: str
    durable: bool = True
    auto_delete: bool = False
    arguments: Dict = None

class RabbitMQManager:
    """Manager class for RabbitMQ operations"""
    
    def __init__(self, host: str = 'localhost', port: int = 5672,
                 username: str = 'admin', password: str = 'admin',
                 virtual_host: str = '/'):
        self.credentials = pika.PlainCredentials(username, password)
        self.parameters = pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=virtual_host,
            credentials=self.credentials
        )
        self.subscriptions: Dict[str, List[str]] = {}  # queue_id -> list of user_ids
        self.logger = logging.getLogger(__name__)

    def _get_connection(self) -> pika.BlockingConnection:
        """Create and return a new connection"""
        return pika.BlockingConnection(self.parameters)

    def create_queue(self, queue_config: QueueConfig) -> bool:
        try:
            connection = self._get_connection()
            channel = connection.channel()
            
            if queue_config.arguments is None:
                queue_config.arguments = {}
            
            # Remove or don't add the x-message-ttl argument to avoid message expiration
            if 'x-message-ttl' in queue_config.arguments:
                del queue_config.arguments['x-message-ttl']
            
            # Declare the main queue
            channel.queue_declare(
                queue=queue_config.queue_id,
                durable=queue_config.durable,
                auto_delete=queue_config.auto_delete,
                arguments=queue_config.arguments
            )
            
            connection.close()
            return True
        except Exception as e:
            self.logger.error(f"Error creating queue: {str(e)}")
            return False

    def publish_message(self, queue_id: str, message: Any) -> bool:
        try:
            connection = self._get_connection()
            channel = connection.channel()
            
            message_with_metadata = {
                'content': message,
                'timestamp': datetime.utcnow().isoformat(),
            }
            
            # Publish to main queue (ensure persistence)
            channel.basic_publish(
                exchange='',
                routing_key=queue_id,
                body=json.dumps(message_with_metadata),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                )
            )
            
            # Publish to fanout exchange for subscribers
            channel.exchange_declare(exchange=f"{queue_id}_fanout", exchange_type='fanout', durable=True)
            channel.basic_publish(
                exchange=f"{queue_id}_fanout",
                routing_key='',
                body=json.dumps(message_with_metadata),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                )
            )
            
            connection.close()
            return True
        except Exception as e:
            self.logger.error(f"Error publishing message: {str(e)}")
            return False

    def subscribe_to_queue(self, queue_id: str, user_id: str) -> bool:
        try:
            connection = self._get_connection()
            channel = connection.channel()

            # Declare the queue for the user (separate queue for each user)
            user_queue = f"{queue_id}_sub_{user_id}"
            channel.queue_declare(queue=user_queue, durable=True)

            # Bind the user queue to the fanout exchange to receive all messages
            channel.queue_bind(exchange=f"{queue_id}_fanout", queue=user_queue)

            # Ensure the subscriptions dictionary is initialized
            if queue_id not in self.subscriptions:
                self.subscriptions[queue_id] = []

            if user_id not in self.subscriptions[queue_id]:
                self.subscriptions[queue_id].append(user_id)

            connection.close()
            return True
        except Exception as e:
            self.logger.error(f"Error subscribing to queue: {str(e)}")
            return False

    def consume_messages(self, queue_id: str, callback, user_id: Optional[str] = None) -> None:
        """Consume messages from a queue"""
        try:
            connection = self._get_connection()
            channel = connection.channel()

            # Determine which queue to consume from
            consume_queue = queue_id
            if user_id:
                consume_queue = f"{queue_id}_sub_{user_id}"
                # Declare the user-specific queue to ensure it exists
                channel.queue_declare(queue=consume_queue, durable=True)

                # Bind the user-specific queue to the fanout exchange
                channel.queue_bind(
                    exchange=f"{queue_id}_fanout",
                    queue=consume_queue
                )

            def message_handler(ch, method, properties, body):
                try:
                    message = json.loads(body)
                    callback(message)  # Process message
                except Exception as e:
                    self.logger.error(f"Error processing message: {str(e)}")
                finally:
                    ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge after processing

            # Ensure we consume one message at a time
            channel.basic_qos(prefetch_count=1)

            # Start consuming
            channel.basic_consume(
                queue=consume_queue,
                on_message_callback=message_handler
            )

            channel.start_consuming()

        except Exception as e:
            self.logger.error(f"Error consuming messages: {str(e)}")
            connection.close()
