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
        """
        Initialize RabbitMQ connection parameters
        
        Args:
            host: RabbitMQ server host
            port: RabbitMQ server port
            username: RabbitMQ username
            password: RabbitMQ password
            virtual_host: RabbitMQ virtual host
        """
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
        """
        Create a new queue with specified configuration
        
        Args:
            queue_config: QueueConfig object containing queue configuration
            
        Returns:
            bool: True if queue created successfully, False otherwise
        """
        try:
            connection = self._get_connection()
            channel = connection.channel()
            
            # Ensure queue configuration arguments are initialized
            if queue_config.arguments is None:
                queue_config.arguments = {}
            
            # Add arguments conditionally
            if 'x-message-ttl' not in queue_config.arguments:
                queue_config.arguments['x-message-ttl'] = 60000  # Messages expire after 60 seconds (1 minute)
            
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
        """
        Publish a message to specified queue
        
        Args:
            queue_id: ID of the queue to publish to
            message: Message content to publish
            
        Returns:
            bool: True if message published successfully, False otherwise
        """
        try:
            connection = self._get_connection()
            channel = connection.channel()
            
            # Add timestamp to message
            message_with_metadata = {
                'content': message,
                'timestamp': datetime.utcnow().isoformat(),
            }
            
            # Publish to main queue
            channel.basic_publish(
                exchange='',
                routing_key=queue_id,
                body=json.dumps(message_with_metadata),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                )
            )
            
            # Publish to fanout exchange for subscribers
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
        """
        Subscribe a user to a queue
        
        Args:
            queue_id: ID of the queue to subscribe to
            user_id: ID of the user subscribing
            
        Returns:
            bool: True if subscribed successfully, False otherwise
        """
        try:
            connection = self._get_connection()
            channel = connection.channel()
            
            # Use the same queue for both consumers
            subscriber_queue = queue_id  # Both consumers use the same queue
            
            # Declare the queue if not already declared
            channel.queue_declare(queue=subscriber_queue, durable=True)
            
            # Update subscriptions tracking
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
        """
        Consume messages from a queue
        
        Args:
            queue_id: ID of the queue to consume from
            callback: Callback function to process messages
            user_id: Optional user_id for subscribed consumers
        """
        try:
            connection = self._get_connection()
            channel = connection.channel()
            
            # Determine which queue to consume from
            consume_queue = queue_id
            if user_id:
                consume_queue = f"{queue_id}_sub_{user_id}"
            
            def message_handler(ch, method, properties, body):
                try:
                    message = json.loads(body)
                    callback(message)
                except Exception as e:
                    self.logger.error(f"Error processing message: {str(e)}")
                finally:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            
            # Set prefetch count to 1 to ensure fair dispatch
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

    def get_queue_subscribers(self, queue_id: str) -> List[str]:
        """
        Get list of subscribers for a queue
        
        Args:
            queue_id: ID of the queue
            
        Returns:
            List[str]: List of user IDs subscribed to the queue
        """
        return self.subscriptions.get(queue_id, [])