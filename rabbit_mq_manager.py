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
                 username: str = 'guest', password: str = 'guest',
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

    def subscribe_to_queue(self, queue_id: str, user_id: str, session_id: str) -> bool:
        """
        Subscribe a user session to a queue.
        Each session gets its own exclusive queue to replay messages.
        """
        try:
            connection = self._get_connection()
            channel = connection.channel()

            # Define the main persistent queue for storage
            main_queue = queue_id
            channel.queue_declare(queue=main_queue, durable=True)

            # Define a unique session-specific queue (exclusive)
            session_queue = f"{queue_id}_sub_{user_id}_{session_id}"
            channel.queue_declare(queue=session_queue, durable=False, exclusive=False)

            # Replay all existing messages from the main queue to the session queue
            while True:
                method_frame, properties, body = channel.basic_get(queue=main_queue, auto_ack=False)
                if method_frame:
                    channel.basic_publish(
                        exchange='',
                        routing_key=session_queue,
                        body=body,
                        properties=properties
                    )
                    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                else:
                    # No more messages in the main queue
                    break

            # Bind the session queue to the fanout exchange for live updates
            channel.exchange_declare(exchange=f"{queue_id}_fanout", exchange_type='fanout', durable=True)
            channel.queue_bind(exchange=f"{queue_id}_fanout", queue=session_queue)

            connection.close()
            return True
        except Exception as e:
            self.logger.error(f"Error subscribing session to queue: {str(e)}")
            return False
    
    def consume_messages(self, queue_id: str, callback, user_id: str, session_id: str) -> None:
        """
        Consume messages from a user session-specific queue.
        """
        try:
            connection = self._get_connection()
            channel = connection.channel()

            # Determine the session-specific queue
            session_queue = f"{queue_id}_sub_{user_id}_{session_id}"

            def message_handler(ch, method, properties, body):
                try:
                    message = json.loads(body)
                    callback(message)  # Process the message
                except Exception as e:
                    self.logger.error(f"Error processing message: {str(e)}")
                finally:
                    ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge after processing

            # Ensure we consume one message at a time
            channel.basic_qos(prefetch_count=1)

            # Start consuming
            channel.basic_consume(
                queue=session_queue,
                on_message_callback=message_handler
            )

            channel.start_consuming()

        except Exception as e:
            self.logger.error(f"Error consuming messages: {str(e)}")
            connection.close()


    def delete_queue(self, queue_id: str) -> bool:
        """
        Delete the specified queue if it exists.
        """
        try:
            connection = self._get_connection()
            channel = connection.channel()

            # Check if queue exists before attempting to delete
            queue_exists = True
            try:
                channel.queue_declare(queue=queue_id, passive=True)
            except pika.exceptions.ChannelClosedByBroker:
                queue_exists = False
                self.logger.info(f"Queue {queue_id} does not exist.")
            
            if queue_exists:
                channel.queue_delete(queue=queue_id)
                self.logger.info(f"Queue {queue_id} has been deleted.")
            
            connection.close()
            return queue_exists

        except Exception as e:
            self.logger.error(f"Error deleting queue: {str(e)}")
            return False
