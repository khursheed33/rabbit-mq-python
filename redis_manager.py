import redis
import asyncio
from typing import List, AsyncGenerator, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RedisManager:
    def __init__(self, host: str, port: int, password: str):
        try:
            self.redis_client = redis.StrictRedis(
                host=host, 
                port=port, 
                password=password, 
                decode_responses=True
            )
            self.redis_client.ping()
            logger.info("Redis connection successful!")
            self._subscriptions = {}
        except Exception as e:
            logger.error(f"Error connecting to Redis: {e}")
            raise

    def subscribe(self, channel: str) -> redis.client.PubSub:
        """
        Create a new subscription to a channel
        Returns the pubsub object for the subscription
        """
        pubsub = self.redis_client.pubsub()
        pubsub.subscribe(channel)
        self._subscriptions[channel] = pubsub
        logger.info(f"Created new subscription for channel: {channel}")
        return pubsub

    def unsubscribe(self, channel: str):
        """
        Unsubscribe from a channel and cleanup the subscription
        """
        if channel in self._subscriptions:
            pubsub = self._subscriptions[channel]
            pubsub.unsubscribe(channel)
            pubsub.close()
            del self._subscriptions[channel]
            logger.info(f"Unsubscribed from channel: {channel}")

    async def generic_message_publisher(self, channel: str, message_generator, stop_event: asyncio.Event):
        """
        Publishes messages from a generator to a Redis channel
        Stores messages in a queue for later retrieval
        """
        logger.info(f"Started generic message publishing to channel: {channel}")
        try:
            async for message in message_generator:
                if stop_event.is_set():
                    break
                # Store message in both pub/sub and queue
                self.redis_client.publish(channel, message)
                self.redis_client.lpush(f"message_queue_{channel}", message)
                logger.info(f"Published and stored message to {channel}: {message}")
        except Exception as e:
            logger.error(f"Error in message publisher for channel {channel}: {e}")
            raise
        finally:
            logger.info(f"Stopped message publishing to channel: {channel}")

    async def redis_message_stream(self, channel: str) -> AsyncGenerator[str, None]:
        """
        Creates a message stream that first sends stored messages and then
        streams new messages as they arrive
        """
        pubsub = self.subscribe(channel)
        logger.info(f"Started message stream for channel: {channel}")

        try:
            # Send stored messages first (in reverse order to maintain chronological order)
            stored_messages = self.redis_client.lrange(f"message_queue_{channel}", 0, -1)
            for message in reversed(stored_messages):
                logger.info(f"Sending stored message: {message}")
                yield f"data: {message}\n\n"

            # Stream new messages
            while True:
                message = pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message:
                    logger.info(f"Consumed message from {channel}: {message['data']}")
                    yield f"data: {message['data']}\n\n"
                await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Error in message stream for channel {channel}: {e}")
            raise
        finally:
            self.unsubscribe(channel)
            logger.info(f"Closed message stream for channel: {channel}")

    def get_stored_messages(self, channel: str) -> List[str]:
        """
        Retrieve all stored messages for a channel
        """
        return self.redis_client.lrange(f"message_queue_{channel}", 0, -1)

    def clear_channel(self, channel: str):
        """
        Clear all stored messages for a channel
        """
        self.redis_client.delete(f"message_queue_{channel}")
        logger.info(f"Cleared all stored messages for channel: {channel}")

    def close(self):
        """
        Close all subscriptions and Redis connection
        """
        for channel in list(self._subscriptions.keys()):
            self.unsubscribe(channel)
        self.redis_client.close()
        logger.info("Redis connection closed.")