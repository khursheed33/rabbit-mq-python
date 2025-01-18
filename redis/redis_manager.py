
# redis_manager.py
import redis
import asyncio
import json
from typing import AsyncGenerator, Any, Optional
from datetime import datetime

class RedisManager:
    def __init__(self, host: str, port: int, password: str):
        self.redis_client = redis.StrictRedis(
            host=host, 
            port=port, 
            password=password, 
            decode_responses=True
        )
        self._validate_connection()
        self.active_streams = {}  # Track active streams by channel

    def add_stream(self, channel: str, stop_event: asyncio.Event):
        """Track active streams for a channel."""
        if channel not in self.active_streams:
            self.active_streams[channel] = []
        self.active_streams[channel].append(stop_event)

    def stop_streams(self, channel: str):
        """Stop all active streams for a channel."""
        if channel in self.active_streams:
            for stop_event in self.active_streams[channel]:
                stop_event.set()
            del self.active_streams[channel]
        
    def _validate_connection(self) -> None:
        try:
            self.redis_client.ping()
            print("Redis connection successful!")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Redis: {e}")

    def _get_message_key(self, channel: str) -> str:
        return f"messages:{channel}"

    def _get_sequence_key(self, channel: str) -> str:
        return f"sequence:{channel}"

    async def publish_message(self, channel: str, message: Any) -> None:
        """Publish a message with sequence number for ordered delivery"""
        sequence = self.redis_client.incr(self._get_sequence_key(channel))
        message_data = {
            'sequence': sequence,
            'timestamp': datetime.utcnow().isoformat(),
            'data': message
        }
        
        # Store in sorted set for ordered retrieval
        message_str = json.dumps(message_data)
        self.redis_client.zadd(self._get_message_key(channel), {message_str: sequence})
        self.redis_client.publish(channel, message_str)

    async def consume_messages(self, channel: str, last_sequence: Optional[int] = None) -> AsyncGenerator[str, None]:
        """Consume messages with guaranteed ordering and no message loss"""
        pubsub = self.redis_client.pubsub()
        pubsub.subscribe(channel)
        
        try:
            # Get the current maximum sequence
            message_key = self._get_message_key(channel)
            
            # If no last_sequence provided, start from the beginning
            start_sequence = last_sequence if last_sequence is not None else 0
            
            # First yield all stored messages from the requested sequence
            stored_messages = self.redis_client.zrangebyscore(
                message_key,
                start_sequence + 1,
                '+inf'
            )
            
            for message in stored_messages:
                yield f"data: {message}\n\n"
            
            # Then continue with new messages
            while True:
                message = pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message['type'] == 'message':
                    yield f"data: {message['data']}\n\n"
                await asyncio.sleep(0.1)
                
        finally:
            pubsub.unsubscribe(channel)
            print(f"Unsubscribed from channel: {channel}")

    def clear_channel(self, channel: str) -> None:
        """Clear all messages and sequence for a channel"""
        self.redis_client.delete(self._get_message_key(channel))
        self.redis_client.delete(self._get_sequence_key(channel))

    def close(self) -> None:
        """Close Redis connection"""
        self.redis_client.close()
        print("Redis connection closed.")
