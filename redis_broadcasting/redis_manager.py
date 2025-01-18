import redis
import json
from datetime import datetime, timedelta
import logging
from typing import Dict, Any, List
import uuid
from redis_broadcasting.models import Settings
from redis import Redis
from redis.client import PubSub

class RedisManager:
    def __init__(self, host: str = 'localhost', port: int = 6379, password: str = 'root'):
        self.redis_client: Redis = redis.StrictRedis(
            host=host,
            port=port,
            password=password,
            decode_responses=True
        )
        self.settings = Settings()
        self._test_connection()

    def _test_connection(self) -> None:
        try:
            self.redis_client.ping()
            logging.info("Redis connection successful!")
        except Exception as e:
            logging.error(f"Error connecting to Redis: {e}")
            raise

    def _generate_message_id(self) -> str:
        return str(uuid.uuid4())

    def store_message(self, channel: str, message: Dict[str, Any]) -> None:
        """Store message with timestamp for ordering"""
        message_with_metadata = {
            **message,
            "timestamp": datetime.utcnow().isoformat(),
            "message_id": self._generate_message_id()
        }
        
        # Store in sorted set with timestamp as score for ordering
        score = datetime.fromisoformat(message_with_metadata["timestamp"]).timestamp()
        self.redis_client.zadd(
            f"messages:{channel}",
            {json.dumps(message_with_metadata): score}
        )
        
        # Publish to real-time subscribers
        self.redis_client.publish(channel, json.dumps(message_with_metadata))
        
        # Set expiration on the sorted set if not already set
        if not self.redis_client.ttl(f"messages:{channel}") > 0:
            self.redis_client.expire(
                f"messages:{channel}",
                timedelta(days=self.settings.MESSAGE_RETENTION_DAYS).total_seconds()
            )

    def get_all_messages(self, channel: str) -> List[Dict[str, Any]]:
        """Get all messages for a channel in chronological order"""
        messages = self.redis_client.zrange(
            f"messages:{channel}",
            0,
            -1,
            withscores=True
        )
        return [json.loads(msg[0]) for msg in messages]

    def subscribe(self, channel: str) -> PubSub:
        """Create a subscription to a channel"""
        pubsub: PubSub = self.redis_client.pubsub()
        pubsub.subscribe(channel)
        return pubsub

    def unsubscribe(self, pubsub: PubSub, channel: str) -> None:
        """Unsubscribe from a channel"""
        pubsub.unsubscribe(channel)
        pubsub.close()

    def close(self) -> None:
        """Close Redis connection"""
        self.redis_client.close()
