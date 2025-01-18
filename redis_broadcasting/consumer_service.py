
# services/consumer_service.py
import asyncio
import json
from typing import AsyncGenerator
from redis_manager import RedisManager
from models import Settings

class ConsumerService:
    def __init__(self):
        self.settings = Settings()
        self.redis_manager = RedisManager(
            host=self.settings.REDIS_HOST,
            port=self.settings.REDIS_PORT,
            password=self.settings.REDIS_PASSWORD
        )

    async def stream_messages(self, channel: str) -> AsyncGenerator[str, None]:
        """Streams messages to consumers"""
        # Send all existing messages first
        existing_messages = self.redis_manager.get_all_messages(channel)
        for message in existing_messages:
            yield f"data: {json.dumps(message)}\n\n"

        # Subscribe to new messages
        pubsub = self.redis_manager.subscribe(channel)
        try:
            while True:
                message = pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message["type"] == "message":
                    yield f"data: {message['data']}\n\n"
                await asyncio.sleep(0.1)
        finally:
            self.redis_manager.unsubscribe(pubsub, channel)
