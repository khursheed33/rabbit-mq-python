# services/producer_service.py
import asyncio
import logging
from typing import Dict
from fastapi import BackgroundTasks
from message_generator import MessageGenerator
from redis_manager import RedisManager
from models import Settings

class ProducerService:
    def __init__(self):
        self.settings = Settings()
        self.redis_manager = RedisManager(
            host=self.settings.REDIS_HOST,
            port=self.settings.REDIS_PORT,
            password=self.settings.REDIS_PASSWORD
        )
        self.active_channels: Dict[str, asyncio.Event] = {}

    async def start_broadcasting(self, channel: str, background_tasks: BackgroundTasks):
        if channel in self.active_channels:
            return {"status": "Channel already broadcasting", "channel": channel}
        
        stop_event = asyncio.Event()
        self.active_channels[channel] = stop_event
        background_tasks.add_task(self._produce_messages, channel, stop_event)
        logging.info(f"Started broadcasting on channel: {channel}")
        return {"status": "Broadcasting started", "channel": channel}

    async def stop_broadcasting(self, channel: str):
        if channel in self.active_channels:
            self.active_channels[channel].set()
            del self.active_channels[channel]
            return {"status": "Broadcasting stopped", "channel": channel}
        return {"status": "Channel not found", "channel": channel}

    async def shutdown(self):
        for stop_event in self.active_channels.values():
            stop_event.set()
        self.active_channels.clear()
        self.redis_manager.close()

    def is_channel_active(self, channel: str) -> bool:
        return channel in self.active_channels

    async def _produce_messages(self, channel: str, stop_event: asyncio.Event):
        """Produces messages for a channel"""
        counter = 0
        while not stop_event.is_set():
            counter += 1
            message = MessageGenerator.generate_message(counter)
            self.redis_manager.store_message(channel, message)
            logging.info(f"Published message to {channel}: {message}")
            await asyncio.sleep(5)
