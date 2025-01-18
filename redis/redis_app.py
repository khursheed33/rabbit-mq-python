
# app.py
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks, Query
from fastapi.responses import StreamingResponse
import asyncio
from typing import Dict, Optional
import uvicorn
from config import settings
from redis_manager import RedisManager

app = FastAPI()
redis_manager = RedisManager(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    password=settings.REDIS_PASSWORD
)

# Store background tasks and their stop events
active_tasks: Dict[str, asyncio.Event] = {}

async def message_publisher(channel: str, stop_event: asyncio.Event):
    """Example publisher function - replace with your actual message generation logic"""
    counter = 0
    while not stop_event.is_set():
        counter += 1
        message = {
            "id": counter,
            "content": f"Message {counter}",
            "timestamp": datetime.utcnow().isoformat()
        }
        await redis_manager.publish_message(channel, message)
        await asyncio.sleep(1)

@app.post("/start/{channel}")
async def start_broadcast(channel: str, background_tasks: BackgroundTasks):
    if channel in active_tasks:
        return {"status": "error", "message": "Channel already active"}
    
    stop_event = asyncio.Event()
    active_tasks[channel] = stop_event
    background_tasks.add_task(message_publisher, channel, stop_event)
    return {"status": "success", "message": f"Started broadcasting on channel: {channel}"}

@app.get("/consume/{channel}")
async def consume_messages(
    channel: str,
    last_sequence: Optional[int] = Query(None, description="Last received sequence number")
):
    if channel not in active_tasks:
        return {"status": "error", "message": "Channel not active"}
    
    stop_event = asyncio.Event()
    redis_manager.add_stream(channel, stop_event)

    async def stream_generator():
        async for message in redis_manager.consume_messages(channel, last_sequence):
            if stop_event.is_set():
                break
            yield message
    
    return StreamingResponse(
        stream_generator(),
        media_type="text/event-stream"
    )

@app.post("/stop/{channel}")
async def stop_broadcast(channel: str):
    if channel not in active_tasks:
        return {"status": "error", "message": "Channel not found"}
    
    # Stop background task
    stop_event = active_tasks.pop(channel)
    stop_event.set()

    # Stop active streams for the channel
    redis_manager.stop_streams(channel)

    # Clear Redis data
    redis_manager.clear_channel(channel)
    return {"status": "success", "message": f"Stopped broadcasting on channel: {channel}"}


@app.post("/shutdown")
async def shutdown():
    for stop_event in active_tasks.values():
        stop_event.set()
    active_tasks.clear()
    redis_manager.close()
    return {"status": "success", "message": "Server shutting down"}

if __name__ == "__main__":
    print(f"Starting FastAPI server on http://{settings.API_HOST}:{settings.API_PORT}")
    uvicorn.run(
        app=app,
        host=settings.API_HOST,
        port=settings.API_PORT
    )