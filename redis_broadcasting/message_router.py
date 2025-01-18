# routers/message_router.py
from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import StreamingResponse
from producer_service import ProducerService
from consumer_service import ConsumerService

router = APIRouter()
producer_service = ProducerService()
consumer_service = ConsumerService()

@router.post("/start/{channel}")
async def start_broadcast(channel: str, background_tasks: BackgroundTasks):
    return await producer_service.start_broadcasting(channel, background_tasks)

@router.get("/consume/{channel}")
async def consume_messages(channel: str):
    if not producer_service.is_channel_active(channel):
        return {"error": "Channel not found"}
    
    return StreamingResponse(
        consumer_service.stream_messages(channel),
        media_type="text/event-stream"
    )

@router.post("/stop/{channel}")
async def stop_broadcast(channel: str):
    return await producer_service.stop_broadcasting(channel)

@router.post("/shutdown")
async def shutdown():
    await producer_service.shutdown()
    return {"status": "Service shutting down"}
