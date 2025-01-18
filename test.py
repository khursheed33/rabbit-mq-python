from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import StreamingResponse
import redis
import asyncio
from typing import AsyncGenerator

import uvicorn

# Initialize FastAPI app and Redis connection
app = FastAPI()

try:
    redis_client = redis.StrictRedis(host='192.168.1.102', port=6379, password='root', decode_responses=True)
    redis_client.ping()  # Test the connection
    print("Redis connection successful!")
except Exception as e:
    print(f"Error connecting to Redis: {e}")
