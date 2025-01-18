# main.py
import logging
import uvicorn
from fastapi import FastAPI
from message_router import router

logging.basicConfig(level=logging.INFO)

app = FastAPI()
app.include_router(router, prefix="/api/v1")

def run_app():
    logging.info("Starting FastAPI server on http://0.0.0.0:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    run_app()