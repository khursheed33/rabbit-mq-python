from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any
import json
import pyodbc
from datetime import datetime
import asyncio
from dataclasses import dataclass
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"Client connected. Total connections: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"Client disconnected. Total connections: {len(self.active_connections)}")

    async def broadcast(self, message: Dict[str, Any]):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Error broadcasting message: {str(e)}")

class DatabaseManager:
    def __init__(self):
        self.conn_str = (
            "DRIVER={SQL Server};"
            "SERVER=172.52.50.171,5533;"
            "DATABASE=DGO;"
            "UID=Khursheed.Gaddi;"
            "PWD=passs;"
            "Trusted_Connection=yes;"
        )

    def save_message(self, message: Dict[str, Any]):
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO Messages (Content, UserName, Timestamp)
                    VALUES (?, ?, ?)
                """, (message['content'], message['userName'], message['timestamp']))
                conn.commit()
                logger.info("Message saved to database")
        except Exception as e:
            logger.error(f"Database error: {str(e)}")
            raise

    def get_message_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT TOP (?) Content, UserName, Timestamp 
                    FROM Messages 
                    ORDER BY Timestamp DESC
                """, limit)
                
                messages = []
                for row in cursor.fetchall():
                    messages.append({
                        'content': row[0],
                        'userName': row[1],
                        'timestamp': row[2].isoformat()
                    })
                return messages
        except Exception as e:
            logger.error(f"Database error: {str(e)}")
            raise

app = FastAPI()
manager = ConnectionManager()
db = DatabaseManager()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    # Create tables if they don't exist
    try:
        with pyodbc.connect(db.conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Messages' AND xtype='U')
                CREATE TABLE Messages (
                    Id INT IDENTITY(1,1) PRIMARY KEY,
                    Content NVARCHAR(MAX),
                    UserName NVARCHAR(100),
                    Timestamp DATETIME2 DEFAULT GETDATE()
                )
            """)
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        raise

@app.get("/messages")
async def get_messages():
    """Get message history"""
    try:
        messages = db.get_message_history()
        return {"messages": messages}
    except Exception as e:
        logger.error(f"Error retrieving messages: {str(e)}")
        return {"error": str(e)}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            message_data = await websocket.receive_text()
            message = json.loads(message_data)
            message['timestamp'] = datetime.now().isoformat()
            
            # Save to database
            db.save_message(message)
            
            # Broadcast to all connected clients
            await manager.broadcast(message)
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {str(e)}")
        manager.disconnect(websocket)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
