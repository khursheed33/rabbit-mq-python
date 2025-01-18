from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from signalrcore.hub.base_hub_connection import BaseHubConnection
from signalrcore.hub_connection_builder import HubConnectionBuilder
import asyncio

app = FastAPI()

# HTML for the client
@app.get("/", response_class=HTMLResponse)
async def get_html():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>SignalR with FastAPI</title>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/microsoft-signalr/5.0.15/signalr.min.js"></script>
    </head>
    <body>
        <h1>SignalR with FastAPI</h1>
        <button id="sendMessage">Send Message</button>
        <div id="messages"></div>
        <script>
                const connection = new signalR.HubConnectionBuilder()
                        .withUrl("ws://localhost:3301/ws")  // Update port to 3301
                        .build();

            connection.on("ReceiveMessage", function (message) {
                const messagesDiv = document.getElementById("messages");
                const newMessage = document.createElement("div");
                newMessage.innerText = message;
                messagesDiv.appendChild(newMessage);
            });

            connection.start()
                .then(() => console.log("Connected to SignalR Hub"))
                .catch(err => console.error(err.toString()));

            document.getElementById("sendMessage").addEventListener("click", function () {
                fetch("/send_message", {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                    },
                    body: JSON.stringify({ message: "Hello from SignalR!" }),
                });
            });
        </script>
    </body>
    </html>
    """

# SignalR Hub Connection
class SignalRHub:
    def __init__(self):
        self.connection = None

    def start(self):
        self.connection = (
            HubConnectionBuilder()
            .with_url("http://localhost:3301/ws")  # Ensure this URL is correct
            .with_automatic_reconnect(
                {
                    "type": "raw",
                    "keep_alive_interval": 10,
                    "reconnect_interval": 5,
                }
            )
            .build()
        )
        self.connection.on("ReceiveMessage", self.receive_message)
        self.connection.start()

    def receive_message(self, message):
        print("Received message from client:", message)

    def send_message(self, message):
        if self.connection:
            self.connection.send("ReceiveMessage", [message])

signalr_hub = SignalRHub()

@app.on_event("startup")
async def startup_event():
    signalr_hub.start()

@app.post("/send_message")
async def send_message(message: dict):
    msg = message.get("message", "Default Message")
    signalr_hub.send_message(msg)
    return {"status": "Message sent", "message": msg}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    while True:
        try:
            data = await websocket.receive_text()
            await websocket.send_text(f"Message received: {data}")
        except Exception as e:
            print(f"WebSocket connection error: {e}")
            break

# Run the FastAPI app using Uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3301)
