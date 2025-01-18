import React, { useState, useEffect, useRef } from 'react';
import { formatDistanceToNow } from 'date-fns';

const ChatApp = () => {
  const [messages, setMessages] = useState([]);
  const [newMessage, setNewMessage] = useState('');
  const [userName, setUserName] = useState('');
  const [isConnected, setIsConnected] = useState(false);
  const [error, setError] = useState(null);
  const websocket = useRef(null);
  const messagesEndRef = useRef(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  useEffect(() => {
    // Fetch message history
    const fetchMessages = async () => {
      try {
        const response = await fetch('http://localhost:8000/messages');
        const data = await response.json();
        if (data.messages) {
          setMessages(data.messages.reverse());
        }
      } catch (err) {
        setError('Failed to load message history');
        console.error('Error:', err);
      }
    };

    fetchMessages();
  }, []);

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  useEffect(() => {
    // Connect to WebSocket
    const connectWebSocket = () => {
      websocket.current = new WebSocket('ws://localhost:8000/ws');

      websocket.current.onopen = () => {
        setIsConnected(true);
        setError(null);
      };

      websocket.current.onmessage = (event) => {
        const message = JSON.parse(event.data);
        setMessages(prev => [...prev, message]);
      };

      websocket.current.onclose = () => {
        setIsConnected(false);
        // Attempt to reconnect after 5 seconds
        setTimeout(connectWebSocket, 5000);
      };

      websocket.current.onerror = (error) => {
        setError('WebSocket error occurred');
        console.error('WebSocket error:', error);
      };
    };

    connectWebSocket();

    return () => {
      if (websocket.current) {
        websocket.current.close();
      }
    };
  }, []);

  const handleSubmit = (e) => {
    e.preventDefault();
    if (!userName.trim()) {
      setError('Please enter a username');
      return;
    }
    if (!newMessage.trim()) {
      return;
    }

    const message = {
      content: newMessage,
      userName: userName
    };

    if (websocket.current && websocket.current.readyState === WebSocket.OPEN) {
      websocket.current.send(JSON.stringify(message));
      setNewMessage('');
    } else {
      setError('Not connected to server');
    }
  };

  return (
    <div className="max-w-4xl mx-auto p-4">
      <div className="mb-4">
        <h1 className="text-2xl font-bold mb-2">Real-time Chat</h1>
        <div className="mb-2">
          <input
            type="text"
            value={userName}
            onChange={(e) => setUserName(e.target.value)}
            placeholder="Enter your username"
            className="w-full p-2 border rounded"
            disabled={isConnected}
          />
        </div>
        {error && (
          <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-2 rounded mb-4">
            {error}
          </div>
        )}
        <div className="bg-green-100 border border-green-400 text-green-700 px-4 py-2 rounded mb-4">
          Status: {isConnected ? 'Connected' : 'Disconnected'}
        </div>
      </div>

      <div className="bg-white shadow rounded-lg">
        <div className="h-[500px] overflow-y-auto p-4 border-b">
          {messages.map((message, index) => (
            <div key={index} className="mb-4">
              <div className="flex items-baseline">
                <span className="font-bold">{message.userName}</span>
                <span className="text-gray-500 text-sm ml-2">
                  {formatDistanceToNow(new Date(message.timestamp), { addSuffix: true })}
                </span>
              </div>
              <p className="mt-1">{message.content}</p>
            </div>
          ))}
          <div ref={messagesEndRef} />
        </div>

        <form onSubmit={handleSubmit} className="p-4">
          <div className="flex gap-2">
            <input
              type="text"
              value={newMessage}
              onChange={(e) => setNewMessage(e.target.value)}
              placeholder="Type your message..."
              className="flex-1 p-2 border rounded"
            />
            <button
              type="submit"
              disabled={!isConnected || !userName}
              className="bg-blue-500 text-white px-4 py-2 rounded disabled:bg-gray-300"
            >
              Send
            </button>
          </div>
        </form>
      </div>
    </div>
  );
};

export default ChatApp;
