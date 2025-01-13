# RabbitMQ Manager

This project provides a simple RabbitMQ manager for creating queues, publishing messages, and subscribing to queues. It uses the `pika` library to interact with RabbitMQ, allowing the following functionalities:

- **Queue Management**: Create RabbitMQ queues with customizable settings.
- **Message Publishing**: Publish messages to queues with optional metadata.
- **Subscription Management**: Subscribe users to queues to receive messages.
- **Message Consumption**: Consume messages from queues with a callback function.

## Requirements

- Python 3.x
- `pika` library (RabbitMQ client for Python)

Install dependencies using:

```bash
pip install pika
```

## Project Structure

- `rabbitmq_manager.py`: Contains the `RabbitMQManager` class and supporting code for managing queues, publishing messages, and subscribing to queues.
- `README.md`: This file.

## Usage

### Initialize the RabbitMQ Manager

To initialize the `RabbitMQManager`, provide the connection details (hostname, port, username, password, virtual host).

```python
from rabbitmq_manager import RabbitMQManager

# Initialize RabbitMQManager with default settings
rabbitmq_manager = RabbitMQManager(host='localhost', port=5672, username='admin', password='admin')
```

### Create a Queue

To create a queue, define a `QueueConfig` object with the required configuration parameters, and call the `create_queue` method.

```python
from rabbitmq_manager import RabbitMQManager, QueueConfig

queue_config = QueueConfig(queue_id="my_queue")
rabbitmq_manager.create_queue(queue_config)
```

### Publish a Message

To publish a message to a queue, use the `publish_message` method. The message will be published with metadata (timestamp).

```python
message = {"text": "Hello, world!"}
rabbitmq_manager.publish_message(queue_id="my_queue", message=message)
```

### Subscribe a User to a Queue

To subscribe a user to a queue, call the `subscribe_to_queue` method, providing a user ID.

```python
rabbitmq_manager.subscribe_to_queue(queue_id="my_queue", user_id="user_1")
```

### Consume Messages

To consume messages from a queue, define a callback function that will be invoked for each message, and use the `consume_messages` method.

```python
def message_callback(message):
    print(f"Received message: {message}")

rabbitmq_manager.consume_messages(queue_id="my_queue", callback=message_callback)
```

You can also specify a `user_id` to consume messages from a user-specific queue:

```python
rabbitmq_manager.consume_messages(queue_id="my_queue", callback=message_callback, user_id="user_1")
```

## Features

- **Queue Configuration**: Create durable queues with customizable settings (auto-delete, custom arguments).
- **Fanout Exchange**: Publish messages to a fanout exchange for all subscribers to receive the message.
- **User-Specific Subscriptions**: Users can subscribe to a specific queue and receive messages on their individual queues.
- **Message Persistence**: Ensure that messages are persistent and not lost on RabbitMQ restarts.

## Logging

The project uses Python's built-in logging module to log errors. You can configure the logger as needed to log messages to a file or other outputs.

```python
import logging

logging.basicConfig(level=logging.DEBUG)  # Set the logging level
```

## Error Handling

The manager methods log errors and return `False` if any issues arise. If you encounter any problems, check the logs for more detailed error messages.

## Dependencies

- `pika` - RabbitMQ client for Python

Install via:

```bash
pip install pika
```

## Contributing

Feel free to fork the repository and submit pull requests. For any bugs or issues, please open an issue in the GitHub repository.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
