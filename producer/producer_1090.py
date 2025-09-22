import socket
import pika
import json
import time
import threading
from collections import deque
import logging
import logging.handlers as handlers
import sys
import os

# --- Configuration from ENV vars (with defaults) ---
TCP_HOST = os.getenv("TCP_HOST", "localhost")
TCP_PORT = int(os.getenv("TCP_PORT", "30002"))

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "skyfollower.adsb1090raw")

# In-memory message queue for storing messages if RabbitMQ is disconnected
message_queue = deque()
message_event = threading.Event()  # Event to trigger async processing


def rabbitmq_connect():
    connection = None
    channel = None
    while connection is None:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
            logger.info(f"Connected to RabbitMQ server {RABBITMQ_HOST}")
        except pika.exceptions.AMQPConnectionError:
            logger.critical(f"Failed to connect to RabbitMQ server {RABBITMQ_HOST}, retrying in 10 seconds...")
            time.sleep(10)
        except pika.exceptions.ChannelWrongStateError:
            logger.critical("Channel error. Reinitializing connection.")
            time.sleep(10)
    return connection, channel


def send_to_rabbitmq():
    global message_queue
    connection, channel = None, None
    while True:
        message_event.wait()
        if connection is None or connection.is_open is False or channel is None:
            logger.critical("Reconnecting to RabbitMQ...")
            connection, channel = rabbitmq_connect()
        while message_queue:
            message = message_queue[0]
            try:
                channel.basic_publish(
                    exchange="",
                    routing_key=RABBITMQ_QUEUE,
                    body=json.dumps(message),
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                logger.debug(f"Sent message to RabbitMQ: {message['data']}")
                message_queue.popleft()
            except pika.exceptions.AMQPConnectionError:
                logger.critical("Lost connection to RabbitMQ, retrying...")
                break
            except pika.exceptions.ChannelWrongStateError:
                logger.critical("Channel state error. Reinitializing channel.")
                connection, channel = rabbitmq_connect()
                break
        if not message_queue:
            message_event.clear()


def handle_tcp_messages(sock_file):
    global message_queue
    for line in sock_file:
        line = line.strip()
        if not line:
            continue
        payload = {"time": int(time.time() * 1000), "data": line}
        message_queue.append(payload)
        logger.debug(f"Received message: {payload['data']}")
        message_event.set()


def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((TCP_HOST, TCP_PORT))
    sock_file = sock.makefile("r")
    logger.info(f"Connected to TCP server {TCP_HOST}:{TCP_PORT}")
    send_thread = threading.Thread(target=send_to_rabbitmq, daemon=True)
    send_thread.start()
    try:
        handle_tcp_messages(sock_file)
    except KeyboardInterrupt:
        logger.info("\nShutting down...")
    finally:
        sock.close()


def setup():
    
    global logger

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logger = logging.getLogger()
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] - %(message)s'
    )
    streamHandler = logging.StreamHandler(sys.stdout)
    streamHandler.setFormatter(formatter)   

    logger.addHandler(streamHandler)
    logger.setLevel(logging.INFO)
    logger.info("Application started.")


if __name__ == "__main__":
    setup()
    main()