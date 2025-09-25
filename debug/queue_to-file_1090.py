import pika
import json
import jsonlines
import os
import sys

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "skyfollower.1090")
MAXIMUM_MESSAGES_TO_READ = os.getenv("MAXIMUM_MESSAGES_TO_WRITE", 9999999999999999)

def callback(ch, method, properties, body):

    global messageCount

    if messageCount >= MAXIMUM_MESSAGES_TO_READ:
        sys.exit(0)

    try:
        message = json.loads(body.decode("utf-8"))

        filePath = os.path.join(os.path.dirname(os.path.abspath(__file__)),f'{RABBITMQ_QUEUE}.jsonl')
        with jsonlines.open(filePath, mode='a') as writer:
            writer.write(message)

        messageCount = messageCount + 1

    except Exception as e:
        print(f"Failed to process message: {e}")


def main():
    global messageCount
    messageCount = 0
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

    channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback, auto_ack=True)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Exiting...")
    finally:
        connection.close()


if __name__ == "__main__":
    main()