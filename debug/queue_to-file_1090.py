import pika
import json
import jsonlines
import os
import sys

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "skyfollower.1090")
MAXIMUM_MESSAGES_TO_READ = int(os.getenv("MAXIMUM_MESSAGES_TO_READ", 9999999999999999))


def callback(ch, method, properties, body):

    global messageCount
    global filePath

    try:
        if messageCount >= MAXIMUM_MESSAGES_TO_READ:
            sys.exit(0)          
    
        message = json.loads(body.decode("utf-8"))

        with jsonlines.open(filePath, mode='a') as writer:
            writer.write(message)

        messageCount = messageCount + 1

    except Exception as e:
        print(f"Failed to process message: {e}")


def main():
    global messageCount
    global filePath

    messageCount = 0

    filePath = os.path.join(os.path.dirname(os.path.abspath(__file__)),f'{RABBITMQ_QUEUE}.jsonl')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
    channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback, auto_ack=True)

    print(f"Writing messages from {RABBITMQ_QUEUE} to file; Press ctrl+c to exit...")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Exiting...")
    finally:
        connection.close()


if __name__ == "__main__":
    main()