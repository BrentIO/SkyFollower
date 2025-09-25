import pika
import json
import jsonlines
import os
import zipfile
import io

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "skyfollower.1090.dev")
JSONL_FILE = os.getenv("JSONL_FILE", "")
MAXIMUM_MESSAGES_TO_WRITE = os.getenv("MAXIMUM_MESSAGES_TO_WRITE", 9999999999999999)

def main():

    try:
    
        if os.path.exists(JSONL_FILE) != True:
            raise Exception(f"File {JSONL_FILE} does not exist.")

        global messageCount
        messageCount = 0
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

        filePath = os.path.join(os.path.dirname(os.path.abspath(__file__)),f'{JSONL_FILE}')

        with zipfile.ZipFile(filePath, 'r') as zf:
            with zf.open(os.path.basename(JSONL_FILE).replace(".zip","")) as jsonline_bytes:
                with io.TextIOWrapper(jsonline_bytes, encoding='utf-8') as jsonl_lines:
                    print(f"Exporting messages...")
                    for line in jsonl_lines:
                        if messageCount < int(MAXIMUM_MESSAGES_TO_WRITE):

                            channel.basic_publish(
                                exchange="",
                                routing_key=RABBITMQ_QUEUE,
                                body=json.dumps(line),
                                properties=pika.BasicProperties(delivery_mode=2)
                            )

                            messageCount = messageCount + 1
                        
    except pika.exceptions.AMQPConnectionError:
        print(f"Lost connection to RabbitMQ host {RABBITMQ_HOST} with {messageCount} messages exported.")
        
    except pika.exceptions.ChannelWrongStateError:
        print("Channel state error.")

    except Exception as ex:
        print(ex)


if __name__ == "__main__":
    main()