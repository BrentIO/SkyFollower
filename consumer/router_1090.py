import pika
import json
import pyModeS as pms
import logging
import os
import sys
import time


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "skyfollower.1090")
COUNT_PROCS_CONSUMER_1090 = int(os.getenv("COUNT_PROCS_CONSUMER_1090", 0))

logger = None


def callback(ch, method, properties, body):
    try:
        message = json.loads(body.decode("utf-8"))
        raw_data = message['data'].replace("*", "").replace(";", "")
        icao_hex = pms.adsb.icao(raw_data)

        if not icao_hex:
            logger.warning(f"Invalid ICAO hex from message: {raw_data}")
            return

        proc_consumer_1090 = int(icao_hex, 16) % COUNT_PROCS_CONSUMER_1090
        routing_queue = f"{RABBITMQ_QUEUE}.{proc_consumer_1090}"

        ch.basic_publish(
            exchange="",
            routing_key=routing_queue,
            body=body,
            properties=pika.BasicProperties(delivery_mode=2)
        )

        logger.debug(f"Routed ICAO {icao_hex} to {routing_queue}")

    except Exception as ex:
        logger.error(f"Exception [{type(ex).__name__}] while processing message: {str(ex)}")


def connect_and_consume():
    try:
        while True:
            connection = None
            channel = None
            try:
                logger.info(f"Connecting to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}...")
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
                )

                channel = connection.channel()
                channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

                for i in range(COUNT_PROCS_CONSUMER_1090):
                    queue = f"{RABBITMQ_QUEUE}.{i}"
                    channel.queue_declare(queue=queue, durable=True)
                    logger.info(f"Declared queue: {queue}")

                logger.info(f"Routing messages on '{RABBITMQ_QUEUE}'. Press Ctrl+C to exit.")
                channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback, auto_ack=True)

                # Start consuming (blocks here)
                channel.start_consuming()

            except pika.exceptions.AMQPConnectionError as e:
                logger.warning(f"Connection lost. Retrying in 5 seconds... ({type(e).__name__})")
                time.sleep(5)

            except pika.exceptions.StreamLostError as e:
                logger.warning(f"Stream lost. Retrying in 5 seconds... ({type(e).__name__})")
                time.sleep(5)

            except Exception as e:
                logger.error(f"Unexpected error: {type(e).__name__} - {str(e)}")
                logger.info("Retrying in 5 seconds...")
                time.sleep(5)

            finally:
                try:
                    if channel and channel.is_open:
                        channel.close()
                    if connection and connection.is_open:
                        connection.close()
                except Exception:
                    pass

    except KeyboardInterrupt:
        logger.info("Shutdown requested by user (Ctrl+C). Exiting gracefully...")
        try:
            if channel and channel.is_open:
                channel.close()
            if connection and connection.is_open:
                connection.close()
        except Exception:
            pass
        sys.exit(0)


def setup():
    global logger

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] - %(message)s'
    )
    streamHandler = logging.StreamHandler(sys.stdout)
    streamHandler.setFormatter(formatter)   

    logger.addHandler(streamHandler)

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    if log_level not in ["INFO", "WARNING", "ERROR", "CRITICAL", "DEBUG"]:
        log_level = "INFO"

    if log_level == "INFO":
        logger.setLevel(logging.INFO)

    if log_level == "WARNING":
        logger.setLevel(logging.WARNING)
        logger.log(logging.WARNING, f"Set log level to {log_level}")

    if log_level == "ERROR":
        logger.setLevel(logging.ERROR)
        logger.log(logging.ERROR, f"Set log level to {log_level}")

    if log_level == "CRITICAL":
        logger.setLevel(logging.CRITICAL)
        logger.log(logging.CRITICAL, f"Set log level to {log_level}")

    if log_level == "DEBUG":
        logger.setLevel(logging.DEBUG)
        logger.log(logging.DEBUG, f"Set log level to {log_level}")



    if COUNT_PROCS_CONSUMER_1090 < 1:
        logger.error("COUNT_PROCS_CONSUMER_1090 must be >= 1")
        sys.exit(-1)

    logger.info("Application started.")


if __name__ == "__main__":
    setup()
    connect_and_consume()