import pika
import time
import sys

RABBITMQ_HOST = 'rabbitmq'
EXCHANGE_NAME = 'user_data'
MESSAGE_COUNT = 50
TIMEOUT = 50

def verify_rabbitmq():
    connection = None
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()

        channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='fanout', durable=True)

        result = channel.queue_declare(queue='default_queue', exclusive=True)
        queue_name = result.method.queue

        channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name)

        received_messages = 0
        start_time = time.time()

        print(f"Waiting for {MESSAGE_COUNT} messages on '{EXCHANGE_NAME}' exchange for {TIMEOUT} seconds...")

        for method_frame, properties, body in channel.consume(queue_name, inactivity_timeout=1):
            if method_frame:
                print(f" [x] Received {body.decode()}")
                received_messages += 1
                channel.basic_ack(method_frame.delivery_tag)
                if received_messages == MESSAGE_COUNT:
                    break
            elif time.time() - start_time > TIMEOUT:
                print(f"Timeout reached. Received {received_messages} out of {MESSAGE_COUNT} messages.")
                break
            else:
                print("No messages received in the last second, still waiting...")

        if received_messages == MESSAGE_COUNT:
            print(f"Successfully received {MESSAGE_COUNT} messages. Verification successful!")
            sys.exit(0)
        else:
            print(f"Failed to receive {MESSAGE_COUNT} messages. Received only {received_messages}.")
            sys.exit(1)

    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error connecting to RabbitMQ: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)
    finally:
        if connection:
            connection.close()

if __name__ == '__main__':
    verify_rabbitmq()