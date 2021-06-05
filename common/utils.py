import pika
import time

def wait_for_rabbit():
    time.sleep(15)

def create_connection_and_channel():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    return connection, connection.channel()

def create_queue(channel, queue_name):
    channel.queue_declare(queue=queue_name, durable=True)

def send_message(channel, queue_name, body):
    channel.basic_publish(
    exchange='',
    routing_key=queue_name,
    body=body,
    properties=pika.BasicProperties(
        delivery_mode=2,  # make message persistent
    ))
    