import pika
import time

def wait_for_rabbit():
    time.sleep(15)

def create_connection_and_channel():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    return connection, connection.channel()

def create_queue(channel, queue_name):
    channel.queue_declare(queue=queue_name, durable=True)
