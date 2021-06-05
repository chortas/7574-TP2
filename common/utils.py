import pika
import time

def wait_for_rabbit():
    time.sleep(15)

def create_connection_and_channel():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    return connection, connection.channel()

def create_queue(channel, queue_name):
    channel.queue_declare(queue=queue_name, durable=True)

def create_exchange(channel, exchange_name, exchange_type):
    channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type)

def send_message(channel, body, queue_name='', exchange_name=''):
    channel.basic_publish(exchange=exchange_name, routing_key=queue_name, body=body,
    properties=pika.BasicProperties(
        delivery_mode=2,  # make message persistent
    ))

def create_and_bind_anonymous_queue(channel, exchange_name):
    result = channel.queue_declare(queue='', durable=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange=exchange_name, queue=queue_name)
    return queue_name