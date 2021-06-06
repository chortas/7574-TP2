#!/usr/bin/env python3
import logging
import json
from common.utils import *

class Broadcaster():
    def __init__(self, queue_name, exchange_name):
        self.queue_name = queue_name
        self.exchange_name = exchange_name

    def start(self):
        wait_for_rabbit()

        connection, channel = create_connection_and_channel()

        create_queue(channel, self.queue_name)
        create_exchange(channel, self.exchange_name, "fanout")

        self.__consume_matches(channel)

    def __consume_matches(self, channel):
        logging.info('Waiting for messages. To exit press CTRL+C')
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=self.queue_name, on_message_callback=self.__callback)
        channel.start_consuming()

    def __callback(self, ch, method, properties, body):
        logging.info(f"Received {body} from client")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        send_message(ch, body, exchange_name=self.exchange_name)