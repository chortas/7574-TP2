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

        consume(channel, self.queue_name, self.__callback)

    def __callback(self, ch, method, properties, body):
        logging.info(f"Received {len(json.loads(body))} from client")
        send_message(ch, body, exchange_name=self.exchange_name)