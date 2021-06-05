#!/usr/bin/env python3
import logging
import json
from datetime import datetime, timedelta
from common.utils import *

class MatchesBroadcaster():
    def __init__(self, match_queue, match_exchange):
        self.match_queue = match_queue
        self.match_exchange = match_exchange

    def start(self):
        wait_for_rabbit()

        connection, channel = create_connection_and_channel()

        create_queue(channel, self.match_queue)
        create_exchange(channel, self.match_exchange, "fanout")

        self.__consume_matches(channel)

    def __consume_matches(self, channel):
        logging.info('Waiting for messages. To exit press CTRL+C')
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=self.match_queue, on_message_callback=self.__callback)
        channel.start_consuming()

    def __callback(self, ch, method, properties, body):
        logging.info(f"Received {body} from client")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        send_message(ch, body, exchange_name=self.match_exchange)