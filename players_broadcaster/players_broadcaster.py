#!/usr/bin/env python3
import logging
import json
from common.utils import *

class PlayersBroadcaster():
    def __init__(self, player_queue, player_exchange):
        self.player_queue = player_queue
        self.player_exchange = player_exchange

    def start(self):
        wait_for_rabbit()

        connection, channel = create_connection_and_channel()

        create_queue(channel, self.player_queue)
        create_exchange(channel, self.player_exchange, "fanout")

        self.__consume_playeres(channel)

    def __consume_playeres(self, channel):
        logging.info('Waiting for messages. To exit press CTRL+C')
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=self.player_queue, on_message_callback=self.__callback)
        channel.start_consuming()

    def __callback(self, ch, method, properties, body):
        logging.info(f"Received {body} from client")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        send_message(ch, body, exchange_name=self.player_exchange)