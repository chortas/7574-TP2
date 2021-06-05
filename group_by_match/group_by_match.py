#!/usr/bin/env python3
import pika
import time
import os
import logging
import json
import re
from datetime import datetime, timedelta
from common.utils import *

class GroupByMatch():
    def __init__(self, player_queue):
        self.player_queue = player_queue

    def start(self):
        wait_for_rabbit()

        connection, channel = create_connection_and_channel()

        create_queue(channel, self.player_queue)

        self.__consume_players(channel)

    def __consume_players(self, channel):
        logging.info('Waiting for messages. To exit press CTRL+C')
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=self.player_queue, on_message_callback=self.__callback)
        channel.start_consuming()

    def __callback(self, ch, method, properties, body):
        logging.info(f"Received {body} from client")
        ch.basic_ack(delivery_tag=method.delivery_tag)
           