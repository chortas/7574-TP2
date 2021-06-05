#!/usr/bin/env python3
import logging
import json
from datetime import datetime, timedelta
from common.utils import *
from hashlib import sha256

class GroupByMatch():
    def __init__(self, player_queue, reducer_queues, match_field):
        self.player_queue = player_queue
        self.reducer_queues = reducer_queues
        self.match_field = match_field

    def start(self):
        wait_for_rabbit()

        connection, channel = create_connection_and_channel()

        create_queue(channel, self.player_queue)
        for reducer_queue in self.reducer_queues:
            create_queue(channel, reducer_queue)

        self.__consume_players(channel)

    def __consume_players(self, channel):
        logging.info('Waiting for messages. To exit press CTRL+C')
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=self.player_queue, on_message_callback=self.__callback)
        channel.start_consuming()

    def __callback(self, ch, method, properties, body):
        logging.info(f"Received {body} from client")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        player = json.loads(body)
        if len(player) == 0:
            logging.info("[GROUP_BY_MATCH] The client already sent all messages")
            for reducer_queue in self.reducer_queues:
                send_message(ch, reducer_queue, body)
            return
        match = player[self.match_field]
        hashed_match = int(sha256(match.encode()).hexdigest(), 16)
        if hashed_match % 2 == 0:
            send_message(ch, self.reducer_queues[0], body)
        else:
            send_message(ch, self.reducer_queues[1], body)
    