#!/usr/bin/env python3
import logging
import json
from datetime import datetime, timedelta
from common.utils import *
from hashlib import sha256

class GroupByMatch():
    def __init__(self, player_exchange, n_reducers, group_by_match_queue, match_field):
        self.player_exchange = player_exchange
        self.reducer_queues = [f"{group_by_match_queue}_{i}" for i in range(1, n_reducers+1)]
        self.n_reducers = n_reducers
        self.match_field = match_field

    def start(self):
        wait_for_rabbit()

        connection, channel = create_connection_and_channel()

        create_exchange(channel, self.player_exchange, "fanout")
        queue_name = create_and_bind_anonymous_queue(channel, self.player_exchange)

        for reducer_queue in self.reducer_queues:
            create_queue(channel, reducer_queue)

        self.__consume_players(channel, queue_name)

    def __consume_players(self, channel, queue_name):
        logging.info('Waiting for messages. To exit press CTRL+C')
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=queue_name, on_message_callback=self.__callback)
        channel.start_consuming()

    def __callback(self, ch, method, properties, body):
        logging.info(f"Received {body} from client")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        player = json.loads(body)
        if len(player) == 0:
            logging.info("[GROUP_BY_MATCH] The client already sent all messages")
            for reducer_queue in self.reducer_queues:
                send_message(ch, body, queue_name=reducer_queue)
            return
        match = player[self.match_field]
        hashed_match = int(sha256(match.encode()).hexdigest(), 16)
        send_message(ch, body, queue_name=self.reducer_queues[hashed_match % self.n_reducers])
    