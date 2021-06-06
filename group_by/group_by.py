#!/usr/bin/env python3
import logging
import json
from datetime import datetime, timedelta
from common.utils import *
from hashlib import sha256

class GroupBy():
    def __init__(self, n_reducers, group_by_queue, group_by_field, exchange_name, queue_name):
        self.reducer_queues = [f"{group_by_queue}_{i}" for i in range(1, n_reducers+1)]
        self.n_reducers = n_reducers
        self.group_by_field = group_by_field
        self.exchange_name = exchange_name
        self.queue_name = queue_name
    
    def start(self):
        wait_for_rabbit()

        connection, channel = create_connection_and_channel()

        if self.exchange_name:
            create_exchange(channel, self.exchange_name, "fanout")
            self.queue_name = create_and_bind_anonymous_queue(channel, self.exchange_name)
        elif self.queue_name:
            create_queue(channel, self.queue_name)

        for reducer_queue in self.reducer_queues:
            create_queue(channel, reducer_queue)

        self.__consume_players(channel)

    def __consume_players(self, channel):
        logging.info('Waiting for messages. To exit press CTRL+C')
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=self.queue_name, on_message_callback=self.__callback)
        channel.start_consuming()

    def __callback(self, ch, method, properties, body):
        logging.info(f"Received {body} from client")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        player = json.loads(body)
        if len(player) == 0:
            logging.info("[GROUP_BY] The client already sent all messages")
            for reducer_queue in self.reducer_queues:
                send_message(ch, body, queue_name=reducer_queue)
            return
        group_by_element = player[self.group_by_field]
        logging.info(f"[GROUP_BY] Group by elemen: {group_by_element}")
        hashed_element = int(sha256(group_by_element.encode()).hexdigest(), 16)
        send_message(ch, body, queue_name=self.reducer_queues[hashed_element % self.n_reducers])
    