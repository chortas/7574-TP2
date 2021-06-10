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
        channel.basic_consume(queue=self.queue_name, on_message_callback=self.__callback, auto_ack=True)
        channel.start_consuming()

    def __callback(self, ch, method, properties, body):
        players = json.loads(body)

        if len(players) == 0:
            for reducer_queue in self.reducer_queues:
                send_message(ch, body, queue_name=reducer_queue)
            return

        # { group_id_field: {players that have that id} }
        message = {}
        for player in players:
            group_by_element = player[self.group_by_field]
            hashed_element = int(sha256(group_by_element.encode()).hexdigest(), 16)
            reducer_id = hashed_element % self.n_reducers
            message[reducer_id] = message.get(reducer_id, [])
            message[reducer_id].append(player)

        for reducer_id, elements in message.items():
            send_message(ch, json.dumps(elements), queue_name=self.reducer_queues[reducer_id])
        
