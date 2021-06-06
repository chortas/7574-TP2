#!/usr/bin/env python3
import logging
import json
from datetime import datetime, timedelta
from common.utils import *
from hashlib import sha256

class Join():
    def __init__(self, match_token_exchange, n_reducers, match_consumer_routing_key, 
    join_queue, match_id_field, player_consumer_routing_key, player_match_field):
        self.match_token_exchange = match_token_exchange
        self.reducer_queues = [f"{join_queue}_{i}" for i in range(1, n_reducers+1)]
        self.n_reducers = n_reducers
        self.match_consumer_routing_key = match_consumer_routing_key
        self.match_id_field = match_id_field
        self.player_consumer_routing_key = player_consumer_routing_key
        self.player_match_field = player_match_field

    def start(self):
        wait_for_rabbit()

        connection, channel = create_connection_and_channel()

        create_exchange(channel, self.match_token_exchange, "direct")
        # todo: add routing key that comes from players
        queue_name = create_and_bind_anonymous_queue(channel, self.match_token_exchange, 
        routing_keys=[self.match_consumer_routing_key, self.player_consumer_routing_key])

        for reducer_queue in self.reducer_queues:
            create_queue(channel, reducer_queue)

        self.__consume_match_tokens(channel, queue_name)

    def __consume_match_tokens(self, channel, queue_name):
        logging.info('Waiting for messages. To exit press CTRL+C')
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=queue_name, on_message_callback=self.__callback)
        channel.start_consuming()

    def __callback(self, ch, method, properties, body):
        logging.info(f"Received {body} from {method.routing_key}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        body_parsed = json.loads(body) # todo: check this when implementing the join
        if len(body_parsed) == 0:
            logging.info("[JOIN] The client already sent all messages")
            for reducer_queue in self.reducer_queues:
                send_message(ch, body, queue_name=reducer_queue)
            return
        id_to_send = None
        if method.routing_key == self.match_consumer_routing_key:
            logging.info(f"Recibí un match")
            id_to_send = body_parsed[self.match_id_field]
        if method.routing_key == self.player_consumer_routing_key:
            logging.info(f"Recibí un player")
            id_to_send = body_parsed[self.player_match_field]
        hashed_id = int(sha256(id_to_send.encode()).hexdigest(), 16)
        send_message(ch, body, queue_name=self.reducer_queues[hashed_id % self.n_reducers])
    