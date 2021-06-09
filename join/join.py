#!/usr/bin/env python3
import logging
import json
from datetime import datetime, timedelta
from common.utils import *
from hashlib import sha256

class Join():
    def __init__(self, match_token_exchange, n_reducers, match_consumer_routing_key, 
    join_exchange, match_id_field, player_consumer_routing_key, player_match_field):
        self.match_token_exchange = match_token_exchange
        self.reducer_exchanges = [f"{join_exchange}_{i}" for i in range(1, n_reducers+1)]
        self.n_reducers = n_reducers
        self.match_consumer_routing_key = match_consumer_routing_key
        self.match_id_field = match_id_field
        self.player_consumer_routing_key = player_consumer_routing_key
        self.player_match_field = player_match_field

    def start(self):
        wait_for_rabbit()

        connection, channel = create_connection_and_channel()

        create_exchange(channel, self.match_token_exchange, "direct")
        queue_name = create_and_bind_anonymous_queue(channel, self.match_token_exchange, 
        routing_keys=[self.match_consumer_routing_key, self.player_consumer_routing_key])

        for reducer_exchange in self.reducer_exchanges:
            create_exchange(channel, reducer_exchange, "direct")

        self.__consume_match_tokens(channel, queue_name)

    def __consume_match_tokens(self, channel, queue_name):
        logging.info('Waiting for messages. To exit press CTRL+C')
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=queue_name, on_message_callback=self.__callback, auto_ack=True)
        channel.start_consuming()

    def __callback(self, ch, method, properties, body):
        elements_parsed = json.loads(body) 
        if len(elements_parsed) == 0:
            logging.info("[JOIN] The client already sent all messages")
            for reducer_exchange in self.reducer_exchanges:
                send_message(ch, body, queue_name=method.routing_key, exchange_name=reducer_exchange)
            return
        
        message = {}
        for element in elements_parsed:
            id_to_hash = self.__get_id_to_hash(method, element)
            hashed_id = int(sha256(id_to_hash.encode()).hexdigest(), 16)
            reducer_id = hashed_id % self.n_reducers
            message[reducer_id] = message.get(reducer_id, [])
            message[reducer_id].append(element)

        logging.info(f"Estoy por enviar")
        for reducer_id, elements in message.items():
            send_message(ch, json.dumps(elements), queue_name=method.routing_key, 
            exchange_name=self.reducer_exchanges[hashed_id % self.n_reducers])
       
    def __get_id_to_hash(self, method, element):
        id_to_hash = None
        if method.routing_key == self.match_consumer_routing_key:
            id_to_hash = element[self.match_id_field]
        if method.routing_key == self.player_consumer_routing_key:
            id_to_hash = element[self.player_match_field]
        return id_to_hash