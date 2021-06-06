#!/usr/bin/env python3
import logging
import json
from common.utils import *

class ReducerJoin():
    def __init__(self, join_exchange, match_consumer_routing_key, player_consumer_routing_key,
    grouped_result_queue, match_id_field, player_match_field):
        self.join_exchange = join_exchange
        self.match_consumer_routing_key = match_consumer_routing_key
        self.player_consumer_routing_key = player_consumer_routing_key
        self.grouped_result_queue = grouped_result_queue
        self.match_id_field = match_id_field
        self.player_match_field = player_match_field
        self.matches_and_players = {}
        self.matches = set()
        self.players = set()
        self.reset_counter = 2 # to know when to stop
        # add data structure to save joins

    def start(self):
        wait_for_rabbit()

        connection, channel = create_connection_and_channel()

        create_exchange(channel, self.join_exchange, exchange_type="direct")
        queue_name = create_and_bind_anonymous_queue(channel, self.join_exchange, 
        routing_keys=[self.match_consumer_routing_key, self.player_consumer_routing_key])

        self.__consume_matches_and_players(channel, queue_name)

    def __consume_matches_and_players(self, channel, queue_name):
        logging.info('Waiting for messages. To exit press CTRL+C')
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=queue_name, on_message_callback=self.__callback)
        channel.start_consuming()

    def __callback(self, ch, method, properties, body):
        logging.info(f"Received {body} from client")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        body_parsed = json.loads(body) 
        if len(body_parsed) == 0:
            self.reset_counter -= 1
            if self.reset_counter == 0:
                logging.info("[REDUCER_JOIN] The client already sent all messages")
                self.__print_join()
            return

        id_to_join = (body_parsed[self.match_id_field] 
                    if method.routing_key == self.match_consumer_routing_key
                    else body_parsed[self.player_match_field])

        self.matches_and_players[id_to_join] =  self.matches_and_players.get(id_to_join, [])

        if method.routing_key == self.match_consumer_routing_key:
            self.matches.add(id_to_join)

        if method.routing_key == self.player_consumer_routing_key:
            self.players.add(id_to_join)
            self.matches_and_players[id_to_join].append(body_parsed)
    
    def __print_join(self):
        logging.info(f"Matches and players: {self.matches_and_players}")
        for token in self.matches_and_players:
            if token in self.matches and token in self.players:
                logging.info(f"Una fila joineada: {self.matches_and_players[token]}")