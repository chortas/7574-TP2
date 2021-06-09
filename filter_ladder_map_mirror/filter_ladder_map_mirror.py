#!/usr/bin/env python3
import logging
import json
from common.utils import *

class FilterLadderMapMirror():
    def __init__(self, match_exchange, match_token_exchange, top_civ_routing_key, 
    rate_winner_routing_key, ladder_field, map_field, mirror_field, id_field):
        self.match_exchange = match_exchange
        self.match_token_exchange = match_token_exchange
        self.top_civ_routing_key = top_civ_routing_key
        self.rate_winner_routing_key = rate_winner_routing_key
        self.ladder_field = ladder_field
        self.map_field = map_field
        self.mirror_field = mirror_field
        self.id_field = id_field
        self.winner_rate_counter = 0
        self.top_civ_counter = 0

    def start(self):
        wait_for_rabbit()

        connection, channel = create_connection_and_channel()

        create_exchange(channel, self.match_exchange, "fanout")
        match_queue_name = create_and_bind_anonymous_queue(channel, self.match_exchange)

        create_exchange(channel, self.match_token_exchange, "direct")

        self.__consume_matches(channel, match_queue_name)

    def __consume_matches(self, channel, queue_name):
        logging.info('Waiting for messages. To exit press CTRL+C')
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=queue_name, on_message_callback=self.__callback, auto_ack=True)
        channel.start_consuming()

    def __callback(self, ch, method, properties, body):
        matches = json.loads(body)
        if len(matches) == 0:
            logging.info(f"WINNER_RATE_COUNTER: {self.winner_rate_counter}")
            logging.info(f"TOP_CIV_COUNTER: {self.top_civ_counter}")
            logging.info(f"[FILTER_LADDER_MAP_MIRROR] The client already sent all messages y mando {matches}")
            send_message(ch, body, queue_name=self.rate_winner_routing_key, exchange_name=self.match_token_exchange)
            send_message(ch, body, queue_name=self.top_civ_routing_key, exchange_name=self.match_token_exchange)
            return

        winner_rate_matches = []
        top_civ_matches = []

        for match in matches:
            # drop extra rows
            new_match = {self.id_field: match[self.id_field]}
            if self.__meets_winner_rate_condition(match):
                self.winner_rate_counter += 1
                winner_rate_matches.append(new_match)
            elif self.__meets_top_civ_condition(match):
                self.top_civ_counter += 1
                top_civ_matches.append(new_match)

        if len(winner_rate_matches) != 0: send_message(ch, json.dumps(winner_rate_matches), queue_name=self.rate_winner_routing_key, exchange_name=self.match_token_exchange)
        if len(top_civ_matches) != 0: send_message(ch, json.dumps(top_civ_matches), queue_name=self.top_civ_routing_key, exchange_name=self.match_token_exchange)

    def __meets_winner_rate_condition(self, match):
        match_ladder = match[self.ladder_field]
        match_map = match[self.map_field]
        match_mirror = match[self.mirror_field]
        return match_ladder == "RM_1v1" and match_map == "arena" and match_mirror == "False"
    
    def __meets_top_civ_condition(self, match):
        match_ladder = match[self.ladder_field]
        match_map = match[self.map_field]
        return match_ladder == "RM_TEAM" and match_map == "islands"