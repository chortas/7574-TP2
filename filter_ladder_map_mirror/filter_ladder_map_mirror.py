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
        channel.basic_consume(queue=queue_name, on_message_callback=self.__callback)
        channel.start_consuming()

    def __callback(self, ch, method, properties, body):
        logging.info(f"Received {body} from client")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        match = json.loads(body)
        if len(match) == 0:
            logging.info("[FILTER_LADDER_MAP_MIRROR] The client already sent all messages")
            send_message(ch, body, queue_name=self.rate_winner_routing_key, exchange_name=self.match_token_exchange)
            send_message(ch, body, queue_name=self.top_civ_routing_key, exchange_name=self.match_token_exchange)
            return
        match_ladder = match[self.ladder_field]
        match_map = match[self.map_field]
        match_mirror = match[self.mirror_field]
        
        # drop extra rows
        new_match = {self.id_field: match[self.id_field]}
        
        if match_ladder == "RM_1v1" and match_map == "arena" and match_mirror == "False":
            send_message(ch, json.dumps(new_match), queue_name=self.rate_winner_routing_key, exchange_name=self.match_token_exchange)
        if match_ladder == "RM_TEAM" and match_map == "islands":
            send_message(ch, json.dumps(new_match), queue_name=self.top_civ_routing_key, exchange_name=self.match_token_exchange)
