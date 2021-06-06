#!/usr/bin/env python3
import logging
import json
from common.utils import *

class FilterLadder():
    def __init__(self, match_exchange, ladder_exchange, match_team_routing_key, 
    match_solo_routing_key, ladder_field):
        self.match_exchange = match_exchange
        self.ladder_exchange = ladder_exchange
        self.match_team_routing_key = match_team_routing_key
        self.match_solo_routing_key = match_solo_routing_key
        self.ladder_field = ladder_field

    def start(self):
        wait_for_rabbit()

        connection, channel = create_connection_and_channel()

        create_exchange(channel, self.match_exchange, "fanout")
        match_queue_name = create_and_bind_anonymous_queue(channel, self.match_exchange)

        create_exchange(channel, self.ladder_exchange, "direct")

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
            ch.basic_publish(exchange=self.ladder_exchange, routing_key=self.match_solo_routing_key, body=body)
            ch.basic_publish(exchange=self.ladder_exchange, routing_key=self.match_team_routing_key, body=body)
            return
        ladder = match[self.ladder_field]
        if ladder == "RM_1v1":
            ch.basic_publish(exchange=self.ladder_exchange, routing_key=self.match_solo_routing_key, body=body)
        if ladder == "RM_TEAM":
            ch.basic_publish(exchange=self.ladder_exchange, routing_key=self.match_team_routing_key, body=body)
