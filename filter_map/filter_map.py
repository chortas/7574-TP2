#!/usr/bin/env python3
import logging
import json
from common.utils import *

class FilterMap():
    def __init__(self, ladder_exchange, match_team_routing_key, 
    match_solo_routing_key):
        self.ladder_exchange = ladder_exchange
        self.match_team_routing_key = match_team_routing_key
        self.match_solo_routing_key = match_solo_routing_key

    def start(self):
        wait_for_rabbit()

        connection, channel = create_connection_and_channel()

        create_exchange(channel, self.ladder_exchange, "direct")
        ladder_queue_name = create_and_bind_anonymous_queue(channel, 
        self.ladder_exchange, routing_keys=[self.match_team_routing_key, self.match_solo_routing_key])

        self.__consume_matches(channel, ladder_queue_name)

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
            return
        logging.info(f"[FILTER_MAP] Routing key: {method.routing_key}")