#!/usr/bin/env python3
import logging
import json
from datetime import datetime, timedelta
from common.utils import *

class ReducerGroupByMatch():
    def __init__(self, group_by_match_queue, match_field, grouped_players_queue):
        self.group_by_match_queue = group_by_match_queue
        self.match_field = match_field
        self.grouped_players_queue = grouped_players_queue
        self.players_by_match = {}

    def start(self):
        wait_for_rabbit()

        connection, channel = create_connection_and_channel()

        create_queue(channel, self.group_by_match_queue)

        self.__consume_players(channel)

    def __consume_players(self, channel):
        logging.info('Waiting for messages. To exit press CTRL+C')
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=self.group_by_match_queue, on_message_callback=self.__callback)
        channel.start_consuming()

    def __callback(self, ch, method, properties, body):
        logging.info(f"Received {body} from client")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        player = json.loads(body)
        if len(player) == 0:
            logging.info("[REDUCER_GROUP_BY_MATCH] The client already sent all messages")
            for match in self.players_by_match:
                logging.info(f"[REDUCER_GROUP_BY_MATCH] Sending {match}")
                result = {match: self.players_by_match[match]}
                send_message(ch, json.dumps(result), queue_name=self.grouped_players_queue)
            return
        match = player[self.match_field]
        self.players_by_match[match] = self.players_by_match.get(match, [])
        self.players_by_match[match].append(player)
