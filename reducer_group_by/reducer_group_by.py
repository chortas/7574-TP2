#!/usr/bin/env python3
import logging
import json
from common.utils import *

class ReducerGroupBy():
    def __init__(self, group_by_queue, group_by_field, grouped_players_queue, sentinel_amount):
        self.group_by_queue = group_by_queue
        self.group_by_field = group_by_field
        self.grouped_players_queue = grouped_players_queue
        self.players_to_group = {}
        self.sentinel_amount = sentinel_amount

    def start(self):
        wait_for_rabbit()

        connection, channel = create_connection_and_channel()

        create_queue(channel, self.group_by_queue)
        create_queue(channel, self.grouped_players_queue)

        self.__consume_players(channel)

    def __consume_players(self, channel):
        logging.info('Waiting for messages. To exit press CTRL+C')
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=self.group_by_queue, on_message_callback=self.__callback, auto_ack=True)
        channel.start_consuming()

    def __callback(self, ch, method, properties, body):
        #logging.info(f"Received {body} from client")
        player = json.loads(body)
        if len(player) == 0:
            return self.__handle_end_group_by(ch)
        group_by_element = player[self.group_by_field]
        self.players_to_group[group_by_element] = self.players_to_group.get(group_by_element, [])
        self.players_to_group[group_by_element].append(player)

    def __handle_end_group_by(self, ch):
        self.sentinel_amount -= 1
        if self.sentinel_amount != 0: return        
        logging.info("[REDUCER_GROUP_BY] The client already sent all messages")
        for group_by_element in self.players_to_group:
            logging.info(f"[REDUCER_GROUP_BY] Sending {group_by_element} to {self.grouped_players_queue}")
            result = {group_by_element: self.players_to_group[group_by_element]}
            send_message(ch, json.dumps(result), queue_name=self.grouped_players_queue)
        send_message(ch, json.dumps({}), queue_name=self.grouped_players_queue)