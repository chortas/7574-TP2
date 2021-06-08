#!/usr/bin/env python3
import logging
import json
from common.utils import *
from collections import Counter

class TopCivCalculator():
    def __init__(self, grouped_players_queue, output_queue, id_field, sentinel_amount):
        self.grouped_players_queue = grouped_players_queue
        self.output_queue = output_queue
        self.id_field = id_field
        self.sentinel_amount = sentinel_amount
        self.civilizations = {}
    
    def start(self):
        wait_for_rabbit()

        connection, channel = create_connection_and_channel()

        create_queue(channel, self.grouped_players_queue)
        create_queue(channel, self.output_queue)

        self.__consume_civilizations(channel)

    def __consume_civilizations(self, channel):
        logging.info('Waiting for messages. To exit press CTRL+C')
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=self.grouped_players_queue, on_message_callback=self.__callback, auto_ack=True)
        channel.start_consuming()

    def __callback(self, ch, method, properties, body):
        #logging.info(f"Received {body} from client")
        players_by_civ = json.loads(body)

        if len(players_by_civ) == 0:
            self.__send_top_5(ch)

        for civ in players_by_civ:
            token_by_civ = set()
            victories = 0
            players = players_by_civ[civ]
            for player in players:
                token = player[self.id_field]
                if token not in token_by_civ:
                    token_by_civ.add(token)
            self.civilizations[civ] = len(token_by_civ)

    def __send_top_5(self, channel):
        self.sentinel_amount -= 1
        if self.sentinel_amount != 0: return
        logging.info("To send top 5")
        top_5_civilizations = dict(Counter(self.civilizations).most_common(5))
        send_message(channel, json.dumps(top_5_civilizations), queue_name=self.output_queue)
