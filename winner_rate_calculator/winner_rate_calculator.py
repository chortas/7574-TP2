#!/usr/bin/env python3
import logging
import json
from common.utils import *

class WinnerRateCalculator():
    def __init__(self, grouped_players_queue, output_queue, winner_field):
        self.grouped_players_queue = grouped_players_queue
        self.output_queue = output_queue
        self.winner_field = winner_field
    
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
        logging.info("Estoy en el callback de winner_rate_calculator")
        players_by_civ = json.loads(body)

        for civ in players_by_civ:
            victories = 0
            players = players_by_civ[civ]
            for player in players:
                if player[self.winner_field] == "True":
                    victories += 1
            winner_rate = (victories / len(players)) * 100
            result = {civ: winner_rate}
            logging.info("Estoy por escribir en output 3")
            send_message(ch, json.dumps(result), queue_name=self.output_queue)