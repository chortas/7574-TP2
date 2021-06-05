#!/usr/bin/env python3
import logging
import json
from datetime import datetime, timedelta
from common.utils import *

class FilterSoloWinnerPlayer():
    def __init__(self, grouped_players_queue, output_queue, rating_field, winner_field):
        self.grouped_players_queue = grouped_players_queue
        self.output_queue = output_queue
        self.rating_field = rating_field
        self.winner_field = winner_field

    def start(self):
        wait_for_rabbit()

        connection, channel = create_connection_and_channel()

        create_queue(channel, self.grouped_players_queue)
        create_queue(channel, self.output_queue)

        self.__consume_players(channel)

    def __consume_players(self, channel):
        logging.info('Waiting for messages. To exit press CTRL+C')
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=self.grouped_players_queue, on_message_callback=self.__callback)
        channel.start_consuming()

    def __callback(self, ch, method, properties, body):
        logging.info(f"Received {body} from client")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        match = json.loads(body)
        if self.__meets_the_condition(match):
            match_id = list(match.keys())[0]
            logging.info(f"[FILTER_SOLO_WINNER_PLAYER] Sending id: {match_id}")
            send_message(ch, self.output_queue, match_id)
        
    def __meets_the_condition(self, match):
        players = list(match.values())[0]
        if len(players) != 2: return False
        rating_winner, rating_loser = (0,0)
        for player in players:
            if not player[self.rating_field]: return False
            is_winner = player[self.winner_field].lower() == "true"
            if is_winner:
                rating_winner = int(player[self.rating_field])
            else:
                rating_loser = int(player[self.rating_field])
        return rating_winner > 1000 and (rating_loser - rating_winner) / rating_loser > 0.3
