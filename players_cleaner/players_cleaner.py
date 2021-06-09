#!/usr/bin/env python3
import logging
import json
from datetime import datetime, timedelta
from common.utils import *
from hashlib import sha256

class PlayersCleaner():
    def __init__(self, player_exchange, match_field, civ_field, winner_field, 
    join_exchange, join_routing_key):
        self.player_exchange = player_exchange
        self.match_field = match_field
        self.civ_field = civ_field
        self.winner_field = winner_field 
        self.join_exchange = join_exchange
        self.join_routing_key = join_routing_key

    def start(self):
        wait_for_rabbit()

        connection, channel = create_connection_and_channel()

        create_exchange(channel, self.player_exchange, "fanout")
        queue_name = create_and_bind_anonymous_queue(channel, self.player_exchange)

        create_exchange(channel, self.join_exchange, "direct")

        self.__consume_players(channel, queue_name)

    def __consume_players(self, channel, queue_name):
        logging.info('Waiting for messages. To exit press CTRL+C')
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=queue_name, on_message_callback=self.__callback, auto_ack=True)
        channel.start_consuming()

    def __callback(self, ch, method, properties, body):
        players = json.loads(body)
        if len(players) == 0:
            logging.info("[PLAYERS_CLEANER] The client already sent all messages")
            send_message(ch, body, queue_name=self.join_routing_key, exchange_name=self.join_exchange)
            return
        
        new_players = []
        for player in players:
            new_players.append({self.match_field: player[self.match_field],
                    self.civ_field: player[self.civ_field],
                    self.winner_field: player[self.winner_field]})
                    
        send_message(ch, json.dumps(new_players), queue_name=self.join_routing_key, exchange_name=self.join_exchange)
