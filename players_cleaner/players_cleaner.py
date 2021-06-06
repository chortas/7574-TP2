#!/usr/bin/env python3
import logging
import json
from datetime import datetime, timedelta
from common.utils import *
from hashlib import sha256

class PlayersCleaner():
    def __init__(self, player_exchange, match_field, civ_field, winner_field):
        self.player_exchange = player_exchange
        self.match_field = match_field
        self.civ_field = civ_field
        self.winner_field = winner_field 

    def start(self):
        wait_for_rabbit()

        connection, channel = create_connection_and_channel()

        create_exchange(channel, self.player_exchange, "fanout")
        queue_name = create_and_bind_anonymous_queue(channel, self.player_exchange)

        # create queue to produce join result

        self.__consume_players(channel, queue_name)

    def __consume_players(self, channel, queue_name):
        logging.info('Waiting for messages. To exit press CTRL+C')
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=queue_name, on_message_callback=self.__callback)
        channel.start_consuming()

    def __callback(self, ch, method, properties, body):
        logging.info(f"Received {body} from client")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        player = json.loads(body)
        if len(player) == 0:
            logging.info("[GROUP_BY_MATCH] The client already sent all messages")
            # send centinel to join
            return
        new_player = {self.match_field: player[self.match_field],
                    self.civ_field: player[self.civ_field],
                    self.winner_field: player[self.winner_field]}
        logging.info(f"New player: {new_player}")
        # send to join new_player
