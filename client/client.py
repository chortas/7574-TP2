#!/usr/bin/env python3
import pika
import sys
import random
import time
import logging
import csv
import json
from threading import Thread
from common.utils import *

class Client:
    def __init__(self, match_queue, match_file, player_queue, player_file):
        self.match_queue = match_queue
        self.match_file = match_file
        self.player_queue = player_queue
        self.player_file = player_file
        self.match_sender = Thread(target=self.__send_matches)
        self.player_sender = Thread(target=self.__send_players)

    def start(self):
        wait_for_rabbit()
        
        self.match_sender.start()
        #self.player_sender.start()
    
    def __send_players(self):
        connection, channel = create_connection_and_channel()

        channel.queue_declare(queue=self.player_queue, durable=True)

        with open(self.player_file, mode='r') as players_file:
            players_reader = csv.DictReader(players_file)
            counter = 0
            for player in players_reader:
                counter += 1
                if counter == 1000: #TODO: delete this in demo
                    break
                channel.basic_publish(
                    exchange='',
                    routing_key=self.player_queue,
                    body=json.dumps(player),
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # make message persistent
                    ))
                logging.info(f"Sent {player} to group_by_match")

        connection.close()

    def __send_matches(self):
        connection, channel = create_connection_and_channel()

        create_queue(channel, self.match_queue)

        with open(self.match_file, mode='r') as matches_file:
            matches_reader = csv.DictReader(matches_file)
            counter = 0
            for match in matches_reader:
                counter += 1
                if counter == 1000: #TODO: delete this in demo
                    break
                channel.basic_publish(
                    exchange='',
                    routing_key=self.match_queue,
                    body=json.dumps(match),
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # make message persistent
                    ))
            logging.info(f"Sent {match} to filter_rating_server_duration")

        connection.close()
