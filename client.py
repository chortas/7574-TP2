#!/usr/bin/env python3
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
        self.__read_and_send(self.player_file, self.player_queue)

    def __send_matches(self):
        self.__read_and_send(self.match_file, self.match_queue)

    def __read_and_send(self, file_name, queue):
        connection, channel = create_connection_and_channel()

        create_queue(channel, queue)

        with open(file_name, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            counter = 0
            for element in csv_reader:
                logging.info(f"Counter: {counter}")
                send_message(channel, json.dumps(element), queue_name=queue)
                if counter == 5: #TODO: delete this in demo
                    send_message(channel, json.dumps({}), queue_name=queue)                    
                    break
                #logging.info(f"Sent {element} to queue {queue}")
                counter += 1

        connection.close()