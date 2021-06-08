#!/usr/bin/env python3
import logging
import csv
import json
from threading import Thread
from common.utils import *

BATCH = 100000

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
        
        #self.match_sender.start()
        self.player_sender.start()
    
    def __send_players(self):
        self.__read_and_send(self.player_file, self.player_queue)

    def __send_matches(self):
        self.__read_and_send(self.match_file, self.match_queue)

    def __read_and_send(self, file_name, queue):
        connection, channel = create_connection_and_channel()

        create_queue(channel, queue)

        with open(file_name, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)    
            counter_lines = 0
            global_counter = 0
            lines = []
            for element in csv_reader:
                global_counter += 1
                lines.append(element)
                counter_lines += 1
                if counter_lines == BATCH: #TODO: delete this in demo
                    logging.info(f"[{file_name}] Read {BATCH} lines and global counter is {global_counter}")
                    send_message(channel, json.dumps(lines), queue_name=queue)
                    lines = []
                    counter_lines = 0

        if len(lines) != 0: send_message(channel, json.dumps(lines), queue_name=queue)                    
        
        #send the sentinel
        send_message(channel, json.dumps({}), queue_name=queue)     

        connection.close()