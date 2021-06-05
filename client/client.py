#!/usr/bin/env python3
import pika
import sys
import random
import time
import logging
import csv
import json

class Client:
    def __init__(self, match_queue, match_file):
        self.match_queue = match_queue
        self.match_file = match_file

    def start(self):
        # Wait for rabbitmq to come up
        time.sleep(10)

        # Create RabbitMQ communication channel
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq'))
        channel = connection.channel()

        channel.queue_declare(queue=self.match_queue, durable=True)

        with open(self.match_file, mode='r') as matches_file:
            matches_reader = csv.DictReader(matches_file)
            for match in matches_reader:
                channel.basic_publish(
                    exchange='',
                    routing_key=self.match_queue,
                    body=json.dumps(match),
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # make message persistent
                    ))
            logging.info(f"Sent {match} to filter_rating_server_duration")

        connection.close()
