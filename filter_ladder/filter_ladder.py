#!/usr/bin/env python3
import logging
import json
from common.utils import *

class FilterLadder():
    def __init__(self, match_exchange):
        self.match_exchange = match_exchange

    def start(self):
        wait_for_rabbit()

        connection, channel = create_connection_and_channel()

        create_exchange(channel, self.match_exchange, "fanout")
        queue_name = create_and_bind_anonymous_queue(channel, self.match_exchange)

        self.__consume_matches(channel, queue_name)

    def __consume_matches(self, channel, queue_name):
        logging.info('Waiting for messages. To exit press CTRL+C')
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=queue_name, on_message_callback=self.__callback)
        channel.start_consuming()

    def __callback(self, ch, method, properties, body):
        logging.info(f"Received {body} from client")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        '''match = json.loads(body)
        if self.__meets_the_condition(match):
            send_message(ch, match[self.id_field], queue_name=self.output_queue)
           
    def __meets_the_condition(self, match):
        if len(match) == 0:
            return False
        average_rating = int(match[self.avg_rating_field]) if match[self.avg_rating_field] else 0
        server = match[self.server_field]
        duration = self.__parse_timedelta(match[self.duration_field])
        return average_rating > 2000 and server in ("koreacentral", "southeastasia", "eastus") and duration > timedelta(hours=2)
    '''