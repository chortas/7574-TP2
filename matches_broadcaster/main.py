#!/usr/bin/env python3
import logging
import os

from matches_broadcaster import MatchesBroadcaster

def parse_config_params():
    config_params = {}
    try:
        config_params["match_queue"] = os.environ["MATCH_QUEUE"]
        config_params["match_exchange"] = os.environ["MATCH_EXCHANGE"]

    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting".format(e))

    return config_params

def main():
    initialize_log()

    config_params = parse_config_params()

    matches_broadcaster = MatchesBroadcaster(config_params["match_queue"], config_params["match_exchange"])
    matches_broadcaster.start()

def initialize_log():
    """
    Python custom logging initialization
    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
    )

if __name__== "__main__":
    main()