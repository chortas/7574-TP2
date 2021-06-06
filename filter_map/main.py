#!/usr/bin/env python3
import logging
import os

from filter_map import FilterMap

def parse_config_params():
    config_params = {}
    try:
        config_params["ladder_exchange"] = os.environ["LADDER_EXCHANGE"]
        config_params["match_team_routing_key"] = os.environ["MATCH_TEAM_ROUTING_KEY"]
        config_params["match_solo_routing_key"] = os.environ["MATCH_SOLO_ROUTING_KEY"]

    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting block manager".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting block manager".format(e))

    return config_params

def main():
    initialize_log()

    config_params = parse_config_params()

    filter_map = FilterMap(config_params["ladder_exchange"],
    config_params["match_team_routing_key"], config_params["match_solo_routing_key"])
    filter_map.start()

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