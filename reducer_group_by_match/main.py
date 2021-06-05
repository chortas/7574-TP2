#!/usr/bin/env python3
import logging
import os

from reducer_group_by_match import ReducerGroupByMatch

def parse_config_params():
    config_params = {}
    try:
        config_params["group_by_match_queue"] = os.environ["GROUP_BY_MATCH_QUEUE"]
        config_params["match_field"] = os.environ["MATCH_FIELD"]

    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting block manager".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting block manager".format(e))

    return config_params

def main():
    initialize_log()

    config_params = parse_config_params()

    reducer_group_by_match = ReducerGroupByMatch(config_params["group_by_match_queue"],
    config_params["match_field"])
    reducer_group_by_match.start()

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