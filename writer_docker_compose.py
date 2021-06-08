DOCKER_COMPOSE_FILE_NAME = "docker-compose.yml"

HEADER_AND_RABBIT = """version: '3'
services:
  rabbitmq:
    build:
      context: ./rabbitmq
      dockerfile: Dockerfile
    ports:
      - 15672:15672
      - 5672:5672
"""

N_REDUCERS_GROUP_BY_MATCH = 5
N_REDUCERS_RATE_WINNER_JOIN = 2
N_REDUCERS_TOP_CIV_JOIN = 2
N_REDUCERS_GROUP_BY_CIV_RATE_WINNER = 2
N_REDUCERS_GROUP_BY_CIV_TOP_CIV = 2
N_FILTER_SOLO_WINNER_PLAYER = 3

def write_header_and_rabbit(compose_file):
    compose_file.write(HEADER_AND_RABBIT)

def write_section(compose_file, container_name, image, env_variables):
    section = f"""\n  {container_name}:
    container_name: {container_name}
    image: {image}:latest
    entrypoint: python3 /main.py
    restart: on-failure
    depends_on:
      - rabbitmq
    links: 
      - rabbitmq
    environment:\n"""
    for env, variable in env_variables.items():
      section += f"      - {env}={variable}\n"
    compose_file.write(section)

with open(DOCKER_COMPOSE_FILE_NAME, "w") as compose_file:
    write_header_and_rabbit(compose_file)

    #filter_avg_rating_server_duration
    env_variables = {"MATCH_EXCHANGE": "match_exchange", "OUTPUT_QUEUE": "output_queue_1", 
    "AVG_RATING_FIELD": "average_rating", "SERVER_FIELD": "server", "DURATION_FIELD": "duration", "ID_FIELD": "token"}
    write_section(compose_file, "filter_avg_rating_server_duration", "filter_avg_rating_server_duration", env_variables)

    #group_by_match
    GROUP_BY_MATCH_QUEUE = "group_by_match_queue"
    env_variables = {"EXCHANGE_NAME": "player_exchange", "N_REDUCERS": N_REDUCERS_GROUP_BY_MATCH, "GROUP_BY_QUEUE": GROUP_BY_MATCH_QUEUE,
    "GROUP_BY_FIELD": "match"}    
    write_section(compose_file, "group_by_match", "group_by", env_variables)

    #reducers_group_by_match
    for i in range(1, N_REDUCERS_GROUP_BY_MATCH+1):
      env_variables = {"GROUP_BY_QUEUE": f"{GROUP_BY_MATCH_QUEUE}_{i}", "GROUP_BY_FIELD": "match", 
      "GROUPED_PLAYERS_QUEUE": "grouped_players_queue"}        
      write_section(compose_file, f"reducer_group_by_match_{i}", "reducer_group_by", env_variables)

    # filter_solo_winner_player
    env_variables = {"GROUPED_PLAYERS_QUEUE": "grouped_players_queue", "OUTPUT_QUEUE": "output_queue_2", 
    "RATING_FIELD": "rating", "WINNER_FIELD": "winner"}    
    for i in range(1, N_FILTER_SOLO_WINNER_PLAYER+1):
      write_section(compose_file, f"filter_solo_winner_player_{i}", "filter_solo_winner_player", env_variables)

    # matches_broadcaster
    env_variables = {"QUEUE_NAME": "match_queue", "EXCHANGE_NAME": "match_exchange"}    
    write_section(compose_file, "matches_broadcaster", "broadcaster", env_variables)
    
    # filter_ladder_map_mirror
    env_variables = {"MATCH_EXCHANGE": "match_exchange", "MATCH_TOKEN_EXCHANGE": "match_token_exchange", 
    "TOP_CIV_ROUTING_KEY": "top_civ_routing_key", "RATE_WINNER_ROUTING_KEY": "rate_winner_routing_key", 
    "LADDER_FIELD": "ladder", "MAP_FIELD": "map", "MIRROR_FIELD": "mirror", "ID_FIELD": "token"}    
    write_section(compose_file, "filter_ladder_map_mirror", "filter_ladder_map_mirror", env_variables)

    # players_broadcaster
    env_variables = {"QUEUE_NAME": "player_queue", "EXCHANGE_NAME": "player_exchange"}    
    write_section(compose_file, "players_broadcaster", "broadcaster", env_variables)    
    
    # players_cleaner
    env_variables = {"PLAYER_EXCHANGE": "player_exchange", "MATCH_FIELD": "match", "CIV_FIELD": "civ", "WINNER_FIELD": "winner",
    "JOIN_EXCHANGE": "match_token_exchange", "JOIN_ROUTING_KEY": "player_rate_winner_routing_key"}
    write_section(compose_file, "players_cleaner", "players_cleaner", env_variables)    

    # filter_rating
    env_variables = {"PLAYER_EXCHANGE": "player_exchange", "RATING_FIELD": "rating", "MATCH_FIELD": "match", 
    "CIV_FIELD": "civ", "ID_FIELD": "token", "JOIN_EXCHANGE": "match_token_exchange", 
    "JOIN_ROUTING_KEY": "player_top_civ_routing_key" }
    write_section(compose_file, "filter_rating", "filter_rating", env_variables)    

    # join_rate_winner
    JOIN_RATE_EXCHANGE = "join_rate_winner"
    env_variables = {"MATCH_TOKEN_EXCHANGE": "match_token_exchange", "N_REDUCERS": N_REDUCERS_RATE_WINNER_JOIN, 
    "MATCH_CONSUMER_ROUTING_KEY": "rate_winner_routing_key", "JOIN_EXCHANGE": JOIN_RATE_EXCHANGE, 
    "MATCH_ID_FIELD": "token", "PLAYER_CONSUMER_ROUTING_KEY": "player_rate_winner_routing_key", "PLAYER_MATCH_FIELD": "match"}
    write_section(compose_file, "join_rate_winner", "join", env_variables)    

    # join_top_civ
    JOIN_TOP_CIV_EXCHANGE = "join_top_civ"
    env_variables = {"MATCH_TOKEN_EXCHANGE": "match_token_exchange", "N_REDUCERS": N_REDUCERS_TOP_CIV_JOIN, 
    "MATCH_CONSUMER_ROUTING_KEY": "top_civ_routing_key", "JOIN_EXCHANGE": JOIN_TOP_CIV_EXCHANGE, "MATCH_ID_FIELD": "token", 
    "PLAYER_CONSUMER_ROUTING_KEY": "player_top_civ_routing_key", "PLAYER_MATCH_FIELD": "match"}
    write_section(compose_file, "join_top_civ", "join", env_variables)    

    # reducers_rate_winner_join
    for i in range(1, N_REDUCERS_RATE_WINNER_JOIN+1):
      env_variables = {"JOIN_EXCHANGE": f"{JOIN_RATE_EXCHANGE}_{i}", "MATCH_CONSUMER_ROUTING_KEY": "rate_winner_routing_key",
      "PLAYER_CONSUMER_ROUTING_KEY": "player_rate_winner_routing_key", 
      "GROUPED_RESULT_QUEUE": "grouped_rate_winner_queue", "MATCH_ID_FIELD": "token", "PLAYER_MATCH_FIELD": "match"}
      write_section(compose_file, f"reducer_rate_winner_join_{i}", "reducer_join", env_variables)

    # reducers_top_civ_join
    for i in range(1, N_REDUCERS_TOP_CIV_JOIN+1):
      env_variables = {"JOIN_EXCHANGE": f"{JOIN_TOP_CIV_EXCHANGE}_{i}", "MATCH_CONSUMER_ROUTING_KEY": "top_civ_routing_key",
      "PLAYER_CONSUMER_ROUTING_KEY": "player_top_civ_routing_key", 
      "GROUPED_RESULT_QUEUE": "grouped_top_civ_queue", "MATCH_ID_FIELD": "token", "PLAYER_MATCH_FIELD": "match"}
      write_section(compose_file, f"reducer_top_civ_join_{i}", "reducer_join", env_variables)

    # group_by_civ_rate_winner
    GROUP_BY_CIV_RATE_WINNER_QUEUE = "group_by_civ_rate_winner_queue"
    env_variables = {"QUEUE_NAME": "grouped_rate_winner_queue", 
    "N_REDUCERS": N_REDUCERS_GROUP_BY_CIV_RATE_WINNER, 
    "GROUP_BY_QUEUE": GROUP_BY_CIV_RATE_WINNER_QUEUE, "GROUP_BY_FIELD": "civ"}
    write_section(compose_file, "group_by_civ_rate_winner", "group_by", env_variables)

    # reducers_group_by_civ_rate_winner
    for i in range(1, N_REDUCERS_GROUP_BY_CIV_RATE_WINNER+1):
      env_variables = {"GROUP_BY_QUEUE": F"{GROUP_BY_CIV_RATE_WINNER_QUEUE}_{i}",
      "GROUP_BY_FIELD": "civ", "GROUPED_PLAYERS_QUEUE": "winner_rate_calculator_queue",
      "SENTINEL_AMOUNT": N_REDUCERS_RATE_WINNER_JOIN}
      write_section(compose_file, f"reducer_group_by_civ_rate_winner_{i}", "reducer_group_by", env_variables)

    # group_by_civ_top_civ
    GROUP_BY_CIV_TOP_CIV_QUEUE = "group_by_civ_top_civ_queue"
    env_variables = {"QUEUE_NAME": "grouped_top_civ_queue", 
    "N_REDUCERS": N_REDUCERS_GROUP_BY_CIV_TOP_CIV,
    "GROUP_BY_QUEUE": GROUP_BY_CIV_TOP_CIV_QUEUE, "GROUP_BY_FIELD": "civ"}
    write_section(compose_file, "group_by_civ_top_civ", "group_by", env_variables)

    # reducers_group_by_civ_top_civ
    for i in range(1, N_REDUCERS_GROUP_BY_CIV_TOP_CIV+1):
      env_variables = {"GROUP_BY_QUEUE": F"{GROUP_BY_CIV_TOP_CIV_QUEUE}_{i}",
      "GROUP_BY_FIELD": "civ", "GROUPED_PLAYERS_QUEUE": "top_civ_calculator_queue",
      "SENTINEL_AMOUNT": N_REDUCERS_TOP_CIV_JOIN}
      write_section(compose_file, f"reducer_group_by_civ_top_civ_{i}", "reducer_group_by", env_variables)

    # winner_rate_calculator
    env_variables = {"GROUPED_PLAYERS_QUEUE": "winner_rate_calculator_queue", 
    "OUTPUT_QUEUE": "output_queue_3", "WINNER_FIELD": "winner"}
    write_section(compose_file, "winner_rate_calculator", "winner_rate_calculator", env_variables)

    # top_civ_calculator
    env_variables = {"GROUPED_PLAYERS_QUEUE": "top_civ_calculator_queue", 
    "OUTPUT_QUEUE": "output_queue_4", "ID_FIELD": "token", 
    "SENTINEL_AMOUNT": N_REDUCERS_GROUP_BY_CIV_TOP_CIV}
    write_section(compose_file, "top_civ_calculator", "top_civ_calculator", env_variables)
