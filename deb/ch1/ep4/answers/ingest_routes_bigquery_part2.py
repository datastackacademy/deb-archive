import pandas as pd

## Part 1 ##
route_types = {"airline_iata":"string", "src_iata":"string", "dest_iata": "string", "codeshare":"string", "equipment":"string"}
route_df = pd.read_csv('./data/routes-raw.csv', dtype=route_types)

## Part 2 ##
airline_names = ['al_airportID', 'al_name', 'al_alias', 'al_iata', 'al_icao', 'al_callsign', 'al_country', 'al_active']
airline_types = {'al_name':'string' ,'al_alias':'string', 'al_iata':'string', 'al_icao':'string', 'al_callsign':'string', 'al_country':'string', 'al_active':'string'}
airline_df = pd.read_csv('./data/deb-airlines.csv', header=0, names=airline_names, dtype=airline_types)
airport_df = pd.read_parquet("./data/deb-airports.parquet")