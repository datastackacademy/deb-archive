import pandas as pd

## Part 1 ##
route_types = {"airline_iata":"string", "src_iata":"string", "dest_iata": "string", "codeshare":"string", "equipment":"string"}
route_df = pd.read_csv('./data/routes-raw.csv', dtype=route_types)
route_df.info()