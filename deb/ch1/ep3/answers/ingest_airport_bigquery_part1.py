import pandas as pd


## ASC Data ##
#explicitly cast types
asc_types = {"iata":"string", "airport":"string", "city":"string", "state":"string", "country":"string", "lat":"float64", "long":"float64"}
#Read ASC Data
asc_df = pd.read_csv("./data/ASC-airports-raw.csv", delimiter=",", dtype=asc_types)
#check if column names and dtypes casted correctly
asc_df.info()
asc_df.head()


## OF Data ##
#declare ordered column names
of_names = ["id", "name", "city", "country", "iata", "icao", "lat", "long", "altitude", "timezone_utc", "dst", "tz", "type", "source"]
#explicitly cast types
of_types = {"id":"int", "name":"string", "city":"string", "country":"string", "iata":"string", "icao":"string", "lat":"float64", "long":"float64", "altitude":"float64", "timezone_utc":"string", "dst":"string", "tz":"string", "type":"string", "source":"string"}
#pass column names and explicit types into read_csv
of_df = pd.read_csv("./data/OF-airports-raw.csv", delimiter=",", index_col=0, names=of_names, dtype=of_types)
#check if column names and dtypes casted correctly
of_df.info()
of_df.head()
