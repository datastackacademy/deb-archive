import pandas as pd


## ASC Data ##
#explicitly cast types
asc_types = {"iata":"string", "airport":"string", "city":"string", "state":"string", "country":"string", "lat":"float64", "long":"float64"}
#read ASC Data
asc_df = pd.read_csv("./data/ASC-airports-raw.csv", delimiter=",", dtype=asc_types)

## OF Data ##
#declare ordered column names
of_names = ["id", "name", "city", "country", "iata", "icao", "lat", "long", "altitude", "timezone_utc", "dst", "tz", "type", "source"]
#explicitly cast types
of_types = {"id":"int", "name":"string", "city":"string", "country":"string", "iata":"string", "icao":"string", "lat":"float64", "long":"float64", "altitude":"float64", "timezone_utc":"string", "dst":"string", "tz":"string", "type":"string", "source":"string"}
#read OF Data
of_df = pd.read_csv("./data/OF-airports-raw.csv", delimiter=",", index_col=0, names=of_names, dtype=of_types)


asc_of_df = pd.merge(asc_df, of_df, how="left", on="iata") #merge asc and of dataframes on iata code column

#drop unnecessary columns
asc_of_df.drop(axis=1, columns=['airport','city_x','country_x','lat_y','long_y','type','source'], inplace=True)
# rename columns
asc_of_df.columns = ['iata', 'state', 'lat', 'long', 'name', 'city', 'country', 'icao', 'altitude', 'utc_offset', 'dst', 'tz']
# reorder columns
asc_of_df = asc_of_df[['iata', 'name', 'city', 'state', 'country', 'icao', 'lat', 'long', 'altitude', 'utc_offset', 'dst', 'tz']]

#create dataframe with only airports located in the USA
usa_df = asc_of_df[asc_of_df.country == "United States"]

#export new dataframe as parquet
usa_df.to_parquet("./data/deb-airports.parquet")