
# Using Google Cloud SDK bq command line to prfile data using Big Query

# list available BigQuery datasets (databases)
bq ls

# list tables (objects) within the airline_data dataset
bq ls airline_data

# create a new flights table and load January 2019 CSV file into it
bq load --source_format=CSV --autodetect --skip_leading_rows 1 airline_data.tmp_flights gs://<YOUR BUCKET>/bots/csv/2019/flights_2019_01.csv

# get total number of rows loaded
bq query "SELECT count(1) total_rows FROM airline_data.tmp_flights"

# get total number of flights per airline
bq query --nouse_legacy_sql \
'SELECT
  Airline,
  COUNT(1) num_flights
FROM
  airline_data.tmp_flights
GROUP BY 1
ORDER BY 2 DESC'

# See if there are any airlines in the flights table which are missing in the airlines table
bq query --nouse_legacy_sql \
'SELECT
  a.airline,
  b.Name
FROM airline_data.tmp_flights a
LEFT OUTER JOIN airline_data.raw_airline_data b ON
  a.Airline = b.IATA
GROUP BY 1, 2
ORDER BY 1'


# find unique set of Origin flights airports and join it to the airports table
# any airports missing will have NULL values as name or city
bq query --nouse_legacy_sql \
'SELECT  -- outer sub-select
  a.airports,
  b.name,
  b.city
FROM (  -- inner sub-select
  SELECT Origin airports
  FROM airline_data.tmp_flights
  GROUP BY 1
  ) a  -- table alias
LEFT OUTER JOIN airline_data.airports b ON
  a.airports = b.IATA
ORDER BY 1'


# Find list airports which are in flights and NOT in airports
bq query --nouse_legacy_sql \
'SELECT
  a.airport
FROM (
    SELECT
      Origin airport
    FROM airline_data.tmp_flights
    UNION DISTINCT
    SELECT
      Dest airport
    FROM airline_data.tmp_flights
  ) a
WHERE
  a.airport NOT IN (SELECT IATA FROM airline_data.airports)
ORDER BY 1'
