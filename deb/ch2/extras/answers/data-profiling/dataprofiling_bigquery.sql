
-- get total number of flights in the dataset
SELECT COUNT(1) total_rows
FROM airline_data.tmp_flights;


-- Get count of flights per airline
SELECT
  Airline,
  COUNT(1) num_flights
FROM
  airline_data.tmp_flights
GROUP BY 1
ORDER BY 2 DESC;


-- See if there are any airlines in the flights table which are missing in the airlines table
SELECT
  a.airline,
  b.Name
FROM airline_data.tmp_flights a
LEFT OUTER JOIN airline_data.raw_airline_data b ON
  a.Airline = b.IATA
GROUP BY 1, 2
ORDER BY 1;


-- find unique set of Origin flights airports and join it to the airports table
-- any airports missing will have NULL values as name or city
SELECT  -- outer sub-select
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
ORDER BY 1;


-- Find list airports which are in flights and NOT in airports
SELECT
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
ORDER BY 1;
