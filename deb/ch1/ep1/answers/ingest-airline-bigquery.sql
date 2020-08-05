-- TODO: <<ADD CODE HERE>> Look at the first 5 entries in the BigQuery Airlines table
SELECT * 
FROM airline_data.airlines 
LIMIT 5

-- TODO: <<ADD CODE HERE>> Count the number of rows in the BigQuery Airlines table as column named RowCount
SELECT COUNT(*) AS RowCount 
FROM airline_data.airlines

-- TODO: <<ADD CODE HERE>> Count the number of airlines that fly within the USA in the BigQuery Airlines table
SELECT COUNT(*) AS USAirlines 
FROM airline_data.airlines 
WHERE Country = "United States"


-- TODO: <<ADD CODE HERE>> List the number of airlines per country in descending order
SELECT country, COUNT(*) as NumAirlines FROM airline_data.airlines
GROUP BY country
ORDER BY country DESC

-- TODO: <<ADD CODE HERE>> List the number of airlines per country in descending order, including only active airlines that have iata code
SELECT country, COUNT(*) as NumAirlines FROM airline_data.airlines
WHERE active = true AND iata IS NOT NULL
GROUP BY country
ORDER BY country DESC

-- TODO: <<ADD CODE HERE>> Find the countries that have produced airlines which are no longer active
SELECT country, COUNT(*) as DeactivatedAirlines FROM airline_data.airlines
WHERE active = false AND country IS NOT NULL
GROUP BY country
ORDER BY DeactivatedAirlines DESC