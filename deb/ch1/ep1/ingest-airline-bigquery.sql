-- TODO: <<ADD CODE HERE>> Look at the first 5 entries in `airlines` table using LIMIT clause
SELECT <enter column names> FROM <enter table name> LIMIT <...>

-- TODO: <<ADD CODE HERE>> Count the number of rows in the `airlines` table as a column alias: RowCount
SELECT <enter function> AS <enter column name> FROM <enter table name>

-- TODO: <<ADD CODE HERE>> Count the number of airlines that fly within the USA in the `airlines` table
SELECT <FUNC(COLUMN)> AS <COL_NAME >
FROM <TABLE_REF>
WHERE <WHERE_CLAUSE>


-- TODO: <<ADD CODE HERE>> List the number of airlines per country in descending order
SELECT 
  country, 
  <FUNC(COLUMN)> AS <COL NAME> ,
FROM <TABLE> 
WHERE <WHERE CLAUSE>
GROUP BY <GROUPBY_CLAUSE>
ORDER BY <ORDERBY_CLAUSE>

-- TODO: <<ADD CODE HERE>> List the number of airlines per country in descending order, including only active airlines that have iata code
-- TODO: write this SQL on your own. Peek at answers if you need to

-- TODO: <<ADD CODE HERE>> Find the countries that have produced airlines which are no longer active
-- TODO: write this SQL on your own. Use a GROUP BY clause in conjunction with a HAVING condition. Peek at answers if you need to.
