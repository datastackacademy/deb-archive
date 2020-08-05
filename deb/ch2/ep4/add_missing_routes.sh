#!/bin/bash

# todo: <<ADD YOUR CODE>>. Use bq mkdef to create an external table definition file
# create external table definition file
echo "creating table definition file..."
bq help mkdef

# edit the table_def file to skip the header row
sed -i 's/\"skipLeadingRows\"\: 0$/"skipLeadingRows\"\: 1/g' table_def.json
cat table_def.json

# todo: <<ADD YOUR CODE>>. remove exiting external table (if any)
# remove previously created table
bq help rm

# todo: <<ADD YOUR CODE>>. create the external table using the definition file
# create external table
bq help mk

# todo: <<ADD YOUR CODE>>. Add your SQL query to delete previously inserted records into routes table (optional)
# (optional) delete previously inserted routes
echo "deleting previously inserted routes..."
echo "<<ADD YOUR QUERY>>" | bq query --use_legacy_sql=false

# todo: <<ADD YOUR CODE>>. Insert records from external to managed routes table
# insert from external table into routes table
echo "inserting missing routes..."
bq help query

# drop external table (optional)
# bq rm -f -t deb.missing_routes
