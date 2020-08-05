#!/bin/bash

# create external table definition file
echo "creating table definition file..."
bq mkdef --noautodetect \
--ignore_unknown_values \
--source_format=CSV  "gs://deb.gcs.turalabs.com/beam/ch2ep2/output/rejects/missing-routes*.csv" \
"airline:string,src:string,dest:string" > table_def.json

# edit the table_def file to skip the header row
sed -i 's/\"skipLeadingRows\"\: 0$/"skipLeadingRows\"\: 1/g' table_def.json
cat table_def.json

# remove previously created table
bq rm -f -t deb.missing_routes

# create external table
bq mk --external_table_definition table_def.json -t deb.missing_routes

# (optional) delete previously inserted routes
echo "deleting previously inserted routes..."
echo "DELETE FROM deb.routes WHERE equipment = '-'" | bq query --use_legacy_sql=false

# insert from external table into routes table
echo "inserting missing routes..."
echo """INSERT INTO deb.routes
SELECT
  airline,
  src,
  dest,
  NULL as codeshare,
  1 as stops,
  '-' as equipment
FROM deb.missing_routes""" | bq query --use_legacy_sql=false

# drop external table
bq rm -f -t deb.missing_routes
