#!/bin/bash

# 4. Upload the data to GCS
# todo: <<ADD YOUR CODE>>. Use wc (word count) command to count the number of lines in _deb-aircrafts.csv_.
echo "uploading airport data to GCS..."
gsutil cp "./data/deb-airports.parquet" gs://[BUCKET-NAME]
