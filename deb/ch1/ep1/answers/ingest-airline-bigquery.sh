#!/bin/bash

# 1. Inspecting the Airline Dataset

# get line count with wc.
echo "number of lines in airline data..."
wc -l ./data/deb-airlines.csv

# peek at the first few lines in _deb-airlines.csv_ with `head`
echo "first 5 lines of airline data..."
head -n 5 ./data/deb-airlines.csv


### 3. Uploading our Airline Data into GCS using the Cloud SDK

# todo: REPLACE your bucket name.
# Use the gsutil CLI to copy friles from your local machine to the newly created GCS bucket
echo "copy local files to GCS bucket..."
gsutil cp ./data/deb-airlines.csv gs://[BUCKET-NAME]
