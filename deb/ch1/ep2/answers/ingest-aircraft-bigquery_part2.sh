#!/bin/bash

# 2. Upload Data to Google Cloud Storage

# Use gsutil to copy the aircraft CSV file from your local machine to the GCS Bucket 
echo "number of lines in aircraft data..."
gsutil cp ./data/deb-aircraft.csv gs://[BUCKET-NAME]

