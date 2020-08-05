#!/bin/bash

# 1. Inspecting the Airline Dataset

# todo: <<ADD YOUR CODE>>. Use wc (word count) command to count the number of lines in _deb-airlines.csv_.
echo "number of lines in airline data..."
wc -OPTION FILE_NAME

# todo: <<ADD YOUR CODE>>. Use head command to look at the first 5 lines in _deb-airlines.csv_.
echo "first 5 lines of airline data..."
head -OPTION NUMBER_OF_LINES FILE_NAME


### 3. Uploading our Airline Data into GCS using the Cloud SDK

# todo: <<ADD YOUR CODE>>. Use the gsutil CLI to copy friles from your local machine to the newly created GCS bucket
echo "copy local files to GCS bucket..."
gsutil COMMAND LOCAL_PATH GCS_PATH