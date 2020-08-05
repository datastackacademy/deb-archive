#!/bin/bash

# 1. Inspecting the Aircraft Dataset

# Use wc (word count) command to count the number of lines in _deb-aircrafts.csv_.
echo "number of lines in aircraft data..."
wc -l [PATH_TO_AIRLINE_DATA]

# Use head command to look at the first 5 lines in _deb-aircrafts.csv_.
echo "first 5 lines of aircraft data..."
head -n  5 [PATH_TO_AIRLINE_DATA]
