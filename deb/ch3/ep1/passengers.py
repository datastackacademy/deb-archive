import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, col, sha2, concat_ws


# Build session and read the csv into a pypsark dataframe

# Load the passenger data and make sure the names have initial capitalization

# Create full_name column

# Create a sha2 uid based on all the columns of the dataframe

# Save dataframe as a parquet file

