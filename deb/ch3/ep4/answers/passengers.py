from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, concat_ws, col, sha2
from deb.utils.config import config
from deb.utils.logging import logger

logger.info("Starting Passenger data proccessing")
# Build session
sparkql = SparkSession.builder.master('yarn').getOrCreate()

# Load variables from config
bucket = config['defaults']['ch3']['ep4']['input_bucket'].get(str)
passenger_filename = config['defaults']['ch3']['ep4']['input_passengers'].get(str)
passenger_output = config['defaults']['ch3']['ep4']['bq_passengers'].get(str)
logger.info(f"Loading passenger info from {bucket}.{passenger_filename}")

# Load passenger data
sparkql.conf.set('temporaryGcsBucket', bucket) #this gives our job a temporary bucket to use when writint

people_path = 'gs://{}/{}'.format(bucket, passenger_filename)
passengers_df = sparkql.read.csv(people_path, header=True)

# Use withColumn and initcap to standardize the names
passengers_df = passengers_df.withColumn('first_name', initcap(col('first_name')))\
                             .withColumn('middle_name', initcap(col('middle_name')))\
                             .withColumn('last_name', initcap(col('last_name')))

# Create full_name column
passengers_df = passengers_df.withColumn('full_name',
                                         concat_ws(" ",
                                                   col('first_name'),
                                                   col('middle_name'),
                                                   col('last_name')))
passengers_df = passengers_df.withColumn('uid', sha2(col('email'), 256))

# Write to BigQuery
logger.info(f"Writing file to {passenger_output}")
passengers_df.write.format('bigquery') \
  .option('table', passenger_output) \
  .save()
