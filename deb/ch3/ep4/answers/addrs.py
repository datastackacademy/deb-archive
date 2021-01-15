from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, sha2
from deb.utils.config import config
from deb.utils.logging import logger


logger.info("Loading address data")
# Make Dataproc SparkSession
sparkql = SparkSession.builder.master('yarn').getOrCreate()

# Load in both csv
bucket = config['defaults']['ch3']['ep4']['input_bucket'].get(str)
addrs_filepath = config['defaults']['ch3']['ep4']['input_addrs'].get(str)
passenger_bq = config['defaults']['ch3']['ep4']['bq_passengers'].get(str)
addrs_bq = config['defaults']['ch3']['ep4']['bq_addrs'].get(str)


sparkql.conf.set('temporaryGcsBucket', bucket)

addrs_path = 'gs://{}/{}'.format(bucket,addrs_filepath)
logger.info(f"Loading address info from {addrs_path}")

addrs_df = sparkql.read.csv(addrs_path, header=True)

# Create uid for each
addrs_df = addrs_df.withColumn('addrs_uid',
                             sha2(concat_ws("",
                                            col("street_address"),
                                            col("city"),
                                            col("state_code"),
                                            col("from_date"),
                                            col("to_date")
                                            ),
                                  256
                                  ))

# Load in passenger data and join passenger uid on email
logger.info(f"Loading passger data from {passenger_bq}")
passenger_df = sparkql.read.format('bigquery') \
    .option('table', passenger_bq) \
    .load() \
    .withColumnRenamed('uid', 'passenger_uid')

addrs_df = addrs_df.join(passenger_df.select('email', 'passenger_uid'),
                       on='email',
                       how='left')

# Save to BQ
logger.info(f"writing address data to {addrs_bq}")
addrs_df.write.format('bigquery') \
  .option('table', addrs_bq) \
  .save()
