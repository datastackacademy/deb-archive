from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, sha2
from deb.utils.config import config
from deb.utils.logging import logger


logger.info("Loading card data")
# Make Dataproc SparkSession
sparkql = SparkSession.builder.master('yarn').getOrCreate()

# Load in both csv
bucket = config['defaults']['ch3']['ep4']['input_bucket'].get(str)
cards_filepath = config['defaults']['ch3']['ep4']['input_addrs'].get(str)
passenger_bq = config['defaults']['ch3']['ep4']['bq_passengers'].get(str)
cards_bq = config['defaults']['ch3']['ep4']['bq_cards'].get(str)


sparkql.conf.set('temporaryGcsBucket', bucket)

cards_path = 'gs://{}/{}'.format(bucket, cards_filepath)
logger.info(f"Loading address info from {cards_path}")

cards_df = sparkql.read.csv(cards_path, header=True)

# Create uid for each
cards_df = cards_df.withColumn('cards_uid',
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

cards_df = cards_df.join(passenger_df.select('email', 'passenger_uid'),
                         on='email',
                         how='left')

# Save to BQ
logger.info(f"writing card data to {cards_bq}")
cards_df.write.format('bigquery') \
  .option('table', cards_bq) \
  .save()
