from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, sha2


# Make Dataproc SparkSession
sparkql = SparkSession.builder.master('yarn').getOrCreate()

# Load in both csv
bucket = 'YOUR BUCKET NAME'
sparkql.conf.set('temporaryGcsBucket', bucket)

bucket_path = 'gs://{}/'.format(bucket)
addr_path = bucket_path + 'passengers_addrs_1k.csv'
addr_df = sparkql.read.csv(addr_path, header=True)

card_path = bucket_path + 'passengers_cards_1k.csv'
card_df = sparkql.read.csv(card_path, header=True)

# Create uid for each
addr_df = addr_df.withColumn('addr_uid',
                             sha2(concat_ws("",
                                            col("street_address"),
                                            col("city"),
                                            col("state_code"),
                                            col("from_date"),
                                            col("to_date")
                                            ),
                                  256
                                  ))

card_df = card_df.withColumn('card_uid',
                             sha2(concat_ws("",
                                            col("provider"),
                                            col("card_number"),
                                            col("expiration_date"),
                                            col("security_code")
                                            ),
                                  256
                                  ))

# Load in passenger data and join passenger uid on email
bq_dataset = 'YOUR DATASET NAME'
passenger_table_name = 'passengers'
passenger_df = sparkql.read.format('bigquery') \
    .option('table', '{}.{}'.format(bq_dataset, passenger_table_name)) \
    .load() \
    .withColumnRenamed('uid', 'passenger_uid')

addr_df = addr_df.join(passenger_df.select('email', 'passenger_uid'),
                       on='email',
                       how='left')

card_df = card_df.join(passenger_df.select('email', 'passenger_uid'),
                       on='email',
                       how='left')

# Save to BQ
addr_df.write.format('bigquery') \
  .option('table', '{}.{}'.format(bq_dataset, 'addrs')) \
  .save()

card_df.write.format('bigquery') \
  .option('table', '{}.{}'.format(bq_dataset, 'cards')) \
  .save()
