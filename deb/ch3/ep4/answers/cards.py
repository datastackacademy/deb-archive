from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, sha2


# Make Dataproc SparkSession
sparkql = SparkSession.builder.master('yarn').getOrCreate()

# Load in both csv
bucket = 'YOUR BUCKET NAME'
sparkql.conf.set('temporaryGcsBucket', bucket)

bucket_path = 'gs://{}/'.format(bucket)

card_path = bucket_path + 'passengers_cards_1k.csv'
card_df = sparkql.read.csv(card_path, header=True)

# Create uid for each
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

card_df = card_df.join(passenger_df.select('email', 'passenger_uid'),
                       on='email',
                       how='left')

# Save to BQ
card_df.write.format('bigquery') \
  .option('table', '{}.{}'.format(bq_dataset, 'cards')) \
  .save()
