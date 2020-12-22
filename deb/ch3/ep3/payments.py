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

# Create uid for each table.
# todo: create a uid using sha2 on the concatonation of the following columns:
# "street_address", "city", "state_code", "from_date", and "to_date"
addr_df = addr_df.withColumn('addr_uid', )


# todo: create a uid using sha2 on the concatonation of the following columns:
# "provider", "card_number", "expiration_date", and "security_code"
card_df = card_df.withColumn('card_uid',)

# Load in passenger data and join passenger uid on email
bq_dataset = 'YOUR DATASET NAME'
passenger_table_name = 'passengers'
passenger_df = # todo: read the passengers table from BigQuery

# todo: Use a join to add the "passenger_uid" column to addr_df and card_df

# Save to BQ
addr_df.write.format('bigquery') \
  .option('table', '{}.{}'.format(bq_dataset, 'addrs')) \
  .save()

card_df.write.format('bigquery') \
  .option('table', '{}.{}'.format(bq_dataset, 'cards')) \
  .save()
