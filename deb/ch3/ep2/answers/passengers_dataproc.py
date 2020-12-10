from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, concat_ws, col, sha2


# Build session
sparkql = SparkSession.builder.master('yarn').getOrCreate()

# Load passenger data
bucket = <your bucket>
sparkql.conf.set('temporaryGcsBucket', bucket) #this gives our job a temporary bucket to use when writint

bucket_path = 'gs://{}/'.format(bucket)
people_path = bucket_path + 'passengers_1k.csv'
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

bq_dataset = <your dataset>
bq_table = 'passengers'
passengers_df.write.format('bigquery') \
  .option('table', '{}.{}'.format(bq_dataset, bq_table)) \
  .save()