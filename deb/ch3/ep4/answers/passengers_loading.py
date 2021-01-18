from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, concat_ws, col, sha2
from deb.utils.logging import logger


class PassengerUtils:

    def __init__(self, bucket):
        logger.info(f"Starting SparkSession and using {bucket} as our bucket")
        self.sparkql = SparkSession.builder.master('yarn').getOrCreate()
        self.bucket = bucket
        self.sparkql.set('temporaryGcsBucket', bucket)


    def load_passengers(self, passenger_filename, passenger_output):
        self.passenger_filename = passenger_filename
        self.passenger_output = passenger_output
        people_path = 'gs://{}/{}'.format(self.bucket, passenger_filename)

        logger.info(f"Loading passenger info from {self.bucket}.{passenger_filename}")
        passengers_df = sparkql.read.csv(people_path, header=True)

        # Use withColumn and initcap to standardize the names
        passengers_df = passengers_df.withColumn('first_name',
                                                 initcap(col('first_name')))\
                                     .withColumn('middle_name',
                                                 initcap(col('middle_name')))\
                                     .withColumn('last_name',
                                                 initcap(col('last_name')))

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
          self.passengers_df = passengers_df

    def load_subtable(self, csv_filepath, uid_name, uid_col_list, csv_bq, passenger_bq=None):
        csv_path = 'gs://{}/{}'.format(self.bucket, csv_filepath)
        logger.info(f"Loading address info from {csv_path}")
        csv_df = sparkql.read.csv(csv_path, header=True)

        # Create uid for each
        csv_df = csv_df.withColumn(uid_name,
                                       sha2(concat_ws("",
                                                      *uid_col_list
                                                      ),
                                            256
                                            ))
        if passenger_bq:
            passengers_df = sparkql.read.format('bigquery') \
                                 .option('table', passenger_bq) \
                                 .load() \
                                 .withColumnRenamed('uid', 'passenger_uid')
        else:
            passengers_df = self.passenger_uid.withColumnRenamed('uid', 'passenger_uid')

        csv_df = csv_df.join(passenger_df.select('email', 'passenger_uid'),
                                 on='email',
                                 how='left')
        logger.info(f"writing card data to {csv_bq}")
        csv_df.write.format('bigquery') \
          .option('table', csv_bq) \
          .save()