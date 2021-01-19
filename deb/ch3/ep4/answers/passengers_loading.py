from time import time as now
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, concat_ws, col, sha2
from google.cloud import storage

from deb.utils.logging import logger
from deb.utils.config import config


class PassengerUtils:
    """
    Class of methods to load passenger data and associated tables into BigQuery
    """

    def __init__(self, bucket):
        """
        Initialized our util with default parameters
        :param bucket: str name of GCG bucket which will be used by this util
        to load data from and as temporary storage for Dataproc
        """
        logger.info(f"Starting SparkSession and using {bucket} as our bucket")
        self.sparkql = SparkSession.builder.master('yarn').getOrCreate()
        self.bucket = bucket
        self.sparkql.conf.set('temporaryGcsBucket', bucket)
        self.storage_client = storage.Client()
        self.datetime = f"{datetime.now():%Y%m%d%H%M%S}"

    def load_passengers(self, passenger_filename, passenger_output):
        """
        Function to load the passenger data from csv in GCS, clean, add UID,
        and upload to BigQuery
        :param passenger_filename: str input file name
        :param passenger_output: str of project.dataset.table to save passenger data
        """
        self.passenger_filename = passenger_filename
        self.passenger_output = passenger_output
        people_path = 'gs://{}/{}'.format(self.bucket, passenger_filename)

        logger.info(f"Loading passenger info from {self.bucket}.{passenger_filename}")
        passengers_df = self.sparkql.read.csv(people_path, header=True)

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
        """
        Function to load a supporting table to passengers from GCS and save in BigQuery.
        :param csv_filepath: str input filename
        :param uid_name: str name to give the UID column
        :param uid_col_list: list of str column names to combine into UID
        :param csv_bq: str output project.datset.table where the dat will be saved
        :param passenger_bq: str, optional. If passengers_df already has been loaded
        """
        csv_path = 'gs://{}/{}'.format(self.bucket, csv_filepath)
        logger.info(f"Loading address info from {csv_path}")
        csv_df = self.sparkql.read.csv(csv_path, header=True)

        csv_df = csv_df.withColumn(uid_name,
                                       sha2(concat_ws("",
                                                      *uid_col_list
                                                      ),
                                            256
                                            ))
        if passenger_bq:
            passengers_df = self.sparkql.read.format('bigquery') \
                                 .option('table', passenger_bq) \
                                 .load() \
                                 .withColumnRenamed('uid', 'passenger_uid')
        else:
            passengers_df = self.passengers_df.withColumnRenamed('uid', 'passenger_uid')

        csv_df = csv_df.join(passengers_df.select('email', 'passenger_uid'),
                                 on='email',
                                 how='left')
        logger.info(f"writing card data to {csv_bq}")
        csv_df.write.format('bigquery') \
          .option('table', csv_bq) \
          .save()

    def archive_csv(self, input_file):
        """
        Archive a csv in GCS based off date
        :param input_file: str path to file to be archived
        """
        source_bucket = self.storage_client.bucket(self.bucket)
        source_blob = source_bucket.blob(input_file)
        destination_blob_name = f"{self.datetime}/{input_file}"
        logger.info(f"Moving to {destination_blob_name}")
        blob_move = source_bucket.rename_blob(
            source_blob, destination_blob_name
        )




def main():
    """
    Load parameters from our config file and run the PassengerUtil with 
    these parameters
    """
    t0 = now()

    logger.info("Loading configuration")
    bucket = config['defaults']['ch3']['ep4']['input_bucket'].get(str)
    passenger_filename = config['defaults']['ch3']['ep4']['input_passengers'].get(
        str)
    passenger_output = config['defaults']['ch3']['ep4']['bq_passengers'].get(str)
    cards_filepath = config['defaults']['ch3']['ep4']['input_addrs'].get(str)
    cards_bq = config['defaults']['ch3']['ep4']['bq_cards'].get(str)
    bucket = config['defaults']['ch3']['ep4']['input_bucket'].get(str)
    addrs_filepath = config['defaults']['ch3']['ep4']['input_addrs'].get(str)
    addrs_bq = config['defaults']['ch3']['ep4']['bq_addrs'].get(str)

    loader = PassengerUtils(bucket)
    loader.load_passengers(passenger_filename, passenger_output)
    loader.archive_csv(passenger_filename)
    loader.load_subtable(cards_filepath, 'card_uid', ["street_address",
                                                      "city",
                                                      "state_code",
                                                      "from_date",
                                                      "to_date"], cards_bq)
    loader.archive_csv(cards_filepath)
    loader.load_subtable(addrs_filepath, 'addr_uid', ["street_address",
                                                      "city",
                                                      "state_code",
                                                      "from_date",
                                                      "to_date"], addrs_bq) 
    loader.archive_csv(addrs_filepath)
    logger.info(f"total time: {(now() - t0):,.6f} secs")



if __name__ == '__main__':
    main()
