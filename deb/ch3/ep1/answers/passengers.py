from deb.utils.config import config
from deb.utils.logging import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, col, sha2, concat_ws

# ===============================================================
# ‹‹=‹‹=‹‹=‹‹=‹‹=‹‹=‹‹=‹‹= RUN FUNCTIONS =››=››=››=››=››=››=››=››
# ===============================================================

def run():
    # Build session
    sparkql = SparkSession.builder.master('local').getOrCreate()

    # Load config informaiton if __name__ == '__main__':
    people_path = config['defaults']['ch3']['ep1']['passenger_input'].get(str)
    save_path = config['defaults']['ch3']['ep1']['passenger_output'].get(str)
    logger.info(f"Loading passenger info from {people_path}")

    # read csv file into spark dataframe
    passengers_df = sparkql.read.csv(people_path, header=True)
    logger.info(f"There are {passengers_df.count()} rows")

    # Load the passenger data and make sure the names have initial capitalization
    logger.info("Cleaning names and creating full name")
    passengers_df = passengers_df.withColumn('first_name', initcap(col('first_name')))\
                                 .withColumn('middle_name', initcap(col('middle_name')))\
                                 .withColumn('last_name', initcap(col('last_name')))

    # Create full_name column
    passengers_df = passengers_df.withColumn('full_name',
                                             concat_ws(" ",
                                                       col('first_name'),
                                                       col('middle_name'),
                                                       col('last_name')))

    logger.info("Creating sha2 uid from email")
    # Create a sha2 uid based on the email
    passengers_df = passengers_df.withColumn('uid', sha2(col('email'), 256))

    logger.info(f"Saving file to {save_path}")
    # Save dataframe as a parquet file
    passengers_df.write.parquet(save_path)

    
if __name__ == '__main__':
    run()
