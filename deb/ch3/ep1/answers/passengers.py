from deb.utils.config import config
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
    passengers_df = sparkql.read.csv(people_path, header=True)

    # Load the passenger data and make sure the names have initial capitalization
    passengers_df = passengers_df.withColumn('first_name', initcap(col('first_name')))\
                                 .withColumn('middle_name', initcap(col('middle_name')))\
                                 .withColumn('last_name', initcap(col('last_name')))

    # Create full_name column
    passengers_df = passengers_df.withColumn('full_name',
                                             concat_ws(" ",
                                                       col('first_name'),
                                                       col('middle_name'),
                                                       col('last_name')))

    # Create a sha2 uid based on the email
    passengers_df = passengers_df.withColumn('uid',
                                             sha2(concat_ws("|", col('email')),
                                                  256))

    # Save dataframe as a parquet file
    passengers_df.write.parquet(save_path)

    
if __name__ == '__main__':
    run()
