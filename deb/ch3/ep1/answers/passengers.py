import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, col, sha2, concat_ws


# Build session and read the csv into a pypsark dataframe
sparkql = SparkSession.builder.master('local').getOrCreate()
people_path = sys.argv[1]
save_path = sys.argv[2]
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

# Create a sha2 uid based on all the columns of the dataframe
passengers_df = passengers_df.withColumn('uid',
                                         sha2(concat_ws("|", *passengers_df.columns),
                                              256))

# Save dataframe as a parquet file
passengers_df.write.parquet(save_path)

