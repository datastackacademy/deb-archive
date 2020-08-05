"""
Chapter 2 - Episode 2: Data Profiling Using python pandas

TODO:: COPYRIGHT
"""

import pandas as pd
import argparse


def profile_flights_csv(flights_file: str = "/tmp/flights_2019_01.csv"):
    """
    Data Profiling code to analyze and profile flights CSV file.
    Chapter-2 Episode-2 of our Data Engineering Course

    :return:
    """
    # read the CSV as-is and let pandas detect data types dynamically for us
    df = pd.read_csv(flights_file, header=0)

    # ======= Columnar Type Analysis =========
    # list columns and investigate column data types
    df.columns
    df.dtypes

    # get null counts per column
    df.isnull().sum(axis=0)
    # sorted by values
    df.isnull().sum(axis=0).sort_values()

    # ======= Columnar Cardinality Statistical Analysis ========
    # unique value counts per column
    df.nunique()

    # data distribution per Airline
    df["Airline"].unique()
    df["Airline"].value_counts()

    # data distribution per FlightDate and sorted
    df["FlightDate"].value_counts().sort_index()


if __name__ == '__main__':
    # parse script arguments for -f (file option)
    parser = argparse.ArgumentParser(description="Python pandas data profiler for flights CSV.")
    parser.add_argument("-f", "--file", help="flights file to profile", required=False)
    args = parser.parse_args()
    file = args.file if args.file is not None else "/tmp/flights_2019_01.csv"

    # run data profiler
    print("profiling {}...".format(file))
    profile_flights_csv(flights_file=file)
