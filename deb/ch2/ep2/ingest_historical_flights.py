"""
Ingest historical flight records form CSV files into Google BigQuery using Cloud Dataflow (Apache Beam).


Author: Par (turalabs.com)
Contact:

license: GPL v3 - PLEASE REFER TO DEB/LICENSE FILE
"""

import sys
import os
import csv
import argparse
from io import StringIO
from datetime import datetime, time
from time import time as now

import apache_beam as beam
from apache_beam.options.pipeline_options import SetupOptions, PipelineOptions

from deb.utils.logging import logger
from deb.utils.config import config
from deb.utils.answers.bq_utils import BigQueryUtils
from deb.utils.data_models.flights import (FLIGHTS_CSV_COLUMNS,
                                           datamodel_flights_parquet_schema,
                                           datamodel_flights_bigquery_schema)


# =================================================================
# ‹‹=‹‹=‹‹=‹‹=‹‹=‹‹=‹‹=‹‹= BEAM TRANSFORMS =››=››=››=››=››=››=››=››
# =================================================================


class BeamReadCSV(beam.DoFn):

    def __init__(self, header_cols=FLIGHTS_CSV_COLUMNS):
        self.cols = header_cols
        self.row_count = 0
        self.bad_rows = 0
        super(BeamReadCSV, self).__init__()

    def setup(self):
        self.row_count = 0
        self.bad_rows = 0

    def teardown(self):
        logger.info(f"total rows: {self.row_count}, bad row: {self.bad_rows}")

    def process(self, element, *args, **kwargs):
        # todo: finish writing this code. you can cheat and look at deb.ch2.ep3.answers
        raise NotImplementedError("Gotta finish this code!")


class BeamTransformRecords(beam.DoFn):

    def process(self, element: dict, *args, **kwargs):
        # todo: finish writing this code. you can cheat and look at deb.ch2.ep3.answers
        raise NotImplementedError("Gotta finish this code!")


class BeamLookupRoute(beam.DoFn):

    def process(self, element, routes, *args, **kwargs):
        # todo: finish writing this code. you can cheat and look at deb.ch2.ep3.answers
        raise NotImplementedError("Gotta finish this code!")

# ===============================================================
# ‹‹=‹‹=‹‹=‹‹=‹‹=‹‹=‹‹=‹‹= RUN FUNCTIONS =››=››=››=››=››=››=››=››
# ===============================================================


def runtime_args(args=sys.argv):
    """
    Parse command line arguments. Type --help to see list of args.

    :param args: sys.argv
    :return: (known args, beam pipeline args)
    """

    # set default values from config.yaml file
    # *** NOTE ***:
    #   if you get a confuse.exceptions.NotFoundError exception:
    #   add a default value under defaults.chXX.epYY in config.yaml
    defaults = config['defaults']['ch2']['ep3']     # default config path for this episode
    default_input = defaults['input'].as_str()
    default_output = defaults['output'].as_str()
    default_flights_ext_table = defaults['flights_ext_table'].as_str()
    default_flights_table = defaults['flights_table'].as_str()
    default_routes_table = defaults['routes_table'].as_str()

    p = argparse.ArgumentParser(description='Apache Beam (Google Dataflow) process to ingest historical flight records',
                                formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument('--input', default=default_input, required=False, type=str,
                   help=('Input path. Local or gcs URI such as: \n'
                         'gs://deb-airline-data/bots/csv/2018/*.csv'))
    p.add_argument('--output', default=default_output, required=False, type=str,
                   help=('Input path. Local or gcs URI such as: \n'
                         'gs://deb-airline-data-etl/beam/ch2cp3/'))
    p.add_argument('--flights-ext-table', default=default_flights_ext_table, required=False, type=str,
                   help=('BigQuery external table name for staging flights records as: \n'
                         'dataset_name.table.name'))
    p.add_argument('--flights-table', default=default_flights_table, required=False, type=str,
                   help=('BigQuery final flights output table name:\n'
                         'dataset_name.table.name'))
    p.add_argument('--routes-table', default=default_routes_table, required=False, type=str,
                   help=('BigQuery routes lookup table name:\n'
                         'dataset_name.table.name'))

    # parse args
    known_args, beam_args = p.parse_known_args(args)
    # push args to config, confuse adds args to the top level of yaml config
    # parameters would be accessible as config['param_name']
    config.set_args(known_args)

    # print arguments
    logger.info("arguments:")
    for k, v in known_args.__dict__.items():
        logger.info(f"\t{k}={v}")
    logger.info(f"beam args: {beam_args}")

    # return both known command line args and apache beam args
    return known_args, beam_args


def run():
    # parse command line options
    known_args, beam_args = runtime_args()
    # BigQuery utility
    bq_utils = BigQueryUtils()
    # setup apache beam args
    options = PipelineOptions(beam_args)
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:
        # todo: finish writing this code. you can cheat and look at deb.ch2.ep3.answers
        raise NotImplementedError("Gotta finish this code!")


if __name__ == '__main__':
    run()

