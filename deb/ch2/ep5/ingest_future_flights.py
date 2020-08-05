"""
Ingest flight scheduled from REST API into Google BigQuery using Cloud Dataflow (Apache Beam).


Author:
Contact:

license: GPL v3 - PLEASE REFER TO DEB/LICENSE FILE
"""

import argparse
import json
import os
import sys
import typing
from copy import copy
from datetime import datetime, timedelta
from time import sleep, time

import apache_beam as beam
import requests
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from google.cloud import bigquery

from deb.utils.answers.bq_utils import BigQueryUtils
from deb.utils.config import config
from deb.utils.data_models.flights import FUTURE_FLIGHTS_BIGQUERY_SCHEMA
from deb.utils.logging import logger

# module variables
api_url = 'http://localhost:5000/'          # todo:: change with app-engine url
api_token = '<YOUR OWN API TOKEN>'
api_timeout = 30.0
api_max_retries = 3

# module public cached values
AIRLINES = []


# ====================================================================
# ‹‹=‹‹=‹‹=‹‹=‹‹=‹‹=‹‹=‹‹= REST API FUNCTIONS =››=››=››=››=››=››=››=››
# ====================================================================


def api_get_airlines(api_url=api_url,
                     api_token=api_token,
                     tries=0,
                     timeout=api_timeout) -> typing.List[str]:
    """
    Call the REST API to get the list of available airlines

    :param api_url: API base URL
    :param api_token: OAuth Bearer token from turalabs.com/register
    :param tries: max number of API retries before failing
    :param timeout: timeout for API response
    :param kwargs:
    :return:
    """
    global AIRLINES
    if not AIRLINES:
        headers = {'Authorization': f'Bearer {api_token}',
                   'Accept': 'application/json'}
        url = os.path.join(api_url, 'flights/airlines')
        # todo: <<ADD YOUR CODE HERE>>
        # todo: use python requests module to make API calls
    else:
        return AIRLINES


def api_get_flights(airline: str,
                    flight_date: datetime,
                    api_url=api_url,
                    api_token=api_token,
                    tries=0,
                    timeout=api_timeout) -> typing.List[typing.Dict]:
    """
    Call REST API to get flight records for a given date

    :param airline: airline IATA code
    :param flight_date: flight date
    :param api_url: API base URL such as http://localhost:5000/
    :param api_token: OAuth Bearer token
    :param tries:
    :param timeout:
    :return: list of flight records as a dict
    """
    headers = {'Authorization': f'Bearer {api_token}',
               'Accept': 'application/json'}
    params = {'date': flight_date.strftime('%Y-%m-%d'),
              'airline': airline}
    url = os.path.join(api_url, 'flights/flights')
    # todo: <<ADD YOUR CODE HERE>>
    # todo: use python requests module to make API calls


# ==================================================================
# ‹‹=‹‹=‹‹=‹‹=‹‹=‹‹=‹‹=‹‹= HELPER FUNCTIONS =››=››=››=››=››=››=››=››
# ==================================================================


def list_dates(start_date: datetime, end_date: datetime) -> typing.List[datetime]:
    """
    Create a list of dates between two dates including both endpoint dates

    :param start_date: starting date
    :param end_date: ending date
    :return: Array. List of dates in between start_date and end_date
    """
    # todo: <<ADD YOUR CODE HERE>>
    # hint: you can use datetime.timedelta class to add days to a given date
    pass


# =================================================================
# ‹‹=‹‹=‹‹=‹‹=‹‹=‹‹=‹‹=‹‹= BEAM TRANSFORMS =››=››=››=››=››=››=››=››
# =================================================================


class BeamExpandDaysByAirlines(beam.DoFn):
    """
    Expands a date with all possible airline combinations. Takes in a date and will produce multiple
    output records with each airline and the date
    """

    def process(self, element, airlines=[], *args, **kwargs) -> typing.List[typing.Tuple]:
        # todo: <<ADD YOUR CODE HERE>>
        # hint: loop through airlines in global AIRLINES and
        # yield multiple rows per each input date
        pass


class BeamGetFlights(beam.DoFn):
    """
    Call the Flights API and return scheduled flight records. Takes in a tuple of (airline, flight_date) and
    returns flight records from calling the api
    """

    def process(self, element: typing.Tuple, *args, **kwargs) -> typing.List[typing.Dict]:
        # todo: <<ADD YOUR CODE HERE>>
        # hint: call api_get_flights and yield rows from the api
        pass


class BeamTransformFlights(beam.DoFn):
    """
    Transform flights records. Parse dates and times and add day_of_week field
    """

    def process(self, element: typing.Dict, *args, **kwargs) -> typing.List[typing.Dict]:
        # todo: <<ADD YOUR CODE HERE>>
        # todo: Apply the following transforms:
        #   - validate all date and time fields for correct format
        #   - add 1 to weekday (our weekday starts from monday being 1)
        #   - ensure flight_date is in YYYY-MM-DD format for bigquery
        #   - ensure departure and arrival times are in HH:MM:SS format for bigquery
        pass


class BeamLookupAirport(beam.DoFn):
    """
    Takes list of airports as a pandas dataframe airport and looks up src and dest airport city, state.
    It also produces a secondary output with list of airports which are missing in the look up dataframe.
    """

    def process(self, element, airports, *args, **kwargs):
        """
        Lookup src and dest IATA airport codes and add src/dest city/state if found

        Reject the record if either src/dest airport is not found in airports lookup table (as 'rejects' output PCollection).
        Reject missing src/dest airport IATA codes as 'missing_airports' output PCollection.

        :param element: flight dict record
        :param airports: airports dict lookup with airport iata code as keys
        :return: dict, flight record with src and dest airport cities added
        """
        # todo: <<ADD YOUR CODE HERE>>
        # todo: accept airports as a SideInput dict. Lookup both src and dest airports by their IATA
        # codes and add both src and dest city and state columns to the output
        pass


class BeamLookupRoute(beam.DoFn):
    """
    Takes list of airports as a pandas dataframe airport and looks up src and dest airport city, state.
    It also produces a secondary output with list of airports which are missing in the look up dataframe.
    """

    def process(self, element, routes, *args, **kwargs):
        """
        Lookup the flight route (airline, src, dest) in routes lookup table.

        Reject the flight if not found in routes (as 'rejects' output PCollection) AND output missing routes as
        'missing_routes' output PCollection.

        :param element: input flight record as dict
        :param routes: routes lookup table with ('airline', 'src', 'dest') tuple as keys
        :return: output flight dict
        """
        # todo: <<ADD YOUR CODE HERE>>
        # todo: accept routes as a SideInput dict (keyed by airline, src, dest). Look up the route against
        # the SideInput values and route missing routes into a secondary output using beam.pvalue.TaggedOutput
        pass


# ===============================================================
# ‹‹=‹‹=‹‹=‹‹=‹‹=‹‹=‹‹=‹‹= RUN FUNCTIONS =››=››=››=››=››=››=››=››
# ===============================================================

def runtime_args(args=sys.argv):
    """Parse runtime args"""

    def parse_date(date_str):
        try:
            return datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            raise argparse.ArgumentTypeError(f"Invalid date format ('{date_str}'). Date must be in YYYY-MM-DD format.")

    defaults = config['defaults']['ch2']['ep5']
    default_start_date = parse_date(defaults['start_date'].as_str())
    default_end_date = parse_date(defaults['end_date'].as_str())
    # api options
    global api_url, api_token, api_timeout, api_max_retries
    api_url = defaults['api_url'].as_str()
    api_token = defaults['api_token'].as_str()
    api_timeout = defaults['api_timeout'].get(float)
    api_max_retries = defaults['api_max_retries'].get(int)
    # cloud Options
    default_output = defaults['output'].as_str()
    default_flights_ext_table = defaults['flights_ext_table'].as_str()
    default_flights_table = defaults['flights_table'].as_str()
    default_airports_table = defaults['airports_table'].as_str()
    default_routes_table = defaults['routes_table'].as_str()

    p = argparse.ArgumentParser(description='Apache Beam (Google Dataflow) process to query flight schedule records via'
                                            'REST API and ingest into Google BigQuery.',
                                formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument('--start-date', default=default_start_date, required=False, type=parse_date,
                   help=('Starting date wto fetch flights records as: \n'
                         'YYYY-MM-DD ie: 2020-01-15'))
    p.add_argument('--end-date', default=default_end_date, required=False, type=parse_date,
                   help=('End date wto fetch flights records as: \n'
                         'YYYY-MM-DD ie: 2020-01-15'))
    p.add_argument('--output', default=default_output, required=False, type=str,
                   help=('Google Storage output bucket path for the external table. ie: \n'
                         'gs://bucket-name/beam/ch2ep5/\n\n'
                         'Be sure to include the trailing / in path.\n\n'
                         'Warning: bucket location must be the same BigQuery dataset location\n'
                         'Warning: content of this bucket is emptied upon execution\n'))
    p.add_argument('--flights-ext-table', default=default_flights_ext_table, required=False, type=str,
                   help=('BigQuery external table name for staging flights records as: \n'
                         'dataset_name.table.name'))
    p.add_argument('--flights-table', default=default_flights_table, required=False, type=str,
                   help=('BigQuery final flights output table name:\n'
                         'dataset_name.table.name'))
    p.add_argument('--airports-table', default=default_airports_table, required=False, type=str,
                   help=('BigQuery airports lookup table name:\n'
                         'dataset_name.table.name'))
    p.add_argument('--routes-table', default=default_routes_table, required=False, type=str,
                   help=('BigQuery routes lookup table name:\n'
                         'dataset_name.table.name'))
    p.add_argument('--api-url', default=api_url, required=False, type=str,
                   help=('Flight API full URL as:\n'
                         'http://localhost:5000/\n\n'
                         'Be sure to include the trailing /\n'))
    p.add_argument('--api-token', default=api_token, required=False,
                   help=('Flights API Bearer auth token'))
    # parse args
    known_args, beam_args = p.parse_known_args(args)
    # set command line args into config. confuse will load args at the top level
    config.set_args(known_args)

    # print args
    logger.info("printing args:")
    for k, v in known_args.__dict__.items():
        logger.info(f"\t{k}={v}")
    logger.info(f"pipeline args: {beam_args}")

    # return args
    return known_args, beam_args


def run_simple():
    t0 = time()

    # parse command line arguments
    known_args, beam_args = runtime_args()

    # BigQuery Utility
    bq_utils = BigQueryUtils()

    # pass in the pipeline options
    options = PipelineOptions(beam_args)
    options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=options) as p:
        # todo: <<ADD YOUR CODE HERE>>
        # todo: calling beam transforms to call rest API, transform records, and output them into files
        pass

    # todo: create an external table using the output files and insert records into BigQuery

    logger.info(f"process completed in {(time() - t0):,.3f} seconds")


def run_with_lookups():
    t0 = time()

    # parse command line arguments
    known_args, pipeline_args = runtime_args()

    # BigQuery Utility
    bq_utils = BigQueryUtils()

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:
        # todo: <<ADD YOUR CODE HERE>>
        # todo: calling beam transforms to call rest API, transform records, and output them into files
        # todo: record missing routes and airports into separate files
        pass

    # todo: create an external table using the output files and insert records into BigQuery

    logger.info(f"process completed in {(time() - t0):,.3f} seconds")


if __name__ == '__main__':
    run_simple()
    # run_with_lookups()
