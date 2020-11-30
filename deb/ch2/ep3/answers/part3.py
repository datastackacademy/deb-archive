"""
Ingest flight scheduled from REST API into Google BigQuery using Cloud Dataflow (Apache Beam).


Author: Par (turalabs.com)
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

from deb.utils.config import config
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

    :param api_url:
    :param api_token:
    :param tries:
    :param timeout:
    :param kwargs:
    :return:
    """
    global AIRLINES
    if not AIRLINES:
        headers = {'Authorization': f'Bearer {api_token}',
                   'Accept': 'application/json'}
        url = os.path.join(api_url, 'flights/airlines')
        logger.debug(f"Calling airlines REST API (url={url})")
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            r.raise_for_status()
            AIRLINES = r.json()['airlines']
            logger.info(f"flight airlines api call success in {r.elapsed.total_seconds():.3}s. Airlines: {AIRLINES}")
            return AIRLINES
        except requests.exceptions.ConnectTimeout:
            # REST API connection timeout is reached. Most likely we're sending too many requests and need to ease off
            # let's exponentially increase the timeout and retry until the max num of retires is reached
            logger.warning(f"REST API call to get airlines timed out. (timeout={timeout}")
            tries, timeout = tries + 1, timeout * 2
            if tries < api_max_retries:
                # sleep the current thread to back off from submitting too many requests
                logger.debug(f"thread sleep to back off from submitting too many REST API requests. sleep: {timeout}s")
                sleep(timeout)
                logger.warning(f"Increasing timeout to {timeout} and retrying. Retry number={tries}")
                return api_get_airlines(api_url, api_token, tries, timeout)
            else:
                logger.fatal("Max number of API retries is reached. Quiting.")
                sys.exit(1)
        except requests.ConnectionError:
            # if cannot establish connection with the REST API. Most likely the URL is incorrect or API Flask server
            # is not running
            logger.fatal(f"Could not establish connection to the REST API to get airlines (url={url})")
            sys.exit(1)
        except requests.exceptions.RequestException as err:
            logger.error("Unknown error while connecting to REST API to get airlines: {}".format(str(err)))
            return AIRLINES
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
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        # raise exception if response returned an unsuccessful status code
        r.raise_for_status()
        # return flight records
        data = r.json()
        logger.debug(f"Flight API returned {data['records']} flights in {r.elapsed.total_seconds():.3}s")
        return copy(data['flights'])
    except requests.exceptions.ConnectTimeout:
        # REST API connection timeout is reached. Most likely we're sending too many requests and need to ease off
        # let's exponentially increase the timeout and retry until the max num of retires is reached
        logger.warning(f"REST API call to get flights timed out. (timeout={timeout}")
        tries, timeout = tries + 1, timeout * 2
        if tries < api_max_retries:
            # sleep the current thread to back off from submitting too many requests
            logger.debug(f"thread sleep to back off from submitting too many REST API requests. sleep: {timeout}s")
            sleep(timeout)
            logger.warning(f"Increasing timeout to {timeout} and retrying. Retry number={tries}")
            return api_get_flights(airline, flight_date, api_url, api_token, tries, timeout)
        else:
            logger.fatal("Max number of API retries is reached. Quiting.")
            sys.exit(1)
    except requests.ConnectionError:
        # if cannot establish connection with the REST API. Most likely the URL is incorrect or API Flask server
        # is not running
        logger.fatal(f"Could not establish connection to the REST API to get flights (url={url})")
        sys.exit(1)
    except requests.exceptions.HTTPError as err:
        logger.error("Flights API has a critical error: {}".format(str(err)))
        logger.error("No records returned by flights api")
        return []
    except requests.exceptions.RequestException as err:
        logger.error("Unknown error while connecting to REST API to get flights: {}".format(str(err)))
        logger.error("No records returned by flights api")
        return []


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
    num_days = (end_date - start_date).days + 1        # add 1 to include end date
    return [start_date + timedelta(days=x) for x in range(num_days)]


# =================================================================
# ‹‹=‹‹=‹‹=‹‹=‹‹=‹‹=‹‹=‹‹= BEAM TRANSFORMS =››=››=››=››=››=››=››=››
# =================================================================


class BeamExpandDaysByAirlines(beam.DoFn):
    """
    Expands a date with all possible airline combinations. Takes in a date and will produce multiple
    output records with each airline and the date
    """

    def process(self, element, airlines=[], *args, **kwargs) -> typing.List[typing.Tuple]:
        return [(element, _) for _ in airlines]


class BeamGetFlights(beam.DoFn):
    """
    Call the Flights API and return scheduled flight records. Takes in a tuple of (airline, flight_date) and
    returns flight records from calling the api
    """

    def process(self, element: typing.Tuple, *args, **kwargs) -> typing.List[typing.Dict]:
        fligh_date, airline = element
        flights = api_get_flights(airline, fligh_date, api_url=kwargs['api_url'], api_token=kwargs['api_token'])
        logger.info(f"flights api call airline={airline}, flight_date={fligh_date} returned {len(flights)} flights")
        return flights


class BeamTransformFlights(beam.DoFn):
    """
    Transform flights records. Parse dates and times and add day_of_week field
    """

    def process(self, element: typing.Dict, *args, **kwargs) -> typing.List[typing.Dict]:
        try:
            # get date format and time format from optional function arguments
            date_fmt = kwargs["date_format"] if "date_format" in kwargs else '%Y-%m-%d'  # set date format to YYYY-MM-DD by default or take it from args
            time_fmt = kwargs["time_format"] if "time_format" in kwargs else '%H%M'  # set time format to HHMM by default or take it from args
            # parse flight dates and times and add day of the week
            flight_date = datetime.strptime(element['flight_date'], date_fmt).date()
            element['flight_date'] = flight_date.strftime('%Y-%m-%d')  # bigquery friendly formatted date
            element['departure_time'] = datetime.strptime(element['departure_time'], time_fmt).strftime('%H:%M:%S')  # bigquery friendly formatted time
            element['arrival_time'] = datetime.strptime(element['arrival_time'], time_fmt).strftime('%H:%M:%S')  # bigquery friendly formatted time
            element['day_of_week'] = flight_date.weekday() + 1      # add 1 to start Monday as 1, ending with Sunday as 7
            yield element
        except TypeError as err:
            logger.warning(f"input flight record is not a proper dict. omitting output")
        except (KeyError, ValueError) as err:
            logger.debug(f"intput flight record is missing critical fields. omitting output")
            logger.debug(str(err))


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

    # pass in the pipeline options
    options = PipelineOptions(beam_args)
    options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=options) as p:
        # pre-process: create a list of date to process and get other side-inputs
        # create a list of flights date to retrieve from api
        days = list_dates(start_date=known_args.start_date, end_date=known_args.end_date)

        # get airline iata codes from the api
        airlines = api_get_airlines(api_url=known_args.api_url, api_token=known_args.api_token)

        # create a beam collection with all days and airlines to get flights for
        input_rows = (p
                      | beam.Create(days)
                      | beam.ParDo(BeamExpandDaysByAirlines(), airlines=airlines)
                      )

        # call flights api to get flights for each record above and
        # call the beam transforms to process the input flights
        flights = (input_rows
                   | beam.ParDo(BeamGetFlights(), api_url=known_args.api_url, api_token=known_args.api_token)
                   | beam.ParDo(BeamTransformFlights())
                   )

        # prepare & write output files
        json_output = (flights
                       | beam.Map(lambda e: json.dumps(e))
                       | beam.io.WriteToText(os.path.join(known_args.output, 'flights'), file_name_suffix='.json')
                       )

    logger.info("apache beam pipeline done")
    logger.info(f"process completed in {(time() - t0):,.3f} seconds")


if __name__ == '__main__':
    run_simple()
