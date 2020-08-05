"""
Ingest flight scheduled from REST API into Google BigQuery using Cloud Dataflow (Apache Beam).


Author: Par (turalabs.com)
Contact:

license: GPL v3 - PLEASE REFER TO DEB/LICENSE FILE
"""

import os
import sys
import typing
import argparse
from copy import copy
from datetime import datetime
from time import sleep, time
from pprint import pprint

import requests

from deb.utils.logging import logger
from deb.utils.config import config

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

    airlines = api_get_airlines(api_url=known_args.api_url, api_token=known_args.api_token)
    logger.info(f"list of airlines: {airlines}")
    flight_date = datetime(2020, 1, 1)
    for airline in airlines:
        flights = api_get_flights(airline, flight_date, api_url=known_args.api_url, api_token=known_args.api_token)
        logger.info(f"got {len(flights)} flight records for {airline} airline on {flight_date.strftime('%Y-%m-%d')}")
        logger.info("first 5 flights...")
        pprint(flights[:5])

    logger.info(f"process completed in {(time() - t0):,.3f} seconds")


if __name__ == '__main__':
    run_simple()

