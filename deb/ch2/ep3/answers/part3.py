
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
        """
        Parse a records CSV line and transpose column headers

        :param element: csv line
        :return: dict {column_name: csv_value}
        """
        # use csv.reader to correctly parse csv. accounting for quoted values
        reader = csv.reader(StringIO(element), delimiter=',')
        for row in reader:
            if len(row) == len(self.cols):
                # transpose the schema onto the csv cols to create a
                # {column_name: csv_value} dict. read about python zip function: https://www.programiz.com/python-programming/methods/built-in/zip
                d = dict(zip(self.cols, row))
                self.row_count += 1
                yield d
            else:
                self.bad_rows += 1
                logger.debug(f"bad row: {row}")


class BeamTransformRecords(beam.DoFn):

    def process(self, element: dict, *args, **kwargs):
        try:
            date_fmt = kwargs['date_fmt'] if 'date_fmt' in kwargs else '%Y-%m-%d'
            time_fmt = kwargs['time_fmt'] if 'time_fmt' in kwargs else '%H%M'

            # convert midnight time: '2400' to '0000'. causes `datetime.strptime()` to throw ValueError exception
            time_cols = ['departure_time', 'actual_departure_time', 'wheels_off', 'wheels_on', 'arrival_time', 'actual_arrival_time']
            for col in time_cols:
                if element[col] == '2400':
                    element[col] = '0000'

            # parse records
            # Pay attention how we use python inline `<expression_if_true> if <condition> else <expression_if_false>` to assert default values
            # for some columns. Parsing columns which are mandatory do not have defaults and will result in an exception
            element['flight_date'] = datetime.strptime(element['flight_date'], date_fmt).date()
            element['day_of_week'] = int(element['day_of_week']) if element['day_of_week'] else element['flight_date'].weekday() + 1
            # remove state from src_city
            element['src_city'] = str(element['src_city']).split(',')[0]
            element['departure_time'] = datetime.strptime(element['departure_time'], time_fmt).time()
            element['actual_departure_time'] = datetime.strptime(element['actual_departure_time'], time_fmt).time() if element['actual_departure_time'] else None
            element['departure_delay'] = float(element['departure_delay']) if element['departure_delay'] else 0.0
            element['taxi_out'] = float(element['taxi_out']) if element['taxi_out'] else None
            element['wheels_off'] = datetime.strptime(element['wheels_off'], time_fmt).time() if element['wheels_off'] else None
            element['wheels_on'] = datetime.strptime(element['wheels_on'], time_fmt).time() if element['wheels_on'] else None
            element['taxi_in'] = float(element['taxi_in']) if element['taxi_in'] else None
            element['arrival_time'] = datetime.strptime(element['arrival_time'], time_fmt).time()
            element['actual_arrival_time'] = datetime.strptime(element['actual_arrival_time'], time_fmt).time() if element['actual_arrival_time'] else None
            element['arrival_delay'] = float(element['arrival_delay']) if element['arrival_delay'] else 0.0
            element['cancelled'] = str(element['cancelled']).strip() == '1'
            element['cancellation_code'] = element['cancellation_code'] if element['cancellation_code'] else None
            element['flight_time'] = float(element['flight_time']) if element['flight_time'] else None
            element['actual_flight_time'] = float(element['actual_flight_time']) if element['actual_flight_time'] else None
            element['air_time'] = float(element['air_time']) if element['air_time'] else None
            element['flights'] = int(element['flights']) if element['flights'] else 1
            element['distance'] = float(element['distance'])
            element['airline_delay'] = float(element['airline_delay']) if element['airline_delay'] else 0.0
            element['weather_delay'] = float(element['weather_delay']) if element['weather_delay'] else 0.0
            element['nas_delay'] = float(element['nas_delay']) if element['nas_delay'] else 0.0
            element['security_delay'] = float(element['security_delay']) if element['security_delay'] else 0.0
            element['late_aircraft_delay'] = float(element['late_aircraft_delay']) if element['late_aircraft_delay'] else 0.0

            # transform time field to HH:MM:SS format. BigQuery friendly
            for k in time_cols:
                if element[k] is not None and isinstance(element[k], time):
                    element[k] = element[k].strftime('%H:%M:%S')

            yield element
        except (KeyError, ValueError, TypeError) as err:
            # logger.debug(f"invalid record: {element}")
            yield beam.pvalue.TaggedOutput('rejects', element)


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
    t0 = now()

    # parse command line options
    known_args, beam_args = runtime_args()

    # BigQuery utility
    bq_utils = BigQueryUtils()

    options = PipelineOptions(beam_args)
    options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=options) as p:
        rows = (p
                | beam.io.ReadFromText(known_args.input, skip_header_lines=1)
                | beam.ParDo(BeamReadCSV(header_cols=FLIGHTS_CSV_COLUMNS))
                | beam.ParDo(BeamTransformRecords(), date_fmt='%Y-%m-%d', time_fmt='%H%M')
                )

        # write parquet output files
        output = (rows
                  | beam.io.WriteToParquet(os.path.join(known_args.output, 'flights'),
                                           schema=datamodel_flights_parquet_schema(), file_name_suffix='.parquet')
                  )

    logger.info("beam pipiline completed.")

    # create bigquery external table and insert into bq flights table
    bq_utils.create_external_table(known_args.flights_ext_table,
                                   source_uris=os.path.join(known_args.output, "flights*.parquet"),
                                   source_format='PARQUET',
                                   delete_if_exists=True)
    # create and replace existing bigquery flights table
    bq_utils.create_table(known_args.flights_table, schema=datamodel_flights_bigquery_schema(), delete_if_exists=True)

    # insert into table as select (itas) statement
    sql = f"""
        INSERT INTO `{known_args.flights_table}`
        SELECT
          a.day_of_week,
          a.flight_date,
          a.airline,
          a.tailnumber,
          a.flight_number,
          a.src,
          a.src_city,
          a.src_state,
          a.dest,
          a.dest_city,
          a.dest_state,
          PARSE_TIME('%H:%M:%S', a.departure_time) as departure_time,
          PARSE_TIME('%H:%M:%S', a.actual_departure_time) as actual_departure_time,
          a.departure_delay,
          a.taxi_out,
          PARSE_TIME('%H:%M:%S', a.wheels_off) as wheels_off,
          PARSE_TIME('%H:%M:%S', a.wheels_on) as wheels_on,
          a.taxi_in,
          PARSE_TIME('%H:%M:%S', a.arrival_time) as arrival_time,
          PARSE_TIME('%H:%M:%S', a.actual_arrival_time) as actual_arrival_time,
          a.arrival_delay,
          a.cancelled,
          a.cancellation_code,
          a.flight_time,
          a.actual_flight_time,
          a.air_time,
          a.flights,
          a.distance,
          a.airline_delay,
          a.weather_delay,
          a.nas_delay,
          a.security_delay,
          a.late_aircraft_delay,
          -- CONCAT(a.flight_date, '_', a.airline, '_', a.flight_number) AS flightDate_airline_flightNumber
        FROM
          `{known_args.flights_ext_table}`  a
        """
    # insert records form parquet external table into final bq managed flights table
    r = bq_utils.execute(sql)

    logger.info(f"total time: {(now() - t0):,.6f} secs")


if __name__ == '__main__':
    run()

