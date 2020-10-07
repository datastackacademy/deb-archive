
# system imports
import argparse
from datetime import datetime, date

# package imports
import pandas as pd
from google.cloud import bigquery

# local imports
from deb.utils.logging import logger
from deb.utils.config import config, pd_context_options


class EngineTypeFileProcessor(object):
    
    def __init__(self, source_file):
        # TODO :: copy/paste code from engine_type_processor.py
        raise NotImplementedError('COPY AND PASTER YOUR CODE HERE!')


class AircraftTypeFileProcessor(object):

    def __init__(self, source_file):
        # TODO :: copy/paste code from aircraft_type_processor.py
        raise NotImplementedError('COPY AND PASTER YOUR CODE HERE!')


class AircraftMasterFileProcessor(object):
    
    @staticmethod
    def parse_registrant_type(v):
        # decode registrant type based on mapping rules below
        mapper = {
            1: 'Individual',
            2: 'Partnership',
            3: 'Corporation',
            4: 'Co-Owned',
            5: 'Government',
            7: 'LLC',
            8: 'Non Citizen Corporation',
            9: 'Non Citizen Co-Owned',
        }
        # TODO :: 
        #   write the decoder using mapping above
        raise NotImplementedError('YOU FORGOT TO WRITE THIS!')
    
    @staticmethod
    def parse_zipcode(v):
        # TODO ::
        #   decode zipcode. shorten long zipcodes to US standard 5-digit zipcode
        raise NotImplementedError('YOU FORGOT TO WRITE THIS!')

    @staticmethod
    def parse_region(v):
        # decode region based on mapping rules below
        mapper = {
            '1': 'Eastern',
            '2': 'Southwestern',
            '3': 'Central',
            '4': 'Western-Pacific',
            '5': 'Alaskan',
            '7': 'Southern',
            '8': 'European',
            'C': 'Great Lakes',
            'E': 'New England',
            'S': 'Northwest Mountain',
        }
        # TODO :: 
        #   write the decoder using mapping above
        raise NotImplementedError('YOU FORGOT TO WRITE THIS!')

    @staticmethod
    def parse_date(v, fmt='%Y%m%d'):
        # TODO :: 
        #   parse dates like 2020-08-27 (year-month-day) return None otherwise
        raise NotImplementedError('YOU FORGOT TO WRITE THIS!')

    @staticmethod
    def parse_status(v):
        # TODO :: 
        #   map following status code to 'V' (or valid): 'M', 'R', 'T', 'V', 'Z'
        #   map everytihng else to 'N' (or not valid)
        raise NotImplementedError('YOU FORGOT TO WRITE THIS!')

    def __init__(self, source_file):
        super(AircraftMasterFileProcessor, self).__init__()
        self.source_file = source_file
        self.extract()
        self.transform()
    
    def extract(self):
        logger.info(f"loading master aircraft file: {self.source_file}")
        # column names to keep from the source file (other columns are not parsed)
        keep_columns = [
            'N-NUMBER',
            'SERIAL NUMBER',
            'MFR MDL CODE',
            'ENG MFR MDL',
            'YEAR MFR',
            'TYPE REGISTRANT',
            'NAME',
            'STREET',
            'STREET2',
            'CITY',
            'STATE',
            'ZIP CODE',
            'REGION',
            'COUNTRY',
            'LAST ACTION DATE',
            'CERT ISSUE DATE',
            'STATUS CODE',
            'AIR WORTH DATE',
            'EXPIRATION DATE',
        ]
        # specific field parsers (converters)
        # TODO :: 
        #   finish the converters below
        #   make sure to parse 'MFR MDL CODE' and 'ENG MFR MDL' as str
        #   parse 'YEAR MFR' as int and everything else should use their own parsers from above
        converters = {
            'MFR MDL CODE': NotImplementedError,
            'ENG MFR MDL': NotImplementedError,
            'YEAR MFR': NotImplementedError,
            'TYPE REGISTRANT': NotImplementedError,
            'ZIP CODE': NotImplementedError,
            'REGION': NotImplementedError,
            'LAST ACTION DATE': NotImplementedError,
            'CERT ISSUE DATE': NotImplementedError,
            'STATUS CODE': NotImplementedError,
            'AIR WORTH DATE': NotImplementedError,
            'EXPIRATION DATE': NotImplementedError,
        }
        # TODO ::
        #   read csv
        raise NotImplementedError('YOU FORGOT TO WRITE THIS!')
        # df = pd.read_csv(...)
        # self.df = df

    def rename_columns(self):
        # rename columns based on the list below and convert all column names to lower case 
        columns = {
            'TYPE REGISTRANT': 'REGISTRANT TYPE',
            'NAME': 'REGISTRANT NAME',
            'YEAR MFR': 'MFR YEAR',
            'CERT ISSUE DATE': 'ISSUE DATE',
            'STATUS CODE': 'STATUS',
            'AIR WORTH DATE': 'AIR READY DATE',
        }
        # TODO ::
        #   rename columns based on the mapping above
        #   lowercase all column names and replace '-' with '_' (making them bigquery friendly)
        raise NotImplementedError('YOU FORGOT TO WRITE THIS!')

    def transform(self):
        logger.debug(f"transforming master aircraft file")
        # TODO ::
        #   rename columns using rename_columns()
        #   set street2 data type to str
        #   set n_number as dataframe index
        raise NotImplementedError('YOU FORGOT TO WRITE THIS!')
    
    def lookup_aircraft_type(self, aircraft_type):
        assert isinstance(aircraft_type, AircraftTypeFileProcessor),  "invalid aircraft type object"
        df = self.df
        lookup = aircraft_type.df
        # narrow down the columns to be added from lookup
        lookup = lookup[['mfr_name', 'mfr_short_name', 'model', 'aircraft_type', 'num_engines', 'num_seats', 'weight_class', 'speed']]
        # join on mfr_mdl_code
        rdf = df.join(lookup, on='mfr_mdl_code', how='left')
        # set the df
        self.df = rdf

    def lookup_engine_type(self, engine_type):
        # TODO ::
        #   finish this function like lookup_aircraft_type to look up engine types (using EngineFileProcessor)
        #   columns to use from EngineFileProcessor: 'eng_mfr_name', 'eng_model', 'eng_type', 'horsepower', 'thrust'
        raise NotImplementedError('YOU FORGOT TO WRITE THIS!')

    def print(self, sample_size=100):
        df = self.df[['mfr_year', 'status', 'issue_date', 'mfr_name', 'model', 'aircraft_type', 'eng_type']]
        # print the dataframe to console
        with pd.option_context(*pd_context_options):    # force pandas to print all columns/rows
            if sample_size < 0:
                print(df)
            else:
                print(df.sample(n=sample_size))


def register_cmdline_args(parser:argparse.ArgumentParser):
    # add command line args
    parser.add_argument('command', choices=('etl', 'test-engine', 'test-aircraft', 'test-master', 'help'), help='what to do')
    parser.add_argument('-p', '--print', action='store_true', help='print to console')
    parser.add_argument('-n', '--row-count', type=int, default=100, 
                        help="number of sample rows to print")
    parser.add_argument('--engine-file', help='aircraft engine file',
                        default=config['defaults']['ch1']['ep5']['engine_file'].get())
    parser.add_argument('--aircraft-file', help='aircraft type file',
                        default=config['defaults']['ch1']['ep5']['aircraft_file'].get())
    parser.add_argument('--master-file', help='aircraft master file',
                        default=config['defaults']['ch1']['ep5']['master_file'].get())
    parser.add_argument('-o', '--output-file', help='output parquet file (for bigquery load)',
                        default=config['defaults']['ch1']['ep5']['output_file'].get())
    parser.add_argument('-t', '--output-table', help='bigquery aircraft output table',
                        default=config['defaults']['ch1']['ep5']['output_table'].get())


def run():
    logger.info("DATA ENGINEERING BOOTCAMP - CHAPTER 1 EPISODE 5")
    logger.info("FAA Aircraft Dataset ETL Process")
    # set command line args
    parser = argparse.ArgumentParser(description='FAA Aircraft Database ETL Process')
    register_cmdline_args(parser)
    # process command line input
    args = parser.parse_args()
    # execute command
    target = None
    if args.command == 'test-engine':
        # test processing engine file
        target = EngineTypeFileProcessor(source_file=args.engine_file)
    elif args.command == 'test-aircraft':
        # test processing aircraft file
        target = AircraftTypeFileProcessor(source_file=args.aircraft_file)
    elif args.command == 'test-master':
        # test processing master file
        engine = EngineTypeFileProcessor(source_file=args.engine_file)
        aircraft = AircraftTypeFileProcessor(source_file=args.aircraft_file)
        master = AircraftMasterFileProcessor(source_file=args.master_file)
        master.lookup_aircraft_type(aircraft)
        master.lookup_engine_type(engine)
        target = master
    elif args.command == 'etl':
        # extract, transform, and load (etl) all 3 files
        engine = EngineTypeFileProcessor(source_file=args.engine_file)
        aircraft = AircraftTypeFileProcessor(source_file=args.aircraft_file)
        master = AircraftMasterFileProcessor(source_file=args.master_file)
        master.lookup_aircraft_type(aircraft)
        master.lookup_engine_type(engine)
        # uncomment this line after adding bigquery load method
        # master.load(output_table=args.output_table, output_file=args.output_file)
        target = master
    elif args.command == 'help':
        parser.print_help()
    # print df
    if args.print and target is not None:
        target.print(sample_size=args.row_count)


if __name__ == "__main__":
    run()
