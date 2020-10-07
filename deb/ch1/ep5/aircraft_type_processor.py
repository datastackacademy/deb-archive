
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

    @staticmethod
    def parse_engine_type(v):
        # decode engine type based on the mapping rules below
        mapper = {
            '0': 'None',
            '1': 'Reciprocating',
            '2': 'Turbo-prop',
            '3': 'Turbo-shaft',
            '4': 'Turbo-jet',
            '5': 'Turbo-fan',
            '6': 'Ramjet',
            '7': '2 Cycle',
            '8': '4 Cycle',
            '9': 'Unknown',
            '10': 'Electric',
            '11': 'Rotary',
        }
        try:
            return mapper[str(v).strip()]   # mapped value
        except (KeyError, ValueError):
            return 'Unknown'                # default value
    
    def __init__(self, source_file):
        super(EngineTypeFileProcessor, self).__init__()
        self.source_file = source_file
        self.extract()
        self.transform()

    def extract(self):
        # column names to keep from the source file (other columns are not parsed)
        keep_columns = [
            'CODE', 'MFR', 'MODEL', 'TYPE', 'HORSEPOWER', 'THRUST',
        ]
        # specific field parsers to apply data types and transformation rules 
        converters = {
            'CODE': (lambda v: str(v).strip()),
            'TYPE': self.parse_engine_type,
            'HORSEPOWER': (lambda v: int(v) if str(v).strip().isdigit() else -1),
            'THRUST': (lambda v: int(v) if str(v).strip().isdigit() else -1),
        }
        logger.info(f"loading aircraft engine file: {self.source_file}")
        # a) set thefirst row as header column names. 
        # b) only parse needed columns.
        # c) use specific field parser (converters)
        df = pd.read_csv(self.source_file,
                         header=0,
                         usecols=keep_columns,
                         converters=converters,
                         low_memory=False)
        self.df = df

    def rename_columns(self):
        # rename columns based on the list below and convert all column names to lower case 
        mapper = {
            # source file column name: new column name
            'CODE': 'ENG CODE',
            'MFR': 'ENG MFR NAME',
            'MODEL': 'ENG MODEL',
            'TYPE': 'ENG TYPE',
        }
        logger.debug(f"renaming aircraft engine file columns")
        df = self.df
        # rename columns based on mapping rules above
        df.rename(columns=mapper, inplace=True, errors='ignore')
        # lowercase columns names and replace special characters
        mapper = {col: str(col).strip().lower().replace(' ', '_').replace('-', '_') for col in list(df.columns)}
        df.rename(columns=mapper, inplace=True)

    def transform(self):
        logger.info(f"applying aircraft engine transforms")
        df = self.df
        # rename columns
        self.rename_columns()
        # set index
        df.set_index(keys='eng_code', inplace=True)

    def get(self, eng_code, default=None):
        # lookup by engine code
        try:
            df = self.df
            return df.loc[eng_code].iloc[0]
        except (AttributeError, KeyError, ValueError):
            return default

    def print(self, sample_size=100):
        # print the dataframe to console
        with pd.option_context(*pd_context_options):    # force pandas to print all columns/rows
            if sample_size < 0:
                print(self.df)
            else:
                print(self.df.sample(n=sample_size))


class AircraftTypeFileProcessor(object):

    @staticmethod
    def parse_aircraft_type(v):
        # decode aircraft type field based on mapping rules below
        mapper = {
            '1': 'Glider',
            '2': 'Balloon',
            '3': 'Blimp/Dirigible',
            '4': 'Fixed wing single engine',
            '5': 'Fixed wing multi engine',
            '6': 'Rotorcraft',
            '7': 'Weight-shift-control',
            '8': 'Powered Parachute',
            '9': 'Gyroplane',
            'H': 'Hybrid Lift',
            'O': 'Other',
        }
        # TODO :: 
        #   decoder function to use the mapping above
        #   use EngineFileProcessor as example
        raise NotImplementedError('YOU FORGOT TO WRITE THIS!')

    def __init__(self, source_file):
        super(AircraftTypeFileProcessor, self).__init__()
        self.source_file = source_file
        self.extract()
        self.transform()

    def extract(self):
        logger.info(f"loading aircraft type file: {self.source_file}")
        # column names to keep from the source file (other columns are not parsed)
        keep_columns = [
            'CODE', 'MFR', 'MODEL', 'TYPE-ACFT', 'NO-ENG', 'NO-SEATS', 'AC-WEIGHT', 'SPEED',
        ]
        # specific field parsers
        # TODO ::
        #   complete the code for converters below.
        #   lambda function to parse CODE and AC-WEIGHT as str
        #   lambda function to parse NO-ENG, NO-SEATS, and SPEED as int
        #   use parse_aircraft_type for TYPE-ACFT
        #   use EngineFileProcessor as example
        converters = {
            'CODE': NotImplementedError,
            'TYPE-ACFT': NotImplementedError,
            'NO-ENG': NotImplementedError,
            'NO-SEATS': NotImplementedError,
            'AC-WEIGHT': NotImplementedError,
            'SPEED': NotImplementedError,
        }
        # read csv, get column names from header row. parse only needed columns using converters
        # TODO ::
        #   read aircrat csv file
        #   set header row as column names
        #   use keep_columns and converters from above
        raise NotImplementedError('YOU FORGOT TO WRITE THIS!')
        # df = pd.read_csv(...)
        # self.df = df

    def rename_columns(self):
        # rename columns based on the list below and convert all column names to lower case 
        columns = {
            'CODE': 'MFR CODE',
            'MFR': 'MFR NAME',
            'TYPE-ACFT': 'AIRCRAFT TYPE',
            'NO-ENG': 'NUM ENGINES',
            'NO-SEATS': 'NUM SEATS',
            'AC-WEIGHT': 'WEIGHT CLASS',
        }
        logger.debug(f"renaming aircraft type columns")
        # TODO ::
        #   rename columns based on the mapping above
        #   lowercase all column names and replace '-' with '_' (making them bigquery friendly)
        #   use EngineFileProcessor as example
        raise NotImplementedError('YOU FORGOT TO WRITE THIS!')

    def transform(self):
        logger.info(f"applying aircraft type transforms")
        # TODO ::
        #   rename columns using rename_columns()
        #   set mfr_code as dataframe index column
        #   add a column named mfr_short_name to be the first word from mfr_name
        raise NotImplementedError('YOU FORGOT TO WRITE THIS!')

    def get(self, mfr_code, default=None):
        # lookup aircraft by mfr_code (manufacturer code)
        try:
            df = self.df
            return df.loc[mfr_code].iloc[0]
        except (AttributeError, KeyError, ValueError):
            return default

    def print(self, sample_size=100):
        # print the dataframe to console
        with pd.option_context(*pd_context_options):    # force pandas to print all columns/rows
            if sample_size < 0:
                print(self.df)
            else:
                print(self.df.sample(n=sample_size))


def register_cmdline_args(parser:argparse.ArgumentParser):
    # add command line args
    parser.add_argument('command', choices=('etl', 'test-engine', 'test-aircraft', 'help'), help='what to do')
    parser.add_argument('-p', '--print', action='store_true', help='print to console')
    parser.add_argument('-n', '--row-count', type=int, default=100, 
                        help="number of sample rows to print")
    parser.add_argument('--engine-file', help='aircraft engine file',
                        default=config['defaults']['ch1']['ep5']['engine_file'].get())
    parser.add_argument('--aircraft-file', help='aircraft type file',
                        default=config['defaults']['ch1']['ep5']['aircraft_file'].get())
    parser.add_argument('--master-file', help='aircraft master file',
                        default=config['defaults']['ch1']['ep5']['master_file'].get())


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
    elif args.command == 'help':
        parser.print_help()
    # print df
    if args.print and target is not None:
        target.print(sample_size=args.row_count)


if __name__ == "__main__":
    run()
