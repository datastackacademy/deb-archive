
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
            'MFR': (lambda v: str(v).strip()),
            'MODEL': (lambda v: str(v).strip()),
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

    def to_csv(self, output_file):
        logger.info(f"writing engines types to csv: {output_file}")
        df = self.df
        df.to_csv(output_file, index=False)


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
        try:
            return mapper[str(v).strip()]
        except (KeyError, ValueError):
            return 'Other'

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
        converters = {
            'CODE': (lambda v: str(v).strip()),
            'MFR': (lambda v: str(v).strip()),
            'MODEL': (lambda v: str(v).strip()),
            'TYPE-ACFT': self.parse_aircraft_type,
            'NO-ENG': (lambda v: int(v) if str(v).strip().isdigit() else -1),
            'NO-SEATS': (lambda v: int(v) if str(v).strip().isdigit() else -1),
            'AC-WEIGHT': (lambda v: str(v).strip()),
            'SPEED': (lambda v: int(v) if str(v).strip().isdigit() else -1),
        }
        # read csv, get column names from header row. parse only needed columns using converters
        df = pd.read_csv(self.source_file,
                         header=0,
                         usecols=keep_columns,
                         converters=converters,
                         low_memory=False)
        self.df = df

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
        df = self.df
        # rename columns based on mapping rules above
        df.rename(columns=columns, inplace=True, errors='ignore')
        # lowercase columns names and replace special characters
        mapper = {col: str(col).strip().lower().replace(' ', '_').replace('-', '_') for col in list(df.columns)}
        df.rename(columns=mapper, inplace=True)

    def transform(self):
        logger.info(f"applying aircraft type transforms")
        df = self.df
        # rename columns
        self.rename_columns()
        # set index
        df.set_index(keys='mfr_code', inplace=True)
        # add a short name column
        df['mfr_short_name'] = df['mfr_name'].map(lambda v: str(v).split()[0])
        logger.info(f"transform done")

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

    def to_csv(self, output_file):
        logger.info(f"writing aircrafts types to csv: {output_file}")
        df = self.df
        df.to_csv(output_file, index=False)


class AircraftMasterFileProcessor(object):
    
    @staticmethod
    def parse_n_number(v):
        # add an uppercase M to the beginning
        v = str(v).strip()
        v = 'N'  + v if v[0].isdigit else v
        return v

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
        try:
            return mapper[int(v)]
        except (ValueError, KeyError):
            return None
    
    @staticmethod
    def erase_personal_info(row):
        if row['registrant_type'] is None:
            return (row['registrant_name'], row['street'], row['street2'])
        elif row['registrant_type'] in ('Individual', 'Partnership', 'Co-Owned'):
            return (None, None, None)
        elif row['registrant_type'] == 'LLC':
            return (row['registrant_name'], None, None)
        else:
            return (row['registrant_name'], row['street'], row['street2'])

    @staticmethod
    def parse_zipcode(v):
        # decode zipcode. shorten long zipcodes to US standard 5-digit zipcode
        v = str(v).strip()
        if v == '':
            return None
        elif len(v) > 5:
            return v[:5]
        else:
            return v

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
        try:
            return mapper[str(v).strip()]
        except KeyError:
            return None

    @staticmethod
    def parse_date(v, fmt='%Y%m%d'):
        # parse date fields
        try:
            return datetime.strptime(str(v).strip(), fmt).date()
        except ValueError:
            return None

    @staticmethod
    def parse_status(v):
        # decode valid/invalid license based documentation (ardata.pdf)
        valid_codes = ('M', 'R', 'T', 'V', 'Z')
        if str(v).strip() in valid_codes:
            return 'V'
        else:
            return 'N'

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
        converters = {
            'N-NUMBER': self.parse_n_number,
            'SERIAL NUMBER': (lambda v: str(v).strip()),
            'MFR MDL CODE': (lambda v: str(v).strip()),
            'ENG MFR MDL': (lambda v: str(v).strip()),
            'YEAR MFR': (lambda v: int(v) if str(v).strip().isdigit() else -1),
            'TYPE REGISTRANT': self.parse_registrant_type,
            'NAME': (lambda v: str(v).strip()),
            'STREET': (lambda v: str(v).strip()),
            'STREET2': (lambda v: str(v).strip()),
            'CITY': (lambda v: str(v).strip()),
            'STATE': (lambda v: str(v).strip()),
            'ZIP CODE': self.parse_zipcode,
            'REGION': self.parse_region,
            'COUNTRY': (lambda v: str(v).strip()),
            'LAST ACTION DATE': self.parse_date,
            'CERT ISSUE DATE': self.parse_date,
            'STATUS CODE': self.parse_status,
            'AIR WORTH DATE': self.parse_date,
            'EXPIRATION DATE': self.parse_date,
        }
        # read csv
        df = pd.read_csv(self.source_file, 
                         header=0, 
                         usecols=keep_columns,
                         converters=converters,
                         low_memory=False)
        self.df = df

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
        df = self.df
        logger.info(f"renaming master file columns")
        # rename columns based on mapping rules above
        df.rename(columns=columns, inplace=True, errors='ignore')
        # lowercase columns names and replace special characters
        mapper = {col: str(col).strip().lower().replace(' ', '_').replace('-', '_') for col in list(df.columns)}
        df.rename(columns=mapper, inplace=True)

   

    def transform(self):
        logger.debug(f"transforming master aircraft file")
        df = self.df
        # rename columns
        self.rename_columns()
        # fix data types
        df['street2'] = df['street2'].astype(str)
        # remove personal info
        df['registrant_name'], df['street'], df['street2'] = zip(*df.apply(self.erase_personal_info, axis=1))
        # set index
        df.set_index(keys='n_number', inplace=True, drop=False)
    
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
        assert isinstance(engine_type, EngineTypeFileProcessor),  "invalid engine type object"
        df = self.df
        lookup = engine_type.df
        # narrow down the columns to be added from lookup
        lookup = lookup[['eng_mfr_name', 'eng_model', 'eng_type', 'horsepower', 'thrust']]
        # join on mfr_mdl_code
        rdf = df.join(lookup, on='eng_mfr_mdl', how='left')
        # set the df
        self.df = rdf

    def lookup_n_number(self, n_numbers):
        if isinstance(n_numbers, str):
            n_numbers = [n_numbers]
        elif isinstance(n_numbers, tuple) or isinstance(n_numbers, set):
            n_numbers = list(n_numbers)
        df = self.df
        for n in n_numbers:
            logger.info(f'looking up N Number: {n}')
            print(df.loc[n])

    def load(self, output_table, output_file):
        self.to_parquet(output_file)
        self.gbq_create(output_table)
        self.gbq_load(output_table, output_file)

    def to_parquet(self, output_file):
        logger.info(f"writing to parquet: {output_file}")
        df: pd.DataFrame = self.df
        # write parquet file
        df.to_parquet(output_file, engine='pyarrow', compression='gzip', index=False)

    def to_csv(self, output_file):
        logger.info(f"writing aircrafts to csv: {output_file}")
        df: pd.DataFrame = self.df
        df.to_csv(output_file, index=False)

    def gbq_create(self, table_name):
        schema = [
            bigquery.SchemaField('n_number', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('serial_number', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('mfr_mdl_code', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('eng_mfr_mdl', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('mfr_year', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('registrant_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('registrant_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('street', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('street2', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('city', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('state', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('zip_code', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('region', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('country', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('last_action_date', 'DATE', mode='NULLABLE'),
            bigquery.SchemaField('issue_date', 'DATE', mode='NULLABLE'),
            bigquery.SchemaField('status', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('air_ready_date', 'DATE', mode='NULLABLE'),
            bigquery.SchemaField('expiration_date', 'DATE', mode='NULLABLE'),
            bigquery.SchemaField('mfr_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('mfr_short_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('model', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('aircraft_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('num_engines', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('num_seats', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('weight_class', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('speed', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('eng_mfr_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('eng_model', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('eng_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('horsepower', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('thrust', 'FLOAT', mode='NULLABLE'),
        ]
        # create a bigquery client
        client = bigquery.Client()
        # delete table if it exists
        logger.debug(f"dropping {table_name} table if it exists")
        client.delete_table(table_name, not_found_ok=True)
        # create a new table
        table = bigquery.Table(table_name, schema=schema)
        table = client.create_table(table)
        # table created
        logger.info(f"bigquery table created: {table_name}")

    def gbq_load(self, table_name, data_file):
        logger.info(f"loading bigquery table: `{table_name}` from {data_file}")
        # Construct a BigQuery client object.
        client = bigquery.Client()
        # Construct a BigQuery client object.
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
        )
        with open(data_file, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_name, job_config=job_config)
        job.result()  # Waits for the job to complete.
        table = client.get_table(table_name)  # get loaded table info
        logger.info(f"loaded {table.num_rows} rows to {table_name}")

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
    parser.add_argument('-w', '--write-csv', action='store_true', help='write a csv output file. use -o for file name')
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
        master.lookup_n_number(['N794JB', 'N518AS', 'N292JB'])
        target = master
    elif args.command == 'etl':
        # extract, transform, and load (etl) all 3 files
        engine = EngineTypeFileProcessor(source_file=args.engine_file)
        aircraft = AircraftTypeFileProcessor(source_file=args.aircraft_file)
        master = AircraftMasterFileProcessor(source_file=args.master_file)
        master.lookup_aircraft_type(aircraft)
        master.lookup_engine_type(engine)
        master.load(output_table=args.output_table, output_file=args.output_file)
        target = master
    elif args.command == 'help':
        parser.print_help()
    # print df
    if args.print and target is not None:
        target.print(sample_size=args.row_count)
    if args.write_csv and target is not None:
        target.to_csv(args.output_file)


if __name__ == "__main__":
    run()
