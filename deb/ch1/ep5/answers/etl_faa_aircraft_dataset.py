
# system imports
import argparse
from datetime import datetime, date
from time import sleep

# package imports
import pandas as pd
from google.cloud import bigquery

# local imports
from debdg.utils.logging import logger
from debdg.utils.config import config, pd_context_options


class AircraftPreProcessesor(object):

    def __init__(self):
        super(AircraftPreProcessesor, self).__init__()

    def load(self):
        logger.info("loading faa aircraft data files...")
        master_file = config['input']['files']['faa_master']
        aircraft_ref_file = config['input']['files']['faa_aircraft_ref']
        engine_ref_file = config['input']['files']['faa_engine_ref']
        logger.debug(f"loading {master_file}...")
        mdf = pd.read_csv(master_file, header=0, low_memory=False)
        logger.debug(f"loading {aircraft_ref_file}...")
        rdf = pd.read_csv(aircraft_ref_file, header=0)
        logger.debug(f"loading {engine_ref_file}...")
        edf = pd.read_csv(engine_ref_file, header=0)
        self.mdf = mdf
        self.rdf = rdf
        self.edf = edf
        logger.info("loaded all data files")

    @staticmethod
    def __erase_personal_info(row):
        # erase individual, partnership, co-owners
        if str(row['TYPE REGISTRANT']).strip() in ('1', '2', '4'):
            return ('', '', '')
        else:
            return (row['NAME'], row['STREET'], row['STREET2'])

    def transform(self):
        logger.info("aircaft transforms...")
        mdf = self.mdf
        rdf = self.rdf
        edf = self.edf
        # strip off whitespaces from fields
        logger.debug("stripping off whitespaces...")
        mdf = mdf.applymap(lambda v: str(v).strip())
        rdf = rdf.applymap(lambda v: str(v).strip())
        edf = edf.applymap(lambda v: str(v).strip())
        # remove personal information
        logger.debug("erasing personal information...")
        mdf['NAME'], mdf['STREET'], mdf['STREET2'] = zip(*mdf.apply(self.__erase_personal_info, axis=1))
        mdf['OTHER NAMES(1)'] = ''
        mdf['OTHER NAMES(2)'] = ''
        mdf['OTHER NAMES(3)'] = ''
        mdf['OTHER NAMES(4)'] = ''
        mdf['OTHER NAMES(5)'] = ''
        # dropping last columns
        logger.debug("dropping trailing column...")
        mdf.drop(columns=(list(mdf.columns)[-1]), inplace=True)
        rdf.drop(columns=(list(rdf.columns)[-1]), inplace=True)
        edf.drop(columns=(list(edf.columns)[-1]), inplace=True)
        # reset the dataframes
        self.mdf = mdf
        self.rdf = rdf
        self.edf = edf
        logger.info("transforms done")

    def to_csv(self):
        logger.info("writing to csv...")
        # set dataframes
        mdf = self.mdf
        rdf = self.rdf
        edf = self.edf
        # get output file names
        master_file = config['output']['files']['faa_master']
        aircraft_ref_file = config['output']['files']['faa_aircraft_ref']
        engine_ref_file = config['output']['files']['faa_engine_ref']
        # write output files
        logger.debug(f"writing {master_file}...")
        mdf.to_csv(master_file, index=False)
        logger.debug(f"writing {aircraft_ref_file}...")
        rdf.to_csv(aircraft_ref_file, index=False)
        logger.debug(f"writing {engine_ref_file}...")
        edf.to_csv(engine_ref_file, index=False)

    def etl(self):
        self.load()
        self.transform()
        self.to_csv()
    

class AircraftMaster(object):
    
    @staticmethod
    def parser_registrant_type(v):
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
    def parser_zipcode(v):
        v = str(v).strip()
        if v == '':
            return None
        elif len(v) > 5:
            return v[:5]
        else:
            return v

    @staticmethod
    def parser_region(v):
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
    def parser_date(v, fmt='%Y%m%d'):
        try:
            return datetime.strptime(str(v).strip(), fmt).date()
        except ValueError:
            return None

    @staticmethod
    def parser_status(v):
        valid_codes = ('M', 'R', 'T', 'V', 'Z')
        if str(v).strip() in valid_codes:
            return 'V'
        else:
            return 'N'

    def __init__(self):
        super(AircraftMaster, self).__init__()
        self.load()
        self.transform()
    
    def load(self, filepath:str=config['input']['files']['faa_master']):
        converters = {
            'MFR MDL CODE': (lambda v: str(v).strip()),
            'ENG MFR MDL': (lambda v: str(v).strip()),
            'YEAR MFR': (lambda v: int(v) if str(v).strip().isdigit() else -1),
            'TYPE REGISTRANT': self.parser_registrant_type,
            'ZIP CODE': self.parser_zipcode,
            'REGION': self.parser_region,
            'LAST ACTION DATE': self.parser_date,
            'CERT ISSUE DATE': self.parser_date,
            'STATUS CODE': self.parser_status,
            'AIR WORTH DATE': self.parser_date,
            'EXPIRATION DATE': self.parser_date,
        }
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
        logger.info(f"loading master aircraft file: {filepath}")
        df = pd.read_csv(filepath, 
                         header=0, 
                         usecols=keep_columns,
                         converters=converters,
                         low_memory=False)
        self.df = df

    def rename_columns(self, columns:dict=None, lowercase=True):
        columns = {
            'TYPE REGISTRANT': 'REGISTRANT TYPE',
            'NAME': 'REGISTRANT NAME',
            'YEAR MFR': 'MFR YEAR',
            'CERT ISSUE DATE': 'ISSUE DATE',
            'STATUS CODE': 'STATUS',
            'AIR WORTH DATE': 'AIR READY DATE',
        } if columns is None else columns
        df = self.df
        logger.info(f"renaming columns")
        # rename columns based on mapping rules above
        df.rename(columns=columns, inplace=True, errors='ignore')
        # lowercase columns names and replace special characters
        if lowercase:
            logger.debug("converting column names to lowercase")
            mapper = {col: str(col).strip().lower().replace(' ', '_').replace('-', '_') for col in list(df.columns)}
            df.rename(columns=mapper, inplace=True)

    def transform(self):
        logger.debug(f"transforming master aircraft")
        df = self.df
        # rename columns
        self.rename_columns()
        # fix data types
        df['street2'] = df['street2'].astype(str)
        # set index
        df.set_index(keys='n_number', inplace=True, drop=False)
        logger.info(f"transforms done")
    
    def lookup_aircraft_ref(self, aircraft_ref):
        assert isinstance(aircraft_ref, AircraftRef),  "invalid aircraft ref object"
        df = self.df
        lookup = aircraft_ref.df
        # narrow down the columns to be added from lookup
        lookup = lookup[['mfr_name', 'mfr_short_name', 'model', 'aircraft_type', 'num_engines', 'num_seats', 'weight_class', 'speed']]
        # join on mfr_mdl_code
        rdf = df.join(lookup, on='mfr_mdl_code', how='left')
        # set the df
        self.df = rdf

    def lookup_engine_ref(self, engine_ref):
        assert isinstance(engine_ref, AircraftEngineRef),  "invalid aircraft ref object"
        df = self.df
        lookup = engine_ref.df
        # narrow down the columns to be added from lookup
        lookup = lookup[['eng_mfr_name', 'eng_model', 'eng_type', 'horsepower', 'thrust']]
        # join on mfr_mdl_code
        rdf = df.join(lookup, on='eng_mfr_mdl', how='left')
        # set the df
        self.df = rdf

    def to_gbq(self):
        df = self.df
        # get bigquery table info from config
        project = config['google']['project']
        dataset = config['google']['bigquery']['dataset']
        table = config['google']['bigquery']['output_aircraft_table']
        logger.debug(f"writing airfract bigquery table: `{project}.{dataset}.{table}``")
        # write to bq
        df.to_gbq(
            destination_table=f"{dataset}.{table}",
            project_id=project,
            chunksize= 2000,
            if_exists='replace',
            progress_bar=False,
        )
        logger.debug('bigquery output done.')

    def to_parquet(self):
        df: pd.DataFrame = self.df
        # get output file name
        filepath = config['output']['files']['aircraft_parquet']
        # write parquet file
        logger.info(f"writing to parquet: {filepath}")
        df.to_parquet(filepath, engine='pyarrow', compression='gzip', index=False)
        logger.info(f"write completed")

    def create_gbq_table(self):
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
            bigquery.SchemaField('weight_class', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('speed', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('eng_mfr_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('eng_model', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('eng_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('horsepower', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('thrust', 'FLOAT', mode='NULLABLE'),
        ]
        # get bigquery table info
        project = config['google']['project']
        dataset = config['google']['bigquery']['dataset']
        table = config['google']['bigquery']['output_aircraft_table']
        table_id = f"{project}.{dataset}.{table}"
        # create a bigquery client
        client = bigquery.Client()
        # delete table if it exists
        logger.debug(f"dropping old table")
        client.delete_table(table_id, not_found_ok=True)
        # create a new table
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        # table created
        logger.info(f"bigquery table (`{table_id}`) created.")

    def load_parquet_file_bigquery(self):
        # get bigquery table info from config
        project = config['google']['project']
        dataset = config['google']['bigquery']['dataset']
        table = config['google']['bigquery']['output_aircraft_table']
        table_id = f"{project}.{dataset}.{table}"
        filepath = config['output']['files']['aircraft_parquet']
        logger.debug(f"writing airfract bigquery table: `{table_id}`")

        # Construct a BigQuery client object.
        client = bigquery.Client()
        # Construct a BigQuery client object.
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
        )
        with open(filepath, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)
        job.result()  # Waits for the job to complete.
        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    

class AircraftRef(object):

    @staticmethod
    def parser_aircraft_type(v):
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

    @staticmethod
    def parser_aircraft_category(v):
        mapper = {
            '1': 'Land',
            '2': 'Sea',
            '3': 'Amphibian',
        }
        try:
            return mapper[str(v).strip()]
        except (KeyError, ValueError):
            return 'Other'

    def __init__(self):
        super(AircraftRef, self).__init__()
        self.load()
        self.transform()

    def load(self, filepath:str=config['input']['files']['faa_aircraft_ref']):
        logger.info(f"loading aircraft ref file: {filepath}")
        converters = {
            'CODE': (lambda v: str(v).strip()),
            'TYPE-ACFT': self.parser_aircraft_type,
            'NO-ENG': (lambda v: int(v) if str(v).strip().isdigit() else -1),
            'NO-SEATS': (lambda v: int(v) if str(v).strip().isdigit() else -1),
            'AC-WEIGHT': (lambda v: int(v) if str(v).strip().isdigit() else -1),
            'SPEED': (lambda v: int(v) if str(v).strip().isdigit() else -1),
        }
        keep_columns = [
            'CODE', 'MFR', 'MODEL', 'TYPE-ACFT', 'NO-ENG', 'NO-SEATS', 'AC-WEIGHT', 'SPEED',
        ]
        df = pd.read_csv(filepath,
                         header=0,
                         usecols=keep_columns,
                         converters=converters,
                         low_memory=False)
        self.df = df
        logger.info(f"load done")      

    def rename_columns(self, columns:dict=None, lowercase=True):
        columns = {
            'CODE': 'MFR CODE',
            'MFR': 'MFR NAME',
            'TYPE-ACFT': 'AIRCRAFT TYPE',
            'NO-ENG': 'NUM ENGINES',
            'NO-SEATS': 'NUM SEATS',
            'AC-WEIGHT': 'WEIGHT CLASS',
        } if columns is None else columns
        logger.debug(f"renaming ref aircraft columns")
        df = self.df
        # rename columns based on mapping rules above
        df.rename(columns=columns, inplace=True, errors='ignore')
        # lowercase columns names and replace special characters
        if lowercase:
            logger.debug("converting column names to lowercase")
            mapper = {col: str(col).strip().lower().replace(' ', '_').replace('-', '_') for col in list(df.columns)}
            df.rename(columns=mapper, inplace=True)

    def transform(self):
        logger.info(f"applying ref aircraft transforms")
        df = self.df
        # rename columns
        self.rename_columns()
        # set index
        df.set_index(keys='mfr_code', inplace=True, drop=False)
        # add a short name column
        df['mfr_short_name'] = df['mfr_name'].map(lambda v: str(v).split()[0])
        logger.info(f"transform done")

    def get(self, mfr_code, default=None):
        try:
            df = self.df
            return df.loc[mfr_code].iloc[0]
        except (AttributeError, KeyError, ValueError):
            return default


class AircraftEngineRef(object):

    @staticmethod
    def parser_engine_type(v):
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
            return mapper[str(v).strip()]
        except (KeyError, ValueError):
            return 'Unknown'
    
    def __init__(self):
        super(AircraftEngineRef, self).__init__()
        self.load()
        self.transform()

    def load(self, filepath:str=config['input']['files']['faa_engine_ref']):
        converters = {
            'CODE': (lambda v: str(v).strip()),
            'TYPE': self.parser_engine_type,
            'HORSEPOWER': (lambda v: int(v) if str(v).strip().isdigit() else -1),
            'THRUST': (lambda v: int(v) if str(v).strip().isdigit() else -1),
        }
        keep_columns = [
            'CODE', 'MFR', 'MODEL', 'TYPE', 'HORSEPOWER', 'THRUST',
        ]
        logger.info(f"loading aircraft engine file: {filepath}")
        df = pd.read_csv(filepath,
                         header=0,
                         usecols=keep_columns,
                         converters=converters,
                         low_memory=False)
        self.df = df
        logger.info(f"load done")

    def rename_columns(self, columns:dict=None, lowercase=True):
        columns = {
            'CODE': 'ENG CODE',
            'MFR': 'ENG MFR NAME',
            'MODEL': 'ENG MODEL',
            'TYPE': 'ENG TYPE',
        } if columns is None else columns
        logger.debug(f"renaming aircraft engine columns")
        df = self.df
        # rename columns based on mapping rules above
        df.rename(columns=columns, inplace=True, errors='ignore')
        # lowercase columns names and replace special characters
        if lowercase:
            logger.debug("converting column names to lowercase")
            mapper = {col: str(col).strip().lower().replace(' ', '_').replace('-', '_') for col in list(df.columns)}
            df.rename(columns=mapper, inplace=True)

    def transform(self):
        logger.info(f"applying aircraft engine transforms")
        df = self.df
        # rename columns
        self.rename_columns()
        # set index
        df.set_index(keys='eng_code', inplace=True, drop=False)
        logger.info(f"transform done")

    def get(self, eng_code, default=None):
        try:
            df = self.df
            return df.loc[eng_code].iloc[0]
        except (AttributeError, KeyError, ValueError):
            return default  


def test():
    # log title
    logger.info('-' * 35)
    logger.info("Running FAA Arcraft tests")
    logger.info('-' * 35)

    ref = AircraftRef()
    engine = AircraftEngineRef()
    master = AircraftMaster()
    master.lookup_aircraft_ref(ref)
    master.lookup_engine_ref(engine)
    target = master
    # master.to_gbq()
    master.to_parquet()
    master.create_gbq_table()
    # let create table take effect
    sleep(2.0)
    # load parquet file
    master.load_parquet_file_bigquery()
    with pd.option_context(*pd_context_options):
        print(target.df.sample(n=30)[['n_number', 'mfr_name', 'eng_mfr_name', 'aircraft_type', 'eng_type', 'thrust']])
        print(list(target.df.columns))


def register_argparse(parser: argparse.ArgumentParser):
    parser.add_argument('command', choices=('pre-process', 'test'), help='what to do')
    parser.add_argument('-p', '--print', action='store_true', help='print to console')
    parser.add_argument('--no-count', action='store_true', help="don't print record count")
    parser.set_defaults(func=run)


def run(args):
    df = None
    if args.command == 'test':
        test()
    elif args.command == 'pre-process':
        pre = AircraftPreProcessesor()
        pre.etl()
        df = pre.mdf
    if args.print and df is not None:
        with pd.option_context(*pd_context_options):
            print(df)
            if not args.no_count:
                print(f"row count: {len(df.index)}")


if __name__ == "__main__":
    logger.warn(f"this script should not be executed by itself. continuing to running tests")
    test()

