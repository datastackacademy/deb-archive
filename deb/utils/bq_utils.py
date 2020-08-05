"""
Google BigQuery Utility class.

Author: YOU!
"""

from time import time as now

import google.api_core.exceptions as google_exceptions
from google.cloud import bigquery
from google.cloud.bigquery.table import RowIterator
from google.cloud.bigquery import client, Table

from deb.utils.logging import logger


class BigQueryUtils:

    def __init__(self):
        self.client: client = bigquery.client()

    def delete_table(self,
                     table_name):
        """
        Delete a table from BigQuery.

        :param table_name: full table name to delete including dataset id (ie: my_dataset.my_table)
        :return: None
        """
        bq = self.client
        try:
            table: Table = bq.get_table(table_name)
            logger.info(f"deleting existing bigquery table: {table.project}:{table.dataset_id}.{table.table_id}")
            bq.delete_table(table)
        except google_exceptions.NotFound:
            # table doesn't exist
            logger.debug(f"bigquery table to delete did not exist: {table_name}")
            pass

    def create_table(self,
                     table_name,
                     schema,
                     delete_if_exists=False):
        """Create a BigQuery table."""
        # todo: finish writing this code. you can cheat and look at deb.utils.answers.bq_utils.py
        raise NotImplementedError("You forgot to finish writing this!!!")

    def create_external_table(self,
                              table_name,
                              source_uris: list = [],
                              schema=None,
                              source_format: str = 'NEWLINE_DELIMITED_JSON',
                              delete_if_exists=True,
                              **kwargs):
        """Create a BigQuery external table using source_uris as data files."""
        # todo: finish writing this code. you can cheat and look at deb.utils.answers.bq_utils.py
        raise NotImplementedError("You forgot to finish writing this!!!")

    def execute(self,
                sql,
                query_params=None,
                **kwargs) -> RowIterator:
        """Execute a BigQuery query and return the results"""
        # todo: finish writing this code. you can cheat and look at deb.utils.answers.bq_utils.py
        raise NotImplementedError("You forgot to finish writing this!!!")

    def execute_as_dict(self,
                        sql: str,
                        keycols=None,
                        query_params=None,
                        **kwargs):
        """
        Execute a query and returns the results as a dict with keycols used as key values. If keycols is a list of multiple columns they the
        returning dict contains a tuple of their values. If keycols is None, a list is returned instead of a dict.

        Example: single keycol
            keycols = 'iata'  (airport IATA code)
            returns =  {'PDX': {'city': 'Portland', 'iata': 'PDX'}, {...}}

        Example: multiple keycols
            keycols = ['iata', 'city']
            returns = {('PDX', 'Portland'): {'city': 'Portland', 'iata': 'PDX'}, {...}}

        :param query_params:
        :param sql: sql command to execute
        :param keycols: columns to use as key values
        :return: dict
        """
        r = list(self.execute(sql, query_params, **kwargs))
        logger.info(f"converting {len(r)} rows as dict, keys: {keycols}")
        if keycols is None:
            return [dict(row) for row in r]
        elif isinstance(keycols, str):
            return {row[keycols]: dict(row) for row in r}
        elif isinstance(keycols, list) or isinstance(keycols, set) or isinstance(keycols, tuple):
            return {tuple(row[kk] for kk in keycols): dict(row) for row in r}
        else:
            raise ValueError("keycols must be None, str, list, set, or tuple.")
