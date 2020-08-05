"""
Google BigQuery Utility class.

Author: Par (turalabs.com)
Contact:

license: GPL v3 - PLEASE REFER TO DEB/LICENSE FILE
"""

from time import time as now

import google.api_core.exceptions as google_exceptions
from google.cloud import bigquery
from google.cloud.bigquery.table import RowIterator
from google.cloud.bigquery import client, Table

from deb.utils.logging import logger


class BigQueryUtils:

    def __init__(self):
        self.client: client = bigquery.Client()

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
        """
        Create a BigQuery table.

        :param table_name: full table name including dataset name
        :param schema: table schema
        :param delete_if_exists: delete table if it exists
        :return: None
        """
        bq = self.client
        if delete_if_exists:
            self.delete_table(table_name)
        full_table_name = f"{bq.project}.{table_name}"
        table = bigquery.Table(full_table_name, schema=schema)
        logger.info(f"creating bigquery table: {full_table_name}")
        table = bq.create_table(table)
        logger.info(f"created bigquery table: {table.dataset_id}.{table.table_id} "
                    f"timestamp: {table.created}, location: {table.location}")
        return table

    def create_external_table(self,
                              table_name,
                              source_uris: list = [],
                              schema=None,
                              source_format: str = 'NEWLINE_DELIMITED_JSON',
                              delete_if_exists=True,
                              **kwargs):
        """
        Create a BigQuery external table using source_uris as data files.

        The default source format is 'NEWLINE_DELIMITED_JSON'. For list of all available table source formats, run:
        > bq help mkdef

        If schema is omitted then table is used with autodetect schema. This only works with source formats which
        contain their own schema such as json or avro files.

        :param table_name: full table name including dataset name
        :param source_uris: list of google storage URI data file paths (ie: gs://bucket_name/myfiles/*)
        :param schema: table schema. If not defined then schema autodetect is used
        :param source_format: default is 'NEWLINE_DELIMITED_JSON'. For the full list run > bq help mkdef
        :param delete_if_exists: delete table if already exists
        :return: None
        """
        bq = self.client

        # delete existing table
        if delete_if_exists:
            self.delete_table(table_name)

        try:
            # create external table
            full_table_name = f"{bq.project}.{table_name}"
            table = bigquery.Table(full_table_name)
            # external table configurations
            external_table_config = bigquery.ExternalConfig(source_format=source_format)
            if schema:
                external_table_config.schema = schema
            else:
                external_table_config.autodetect = True
            external_table_config.ignore_unknown_values = True
            # table_config.max_bad_records = 10
            for k, v in kwargs.items():
                if hasattr(external_table_config, k):
                    setattr(external_table_config, k, v)
                elif hasattr(external_table_config.options, k):
                    setattr(external_table_config.options, k, v)
            external_table_config.source_uris = source_uris
            table.external_data_configuration = external_table_config

            logger.info(f"creating bigquery external table: {table.dataset_id}.{table.table_id} \t "
                        f"format:'{external_table_config.source_format}' "
                        f"source_uris: {external_table_config.source_uris}")

            # create the external table
            table = bq.create_table(table)

            logger.info(f"created bigquery external table: {table.dataset_id}.{table.table_id} "
                        f"timestamp: {table.created}, location: {table.location}")
            return table
        except google_exceptions.BadRequest as err:
            logger.fatal(f"critical error while creating bigquery external table.")
            logger.fatal(str(err))
            raise err

    def execute(self,
                sql,
                query_params=None,
                **kwargs) -> RowIterator:
        """
        Execute a BigQuery query and return the results

        :param query_params: parameterized query params
        :param sql: sql command to execute
        :return: RowIterator
        """
        bq = self.client
        if query_params:
            config = bigquery.QueryJobConfig(allow_large_results=True, query_parameters=query_params, **kwargs)
        else:
            config = bigquery.QueryJobConfig(allow_large_results=True, **kwargs)
        t0 = now()
        logger.info(f"executing bigquery query: \"{sql}\"")
        # execute and get the results
        query_job = bq.query(sql, job_config=config)
        r = query_job.result()
        # log stats
        logger.info(f"executed bigquery query [rows: {r.total_rows}][sec: {(now() - t0):5.3f}]")
        return r

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
