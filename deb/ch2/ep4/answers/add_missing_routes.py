
from google.cloud.bigquery import SchemaField
from deb.utils.logging import logger
from deb.utils.answers.bq_utils import BigQueryUtils


def run():
    external_table_name ='deb.missing_routes'
    table_name = 'deb.routes'

    bq = BigQueryUtils()

    # create external table
    logger.info(f"creating external table: {external_table_name}")
    schema = [SchemaField('airline', 'string'),
              SchemaField('src', 'string'),
              SchemaField('dest', 'string')
              ]
    bq.create_external_table(external_table_name,
                             source_uris='gs://deb.gcs.turalabs.com/beam/ch2ep2/output/rejects/missing-routes*.csv',
                             schema=schema,
                             source_format='CSV',
                             delete_if_exists=True,
                             skip_leading_rows=1)

    # delete previously inserted rows
    logger.info("deleting previously inserted missing routes...")
    sql = f"DELETE FROM {table_name} WHERE equipment = '-'"
    bq.execute(sql)

    logger.info(f"inserting missing routes...")
    sql = f"""INSERT INTO {table_name}
                SELECT
                  airline,
                  src,
                  dest,
                  NULL as codeshare,
                  1 as stops,
                  '-' as equipment
                FROM {external_table_name}
    """
    bq.execute(sql)

    # (optional) delete external table
    logger.info(f"deleting external table: {external_table_name}")
    bq.delete_table(external_table_name)


if __name__ == '__main__':
    run()
