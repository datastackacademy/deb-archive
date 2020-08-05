
from google.cloud.bigquery import SchemaField

from deb.utils.logging import logger
from deb.utils.answers.bq_utils import BigQueryUtils


def run():
    external_table_name ='deb.missing_routes'
    table_name = 'deb.routes'

    bq = BigQueryUtils()

    # create external table
    logger.info(f"creating external table: {external_table_name}")
    # todo: <<ADD YOUR CODE>>. Use the BigQueryUtils class above.

    logger.info(f"inserting missing routes...")
    # todo: <<ADD YOUR CODE>>. Use the BigQueryUtils class with a "INSERT INTO SELECT" statement

    # (optional) delete external table
    logger.info(f"deleting external table: {external_table_name}")
    bq.delete_table(external_table_name)


if __name__ == '__main__':
    run()
