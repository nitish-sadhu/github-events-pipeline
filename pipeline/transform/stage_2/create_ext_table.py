#!/Users/krishnasadhu/github-events-analytics/.venv/bin/python
from utilities.utilities import create_bigquery_client
from params.params import PROJECT, DATASET, PROCESSED_PARQUET_BUCKET
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_ext_table() -> None:
    client = create_bigquery_client()

    table_id = f"{PROJECT}.{DATASET}.raw_gh_events_ext"

    query = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS `{table_id}`
            OPTIONS (
                FORMAT = 'PARQUET',
                URIS = ['gs://{PROCESSED_PARQUET_BUCKET}/*.parquet']
            )
    """

    logger.info("___TABLE_CREATION_STARTS___")

    job = client.query(query, location="asia-south1")
    job.result()

    try:
        client.get_table(table_id)
        logger.info(f"___TABLE_CREATED___: {table_id}")

    except Exception as e:
        logger.error(f"___ERROR___: Table {table_id} does not exist. ERROR: {e}")

    return None

"""
if __name__ == "__main__":

    create_ext_table()
"""