from utilities import get_blob, get_parquet_writer, get_pa_schema, create_storage_client, extract_from_date, normalize_record, create_bigquery_client
from params import PROCESSED_PARQUET_BUCKET, RAW_JSON_BUCKET, PROJECT, DATASET

import gzip
import json
import logging
import pyarrow as pa



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def is_empty_record(record):
    """

    :param record: Record
    :return: Boolean, True - If record exists, False - If record does not exists
    """
    return all(field is None for field in record.values())

def create_ext_table() -> None:
    bq_client = create_bigquery_client()

    table_id = f"{PROJECT}.{DATASET}.raw_gh_events_ext"

    query = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS `{table_id}`
            WITH PARTITION COLUMNS (
                year INT64,
                month INT64,
                day INT64
            )
            OPTIONS (
                FORMAT = 'PARQUET',
                URIS = ['gs://{PROCESSED_PARQUET_BUCKET}/*.parquet'],
                hive_partition_uri_prefix='gs://sadpro-gea-events-parq-raw/',
                require_hive_partition_filter=true
            )
    """

    logger.info("___TABLE_CREATION_STARTS___")

    job = bq_client.query(query, location="asia-south1")
    job.result()

    try:
        bq_client.get_table(table_id)
        logger.info(f"___TABLE_CREATED___: {table_id}")

    except Exception as e:
        logger.error(f"___ERROR___: Table {table_id} does not exist. ERROR: {e}")

    return None


def convert_to_parquet(date, hour, batch_size = 5000):
    """

    :param client:  storage_client - Google Cloud Storage
    :param date:  Date for which data will be processed
    :param hour:  Hour for which data will be processed
    :param batch_size:  size threshold of the batch of records, once reached will be written to a pyarrow table
    :return: None
    """
    writer = None
    batch = []
    year, month, day = extract_from_date(date)
    src_blob_path = f"year={year}/month={month}/day={day}/{hour}.json.gz"
    tgt_blob_path = f"year={year}/month={month}/day={day}/{hour}.parquet"

    schema = get_pa_schema()

    client = create_storage_client()

    src_blob = get_blob(client, RAW_JSON_BUCKET, src_blob_path)
    tgt_blob = get_blob(client, PROCESSED_PARQUET_BUCKET, tgt_blob_path)

    try:
        with src_blob.open("rb") as f:

            logger.info("==============================================================")
            logger.info(f"___CONVERSION_STARTED___: year={year}/month={month}/day={day}/{hour}.json.gz")

            with gzip.open(f, "rt") as gz:
                with tgt_blob.open("wb") as tgt_gcs_file:

                    for line in gz:
                        record = json.loads(line)
                        record.pop("payload", None)

                        if record and not is_empty_record(record):
                            record = normalize_record(record)
                            batch.append(record)

                        if len(batch) >= batch_size:
                            table = pa.Table.from_pylist(batch, schema=schema)

                            if writer is None:
                                writer = get_parquet_writer(tgt_gcs_file, schema)

                            writer.write_table(table)
                            batch = []

                    logger.info("___LAST_BATCH___")

                    if batch:
                        table = pa.Table.from_pylist(batch, schema=schema)

                        if writer is None:
                            writer = get_parquet_writer(tgt_gcs_file, schema)

                        writer.write_table(table)

                    if writer:
                        writer.close()


        if not src_blob.exists():
            logger.info(f"___DOWNLOAD FAILED___: bucket: {PROCESSED_PARQUET_BUCKET} | file: /year={year}/month={month}/day={day}/{hour}.parquet")
            raise

        logger.info(f"___CONVERSION_COMPLETED___: year={year}/month={month}/day={day}/{hour}.parquet")

    except Exception as e:
        logger.error(f"___EXCEPTION___: {e}")
        if True:
            raise

    create_ext_table()


    return None

