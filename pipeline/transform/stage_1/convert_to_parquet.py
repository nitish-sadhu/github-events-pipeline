from utilities.utilities import create_storage_client, extract_from_date, get_pa_schema, normalize_record, \
    create_bucket, get_args
from params.params import RAW_JSON_BUCKET, PROCESSED_PARQUET_BUCKET

import gzip
import json
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_blob(client, bucket, blob_path):

    if not client.lookup_bucket(bucket):
        create_bucket(client, bucket)

    bucket = client.get_bucket(bucket)
    blob = bucket.blob(blob_path)

    return blob

def is_empty_record(record):
    return all(field is None for field in record.values())

def convert_to_parquet(client, date, hour, batch_size = 5000):
    writer = None
    batch = []
    year, month, day = extract_from_date(date)
    src_blob_path = f"{year}/{month}/{day}/{hour}.json.gz"
    tgt_blob_path = f"{year}/{month}/{day}/{hour}.parquet"

    schema = get_pa_schema()

    src_blob = get_blob(client, RAW_JSON_BUCKET, src_blob_path)
    tgt_blob = get_blob(client, PROCESSED_PARQUET_BUCKET, tgt_blob_path)

    try:
        with src_blob.open("rb") as f:
            logger.info("==============================================================")
            logger.info(f"___CONVERSION_STARTED___: {year}/{month}/{day}/{hour}.json.gz")
            with gzip.open(f, "rt") as gz:
                for line in gz:
                    record = json.loads(line)
                    record.pop("payload", None)
                    if record and not is_empty_record(record):
                        record = normalize_record(record)
                        batch.append(record)

                    if len(batch) >= batch_size:
                        table = pa.Table.from_pylist(batch, schema=schema)

                        if writer is None:
                            gcs_file = tgt_blob.open("wb")
                            writer = pq.ParquetWriter(gcs_file, schema=schema, compression="snappy")

                        writer.write_table(table)
                        batch = []

                logger.info("___LAST_BATCH___")
                if batch:
                    table = pa.Table.from_pylist(batch, schema=schema)
                    if writer is None:
                        gcs_file = tgt_blob.open("wb")
                        writer = pq.ParquetWriter(gcs_file, schema=schema, compression="snappy")
                    writer.write_table(table)

        if not src_blob.exists():
            logger.info(f"___DOWNLOAD FAILED___: bucket: {PROCESSED_PARQUET_BUCKET} | file: /{year}/{month}/{day}/{hour}.parquet")
            raise

        logger.info(f"___CONVERSION_COMPLETED___: {year}/{month}/{day}/{hour}.parquet")

    except Exception as e:
        logger.error(f"___EXCEPTION___: {e}")
        if True:
            raise

    if writer:
        writer.close()

    if 'gcs_file' in locals() and gcs_file:
        gcs_file.close()

    return None

if __name__ == "__main__":

    date, hour = get_args()
    logger.info(f"date: {date}, hour: {hour}")

    client = create_storage_client()

    convert_to_parquet(client, date, hour)

    """
    date_range = pd.date_range("2011-02-12", "2011-02-28", freq="D")

    for date in date_range:
        formatted_date = str(date)
        for hour in range(24):
            convert_to_parquet(client, formatted_date, hour)
    """
