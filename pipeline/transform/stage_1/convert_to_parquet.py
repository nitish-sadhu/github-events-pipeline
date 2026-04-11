from utilities.utilities import extract_from_date, get_pa_schema, normalize_record, create_bucket, get_blob, get_parquet_writer
from params.params import RAW_JSON_BUCKET, PROCESSED_PARQUET_BUCKET

import gzip
import json
import logging
import pyarrow as pa
import pyarrow.parquet as pq


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def is_empty_record(record):
    """

    :param record: Record
    :return: Boolean, True - If record exists, False - If record does not exists
    """
    return all(field is None for field in record.values())


def convert_to_parquet(client, date, hour, batch_size = 5000):
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


        if not src_blob.exists():
            logger.info(f"___DOWNLOAD FAILED___: bucket: {PROCESSED_PARQUET_BUCKET} | file: /year={year}/month={month}/day={day}/{hour}.parquet")
            raise

        logger.info(f"___CONVERSION_COMPLETED___: year={year}/month={month}/day={day}/{hour}.parquet")

    except Exception as e:
        logger.error(f"___EXCEPTION___: {e}")
        if True:
            raise

    if writer:
        writer.close()

    return None

