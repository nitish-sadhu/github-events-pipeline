from google.cloud import storage
from utilities.utilities import create_storage_client, get_args, extract_from_date
from params.params import RAW_JSON_BUCKET, PROCESSED_PARQUET_BUCKET

import logging
import argparse

from utilities.utilities import get_args

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def delete_blob(client, date, hour):

    year, month, day = extract_from_date(date)

    json_blob_path = f"{year}/{month}/{day}/{hour}.json.gz"
    parq_blob_path = f"{year}/{month}/{day}/{hour}.parquet"

    json_bucket = client.get_bucket(RAW_JSON_BUCKET)
    parq_bucket = client.get_bucket(PROCESSED_PARQUET_BUCKET)

    json_blob = json_bucket.blob(json_blob_path)
    parq_blob = parq_bucket.blob(parq_blob_path)

    if json_blob.exists() and parq_blob.exists():
        json_blob.delete()
        if not json_blob.exists():
            logger.info(f"___DELETED___: {json_blob} has been deleted.")

    elif json_blob.exists() and not parq_blob.exists():
        logger.warning(f"___WARNING___: {parq_blob} does not exist. create {parq_blob} from {json_blob} to delete {json_blob}")

    elif not json_blob.exists():
        logger.warning(f"___WARNING___: {json_blob} does not exist.")

    return None

if __name__ == "__main__":

    date, hour = get_args()
    #print("start")
    client = create_storage_client()

    delete_blob(client, date, hour)
