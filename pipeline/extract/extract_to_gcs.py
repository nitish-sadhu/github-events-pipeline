from params.params import RAW_JSON_BUCKET
from utilities.utilities import create_storage_client, extract_from_date, create_bucket, get_args

import logging
import pandas as pd
import requests
from tqdm import tqdm



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_to_gcs(date, hour):
    logger.info("____EXTRACT_STARTED____")
    year, month, day = extract_from_date(date)

    storage_client = create_storage_client()
    logger.info(f"____{storage_client}____")

    if not storage_client.lookup_bucket(RAW_JSON_BUCKET):
        logger.info("____CREATING_BUCKET____")
        create_bucket(storage_client, RAW_JSON_BUCKET)

    bucket = storage_client.get_bucket(RAW_JSON_BUCKET)

    blob_path = f"year={year}/month={month}/day={day}/{hour}.json.gz"
    blob = bucket.blob(blob_path)

    url = f"https://data.gharchive.org/{date}-{hour}.json.gz"

    logger.info(f"___UPLOAD_STARTED___: {date}-{hour}.json.gz___")
    with requests.get(url, stream=True) as response:
        response.raise_for_status()

        with blob.open("wb") as f:
            total_size = int(response.headers.get("content-length", 0))
            with tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                desc="Downloading",
            ) as pbar:
                for chunk in response.iter_content(chunk_size = 1024*1024):
                    f.write(chunk)
                    pbar.update(len(chunk))

    if not blob.exists():
        logger.error(f"___DOWNLOAD FAILED___: bucket: {RAW_JSON_BUCKET} | file: /year={year}/month={month}/day={day}/{hour}.json.gz")
        raise

    logger.info(f"___UPLOAD_COMPLETE___: {date}-{hour}.json.gz___")

    return None

"""
if __name__ == "__main__":

    date, hour = get_args()
    logger.info(f"date: {date}, hour:{hour}")
    extract_to_gcs(date, hour)


    start_date = "2011-02-12"
    end_date = "2011-02-28"
    date_range = pd.date_range(start_date, end_date)

    for date in date_range:
        date = date.strftime("%Y-%m-%d")
        logger.info("==================================")
        logger.info(f"___DATE: {date}___:")
        for hour in range(24):
            logger.info("==================================")
            extract_to_gcs(date, hour)
            #logger.info("==================================")
    """
