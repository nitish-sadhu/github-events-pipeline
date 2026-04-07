import logging

from utilities.utilities import create_storage_client
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import importlib
from pipeline.clean_up.clean_up_gcs import delete_blob
from pipeline.extract.extract_to_gcs import extract_to_gcs
import pipeline.transform.stage_1.convert_to_parquet as ctp

importlib.reload(ctp)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

START_DATE = "2015-03-25"
END_DATE = "2015-12-31"


def backfill(date_range) -> None:
    tasks = []

    for date in date_range:
        formatted_date = pd.Timestamp(date).strftime("%Y-%m-%d")
        for hour in range(24):
            tasks.append((formatted_date, hour))

    try:

        logger.info(f"BACKFILL from {START_DATE} to {END_DATE} STARTED")

        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(worker, tasks)

    except Exception as e:
        logger.error(f"__ERROR__: {e}")
        raise
    logger.info("_________RUN_COMPLETE_________")
    return None


def worker(task: list) -> None:
    date, hour = task
    if False:
        #logger.info(f"__________{date}, {hour}___________")
        client = create_storage_client()
        ctp.convert_to_parquet(client, date, hour)
    elif True:
        extract_to_gcs(date, hour)
    elif False:
        client = create_storage_client()
        delete_blob(client, date, hour)

    return None


if __name__ == "__main__":
    date_range = pd.date_range(START_DATE, END_DATE, freq="D")
    backfill(list(date_range))


