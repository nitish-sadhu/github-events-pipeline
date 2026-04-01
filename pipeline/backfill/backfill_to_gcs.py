import logging

from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import importlib
from extract.extract_to_gcs import extract_to_gcs
import transform.stage_1.convert_to_parquet as ctp

importlib.reload(ctp)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

START_DATE = "2011-02-12"
END_DATE = "2011-02-28"


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
    logger.info(f"__________{task}___________")
    if True:
        #logger.info(f"__________{date}, {hour}___________")
        ctp.convert_to_parquet(date, hour)
    else:
        extract_to_gcs(date, hour)

    return None


if __name__ == "__main__":
    date_range = pd.date_range(START_DATE, END_DATE, freq="D")
    backfill(list(date_range))


