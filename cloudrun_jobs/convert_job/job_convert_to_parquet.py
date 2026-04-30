import os
from pipeline.transform.stage_1.convert_to_parquet import convert_to_parquet

if __name__ == "__main__":
    date = os.getenv("DATE")
    hour = os.getenv("HOUR")

    convert_to_parquet(date, hour)