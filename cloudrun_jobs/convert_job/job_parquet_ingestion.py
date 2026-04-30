import os
from parquet_ingestion import convert_to_parquet

if __name__ == "__main__":
    date = os.getenv("DATE")
    hour = os.getenv("HOUR")

    convert_to_parquet(date, hour)