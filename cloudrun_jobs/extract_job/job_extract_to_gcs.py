import os
from pipeline.extract.extract_to_gcs import extract_to_gcs

if __name__ == "__main__":
    date = os.getenv("DATE")
    hour = os.getenv("HOUR")

    extract_to_gcs(date, hour)