FROM python:3.11-slim

WORKDIR /app

COPY ./requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY ./params ./params
COPY ./utilities ./utilities
COPY ./pipeline ./pipeline
COPY ./cloudrun_jobs/convert_job/job_convert_to_parquet.py .

CMD ["python3", "job_convert_to_parquet.py"]