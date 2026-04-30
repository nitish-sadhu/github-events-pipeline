FROM python:3.11-slim

WORKDIR /app

COPY ./requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY ./params ./params
COPY ./utilities ./utilities
COPY ./pipeline ./pipeline
COPY ./cloudrun_jobs/extract_job/job_extract_to_gcs.py .

CMD ["python3", "job_extract_to_gcs.py"]