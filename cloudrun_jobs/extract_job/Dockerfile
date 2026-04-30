FROM python:3.11-slim

WORKDIR /app

COPY ./requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY ~/.config/gcloud ./.config/gcloud
COPY ./params ./params
COPY ./utilites ./utilities
COPY ./pipeline ./pipeline
COPY ./cloudrun_jobs/convert_job/job_extract_to_gcs.py .


CMD ["python3", "job_extract_to_gcs.py"]