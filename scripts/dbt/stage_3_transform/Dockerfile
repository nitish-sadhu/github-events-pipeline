FROM python:3.11-slim

RUN apt-get update && apt-get install -y git

RUN pip install dbt-bigquery

WORKDIR /app

COPY packages.yml dbt_project.yml ./

RUN dbt deps

COPY . .

ENV DBT_PROFILES_DIR=/app

CMD ["dbt", "build"]