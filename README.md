# GitHub Events data pipeline

## Overview
This project is an end-to-end data pipeline to extract, transform, and load GitHub events data into BigQuery for analysis. The project extracts raw data which is in JSON format, transforms it into a structured format, and enables analytics through dimensional data modelling.

## Architecture
Ingestion (Python) -> Transformation -> Parquet -> BigQuery -> dbt -> Dashboard

## Tech Stack
- Python
- Google Cloud Storage (GCS)
- BigQUery
- dbt
- Shell Scripting
- Cron

## Data Flow
1. Extract the raw GitHub events data in JSON format.
2. Transform the JSON data using python.
3. Store the processed data as parquet files in GCS.
4. Create external tables in BigQuery on top of parquet data.
5. Transform data using dbt into fact and dimensional tables (star schema).
6. Make the data ready for visialisation by creating data marts using the fact and dimensional tables.
7. Visialize insights using a dashboard on looker studio.

## key features
- End-to-end data pipeline implementation.
- Use of columnar storage (parquet) for efficient querying
- External tables in BigQuery for scalable data access
- Dimensional data modelling using dbt
- Automated pipeline execution using shell scripts and cron.

## Architecture
![Architectrue Diagram](Architecture.png)

## How to Run
To run the pipeline use the following command.
        `zsh ./master.sh`
