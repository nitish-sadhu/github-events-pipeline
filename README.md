# GitHub Events data pipeline

## Overview
This project implements an end-to-end ELT data pipeline that processes GitHub Events data and transform it into analytics-ready datasets using modern data engineering tools.

Raw JSON data is ingested using Python, optimised into Parquet (columnar format) for efficient storage, and loaded into BigQuery. Transformations are performed using dbt to build a dimensional data model (fact and dimension tables) fro downstream analytics and reporting. 

The pipeline is designed with scalability and cost-efficieny in mind, leveraging partitioning and cloud-native processing.

## Architecture
Ingestion (Python) -> Storage (GCS - Parquet) -> Data Warehouse (BigQuery - External Tables) -> Transformation (dbt - staging, fact, dimension, marts) -> BI Layer (Looker Studio).

Orchestration: Apache Airflow (in Docker)

The pipeline is orchestrated using Apache Airflow running in Docker containers, enabling scheduling, dependency management, and backfilling of workflows.

<b>Architecture Diagram:</b>
![Architectrue Diagram](Architecture.png)

## Tech Stack
- Programming: Python
- Cloud Platform: Google Cloud Platform
- Storage: Google Cloud Storage (GCS - Parquet)
- Data Warehouse: BigQuery
- Transformation: dbt
- Orchestration: Apache Airflow (Dockerised)
- Containerisation: Docker
- Data Modelling: Star schema (Fact & Dimension Tables)

## Data Flow
1. <b>Data Ingestion:</b>
- Extract GitHub Events data in JSON format using python scripts.

2. <b>Data Transformation (Stage 1 - File Optimisation):</b>
- Convert raw JSON data into Parquet format to improve storage efficiency and query performance.
- Store processed files in GCS using <b>Hive-style partitioning</b>(eg: year=YYYY/month=MM/day=DD).
- Enables efficient partition pruning and reduces query cost in BigQuery.

3. <b>Data Warehousing:</b>
- Created external tables in BigQuery on top of Parquet files stored in GCS.
- Enable querying of large datasets without data duplication.

4. <b>Data Transformation:</b>
- Implemented <b>dbt</b> to transform raw data into structured datasets:
  - Staging layer (cleaned raw data)
  - Fact table (events)
  - Dimension tables (users, repositories, etc.,)

5. <b>Data modelling:</b>
- Implement star schema to support efficient analytical queries and reporting.

6. <b>Orchestration:</b>
- Implemented Apache Airflow in docker containers to orchestrate the pipeline:
  - Schedule workflows
  - Manage task dependencies
  - Support backfilling of historical data
 
7. <b>Data Visualisation:</b>
- Build dashboards in <b>Looker Studio</b> using curated mart tables. 

## key features
- Designed and implemented end-to-end <b>ELT data pipeline</b> for processing large-scale GitHub events data
- Optimised data storage and query performance using <b>Parquet (columnar format).</b>
- Implemented Hive-style partitioning in GCS to enable efficient querying and reduce data scan costs.
- Leveraged BigQUery external tables for cost-efficient querying without data duplication.
- Build modular data transformations using <b>dbt</b>, including staging, fact, and dimension layers.
- Implemented dimensional data modelling (star schema) to support scalable analytics.
- Orchestrated workflows using <b>Apache Airflow</b> with support of scheduling, dependency management, and backfilling
- Applied <b>partitioning strategies</b> to reduce query cost and improve performance.
- Integrated a data retention mechanism into Airflow workflows, automatically deleting raw JSON files older than 30 days to control storage costs and maintain data hygiene.

## How to Run
1. <b>Prerequisites</b>
- Python installed
- Google Cloud account with BigQuery and GCS access
- gcloud CLI installed and authenticated
- dbt installed and configured
- Docker installed (for Airflow)

2. <b>Authentication</b>
- Run the following command to authenticate gcloud CLI.

        `gcloud auth application-default login`

3. <b>Run the pipeline</b>
- Start the containers with airflow.

          `docker compose up -d`

- Access Airflow UI at:

          http://localhost:8080

<b>Notes:</b>
- Ensure GCS bucket and BigQuery dataset are created before running.
- Configure profiles.yml for dbt with BigQuery credentials.

## Challenges Faced & Learnings
1. <b>High BigQuery costs due to full table scans</b>
- Initially, the pipeline scanned large valumes of data(~300GB per run), leading to excessive costs.
- This was caused by missing partitioning in the storage layer.
- <b>Resolution</b>: Resolved by implementing <b>Hive-style partitioning in GCS</b>, enabling partition pruning and significantly reducing query cost.
- <b>Learning</b>: Highlighted the importance of data partitioning strategies for cost optimisation in large-scale data processing.

2. <b>Airflow compatability issues on macOS</b>
- Faced issues running Airflow locally due to environment and version mismatches.
- <b>Resolution</b>: Implemented Airflow using docker containers ensuring consistent execution environment and simplifying setup.
- <b>Learning</b>: Reinforced the value of containerisation for environment consistency and reproducibility in data engineering workflows.

3. <b>Slow data processing due to disk I/O bottlenecks</b>
- Writing JSON data to disk before converting to Parquet introduced performance bottlenecks.
- <b>Resolution</b>: Optimised by processing data in-memory, reducing I/O overhead and improving pipeline performance.
- <b>Learning</b>: Demonstrated the impact of I/O operations on performance and the benefits of im-memory processing for high-throughput pipelines.

4. <b>Inefficient full-refresh transformations in dbt models</b>
- Initially implemented dimension tables using full-refresh materialisation, assuming they would remain small.
- As data volume increased, this led to unnecessary recomputation and higher query costs.
- <b>Resolution</b>: Optimised by converting dimension tables to incremental modesl, ensuring only new or updated data is processed improving efficiency and reducing cost.
- <b>Learning</b>: This highlighted the importance of choosing the right materialisation strategy based on data growth patterns.






          
