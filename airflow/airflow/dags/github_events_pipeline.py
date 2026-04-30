from airflow import DAG
from airflow.operators.python import PythonOperator
#from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
import sys

sys.path.append("/opt/airflow")

#from scripts.extract.extract_to_gcs import extract_to_gcs
#from scripts.transform.stage_1.convert_to_parquet import convert_to_parquet
#from scripts.transform.stage_2.create_ext_table import create_ext_table
from scripts.clean_up_gcs import delete_blob

from scripts.utilities import create_storage_client


from datetime import datetime, timedelta


def get_runtime_params(**kwargs) -> tuple[str, int]:
    today = kwargs["execution_date"]
    hour = kwargs["execution_date"].hour
    datetime_stamp = today - timedelta(days=2)
    date = datetime_stamp.strftime("%Y-%m-%d")

    return str(date), int(hour)

"""
def extract_task(**kwargs) -> None:
    date, hour = get_runtime_params(**kwargs)
    extract_to_gcs(date, hour)

    return None

def convert_task(**kwargs) -> None:
    date, hour = get_runtime_params(**kwargs)
    client = create_storage_client()
    convert_to_parquet(client, date, hour)

    return None


def create_ext_table_task() -> None:
    create_ext_table()

    return None
"""


def clean_up_gcs(**kwargs) -> None:
    today = kwargs["execution_date"]
    hour = kwargs["execution_date"].hour
    clean_up_date = today - timedelta(days=30)

    client = create_storage_client()

    delete_blob(client, clean_up_date, hour)

    return None


with DAG(
    "github_events_pipeline",
    start_date=datetime(2025, 12, 1),
    #schedule_interval="0 * * * *",
    schedule = None,
    catchup=False,
    max_active_runs = 1,
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    },
    description="github_events_pipeline"
) as dag:

    task_1 = CloudRunExecuteJobOperator(
        task_id = "extract_to_gcs",
        job_name = "job-extract-to-gcs",
        project_id = "github-events-analysis",
        region = "asia-south1",
        overrides = {
            "container_overrides" : [
                {
                    "env" : [
                        {"name" : "DATE", "value" : "{{ macros.ds_add(ds, -2) }}"},
                        {"name": "HOUR", "value": "{{ execution_date.hour }}"}
                    ]
                }
            ]
        }
    )

    task_2 = CloudRunExecuteJobOperator(
        task_id="parquet_ingestion",
        job_name="job-convert-to-parquet",
        project_id="github-events-analysis",
        region="asia-south1",
        overrides={
            "container_overrides": [
                {
                    "env": [
                        {"name": "DATE", "value": "{{ macros.ds_add(ds, -2) }}"},
                        {"name": "HOUR", "value": "{{ execution_date.hour }}"}
                    ]
                }
            ]
        }
    )

#    task_3 = PythonOperator(
#        task_id = "create_ext_table",
#        python_callable = create_ext_table_task,
#        op_kwargs={}
#    )

    task_4 = CloudRunExecuteJobOperator(
        task_id="dbt_stage3_transform",
        job_name="dbt-stage3-job",
        project_id="github-events-analysis",
        region="asia-south1"
    )

    task_5 = PythonOperator(
        task_id = "cleanup_gcs_bucket",
        python_callable = clean_up_gcs,
        op_kwargs={}
    )

    task_1 >> task_2 >> task_4 >> task_5
