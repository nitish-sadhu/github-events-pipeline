from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context
import sys

sys.path.append("/opt/project")

from pipeline.extract.extract_to_gcs import extract_to_gcs
from pipeline.transform.stage_1.convert_to_parquet import convert_to_parquet
from pipeline.transform.stage_2.create_ext_table import create_ext_table


from utilities.utilities import create_storage_client


from datetime import datetime, timedelta


def get_runtime_params(**kwargs):
    today = kwargs["execution_date"]
    hour = kwargs["execution_date"].hour
    datetime_stamp = today - timedelta(days=2)
    date = datetime_stamp.strftime("%Y-%m-%d")

    return str(date), int(hour)

def extract_task(**kwargs):
    date, hour = get_runtime_params(**kwargs)
    extract_to_gcs(date, hour)

def convert_task(**kwargs):
    date, hour = get_runtime_params(**kwargs)
    client = create_storage_client()
    convert_to_parquet(client, date, hour)

def create_ext_table_task(**kwargs):
    create_ext_table()


with DAG(
    "github_events_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 * * * *",
    catchup=False,
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    },
    description="github_events_pipeline"
) as dag:

    task_1 = PythonOperator(
        task_id = "extract_to_gcs",
        python_callable = extract_task,
        op_kwargs={}

    )

    task_2 = PythonOperator(
        task_id = "convert_to_parquet",
        python_callable = convert_task,
        op_kwargs={}
    )

    task_3 = PythonOperator(
        task_id = "create_ext_table",
        python_callable = create_ext_table_task,
        op_kwargs={}
    )

    task_4 = BashOperator(
        task_id = "dbt_run",
        bash_command =  "/opt/project/pipeline/transform/stage_3/dbt_run.sh "
    )

    task_1 >> task_2 >> task_3 >> task_4