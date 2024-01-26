from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    "ad_data_product",
    start_date=datetime(year=2022, month=5, day=24),
    schedule_interval="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:
    data_transformation_job = DatabricksSubmitRunOperator(
        task_id="task1",
        dag=dag,
        databricks_conn_id="default-databricks-conn",
        json={
            "run_name": "Ad Data Product",
            "new_cluster": {
                "spark_version": "11.3.x-scala2.12",
            },
            "libraries": [{"whl": "s3://itv-ad-data-product/artefacts/app.whl"}],
            "python_wheel_task": {
                "entry_point": "main",
                "package_name": ".",
                "parameters": [],
            },
        },
    )
