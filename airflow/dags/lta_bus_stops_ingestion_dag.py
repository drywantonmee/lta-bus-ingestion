from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'lta_bus_stops_ingestion',
    default_args=default_args,
    description='Daily ingestion of LTA bus stops data',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

script_path = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "scripts", "bus_stops_ingestion.py")
jar_path = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "jars", "postgresql-42.7.3.jar")

ingest_bus_stops_task = SparkSubmitOperator(
    task_id='ingest_bus_stops_data',
    application=script_path,
    conn_id='spark_default',
    conf={"spark.driver.extraJavaOptions": "-Djava.security.manager=allow"},
    jars=jar_path,
    packages='org.postgresql:postgresql:42.7.3',
    dag=dag,
)
