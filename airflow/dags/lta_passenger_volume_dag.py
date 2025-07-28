from datetime import datetime, timedelta
from airflow import DAG 
from airflow.providers.standard.operators.python import PythonOperator
import requests 
import zipfile 
import io
from pyspark.sql import SparkSession
import os

default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'lta_passenger_volume_ingestion', 
    default_args=default_args, 
    description = 'Monthly ingestion of LTA bus stop passenger volume', 
    schedule_interval = '0 0 11 * *', 
    catchup= False,
)

def fetch_and_process(**kwargs): 
    api_key = os.environ.get("LTA_API_KEY")
    if not api_key:
        raise ValueError("LTA_API_KEY not set in .env file or environment")

    prev_month = (datetime.now() - timedelta(days=30)).strftime("%Y%m")
    url = f"http://datamall2.mytransport.sg/ltaodataservice/PV/BusStop?Date={prev_month}"
    headers = {"Account Key": api_key}

    response = requests.get(url, headers=headers)
    data = response.json()["value"]
    if not data: 
        raise ValueError("No data for the month")
    zip_link = data[0]["Link"]

    zip_response = requests.get(zip_link)
    zip_file = zipfile.ZipFile(io.BytesIO(zip_response.content))
    csv_name = zip_file.namelist()[0]
    csv_data = zip_file.read(csv_name)

    # Construct the absolute path for the JAR file relative to the DAG's location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    jar_path = os.path.join(script_dir, '..', '..', 'jars', 'postgresql-42.7.3.jar')

    # Ensure absolute path for temporary CSV file
    project_root = os.path.abspath(os.path.join(script_dir, '..', '..'))
    temp_csv_path = os.path.join(project_root, "data", "raw", "origin_destination_bus.csv")
    with open(temp_csv_path, "wb") as f:
        f.write(csv_data)

    spark = SparkSession.builder.appName("LTA Passenger Volume").config("spark.jars", jar_path).getOrCreate()
    df = spark.read.csv(temp_csv_path, header=True, inferSchema=True)

    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_PASSWORD")

    if not db_user or not db_password:
        raise ValueError("DB_USER and DB_PASSWORD must be set in the .env file or environment")

    df.write         .format("jdbc").option("url", "jdbc:postgresql://localhost:5432/lta_bus_data").option("dbtable", "passenger_volume")         .option("user", db_user).option("password", db_password).option("driver", "org.postgresql.Driver")         .mode("append").save()
    
    spark.stop()
    os.remove(temp_csv_path)

ingest_task = PythonOperator(
    task_id = 'fetch_process_load', 
    python_callable=fetch_and_process, 
    provide_context = True, 
    dag=dag
)

ingest_task