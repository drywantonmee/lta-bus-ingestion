

import requests
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from dotenv import load_dotenv

# Load environment variables from .env
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../.env'))
if not os.path.exists(dotenv_path):
    dotenv_path = os.path.abspath(".env")
    print(f"Falling back to default .env path: {dotenv_path}")
load_dotenv(dotenv_path=dotenv_path)

# Initialize Spark session
# Construct the absolute path for the JAR file relative to the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
jar_path = os.path.join(script_dir, '..', 'jars', 'postgresql-42.7.3.jar')
spark = SparkSession.builder     .appName("LTA Traffic Incidents Ingestion")     .config("spark.jars", jar_path).config("spark.driver.bindAddress", "localhost").config("spark.driver.host", "localhost").config("spark.master", "local[*]").config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")     .getOrCreate()

# Fetch data
def fetch_traffic_incidents(api_key):
    url = "https://datamall2.mytransport.sg/ltaodataservice/TrafficIncidents"
    headers = {
        "AccountKey": api_key,
        "Accept": "application/json"
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json().get("value", [])
        print(f"Fetched {len(data)} traffic incidents")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        raise

# Get API key with error handling
api_key = os.environ.get("LTA_API_KEY")
if not api_key:
    print("Error: LTA_API_KEY not set in .env file or environment")
    raise ValueError("LTA_API_KEY not set in .env file or environment. Please check /Users/jaydenlim/Desktop/DE project/Bus stop/lta-bus-data-pipeline/.env")

# Fetch data
traffic_incidents_data = []
try:
    traffic_incidents_data = fetch_traffic_incidents(api_key)
except Exception as e:
    print(f"Failed to fetch API data: {e}")

# Create dataframe
schema = StructType([
    StructField("Type", StringType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("Message", StringType(), True)
])
df = spark.createDataFrame(traffic_incidents_data, schema)
df.show(10, truncate=False)  # Print 10 rows for testing

# Write to PostgreSQL
db_user = os.environ.get("DB_USER")
db_password = os.environ.get("DB_PASSWORD")

if not db_user or not db_password:
    raise ValueError("DB_USER and DB_PASSWORD must be set in the .env file")

df.write.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/lta_bus_data").option("dbtable", "traffic_incidents")\
.option("user", db_user).option("password", db_password).option("driver", "org.postgresql.Driver")\
.mode("overwrite").save()

print("Successfully wrote traffic incidents data to PostgreSQL.")
