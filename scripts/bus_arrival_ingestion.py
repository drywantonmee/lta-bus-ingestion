import requests
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from dotenv import load_dotenv

# Load environment variables from .env
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../.env'))
if not os.path.exists(dotenv_path):
    dotenv_path = os.path.abspath(".env")
    print(f"Falling back to default .env path: {dotenv_path}")
load_dotenv(dotenv_path=dotenv_path)

# Initialize Spark session
script_dir = os.path.dirname(os.path.abspath(__file__))
jar_path = os.path.join(script_dir, '..', 'jars', 'postgresql-42.7.3.jar')
spark = SparkSession.builder \
    .appName("LTA Bus Arrivals Ingestion") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.bindAddress", "localhost") \
    .config("spark.driver.host", "localhost") \
    .config("spark.master", "local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()

# Fetch data
def fetch_bus_arrivals(api_key, bus_stop_code):
    base_url = "https://datamall2.mytransport.sg/ltaodataservice/v3/BusArrival"
    headers = {
        "AccountKey": api_key,
        "Accept": "application/json"
    }
    params = {"BusStopCode": bus_stop_code}
    
    try:
        response = requests.get(base_url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json().get("Services", [])
        return data
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error: {http_err}")
        print(f"Response content: {response.text}")
        raise
    except requests.exceptions.JSONDecodeError as json_err:
        print(f"JSON decode error: {json_err}")
        print(f"Response content: {response.text}")
        raise
    except requests.exceptions.RequestException as req_err:
        print(f"Request error: {req_err}")
        raise

# Get API key with error handling
api_key = os.environ.get("LTA_API_KEY")
print (f"api key: {api_key}")
if not api_key:
    print("Error: LTA_API_KEY not set in .env file or environment")
    raise ValueError("LTA_API_KEY not found.")

# --- Configuration for Bus Arrivals ---
BUS_STOP_CODE = "83139"
TABLE_NAME = "bus_arrivals"
# --- End Configuration ---

# Fetch data
try:
    bus_arrivals_data = fetch_bus_arrivals(api_key, BUS_STOP_CODE)
except Exception as e:
    print(f"Failed to fetch API data for bus stop {BUS_STOP_CODE}: {e}")
    print("Falling back to sample data (if available, otherwise script will fail).")
    bus_arrivals_data = []

# Prepare data for Spark DataFrame
# Flatten the nested structure for simplicity
flattened_data = []
for service in bus_arrivals_data:
    service_no = service.get("ServiceNo")
    operator = service.get("Operator")
    
    for i in range(1, 4): # Iterate through NextBus, NextBus2, NextBus3
        next_bus_key = f"NextBus{'' if i == 1 else i}"
        next_bus = service.get(next_bus_key, {})
        
        if next_bus:
            flattened_data.append({
                "BusStopCode": BUS_STOP_CODE,
                "ServiceNo": service_no,
                "Operator": operator,
                "EstimatedArrival": next_bus.get("EstimatedArrival"),
                "Latitude": next_bus.get("Latitude"),
                "Longitude": next_bus.get("Longitude"),
                "VisitNumber": next_bus.get("VisitNumber"),
                "Load": next_bus.get("Load"),
                "Feature": next_bus.get("Feature"),
                "Type": next_bus.get("Type"),
                "BusInstance": f"NextBus{i}" # To distinguish between NextBus1, NextBus2, NextBus3
            })

# Define schema for bus arrivals
schema = StructType([
    StructField("BusStopCode", StringType(), True),
    StructField("ServiceNo", StringType(), True),
    StructField("Operator", StringType(), True),
    StructField("EstimatedArrival", StringType(), True), 
    StructField("Latitude", StringType(), True), 
    StructField("Longitude", StringType(), True), 
    StructField("VisitNumber", StringType(), True), 
    StructField("Load", StringType(), True),
    StructField("Feature", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("BusInstance", StringType(), True)
])

df = spark.createDataFrame(flattened_data, schema)
df.show(10, truncate=False)  # Print 10 rows for testing

# Write to PostgreSQL
db_user = os.environ.get("DB_USER")
db_password = os.environ.get("DB_PASSWORD")

if not db_user or not db_password:
    raise ValueError("DB_USER and DB_PASSWORD must be set in the .env file")

df.write.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/lta_bus_data") \
    .option("dbtable", TABLE_NAME) \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print(f"Successfully wrote bus arrivals data for bus stop {BUS_STOP_CODE} to PostgreSQL table {TABLE_NAME}.")
