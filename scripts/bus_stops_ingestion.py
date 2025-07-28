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
script_dir = os.path.dirname(os.path.abspath(__file__))
jar_path = os.path.join(script_dir, '..', 'jars', 'postgresql-42.7.3.jar')
spark = SparkSession.builder \
    .appName("LTA Bus Stops Ingestion") \
    .config("spark.jars", jar_path).config("spark.driver.bindAddress", "localhost").config("spark.driver.host", "localhost").config("spark.master", "local[*]").config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()

# Fetch data
def fetch_all_bus_stops(api_key):
    base_url = "https://datamall2.mytransport.sg/ltaodataservice/BusStops"
    headers = {
    "AccountKey": api_key,
    "Content-Type": "application/json",
    "Accept": "application/json"
    }
    all_data = []
    skip = 0
    while True:
        try:
            response = requests.get(f"{base_url}?$skip={skip}", headers=headers)
            response.raise_for_status()
            data = response.json().get("value", [])
            if not data:
                break
            all_data.extend(data)
            skip += 500
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
    return all_data

# Get API key
api_key = os.environ.get("LTA_API_KEY")
if not api_key:
    print("Error: LTA_API_KEY not set in .env file or environment")
    raise ValueError("LTA_API_KEY not set in .env file or environment. Please check /Users/jaydenlim/Desktop/DE project/Bus stop/lta-bus-data-pipeline/.env")

# Fetch data
try:
    bus_stops_data = fetch_all_bus_stops(api_key)
except Exception as e:
    print(f"Failed to fetch API data: {e}")
    print("Falling back to sample BusStops.json")
    with open("../docs/BusStops.json", "r") as f:
        bus_stops_data = json.load(f)["value"]

# Create dataframe
schema = StructType([
    StructField("BusStopCode", StringType(), True),
    StructField("RoadName", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True)
])
df = spark.createDataFrame(bus_stops_data, schema)
df.show(10, truncate=False)  # Print 10 rows for testing

# Write to PostgreSQL
db_user = os.environ.get("DB_USER")
db_password = os.environ.get("DB_PASSWORD")

if not db_user or not db_password:
    raise ValueError("DB_USER and DB_PASSWORD must be set in the .env file")

df.write.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/lta_bus_data").option("dbtable", "bus_stops")\
.option("user", db_user).option("password", db_password).option("driver", "org.postgresql.Driver")\
.mode("overwrite").save()

print("Successfully wrote bus stops data to PostgreSQL.")
