# LTA Bus Data Pipeline

## Project Overview

This project implements a data engineering pipeline to ingest, process, and store real-time bus data from the Land Transport Authority (LTA) DataMall API. It is designed to demonstrate a practical application of data engineering principles and tools, including data ingestion, orchestration, and processing.

## Skills Used: 
- **Data Ingestion:** Fetching data from external APIs.
- **Data Orchestration:** Managing and scheduling data workflows with Apache Airflow.
- **Data Processing:** Transforming and processing data with Apache Spark.
- **System Architecture:** Designing and containerizing a multi-service application using Docker.

## Folder Structure

```
lta-bus-data-pipeline/
├── airflow/                
│   ├── dags/
│   └── Dockerfile
├── config/                 
├── data/                   
├── jars/                   
├── scripts/                
├── .env                    
├── .gitignore
├── docker-compose.yml      
└── requirements.txt        
```

## How to Use

1.  **Set up Environment:** Create a `.env` file and add your LTA API key.
2.  **Run the Pipeline:** Use Docker Compose to build and run the services:
    ```bash
    docker-compose up -d
    ```
3.  **Access Airflow:** The Airflow UI will be available at `http://localhost:8080` to monitor and manage the data pipelines.
