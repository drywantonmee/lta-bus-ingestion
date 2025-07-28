FROM apache/airflow:2.9.2-python3.10

USER airflow

WORKDIR /opt/airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir apache-airflow-providers-postgres
