# Dockerfile
FROM apache/airflow:2.5.0

USER airflow
RUN python -m pip install --upgrade pip
RUN pip install python-dotenv 
RUN pip install redshift_connector

USER airflow
