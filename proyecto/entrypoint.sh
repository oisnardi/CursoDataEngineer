#!/bin/bash

# Inicializa la base de datos de Airflow
airflow db init &&
airflow variables import /app/airflow/variables_airflow.json

# Inicia el scheduler y el webserver
airflow scheduler & airflow webserver -p 8080
