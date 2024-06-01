#!/bin/bash

# Inicializa la base de datos de Airflow
airflow db init

# Inicia el scheduler y el webserver
airflow scheduler & airflow webserver -p 8080
