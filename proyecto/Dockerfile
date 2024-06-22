# Usa la imagen oficial de Python como base
FROM python:3.12-slim

RUN pwd

USER root

# Establece el directorio de trabajo
WORKDIR /app

# Establece variables de entorno necesarias para Airflow
ENV AIRFLOW_HOME=/app/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=false
ENV AIRFLOW__CORE__DEFAULT_TIMEZONE=America/Argentina/Buenos_Aires

# Instala dependencias necesarias
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Instala Apache Airflow y otras dependencias de Python
RUN pip install --no-cache-dir apache-airflow \
    && pip install --no-cache-dir python-dotenv redshift_connector pandas

# Copia el archivo de DAG a la carpeta de Airflow
# dags
COPY ./dags /app/airflow/dags
# keys
COPY ./keys/db.txt /app/keys/db.txt
COPY ./keys/user.txt /app/keys/user.txt
COPY ./keys/pwd.txt /app/keys/pwd.txt
COPY ./keys/email_login.txt /app/keys/email_login.txt
COPY ./keys/email_pwd.txt /app/keys/email_pwd.txt
# raw_data
COPY ./raw_data/feriados.json /app/raw_data/feriados.json
COPY ./raw_data/variables_bcra.json /app/raw_data/variables_bcra.json

# Imprime el contenido del directorio de destino después de la copia
RUN ls -la /app/keys

# Exponer el puerto 8080 para el webserver de Airflow
EXPOSE 8080

# Inicializa la base de datos de Airflow y crea un usuario administrador
RUN airflow db init 
RUN airflow users create --username airflow --password airflow --firstname Alejandro --lastname Isnardi --role Admin --email admin@example.com

# Copia y establece el entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Define el comando por defecto para ejecutar Airflow
ENTRYPOINT ["/entrypoint.sh"]