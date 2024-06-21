# Curso DataEngineer CoderHouse

# BCRA Data Extraction and Loading Script

# Entrega Proyecto final:
- Pipeline obtiene datos de 3 Apis del BCRA y utiliza 2 fuentes de datos locales en desde raw_data en formato JSon.

- Los datos se almacenan en Data Warehouse en Amazon RedShift

- El proceso está automatizado, extraé, transforma y carga.

- El proceso tiene 2 alertas por email, con valores configurables desde Airflow. Configurado servidor con SMTP

- El codigo se separó en varios archivos Python, dags, helpers, tasks, etc. a fin de facilitar el mantenimiento y reutilización de código.

- Mecanismos de alerta, se validan datos máximos y se notifica por mail, los valores, destinatarios pueden ser modificados desde Airflow.

- DAG: Todo está automatizado para iniciar correctamente, con tareas previas de validación.

- Docker: El contenedor pesa menos de 1GB

- Se agrega un Script en carpeta anexa, pero el mismo nos se necesita, el codigo tiene la capacidad de generar los scripts DDL para crear las tablas necesarias.

Adicionales:
- Se utiliza backfill con context entre tareas, para compartir datos.
- Extración de datos desde archivos locales, ya mencionado

## Requisitos

- Python 3.x
- Paquetes:
  - apache-airflow==2.9.2
  - numpy==2.0.0
  - pandas==1.4.2
  - redshift_connector==2.1.1
  - Requests==2.32.3
  - urllib3==2.2.2

Puedes instalar los paquetes requeridos utilizando pip:

```sh
pip install requests redshift_connector pandas python-dotenv numpy urllib3
```

## Docker
Commandos
```
docker-compose build
docker-compose up -d
docker-compose down
```

## Login Airflow
http://localhost:8080
```
Usr: airflow
Pwd: airflow
```

## Flujo del Script

- 1- Carga las variables de configuración desde el archivo .env.
- 2- Establece las fechas de hoy y ayer.
- 3- Extrae y carga los datos de las siguientes variables:
- 4- Principales variables BCRA
  - 4.1- Tipo de Cambio Minorista
  - 4.2- Tipo de Cambio Mayorista
  - 4.3- Tasa de Política Monetaria
  - 4.4- BADLAR en pesos de bancos privados


## Configuración
El script utiliza un archivo .env para cargar las configuraciones necesarias. Crea un archivo llamado parameters.env en el mismo directorio que el script y agrega las siguientes variables:

```
aws_host=your_redshift_host
aws_db=your_redshift_database
aws_port=your_redshift_port
aws_usr=your_redshift_user
aws_pwd=your_redshift_password
bcra_baseurl=https://api.bcra.gob.ar/estadisticas/v1/
bcra_principalesvariables=principalesvariables?Accept-Language=es-AR
bcra_datosvariables=datosvariables/{variable}?fecha_desde={fechadesde}&fecha_hasta={fechahasta}&Accept-Language=es-AR
```

## Backfill command
``` bash
airflow dags backfill BCRA_ETL -s 2024-06-01 -e 2024-06-20
```