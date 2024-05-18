# Curso DataEngineer CoderHouse

# BCRA Data Extraction and Loading Script

Este script extrae datos de la API del Banco Central de la República Argentina (BCRA) y los carga en una base de datos Amazon Redshift. Los datos extraídos incluyen principales variables, tipo de cambio minorista y mayorista, tasa de política monetaria y BADLAR en pesos de bancos privados.

## Requisitos

- Python 3.x
- Paquetes:
  - `requests`
  - `redshift_connector`
  - `pandas`
  - `dotenv`
  - `numpy`
  - `urllib3`

Puedes instalar los paquetes requeridos utilizando pip:

```sh
pip install requests redshift_connector pandas python-dotenv numpy urllib3
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
