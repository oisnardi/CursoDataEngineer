import datetime
import requests
import redshift_connector
import time
import pandas as pd
from dotenv import dotenv_values
import numpy as np
from urllib3.exceptions import InsecureRequestWarning

# Cargar las variables desde el archivo .env
config = dotenv_values("parameters.env")

# Configura los headers con el token de autorización
headers = {
    "Accept-Language": "es-AR",
    'content-type': 'application/json; charset=utf8'
}

def log_time(message, start_time):
    elapsed_time = time.time() - start_time
    print(f"{message}: {elapsed_time:.2f} segundos")

def get_data(url):
    # Realiza la solicitud GET a la API con los headers de autorización
    try:
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()  # Verifica si hay errores en la respuesta
        data_json = response.json()

        if response.status_code == 200:
            return pd.DataFrame.from_dict(data_json["results"])
        else:
            raise Exception(f"Error {data_json['status']}. {'.'.join(data_json['errorMessages'])}")
    except requests.exceptions.RequestException as e:
        print(f"Error en la solicitud: {e}")
        raise SystemExit(e)

def parse_cols(df: pd.DataFrame) -> pd.DataFrame:
    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], format=r'%d/%m/%Y')
    if "valor" in df.columns:
        df["valor"] = df["valor"].str.replace(".", "").str.replace(",", ".").astype(float)
    return df

def create_table_from_dataframe(conn, dataframe, table_name):
    dtypes = dataframe.dtypes
    cols = list(dtypes.index)
    tipos = list(dtypes.values)

    # Create the SQL DDL statement for the table schema
    sql_dtypes = [
        f"{col_name} {get_redshift_dtype(dtype)}"
        for col_name, dtype in zip(cols, tipos)
    ]
    table_schema = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(sql_dtypes)}, CreatedDate datetime default sysdate 
        );
    """

    # Create the table using the SQL statement
    try:
        cursor = conn.cursor()
        cursor.execute(table_schema)
        conn.commit()
    except redshift_connector.error.ProgrammingError as exception:
        print(dict(exception.args[0]).get('M'))

    # Convert DataFrame to list of tuples
    values = dataframe.to_numpy().tolist()

    # Insert the data into the table
    try:
        #Cantidad de columnas a mapear
        values_format = "%s"
        for x in range(len(cols)-1):
            values_format = values_format + ", %s"

        insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({values_format})"
        
        cursor.executemany(insert_sql, values)
        conn.commit()
    except redshift_connector.error.ProgrammingError as exception:
        print(dict(exception.args[0]).get('M'))

    print(f"Tabla '{table_name}' creada y datos cargados correctamente.")

def get_redshift_dtype(dtype):
    """
    Maps Pandas data types to their corresponding Redshift data types.
    Args:
        dtype (pandas.Dtype): The Pandas data type.
    Returns:
        str: The Redshift data type equivalent.
    """
    if dtype == np.int64:
        return "INT"
    elif dtype == np.float64:
        return "FLOAT"
    elif dtype == np.object_:
        return "VARCHAR(255)"  # Adjust as needed for longer text
    elif dtype == np.datetime64:
        return "DATE"
    elif dtype == 'datetime64[ns]':
        return "DATE"
    else:
        raise ValueError(f"Unsupported Pandas data type: {dtype}")

def get_redshift_connection():
    config = dotenv_values("parameters.env")
    conn = redshift_connector.connect(
        host=config.get("aws_host"),
        database=config.get("aws_db"),
        port=int(config.get("aws_port")),
        user=config.get("aws_usr"),
        password=config.get("aws_pwd")
        )
    return conn

def process_df(df, table_name, start_time):
    if df.empty:
        print("No hay Datos.")
        return False
    else:
        df = parse_cols(df)
        print(f"Se encontraron {len(df)} registros")

        conn = get_redshift_connection()
        create_table_from_dataframe(conn, df, table_name)

        conn.close

        log_time(f"Proceso completo para '{table_name}'", start_time)

        return True

def start_delay():
    print("Delay")
    time.sleep(5)
    print("\n")

# Start

#region Set Variables
baseurl = config.get("bcra_baseurl")

# Seteamos las fechas a actualizar data
fechahoy = datetime.date.today()
fechaayer = fechahoy - datetime.timedelta(days=1)
fechadesde= fechaayer.strftime("%Y-%m-%d")
fechahasta = fechahoy.strftime("%Y-%m-%d")

print(f"BaseURL: {baseurl}")
print(f"fechadesde: {fechadesde}")
print(f"fechahasta: {fechahasta}")
print("\n")

#endregion

#*****************************
#region BCRA principales variables
print("Obteniendo Principales Variables BCRA")
start_time = time.time()

bcra_principalesvariables = config.get("bcra_principalesvariables")
url_full = f"{baseurl}{bcra_principalesvariables}"
table_name = "BCRA_principales_variables"

print(url_full)

df = get_data(url_full)
process_df(df, table_name, start_time)

start_delay()
#endregion

#*****************************
#region Tipo de Cambio Minorista ($ por USD) Comunicación B 9791 - Promedio vendedor
print("Tipo de Cambio Minorista ($ por USD) Comunicación B 9791 - Promedio vendedor")
start_time = time.time()

datosvariables = config.get("bcra_datosvariables")
url_full = f"{baseurl}{datosvariables}"
table_name = "BCRA_Tipo_Cambio_Minorista"

print(url_full)
# Set Variable
url_full = url_full.replace("{variable}", "4")
# Set FechaDesde
url_full = url_full.replace("{fechadesde}", fechadesde)
# Set FechaHasta
url_full = url_full.replace("{fechahasta}", fechahasta)

df = get_data(url_full)
process_df(df, table_name, start_time)

start_delay()
#endregion

#*****************************
#region Tipo de Cambio Mayorista ($ por USD) Comunicación A 3500 - Referencia
print("Tipo de Cambio Mayorista ($ por USD) Comunicación A 3500 - Referencia")
start_time = time.time()

datosvariables = config.get("bcra_datosvariables")
url_full = f"{baseurl}{datosvariables}"
table_name = "BCRA_Tipo_Cambio_Mayorista"

print(url_full)
# Set Variable
url_full = url_full.replace("{variable}", "5")
# Set FechaDesde
url_full = url_full.replace("{fechadesde}", fechadesde)
# Set FechaHasta
url_full = url_full.replace("{fechahasta}", fechahasta)

df = get_data(url_full)
process_df(df, table_name, start_time)

start_delay()
#endregion

#*****************************
#region Tasa de Política Monetaria
print("Tasa de Política Monetaria")
start_time = time.time()

datosvariables = config.get("bcra_datosvariables")
url_full = f"{baseurl}{datosvariables}"
table_name = "BCRA_Tasa_Politica_Monetaria"

print(url_full)
# Set Variable
url_full = url_full.replace("{variable}", "6")
# Set FechaDesde
url_full = url_full.replace("{fechadesde}", fechadesde)
# Set FechaHasta
url_full = url_full.replace("{fechahasta}", fechahasta)

df = get_data(url_full)
process_df(df, table_name, start_time)

start_delay()
#endregion

#*****************************
#region BADLAR en pesos de bancos privados (en % n.a.)
print("BADLAR en pesos de bancos privados (en % n.a.)")
start_time = time.time()

datosvariables = config.get("bcra_datosvariables")
url_full = f"{baseurl}{datosvariables}"
table_name = "BCRA_BADLAR_pesos_bancos_privados"

print(url_full)
# Set Variable
url_full = url_full.replace("{variable}", "7")
# Set FechaDesde
url_full = url_full.replace("{fechadesde}", fechadesde)
# Set FechaHasta
url_full = url_full.replace("{fechahasta}", fechahasta)

df = get_data(url_full)
process_df(df, table_name, start_time)

start_delay()
#endregion