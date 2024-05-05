import os, requests, redshift_connector, time
import pandas as pd
from dotenv import load_dotenv
import numpy as np


load_dotenv("parameters.env")

# Configura los headers con el token de autorización
headers = {
    "Accept-Language": "es-AR",
    'content-type': 'application/json; charset=utf8'
}

def GetData(url):
    # Realiza la solicitud GET a la API con los headers de autorización
    try:
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()  # Verifica si hay errores en la respuesta
        json = response.json()

        if response.status_code == 200:
            return pd.DataFrame.from_dict(json["results"])
        else:
            raise Exception(f"Error {json['status']}. {'.'.join(json['errorMessages'])}")
    except requests.exceptions.RequestException as e:
        print(f"Error en la solicitud: {e}")
        # Maneja el error según sea necesario
        raise SystemExit(e)

def parse_cols(df: pd.DataFrame) -> pd.DataFrame:
    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], format="%d/%m/%Y").dt.strftime("%Y-%m-%d")
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
            {', '.join(sql_dtypes)}
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
        return "TIMESTAMP"
    else:
        raise ValueError(f"Unsupported Pandas data type: {dtype}")

# Start
baseurl = os.getenv("bcra_baseurl")

conn = redshift_connector.connect(
     host=os.getenv("aws_srv"),
     database=os.getenv("aws_db"),
     port=os.getenv("aws_port"),
     user=os.getenv("aws_usr"),
     password=os.getenv("aws_pwd")
)

#*****************************
#region BCRA principales variables
print("Obteniendo Principales Variables BCRA")
bcra_principalesvariables = os.getenv("bcra_principalesvariables")
url_full = f"{baseurl}{bcra_principalesvariables}"

print(url_full)

df = GetData(url_full)
df = parse_cols(df)

conn = redshift_connector.connect(
     host=os.getenv("aws_srv"),
     database=os.getenv("aws_db"),
     port=os.getenv("aws_port"),
     user=os.getenv("aws_usr"),
     password=os.getenv("aws_pwd")
)

table_name = "BCRA_principales_variables"

create_table_from_dataframe(conn, df, table_name)

conn.close
time.sleep(5)
#endregion

#*****************************
#region Tipo de Cambio Minorista ($ por USD) Comunicación B 9791 - Promedio vendedor
print("Tipo de Cambio Minorista ($ por USD) Comunicación B 9791 - Promedio vendedor")
datosvariables = os.getenv("bcra_datosvariables")
url_full = f"{baseurl}{datosvariables}"

print(url_full)
# Set Variable
url_full = url_full.replace("{variable}", "4")
# Set FechaDesde
url_full = url_full.replace("{fechadesde}", "2001-04-29")
# Set FechaHasta
url_full = url_full.replace("{fechahasta}", "2024-05-05")

df = GetData(url_full)
df = parse_cols(df)



table_name = "BCRA_Tipo_Cambio_Minorista"

create_table_from_dataframe(conn, df, table_name)
print("Completado")

conn.close
print("Delay")
time.sleep(5)
#endregion

#*****************************
#region Tipo de Cambio Mayorista ($ por USD) Comunicación A 3500 - Referencia
print("Tipo de Cambio Mayorista ($ por USD) Comunicación A 3500 - Referencia")
datosvariables = os.getenv("bcra_datosvariables")
url_full = f"{baseurl}{datosvariables}"

print(url_full)
# Set Variable
url_full = url_full.replace("{variable}", "5")
# Set FechaDesde
url_full = url_full.replace("{fechadesde}", "2001-04-29")
# Set FechaHasta
url_full = url_full.replace("{fechahasta}", "2024-05-05")

df = GetData(url_full)
df = parse_cols(df)

conn = redshift_connector.connect(
     host=os.getenv("aws_srv"),
     database=os.getenv("aws_db"),
     port=os.getenv("aws_port"),
     user=os.getenv("aws_usr"),
     password=os.getenv("aws_pwd")
)

table_name = "BCRA_Tipo_Cambio_Mayorista"

create_table_from_dataframe(conn, df, table_name)

conn.close
time.sleep(5)
#endregion

#*****************************
#region Tasa de Política Monetaria
print("Tasa de Política Monetaria")
datosvariables = os.getenv("bcra_datosvariables")
url_full = f"{baseurl}{datosvariables}"

print(url_full)
# Set Variable
url_full = url_full.replace("{variable}", "6")
# Set FechaDesde
url_full = url_full.replace("{fechadesde}", "2001-04-29")
# Set FechaHasta
url_full = url_full.replace("{fechahasta}", "2024-05-05")

df = GetData(url_full)
df = parse_cols(df)

conn = redshift_connector.connect(
     host=os.getenv("aws_srv"),
     database=os.getenv("aws_db"),
     port=os.getenv("aws_port"),
     user=os.getenv("aws_usr"),
     password=os.getenv("aws_pwd")
)

table_name = "BCRA_Tasa_Politica_Monetaria"

create_table_from_dataframe(conn, df, table_name)

conn.close
time.sleep(5)
#endregion

#*****************************
#region BADLAR en pesos de bancos privados (en % n.a.)
print("BADLAR en pesos de bancos privados (en % n.a.)")
datosvariables = os.getenv("bcra_datosvariables")
url_full = f"{baseurl}{datosvariables}"

print(url_full)
# Set Variable
url_full = url_full.replace("{variable}", "7")
# Set FechaDesde
url_full = url_full.replace("{fechadesde}", "2001-04-29")
# Set FechaHasta
url_full = url_full.replace("{fechahasta}", "2024-05-05")

df = GetData(url_full)
df = parse_cols(df)

conn = redshift_connector.connect(
     host=os.getenv("aws_srv"),
     database=os.getenv("aws_db"),
     port=os.getenv("aws_port"),
     user=os.getenv("aws_usr"),
     password=os.getenv("aws_pwd")
)

table_name = "BCRA_BADLAR_pesos_bancos_privados"

create_table_from_dataframe(conn, df, table_name)

conn.close
time.sleep(5)
#endregion