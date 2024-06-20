# Autor: Alejandro Isnardi
# Fecha: 17/06/2024
# BRCR Helper

from datetime import date, timedelta
import json
import os
import time

import requests
import redshift_connector
import pandas as pd
import numpy as np
from urllib3.exceptions import InsecureRequestWarning
from io import StringIO
from airflow.models import Variable
from airflow.exceptions import AirflowException

from BCRA_ETL.helpers.fechas import validar_dia_no_laborable, validar_feriado
from BCRA_ETL.helpers.email import EnviarCorreo
from dotenv import dotenv_values

#region Variables

# BCRA API v2.0
dag_path = os.getcwd() 

config = dotenv_values(dag_path+'/dags/BCRA_ETL/'+"parameters.env")

bcra_baseurl = "https://api.bcra.gob.ar"
bcra_principalesvariables = "/estadisticas/v2.0/principalesvariables"
bcra_datosvariables = "/estadisticas/v2.0/DatosVariable/{variable}/{fechadesde}/{fechahasta}"

print(bcra_baseurl)
print(bcra_principalesvariables)
print(bcra_datosvariables)

# Configura los headers con el token de autorización
headers = {
    "Accept-Language": "es-AR",
    'content-type': 'application/json; charset=utf8'
}

# AWS RedShift Settings
aws_host = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
aws_port = 5439
with open(dag_path+'/keys/'+"db.txt",'r') as f:
    aws_db= f.read()
with open(dag_path+'/keys/'+"user.txt",'r') as f:
    aws_usr= f.read()
with open(dag_path+'/keys/'+"pwd.txt",'r') as f:
    aws_pwd= f.read()

# Seteamos las fechas a actualizar data
fechahoy = date.today()
fechaayer = fechahoy - timedelta(days=1)
fechadesde= fechaayer.strftime("%Y-%m-%d")
fechahasta = fechahoy.strftime("%Y-%m-%d")

print(f"BaseURL: {bcra_baseurl}")
print(f"fechadesde: {fechadesde}")
print(f"fechahasta: {fechahasta}")
print("\n")

#endregion

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
            print(f"Error en la solicitud: {e}")
            #raise Exception(f"Error {data_json['status']}. {'.'.join(data_json['errorMessages'])}")
    except requests.exceptions.RequestException as e:
        print("*** get_data ***")
        raise AirflowException(f"Error en la solicitud: {e}")

def parse_cols(df: pd.DataFrame) -> pd.DataFrame:
    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], format=r'%Y-%m-%d')
    if "valor" in df.columns:
        df["valor"] = df["valor"].astype(float)
            
    return df

def create_table_from_dataframe(conn, dataframe, table_name):
    dtypes = dataframe.dtypes
    cols = list(dtypes.index)
    tipos = list(dtypes.values)

    # Crear la parte de la declaración SQL para definir las columnas y la clave primaria
    sql_columns = [
        f"{col_name} {get_redshift_dtype(dtype)}"
        for col_name, dtype in zip(cols, tipos)
    ]

    primary_key = "idVariable, fecha"  # Usar las columnas como clave primaria

    # Agregar la columna CreatedDate al esquema de la tabla
    sql_columns.append("CreatedDate DATETIME default sysdate")

    # Combinar las columnas en la declaración SQL
    table_schema = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(sql_columns)},
            PRIMARY KEY ({primary_key})
        );
    """
    print(table_schema)

    # Create the table using the SQL statement
    try:
        cursor = conn.cursor()
        cursor.execute(table_schema)
        conn.commit()
    except redshift_connector.error.ProgrammingError as exception:
        print("*** create_table_from_dataframe ***")
        print(dict(exception.args[0]).get('M'))



    # Convert DataFrame to list of tuples
    fechas = dataframe["fecha"].dt.strftime('%Y-%m-%d').tolist()
    fechas_unicas = list(set(fechas))
    fechas_con_comillas = ["'{}'".format(fecha) for fecha in fechas_unicas]

    lstdvariables = dataframe["idVariable"].astype(str).tolist()
    lstdvariables_unicas  = list(set(lstdvariables))

    print(fechas_con_comillas)
    # Delete existing data in the table
    try:
        delete_sql = f"DELETE {table_name} WHERE fecha in ({', '.join(fechas_con_comillas)}) AND idVariable in ({', '.join(lstdvariables_unicas)})"
        print(delete_sql)

        cursor.execute(delete_sql)
        conn.commit()
    except redshift_connector.error.ProgrammingError as exception:
        print("*** create_table_from_dataframe2 ***")
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
        print(insert_sql)

        cursor.executemany(insert_sql, values)
        conn.commit()
    except redshift_connector.error.ProgrammingError as exception:
        print("*** create_table_from_dataframe3 ***")
        print(dict(exception.args[0]).get('M'))

    print(f"Tabla '{table_name}' creada y datos cargados correctamente.")

def get_redshift_dtype(dtype):
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
    print(f"Conectandose a la BD en la fecha: ") 
    conn = redshift_connector.connect(
        host=aws_host,
        database=aws_db,
        port=aws_port,
        user=aws_usr,
        password=aws_pwd
        )
    return conn

def process_df(df, table_name, start_time):
    if df is None:
        print(f"Error tabla {table_name}")
        return False
    elif df.empty:
        print(f"No hay Datos para la tabla {table_name}")
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
    #print("Delay")
    for _ in range(4):
        print("Please wait...", flush=True)
        time.sleep(1)    
    print("\n")

def get_variable_by_id(id):
    file_path = dag_path + '/raw_data/' + "variables_bcra.json"

    with open(file_path, 'r') as file:
        data = json.load(file)
    
    variable = None
    
    for record in data["variables"]:
        print(record["id"])
        if record["id"] == id:
            variable = record
            break
        
    variable_dict = None
    if variable:
        # Convertir el registro en un diccionario (ya es un diccionario en este caso)
        variable_dict = dict(variable)
        print("Variable encontrada y convertido en diccionario:")
        print(variable_dict)
    else:
        
        print("Variable con id = {id} no encontrado.")
 
    return variable_dict
 
def PrincipalesVariablesBCRA(exec_date, **kwargs):
    print(f"Adquiriendo Principales Variables BCRA para la fecha: {exec_date}")
    #*****************************
    #region BCRA principales variables
    start_time = time.time()

    url_full = f"{bcra_baseurl}{bcra_principalesvariables}"
    table_name = "BCRA_principales_variables"

    print(url_full)

    df = get_data(url_full)

    process_df(df, table_name, start_time)

    kwargs['ti'].xcom_push(key='PrincipalesVariablesBCRA', value=df.to_json())

    start_delay()
    #endregion

def CargarVariable (exec_date, variable, **kwargs):
    start_time = time.time()
    print(f"Inicio carga {variable['title']} para la fecha: {exec_date}")
    #*****************************

    url_full = f"{bcra_baseurl}{bcra_datosvariables}"
    table_name = variable['tablename']

    print(url_full)
    # Set Variable
    url_full = url_full.replace("{variable}", str(variable['id']))
    # Set FechaDesde
    url_full = url_full.replace("{fechadesde}", fechahasta)
    # Set FechaHasta
    url_full = url_full.replace("{fechahasta}", fechahasta)

    print(url_full)

    df = get_data(url_full)
    process_df(df, table_name, start_time)

    start_delay()

    #endregion

def ValidarVariables(exec_date, **kwargs):
    print(f"Validando Tipo de Cambio para la fecha: {exec_date}")
    
    df_json = kwargs['ti'].xcom_pull(key='PrincipalesVariablesBCRA', task_ids='Principales_Variables_BCRA')
    df = pd.read_json(StringIO(df_json))
    
    # Obtener la fecha máxima para cada 'idvariable'
    max_fecha = df.groupby('idVariable')['fecha'].max().reset_index()
    
    result = pd.merge(df, max_fecha, on=['idVariable', 'fecha'])

    tipocambio = result[result['idVariable'] == 4]

    valorhoy = tipocambio['valor'].iloc[0]
    
    max_dolar_minorista = Variable.get("max_dolar_minorista")
    
    print(f"El tipo de cambio es : {valorhoy}")
    print(f"Valor máximo : {max_dolar_minorista}")

    if (float(valorhoy) > float(max_dolar_minorista)):
        EnviarCorreo("Dolar caroooo!! Warning")
    else:
        print("Dolar calmo")

def CargaTipoCambioMinorista(exec_date, **kwargs):
    variable = get_variable_by_id(4)
    if (variable):
        diahabil = kwargs['ti'].xcom_pull(key='diahabil', task_ids='validar_fecha_cotizacion_task')
    
        if (diahabil==False):
            raise AirflowException("CargaTipoCambioMinorista: Día no laborable, no se puede continuar")
        else:
            CargarVariable(exec_date, variable)
    else:
        raise AirflowException("Error CargaTipoCambioMinorista variable inexistente, no se puede continuar")
        
def CargaTipoCambioMayorista (exec_date, **kwargs):
    variable = get_variable_by_id(5)
    if (variable):
        diahabil = kwargs['ti'].xcom_pull(key='diahabil', task_ids='validar_fecha_cotizacion_task')
    
        if (diahabil==False):
            raise AirflowException("CargaTipoCambioMayorista: Día no laborable, no se puede continuar")
        else:
            CargarVariable(exec_date, variable)
    else:
        raise AirflowException("Error CargaTipoCambioMayorista variable inexistente, no se puede continuar")

def validar_dia_cotizacion(hour, **kwargs):
    valor=False
    print(f"{hour}: validando fecha")
    if(validar_dia_no_laborable(fechahasta) or validar_feriado(fechahasta)):
        print(f"La fecha: {fechahasta} es día no laborable o feriado")
        diahabil= False
    else: 
        print(f"La fecha: {fechahasta} corresponde a día hábil")
        diahabil= True
    
    kwargs['ti'].xcom_push(key='diahabil', value=diahabil)
