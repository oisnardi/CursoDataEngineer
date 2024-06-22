# Autor: Alejandro Isnardi
# Fecha: 17/06/2024
# BRCR Helper

import redshift_connector
from BCRA_ETL.helpers.time import log_time
from BCRA_ETL.helpers.redshift import get_redshift_dtype, get_redshift_connection
import json
import os
import time
import requests
import pandas as pd
from urllib3.exceptions import InsecureRequestWarning
from airflow.exceptions import AirflowException
from dotenv import dotenv_values

#region Variables

# BCRA API v2.0
dag_path = os.getcwd() 

env_path= dag_path+"/airflow/dags/BCRA_ETL/"+"parameters.env"
config = dotenv_values(env_path)

bcra_baseurl = config.get("bcra_baseurl")
bcra_principalesvariables = config.get("bcra_principalesvariables")
bcra_datosvariables = config.get("bcra_datosvariables")

print(bcra_baseurl)
print(bcra_principalesvariables)
print(bcra_datosvariables)

# Configura los headers con el token de autorización
headers = {
    "Accept-Language": "es-AR",
    'content-type': 'application/json; charset=utf8'
}

#endregion

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
 
def PrincipalesVariablesBCRA(**kwargs):
    execution_date = kwargs['logical_date']
    print(f"Adquiriendo Principales Variables BCRA para la fecha: {execution_date}")
    #*****************************
    #region BCRA principales variables
    start_time = time.time()

    url_full = f"{bcra_baseurl}{bcra_principalesvariables}"
    table_name = "BCRA_principales_variables"

    print(url_full)

    df = get_data(url_full)

    process_df(df, table_name, start_time)

    kwargs['ti'].xcom_push(key='PrincipalesVariablesBCRA', value=df.to_json())

    #endregion

def CargarVariable (variable, fechahasta):
    start_time = time.time()
    print(f"Inicio carga {variable['title']} para la fecha: {fechahasta}")
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

    return df
    #endregion

def CargaTipoCambioMinorista(**kwargs):
    execution_date = kwargs['logical_date']
    fecha = execution_date.strftime("%Y-%m-%d")
    
    variable = get_variable_by_id(4)
    if (variable):
        ti = kwargs['ti']
        diahabil = ti.xcom_pull(key='diahabil', task_ids='Validar_fecha_cotizacion_task')
        print(f"diahabil es {type(diahabil)}")
              
        if (diahabil==False):
            raise AirflowException("CargaTipoCambioMinorista: Día no laborable, no se puede continuar")
        else:
            df=CargarVariable(variable, fecha)
            ti.xcom_push(key=f'TipoCambioMinorista', value=df.to_json())
    else:
        raise AirflowException("Error CargaTipoCambioMinorista variable inexistente, no se puede continuar")
        
def CargaTipoCambioMayorista (**kwargs):
    execution_date = kwargs['logical_date']
    fecha = execution_date.strftime("%Y-%m-%d")
    
    variable = get_variable_by_id(5)
    if (variable):
        ti = kwargs['ti']
        diahabil = ti.xcom_pull(key='diahabil', task_ids='Validar_fecha_cotizacion_task')
        print(f"diahabil es {type(diahabil)}")

        if (diahabil==False):
            raise AirflowException("CargaTipoCambioMayorista: Día no laborable, no se puede continuar")
        else:
            df=CargarVariable(variable, fecha)
            ti.xcom_push(key=f'TipoCambioMayorista', value=df.to_json())
    else:
        raise AirflowException("Error CargaTipoCambioMayorista variable inexistente, no se puede continuar")


