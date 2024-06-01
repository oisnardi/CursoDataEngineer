from datetime import date, timedelta, datetime
import os
import time
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import requests
import redshift_connector
import pandas as pd
from dotenv import dotenv_values, load_dotenv
import numpy as np
from urllib3.exceptions import InsecureRequestWarning

#region Variables

# BCRA APÏ v2.0
bcra_baseurl = "https://api.bcra.gob.ar"
bcra_principalesvariables = "/estadisticas/v2.0/principalesvariables"
bcra_datosvariables = "/estadisticas/v2.0/DatosVariable/{variable}/{fechadesde}/{fechahasta}"

variables = [
        #{'id': '1', 'title': 'Reservas Internacionales del BCRA (en millones de dólares - cifras provisorias sujetas a cambio de valuación)', 'tablename': 'Reservas_Internacionales_BCRA', 'filterbyDate': True},
        {'id': '4', 'title': 'Tipo de Cambio Minorista ($ por USD) Comunicación B 9791 - Promedio vendedor', 'tablename': 'Tipo_Cambio_Minorista', 'filterbyDate': True},
        {'id': '5', 'title': 'Tipo de Cambio Mayorista ($ por USD) Comunicación A 3500 - Referencia', 'tablename': 'Tipo_Cambio_Mayorista', 'filterbyDate': True},
        {'id': '6', 'title': 'Tasa de Política Monetaria (en % n.a.)', 'tablename': 'Tasa_Politica_Monetaria_NA', 'filterbyDate': True},
        {'id': '7', 'title': 'BADLAR en pesos de bancos privados (en % n.a.)', 'tablename': 'BADLAR_Pesos_Bancos_Privados_NA', 'filterbyDate': True},
        #{'id': '8', 'title': 'TM20 en pesos de bancos privados (en % n.a.)', 'tablename': 'TM20_Pesos_Bancos_Privados_NA', 'filterbyDate': True},
        {'id': '9', 'title': 'Tasas de interés de las operaciones de pase activas para el BCRA, a 1 día de plazo (en % n.a.)', 'tablename': 'Tasas_Interes_Pase_Activas_BCRA_NA', 'filterbyDate': True},
        {'id': '10', 'title': 'Tasas de interés de las operaciones de pase pasivas para el BCRA, a 1 día de plazo (en % n.a.)', 'tablename': 'Tasas_Interes_Pase_Pasivas_BCRA_NA', 'filterbyDate': True},
        {'id': '11', 'title': 'Tasas de interés por préstamos entre entidades financiera privadas (BAIBAR) (en % n.a.)', 'tablename': 'Tasas_Interes_Prestamos_Entidades_Privadas_NA', 'filterbyDate': True},
        {'id': '12', 'title': 'Tasas de interés por depósitos a 30 días de plazo en entidades financieras (en % n.a.)', 'tablename': 'Tasas_Interes_Depositos_30_Dias_NA', 'filterbyDate': True},
        {'id': '13', 'title': 'Tasa de interés de préstamos por adelantos en cuenta corriente', 'tablename': 'Tasa_Interes_Adelantos_Cuenta_Corriente', 'filterbyDate': True},
        {'id': '14', 'title': 'Tasa de interés de préstamos personales', 'tablename': 'Tasa_Interes_Prestamos_Personales', 'filterbyDate': True},
        #{'id': '15', 'title': 'Base monetaria - Total (en millones de pesos)', 'tablename': 'Base_Monetaria_Total', 'filterbyDate': True},
        #{'id': '16', 'title': 'Circulación monetaria (en millones de pesos)', 'tablename': 'Circulacion_Monetaria', 'filterbyDate': True},
        #{'id': '17', 'title': 'Billetes y monedas en poder del público (en millones de pesos)', 'tablename': 'Billetes_Monedas_Publico', 'filterbyDate': True},
        #{'id': '18', 'title': 'Efectivo en entidades financieras (en millones de pesos)', 'tablename': 'Efectivo_Entidades_Financieras', 'filterbyDate': True},
        #{'id': '19', 'title': 'Depósitos de los bancos en cta. cte. en pesos en el BCRA (en millones de pesos)', 'tablename': 'Depositos_Bancos_Cta_Cte_BCRA', 'filterbyDate': True},
        #{'id': '21', 'title': 'Depósitos en efectivo en las entidades financieras - Total (en millones de pesos)', 'tablename': 'Depositos_Efectivo_Entidades_Financieras', 'filterbyDate': True},
        #{'id': '22', 'title': 'En cuentas corrientes (neto de utilización FUCO) (en millones de pesos)', 'tablename': 'Cuentas_Corrientes_Neto_FUCO', 'filterbyDate': True},
        #{'id': '23', 'title': 'En Caja de ahorros (en millones de pesos)', 'tablename': 'Caja_Ahorros', 'filterbyDate': True},
        #{'id': '24', 'title': 'A plazo (incluye inversiones y excluye CEDROS) (en millones de pesos)', 'tablename': 'Depositos_Plazo', 'filterbyDate': True},
        #{'id': '25', 'title': 'M2 privado, promedio móvil de 30 días, variación interanual (en %)', 'tablename': 'M2_Privado_Variacion_Interanual', 'filterbyDate': True},
        #{'id': '26', 'title': 'Préstamos de las entidades financieras al sector privado (en millones de pesos)', 'tablename': 'Prestamos_Entidades_Financieras_Sector_Privado', 'filterbyDate': True},
        #{'id': '27', 'title': 'Inflación mensual (variación en %)', 'tablename': 'Inflacion_Mensual', 'filterbyDate': True},
        #{'id': '28', 'title': 'Inflación interanual (variación en % i.a.)', 'tablename': 'Inflacion_Interanual', 'filterbyDate': True},
        #{'id': '29', 'title': 'Inflación esperada - REM próximos 12 meses - MEDIANA (variación en % i.a)', 'tablename': 'Inflacion_Esperada_REM', 'filterbyDate': True},
        {'id': '30', 'title': 'CER (Base 2.2.2002=1)', 'tablename': 'CER', 'filterbyDate': True},
        {'id': '31', 'title': 'Unidad de Valor Adquisitivo (UVA) (en pesos -con dos decimales-, base 31.3.2016=14.05)', 'tablename': 'Unidad_Valor_Adquisitivo_UVA', 'filterbyDate': True},
        {'id': '32', 'title': 'Unidad de Vivienda (UVI) (en pesos -con dos decimales-, base 31.3.2016=14.05)', 'tablename': 'Unidad_Vivienda_UVI', 'filterbyDate': True},
        {'id': '34', 'title': 'Tasa de Política Monetaria (en % e.a.)', 'tablename': 'Tasa_Politica_Monetaria_EA', 'filterbyDate': True},
        {'id': '35', 'title': 'BADLAR en pesos de bancos privados (en % e.a.)', 'tablename': 'BADLAR_Pesos_Bancos_Privados_EA', 'filterbyDate': True},
        {'id': '40', 'title': 'Índice para Contratos de Locación (ICL-Ley 27.551, con dos decimales, base 30.6.20=1)', 'tablename': 'Indice_Contratos_Locacion', 'filterbyDate': True},
        {'id': '41', 'title': 'Tasas de interés de las operaciones de pase pasivas para el BCRA, a 1 día de plazo (en % e.a.)', 'tablename': 'Tasas_Interes_Pase_Pasivas_BCRA_EA', 'filterbyDate': True}
        #{'id': '42', 'title': 'Pases pasivos para el BCRA - Saldos (en millones de pesos)', 'tablename': 'Pases_Pasivos_BCRA_Saldos', 'filterbyDate': True}
    ]

print(bcra_baseurl)
print(bcra_principalesvariables)
print(bcra_datosvariables)

#config = dotenv_values("/opt/airflow/parameters.env")

# Configura los headers con el token de autorización
headers = {
    "Accept-Language": "es-AR",
    'content-type': 'application/json; charset=utf8'
}

aws_host = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"

dag_path = os.getcwd() 

with open(dag_path+'/keys/'+"db.txt",'r') as f:
    aws_db= f.read()
with open(dag_path+'/keys/'+"user.txt",'r') as f:
    aws_usr= f.read()
with open(dag_path+'/keys/'+"pwd.txt",'r') as f:
    aws_pwd= f.read()

aws_port = 5439

print(f"aws_host: {aws_host}")

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

# argumentos por defecto para el DAG
default_args = {
    'owner': 'Alejandro I.',
    'start_date': datetime(2024,5,29),
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

BC_dag = DAG(
    dag_id='BCRA_ETL',
    default_args=default_args,
    description='Agrega data de BCRA de forma diaria',
    schedule_interval="@daily",
    catchup=False
)

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
        print(f"Error en la solicitud: {e}")

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
    print("Delay")
    time.sleep(5)
    print("\n")

def PrincipalesVariablesBCRA(exec_date):
    print(f"Adquiriendo data para la fecha: {exec_date}")
    #*****************************
    #region BCRA principales variables
    print("Obteniendo Principales Variables BCRA")
    start_time = time.time()

    url_full = f"{bcra_baseurl}{bcra_principalesvariables}"
    table_name = "BCRA_principales_variables"

    print(url_full)

    df = get_data(url_full)
    process_df(df, table_name, start_time)

    start_delay()
    #endregion

def CargaDatosVariables(exec_date):
    print(f"Adquiriendo data para la fecha: {exec_date}")
    #*****************************
    #region Procesar todas las Variables
    
    for variable in variables:
        print(variable['title'])
        start_time = time.time()
        url_full = f"{bcra_baseurl}{bcra_datosvariables}"
        table_name = variable['tablename']

        print(url_full)
        # Set Variable
        url_full = url_full.replace("{variable}", variable['id'])
        # Set FechaDesde
        url_full = url_full.replace("{fechadesde}", fechadesde)
        # Set FechaHasta
        url_full = url_full.replace("{fechahasta}", fechahasta)

        print(url_full)

        df = get_data(url_full)
        process_df(df, table_name, start_time)

        start_delay()

    #endregion


# Tareas
##1. Extraccion
task_1 = PythonOperator(
    task_id='Principales_Variables_BCRA',
    python_callable=PrincipalesVariablesBCRA,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

#2. Transformacion
task_2 = PythonOperator(
    task_id='Carga_Datos_Variables',
    python_callable=CargaDatosVariables,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

# Definicion orden de tareas
task_1 >> task_2