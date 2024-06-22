
import os
import redshift_connector
import numpy as np
from dotenv import dotenv_values

dag_path = os.getcwd() 

env_path= dag_path+"/airflow/dags/BCRA_ETL/"+"parameters.env"

config = dotenv_values(env_path)

# AWS RedShift Settings
aws_host = config.get("aws_host")
aws_port = config.get("aws_port")
with open(dag_path+'/keys/'+"db.txt",'r') as f:
    aws_db= f.read()
with open(dag_path+'/keys/'+"user.txt",'r') as f:
    aws_usr= f.read()
with open(dag_path+'/keys/'+"pwd.txt",'r') as f:
    aws_pwd= f.read()


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