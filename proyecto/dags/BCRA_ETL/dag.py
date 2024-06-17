# Autor: Alejandro Isnardi
# Fecha: 17/06/2024
# BCRA DAG

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from BCRA_ETL.tasks import PrincipalesVariablesBCRA, CargaTipoCambioMinorista, ValidarVariables, CargaTipoCambioMayorista

# Argumentos por defecto para el DAG
default_args = {
    'owner': 'Alejandro I.',
    'start_date': datetime(2024,5,29),
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}
    
BC_dag = DAG(
    dag_id='BCRA_ETL',
    default_args=default_args,
    description='Extre data del BCRA de forma diaria y carga en Amazon RedShift',
    start_date=datetime(2024,6,17),
    tags=['BCRA','AlejandroI'],
    schedule_interval="@daily",
    catchup=False
)

# Tareas
##1. Extraccion
task_1 = PythonOperator(
    task_id='Principales_Variables_BCRA',
    python_callable=PrincipalesVariablesBCRA,
    op_args=["{{ ds }} {{ logical_date.hour }}"],
    dag=BC_dag,
    provide_context=True
)

#2. Extraccion
task_2 = PythonOperator(
    task_id='Carga_Tipo_Cambio_Minorista',
    python_callable=CargaTipoCambioMinorista,
    op_args=["{{ ds }} {{ logical_date.hour }}"],
    dag=BC_dag,
    provide_context=True
)

#3. Process
task_3 = PythonOperator(
    task_id='Validar_Datos',
    python_callable=ValidarVariables,
    op_args=["{{ ds }} {{ logical_date.hour }}"],
    dag=BC_dag,
    provide_context=True
)

#3. Process
task_4 = PythonOperator(
    task_id='Carga_Tipo_Cambio_Mayorista',
    python_callable=CargaTipoCambioMayorista,
    op_args=["{{ ds }} {{ logical_date.hour }}"],
    dag=BC_dag,
    provide_context=True
)

# Definicion orden de tareas
task_1 >> task_3 >> task_2 >> task_4