# Autor: Alejandro Isnardi
# Fecha: 17/06/2024
# BCRA DAG

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from BCRA_ETL.tasks import PrincipalesVariablesBCRA, CargaTipoCambioMinorista, ValidarVariables, CargaTipoCambioMayorista, validar_dia_cotizacion

# Argumentos por defecto para el DAG
default_args = {
    'owner': 'Alejandro I.',
    'start_date': datetime(2024,5,29),
    'max_active_runs': 1,
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
get_prin_var = PythonOperator(
    task_id='Principales_Variables_BCRA',
    python_callable=PrincipalesVariablesBCRA,
    op_args=["{{ ds }} {{ logical_date.hour }}"],
    dag=BC_dag,
    provide_context=True
)

#2. Extraccion
get_tipo_cambio_minorista = PythonOperator(
    task_id='Carga_Tipo_Cambio_Minorista',
    python_callable=CargaTipoCambioMinorista,
    op_kwargs=["{{ ds }} {{ logical_date.hour }}"],
    dag=BC_dag,
    provide_context=True
)

#3. Process
val_variables = PythonOperator(
    task_id='Validar_Datos',
    python_callable=ValidarVariables,
    op_args=["{{ ds }} {{ logical_date.hour }}"],
    dag=BC_dag,
    provide_context=True
)

#3. Process
get_tipo_cambio_mayorista = PythonOperator(
    task_id='Carga_Tipo_Cambio_Mayorista',
    python_callable=CargaTipoCambioMayorista,
    op_args=["{{ ds }} {{ logical_date.hour }}"],
    dag=BC_dag,
    provide_context=True
)

#3. Process
validar_fecha_cotizacion = PythonOperator(
    task_id='validar_fecha_cotizacion_task',
    python_callable=validar_dia_cotizacion,
    op_kwargs={'hour': "{{ logical_date.hour }}"},
    dag=BC_dag,
    provide_context=True
)

# Definicion orden de tareas
get_prin_var >> val_variables
validar_fecha_cotizacion >> get_tipo_cambio_minorista
validar_fecha_cotizacion >> get_tipo_cambio_mayorista