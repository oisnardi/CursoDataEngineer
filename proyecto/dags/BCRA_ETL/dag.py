# Autor: Alejandro Isnardi
# Fecha: 17/06/2024
# BCRA DAG

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from BCRA_ETL.tasks import PrincipalesVariablesBCRA, CargaTipoCambioMinorista, CargaTipoCambioMayorista
from BCRA_ETL.validar import validar_dia_cotizacion, ValidarVariables, validar_tc_minorista, validar_tc_mayorista

# Argumentos por defecto para el DAG
default_args = {
    'owner': 'Alejandro I.',
    'start_date': datetime(2024,6,19),
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

#region OBTENER DATA

get_prin_var = PythonOperator(
    task_id='Principales_Variables_BCRA',
    python_callable=PrincipalesVariablesBCRA,
    dag=BC_dag,
    provide_context=True
)

get_tipo_cambio_minorista = PythonOperator(
    task_id='Carga_Tipo_Cambio_Minorista',
    python_callable=CargaTipoCambioMinorista,
    dag=BC_dag,
    provide_context=True
)

get_tipo_cambio_mayorista = PythonOperator(
    task_id='Carga_Tipo_Cambio_Mayorista',
    python_callable=CargaTipoCambioMayorista,
    dag=BC_dag,
    provide_context=True
)

#endregion


#region VALIDACIONES
val_variables = PythonOperator(
    task_id='Validar_Datos',
    python_callable=ValidarVariables,
    dag=BC_dag,
    provide_context=True
)

validar_fecha_cotizacion = PythonOperator(
    task_id='Validar_fecha_cotizacion_task',
    python_callable=validar_dia_cotizacion,
    dag=BC_dag,
    provide_context=True
)

validar_tipo_cambio_minorista = PythonOperator(
    task_id='Validar_tipo_cambio_minorista',
    python_callable=validar_tc_minorista,
    dag=BC_dag,
    provide_context=True
)

validar_tipo_cambio_mayorista = PythonOperator(
    task_id='Validar_tipo_cambio_mayorista',
    python_callable=validar_tc_mayorista,
    dag=BC_dag,
    provide_context=True
)

#endregion

# Definicion orden de tareas
get_prin_var >> val_variables
validar_fecha_cotizacion >> get_tipo_cambio_minorista >> validar_tipo_cambio_minorista
validar_fecha_cotizacion >> get_tipo_cambio_mayorista >> validar_tipo_cambio_mayorista
