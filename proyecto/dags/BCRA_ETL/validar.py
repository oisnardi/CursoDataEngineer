# Autor: Alejandro Isnardi
# Fecha: 20/06/2024
# BRCR Validaciones

from datetime import date, datetime, timedelta
from airflow.models import Variable
from BCRA_ETL.helpers.fechas import validar_dia_no_laborable, validar_feriado
from BCRA_ETL.helpers.email import EnviarCorreo
from io import StringIO
import pandas as pd

def validar_dia_cotizacion(**kwargs):
    execution_date = kwargs['logical_date']
    fecha_cotizacion = datetime(execution_date.year, execution_date.month, execution_date.day)
    print(f"Validando fecha de cotización: {fecha_cotizacion}")
    
    diahabil=False
    ti = kwargs['ti']
    
    if(validar_dia_no_laborable(fecha_cotizacion) or validar_feriado(fecha_cotizacion)):
        print(f"La fecha: {fecha_cotizacion} es día no laborable o feriado")
        diahabil= False
    else: 
        print(f"La fecha: {fecha_cotizacion} corresponde a día hábil")
        diahabil= True
    
    ti.xcom_push(key='diahabil', value=diahabil)
    

def ValidarVariables(**kwargs):
    execution_date = kwargs['logical_date']
    print(f"Validando Tipo de Cambio para la fecha: {execution_date}")
    
    ti = kwargs['ti']
    df_json = ti.xcom_pull(key='PrincipalesVariablesBCRA', task_ids='Principales_Variables_BCRA')
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


def validar_tc_minorista(**kwargs):
    execution_date = kwargs['logical_date']
    print(f"Validando Tipo de Cambio para la fecha: {execution_date}")
    print(f"Validando Tipo de Cambio Minorista para la fecha: {execution_date}")


def validar_tc_mayorista(**kwargs):
    execution_date = kwargs['logical_date']
    print(f"Validando Tipo de Cambio para la fecha: {execution_date}")
    print(f"Validando Tipo de Cambio Mayorista para la fecha: {execution_date}")