# Autor: Alejandro Isnardi
# Fecha: 20/06/2024
# BRCR Validaciones

from datetime import date, timedelta
from airflow.models import Variable
from BCRA_ETL.helpers.fechas import validar_dia_no_laborable, validar_feriado
from BCRA_ETL.helpers.email import EnviarCorreo
from io import StringIO
import pandas as pd

# Seteamos las fechas a actualizar data
fechahoy = date.today()
fechaayer = fechahoy - timedelta(days=1)
fechadesde= fechaayer.strftime("%Y-%m-%d")
fechahasta = fechahoy.strftime("%Y-%m-%d")

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
    