# Autor: Alejandro Isnardi
# Fecha: 17/06/2024
# BRCRA Control fechas

from datetime import date, datetime
import os
import pandas as pd


dag_path = os.getcwd() 

file_path = dag_path + '/raw_data/' + "feriados.json"

df = pd.read_json(file_path)


def validar_feriado(fecha: datetime):
    date_exists = False

    if 'date' not in df.columns:
        print("ERROR No existe la columna date")
        exit()

    df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')

    # Check if the target date exists in the 'date' column
    date_exists = df['date'].dt.date.eq(fecha.date()).any()

    # Print the result
    if date_exists:
        print(f"La fecha {fecha.date()} es un feriado.")
    else:
        print(f"La fecha {fecha.date()} no es feriado")

    return date_exists

def validar_dia_no_laborable(fecha: datetime):
    try:
        dia_de_la_semana = fecha.weekday()
        return dia_de_la_semana == 5 or dia_de_la_semana == 6
    except ValueError as e:
        print(f"Error al convertir '{fecha}': {e}")
        return False
    
