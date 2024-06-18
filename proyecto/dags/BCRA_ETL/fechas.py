# Autor: Alejandro Isnardi
# Fecha: 17/06/2024
# BRCRA Control fechas

from datetime import datetime
import os
import pandas as pd


dag_path = os.getcwd() 

file_path = dag_path + '/raw_data/' + "feriados.json"

df = pd.read_json(file_path)


def validar_feriado(fecha):
    fecha_fotmated = datetime.strptime(fecha, "%Y-%m-%d")

    print(fecha_fotmated)
    
    date_exists = False

    if 'date' not in df.columns:
        print("ERROR No existe la columna date")
        exit()

    df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')

    # Check if the target date exists in the 'date' column
    date_exists = df['date'].dt.date.eq(fecha_fotmated.date()).any()

    # Print the result
    if date_exists:
        print(f"La fecha {fecha_fotmated} es un feriado.")
    else:
        print(f"La fecha {fecha_fotmated} no es feriado")

    return date_exists

def validar_dia_no_laborable(fecha):
    try:
        fecha_formated = datetime.strptime(fecha, "%Y-%m-%d")
        dia_de_la_semana = fecha_formated.weekday()
        return dia_de_la_semana == 5 or dia_de_la_semana == 6
    except ValueError as e:
        print(f"Error al convertir '{fecha}': {e}")
        return False
    
