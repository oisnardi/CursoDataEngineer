# Autor: Alejandro Isnardi
# Fecha: 18/06/2024
# BRCR Helper

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from airflow.models import Variable

from BCRA_ETL.helpers.time import GetNowTime

dag_path = os.getcwd() 

#Email Settings
subject = Variable.get("email_subject")
to_email = Variable.get("email_to")
from_email = Variable.get("email_from")
smtp_server = "smtp-relay.brevo.com"
smtp_port = 587

with open(dag_path+'/keys/'+"email_login.txt",'r') as f:
    login= f.read()
with open(dag_path+'/keys/'+"email_pwd.txt",'r') as f:
    password= f.read()

def EnviarCorreo(msg):
    current_time = GetNowTime()
    print(f"Enviando correo para la fecha: {current_time}")
    try:
        body=msg
        # Crear mensaje
        msg = MIMEMultipart()
        msg['From'] = from_email
        msg['To'] = to_email
        msg['Subject'] = subject

        # Agregar cuerpo del correo
        msg.attach(MIMEText(body, 'plain'))

        # Configurar la conexión al servidor SMTP
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(login, password)

        # Enviar correo
        server.sendmail(from_email, to_email, msg.as_string())
        server.quit()

        print(f'Correo enviado exitosamente a {to_email}')
    except Exception as e:
        print(f'Error al enviar el correo: {e}')