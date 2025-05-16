import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

def enviar_email(nombre_fabrica, log_path, destinatario):
    try:
        email_host = os.getenv("EMAIL_HOST")
        email_port = int(os.getenv("EMAIL_PORT"))
        email_user = os.getenv("EMAIL_USER")
        email_password = os.getenv("EMAIL_PASSWORD")

        with open(log_path, "r", encoding="utf-8") as f:
            contenido_log = f.read()

        nombres_fabricas = {
            "JUM": "Jumillano",
            "NAFA": "Nafa"
        }
        nombre_descriptivo = nombres_fabricas.get(nombre_fabrica, nombre_fabrica)

        mensaje = MIMEMultipart()
        mensaje["From"] = email_user
        mensaje["To"] = destinatario
        mensaje["Subject"] = f"Novedades sincronizadas - {nombre_descriptivo}"

        cuerpo = f"Hola,\n\nSe han sincronizado las novedades de {nombre_descriptivo}. Aquí tienes el resumen:\n\n{contenido_log}\n\nSaludos."
        mensaje.attach(MIMEText(cuerpo, "plain"))

        with smtplib.SMTP_SSL(email_host, email_port) as server:
            server.login(email_user, email_password)
            server.sendmail(email_user, destinatario, mensaje.as_string())

        print(f"📧 Correo enviado con éxito a {destinatario}")
    except Exception as e:
        print(f"❌ Error al enviar el correo: {e}")