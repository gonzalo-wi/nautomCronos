import pyodbc
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import subprocess
import os
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Cargar variables de entorno
load_dotenv()


def verificar_montaje(ruta, ip, usuario, clave):
    if not os.path.ismount(ruta):
        os.system(f"sudo mount -t cifs //{ip}/CRONOS {ruta} -o username={usuario},password={clave},vers=3.0")
        print(f"✅ Recurso montado en {ruta}")
    else:
        print(f"ℹ️ Recurso {ruta} ya está montado.")

if __name__ == "__main__":
    
    verificar_montaje("/mnt/jum", "192.168.0.9", os.getenv("CRONOS_USERNAME"), os.getenv("CRONOS_PASSWORD"))
    verificar_montaje("/mnt/nafa", "192.168.80.220", os.getenv("CRONOS_NAFA_USERNAME"), os.getenv("CRONOS_NAFA_PASSWORD"))

   
   

def montar_recurso(ruta, ip, usuario, clave):
    if not os.path.exists(ruta):
        try:
            subprocess.run(["sudo", "mkdir", "-p", ruta], check=True)
        except subprocess.CalledProcessError as e:
            print(f"❌ Error creando el directorio {ruta}: {e}")
            exit()

    if not os.path.ismount(ruta):
        try:
            subprocess.run([
                "sudo", "mount", "-t", "cifs",
                f"//{ip}/CRONOS", ruta,
                f"-o,username={usuario},password={clave},vers=3.0"
            ], check=True)
            print(f"✅ Recurso montado en {ruta}")
        except subprocess.CalledProcessError as e:
            print(f"❌ Error al montar {ruta}: {e}")
            exit()
    else:
        print(f"ℹ️ Recurso {ruta} ya está montado.")

def enviar_email(nombre_fabrica, log_path, destinatario):
    try:
        # Configuración del servidor SMTP
        email_host = os.getenv("EMAIL_HOST")
        email_port = int(os.getenv("EMAIL_PORT"))
        email_user = os.getenv("EMAIL_USER")
        email_password = os.getenv("EMAIL_PASSWORD")

        # Leer el contenido del log
        with open(log_path, "r", encoding="utf-8") as f:
            contenido_log = f.read()

        # Crear el mensaje de correo
        mensaje = MIMEMultipart()
        mensaje["From"] = email_user
        mensaje["To"] = destinatario
        mensaje["Subject"] = f"Novedades sincronizadas - {nombre_fabrica}"

        nombres_fabricas = {
            "JUM": "Jumillano",
            "NAFA": "Nafa"
            }
        nombre_descriptivo = nombres_fabricas.get(nombre_fabrica, nombre_fabrica)

        cuerpo = f"Hola,\n\nSe han sincronizado las novedades de {nombre_descriptivo}. Aquí tienes el resumen:\n\n{contenido_log}\n\nSaludos."

        mensaje.attach(MIMEText(cuerpo, "plain"))

        # Conectar al servidor SMTP y enviar el correo
        with smtplib.SMTP_SSL(email_host, email_port) as server:
            server.login(email_user, email_password)
            server.sendmail(email_user, destinatario, mensaje.as_string())

        print(f"📧 Correo enviado con éxito a {destinatario}")
    except Exception as e:
        print(f"❌ Error al enviar el correo: {e}")

def procesar_cronos(nombre_fabrica, ruta_mdb, creador_id, prefijo_legajo, username, password):
    montar_recurso("/mnt/" + nombre_fabrica.lower(), "192.168.80.200", username, password)

    conn_str = r"DRIVER={MDBTools};" + fr"DBQ={ruta_mdb};"
    try:
        with pyodbc.connect(conn_str) as conn:
            # Leer las tablas necesarias
            novedades = pd.read_sql("SELECT * FROM Novedades", conn)
            rel_novedad_persona = pd.read_sql("SELECT * FROM Rel_Novedad_Persona", conn)
            personas = pd.read_sql("SELECT * FROM Personas", conn)
            justificaciones = pd.read_sql("SELECT * FROM justificaciones", conn)

            # Combinar tablas
            df = pd.merge(novedades, rel_novedad_persona, on="idNovedad")
            print(f"🔄 Después de merge con Rel_Novedad_Persona: {len(df)} registros.")
            df = pd.merge(df, personas, on="idPersona")
            print(f"🔄 Después de merge con Personas: {len(df)} registros.")
            df = pd.merge(df, justificaciones, left_on="nov_justificacion", right_on="IdJustificacion", how="left")
            print(f"🔄 Después de merge con Justificaciones: {len(df)} registros.")

            # Validar si la columna jus_descripcion existe
            if "jus_descripcion" not in df.columns:
                print(f"⚠️ La columna 'jus_descripcion' no está presente en los datos de {nombre_fabrica}.")
                return

            # Agregar columnas adicionales
            df["ID_Creador"] = creador_id
            df["ID_Empleado"] = prefijo_legajo + df["per_legajo"].astype(str).str[-4:]

            # Renombrar columnas
            df.rename(columns={
                "jus_descripcion": "Novedad",
                "nov_Desde": "Fecha_inicio",
                "nov_Hasta": "Fecha_fin",
                "nov_DesdeStr": "Fecha_str"
            }, inplace=True)

            # Procesar fechas correctamente
            df["Fecha_str"] = pd.to_datetime(df["Fecha_str"].astype(str), format="%d/%m/%Y", dayfirst=True, errors="coerce")
            df["Fecha_fin"] = pd.to_datetime(df["Fecha_fin"], format="%d/%m/%Y", dayfirst=True, errors="coerce").dt.date

            # Filtrar por fechas
            hoy = datetime.now().date()
            df = df[df["Fecha_fin"] >= hoy]
            print(f"📅 Novedades activas encontradas en {nombre_fabrica}: {len(df)}")

            # Preparar datos para exportar
            export_df = df[["ID_Creador", "Novedad", "ID_Empleado", "Fecha_inicio", "Fecha_fin", "Fecha_str"]].copy()

            if export_df.empty:
                print(f"📭 No se encontraron novedades para sincronizar en {nombre_fabrica}.")
                return

            # Guardar el archivo CSV
            fecha = datetime.now().strftime("%Y-%m-%d")
            output_path = f"./cronos_export_{nombre_fabrica}_{fecha}.csv"
            export_df.to_csv(output_path, index=False, encoding="utf-8-sig")
            print(f"✅ CSV generado: {output_path}")

            # Resto de columnas para Glide
            export_df["comentario"] = ""
            export_df["attachment"] = ""
            export_df["validaciN"] = False
            export_df["idValidador"] = ""
            export_df["lastModified"] = ""
            export_df["regiNAsignadaDesdeEjecuciN"] = ""
            export_df["eliminar"] = False

            export_df.rename(columns={
                "ID_Creador": "idCreador",
                "Novedad": "novedad",
                "ID_Empleado": "idEmpleado",
                "Fecha_inicio": "fechaInicio",
                "Fecha_fin": "fechaFin",
                "Fecha_str": "fechaCreaciN"
            }, inplace=True)

            # Validar y corregir fechas antes de enviar
            for campo in ["fechaInicio", "fechaFin", "fechaCreaciN"]:
                export_df[campo] = export_df[campo].fillna("1900-01-01")
                export_df[campo] = pd.to_datetime(export_df[campo], errors="coerce").dt.strftime("%Y-%m-%d")

            # Cargar registros enviados previamente
            enviados_path = f"enviados_{nombre_fabrica.lower()}.json"
            if os.path.exists(enviados_path):
                with open(enviados_path, "r", encoding="utf-8") as f:
                    enviados = json.load(f)
            else:
                enviados = []

            # Convertir enviados a un conjunto para comparación rápida
            enviados_set = {(e["idEmpleado"], e["novedad"], e["fechaInicio"], e["fechaFin"]) for e in enviados}

            # Filtrar registros no enviados
            nuevos_registros = [
                row for row in export_df.to_dict(orient="records")
                if (row["idEmpleado"], row["novedad"], row["fechaInicio"], row["fechaFin"]) not in enviados_set
            ]

            # Sincronizar con Glide
            errores = 0

            for row in nuevos_registros:
                comando = [
                    "node",
                    "./glide_node_client/index.js",
                    json.dumps(row)
                ]
                try:
                    result = subprocess.run(comando, capture_output=True, text=True)
                    print(result.stdout)
                    if "success" in result.stdout:
                        enviados.append(row)  # Agregar a enviados si se envió correctamente
                    else:
                        errores += 1
                        print(f"❌ Error en registro: {row}")
                        print(f"Salida de Node.js: {result.stderr}")
                except Exception as e:
                    print(f"❌ Error al ejecutar Node.js: {e}")
                    errores += 1

            # Guardar los registros enviados actualizados
            with open(enviados_path, "w", encoding="utf-8") as f:
                json.dump(enviados, f, ensure_ascii=False, indent=4)

            # Guardar log
            log_path = f"./log_{nombre_fabrica}_{fecha}.txt"
            with open(log_path, "w", encoding="utf-8") as f:
                f.write(f"Fecha de sincronización: {fecha}\n")
                f.write(f"Archivo exportado: {output_path}\n")
                f.write(f"Total registros: {len(export_df)}\n")
                f.write(f"Errores: {errores}\n")

            print(f"📝 Log guardado en: {log_path}")

            # Enviar correo con el log
            destinatario = os.getenv("EMAIL_TO")
            enviar_email(nombre_fabrica, log_path, destinatario)

    except pyodbc.Error as e:
        print(f"❌ Error con la base de datos de {nombre_fabrica}: {e}")
    except Exception as e:
        print(f"❌ Error inesperado en {nombre_fabrica}: {e}")


# Procesar las bases de datos de JUM y NAFA
procesar_cronos(
    nombre_fabrica="JUM",
    ruta_mdb="/mnt/cronos/BASE DE DATOS/CronosXXI.mdb",
    creador_id="JUM-2833",
    prefijo_legajo="JUM-",
    username=os.getenv("CRONOS_USERNAME"),
    password=os.getenv("CRONOS_PASSWORD")
)

procesar_cronos(
    nombre_fabrica="NAFA",
    ruta_mdb="/mnt/nafa/BASE DE DATOS/CronosXXI.mdb",
    creador_id="NAF-2833",
    prefijo_legajo="NAF-",
    username=os.getenv("CRONOS_NAFA_USERNAME"),
    password=os.getenv("CRONOS_NAFA_PASSWORD")
)