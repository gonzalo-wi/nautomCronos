from sqlalchemy import create_engine
import pyodbc
from datetime import datetime, date
from file_utils import guardar_log, cargar_enviados, guardar_enviados
from email_utils import enviar_email
import pandas as pd
import json
import os
import subprocess

def limpiar_fechas_dict(row):
    """
    Convierte las fechas en un diccionario al formato ISO (YYYY-MM-DD).
    """
    for k, v in row.items():
        if isinstance(v, (date, datetime, pd.Timestamp)):
            row[k] = v.strftime("%Y-%m-%d")
    return row

def procesar_cronos(nombre_fabrica, ruta_mdb, creador_id, prefijo_legajo):
    """
    Procesa las novedades de una fábrica, sincroniza con Glide y envía un correo con el log.
    """
    conn_str = r"DRIVER={MDBTools};" + fr"DBQ={ruta_mdb};"
    try:
        # Crear conexión con SQLAlchemy
        engine = create_engine(f"access+pyodbc:///?odbc_connect={conn_str}")

        # Leer las tablas necesarias
        novedades = pd.read_sql("SELECT * FROM Novedades", engine)
        rel_novedad_persona = pd.read_sql("SELECT * FROM Rel_Novedad_Persona", engine)
        personas = pd.read_sql("SELECT * FROM Personas", engine)
        justificaciones = pd.read_sql("SELECT * FROM justificaciones", engine)

        # Combinar tablas
        df = pd.merge(novedades, rel_novedad_persona, on="idNovedad")
        df = pd.merge(df, personas, on="idPersona")
        df = pd.merge(df, justificaciones, left_on="nov_justificacion", right_on="IdJustificacion", how="left")

        # Validar columnas necesarias
        if "jus_descripcion" not in df.columns:
            raise KeyError(f"La columna 'jus_descripcion' no está presente en los datos de {nombre_fabrica}.")
        if "per_legajo" not in df.columns:
            raise KeyError(f"La columna 'per_legajo' no está presente en los datos combinados de {nombre_fabrica}. Verifica la tabla Personas.")
        if df["per_legajo"].isnull().any():
            raise ValueError(f"Se encontraron valores nulos en la columna 'per_legajo' en los datos de {nombre_fabrica}.")

        # Depurar datos combinados
        print(f"🔍 Columnas en los datos combinados para {nombre_fabrica}:")
        print(df.columns)
        print(f"🔎 Primeros valores de 'per_legajo' en {nombre_fabrica}:")
        print(df["per_legajo"].head())

        if df["per_legajo"].isnull().all():
            raise ValueError(f"Todos los valores en 'per_legajo' son nulos en los datos de {nombre_fabrica}.")

        # Convertir 'per_legajo' a cadena para evitar problemas con ceros a la izquierda
        df["per_legajo"] = df["per_legajo"].astype(str)

        # Agregar columnas adicionales
        df["ID_Creador"] = creador_id
        print(f"🧪 Creando columna 'ID_Empleado' con prefijo: {prefijo_legajo}")
        df["ID_Empleado"] = prefijo_legajo + df["per_legajo"].str.zfill(4)
        print(f"✅ Primeros valores de 'ID_Empleado':")
        print(df["ID_Empleado"].head())

        # Renombrar columnas
        df.rename(columns={
            "jus_descripcion": "Novedad",
            "nov_Desde": "Fecha_inicio",
            "nov_Hasta": "Fecha_fin",
            "nov_DesdeStr": "Fecha_str"
        }, inplace=True)

        # Procesar fechas correctamente
        df["Fecha_str"] = df["Fecha_str"].fillna("01/01/1900")
        df["Fecha_str"] = df["Fecha_str"].where(df["Fecha_str"].str.match(r"^\d{2}/\d{2}/\d{4}$", na=False), "01/01/1900")
        df["Fecha_str"] = pd.to_datetime(df["Fecha_str"], format="%d/%m/%Y", errors="coerce")
        # Aquí aseguramos que ambas fechas estén en formato ISO
        df["Fecha_inicio"] = pd.to_datetime(df["Fecha_inicio"], format="%d/%m/%Y", errors="coerce").dt.strftime("%Y-%m-%d")
        df["Fecha_fin"] = pd.to_datetime(df["Fecha_fin"], errors="coerce").dt.strftime("%Y-%m-%d")

        # Verificar valores nulos después de la conversión
        if df["Fecha_str"].isnull().any():
            print(f"⚠️ Se encontraron valores nulos en Fecha_str después de la conversión.")
            df["Fecha_str"] = df["Fecha_str"].fillna(pd.Timestamp("1900-01-01"))

        # Filtrar por fechas
        hoy = datetime.now().date()
        df = df[pd.to_datetime(df["Fecha_fin"], errors="coerce") >= pd.Timestamp(hoy)]

        # Mostrar columnas antes de exportar
        print(f"📦 Columnas disponibles antes de exportar en {nombre_fabrica}:")
        print(df.columns)

        # Preparar datos para exportar
        required_columns = ["ID_Creador", "Novedad", "ID_Empleado", "Fecha_inicio", "Fecha_fin", "Fecha_str"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise KeyError(f"Faltan las siguientes columnas en los datos combinados: {missing_columns}")

        export_df = df[required_columns].copy()

        # Renombrar columnas para Glide
        export_df = export_df.rename(columns={
            "ID_Creador": "idCreador",
            "Novedad": "novedad",
            "ID_Empleado": "idEmpleado",
            "Fecha_inicio": "fechaInicio",
            "Fecha_fin": "fechaFin",
            "Fecha_str": "fechaCreaciN"
        })

        # Validar las fechas antes de enviarlas
        for col in ["fechaInicio", "fechaFin", "fechaCreaciN"]:
            if export_df[col].isnull().any():
                raise ValueError(f"Se encontraron valores no válidos en la columna {col}.")

        if export_df.empty:
            print(f"📭 No se encontraron novedades para sincronizar en {nombre_fabrica}.")
            return

        print(f"🔍 Datos en export_df para {nombre_fabrica}:")
        print(export_df.head())

        # Manejo de enviados
        enviados_path = f"enviados_{nombre_fabrica.lower()}.json"
        enviados = cargar_enviados(enviados_path)

        enviados_set = set()
        for e in enviados:
            try:
                clave = (e.get("idEmpleado"), e.get("novedad"), e.get("fechaInicio"), e.get("fechaFin"))
                enviados_set.add(clave)
            except KeyError as err:
                print(f"⚠️ Registro omitido en enviados por falta de campo: {err}")
                continue

        nuevos_registros = [
            limpiar_fechas_dict(row)
            for row in export_df.to_dict(orient="records")
            if (row["idEmpleado"], row["novedad"], row["fechaInicio"], row["fechaFin"]) not in enviados_set
        ]

        # Convertir fechas a formato ISO (por si acaso)
        for row in nuevos_registros:
            for campo in ["fechaInicio", "fechaFin", "fechaCreaciN"]:
                if isinstance(row[campo], (datetime, date, pd.Timestamp)):
                    row[campo] = row[campo].isoformat()
                elif row[campo] is None or row[campo] == "":
                    row[campo] = "1900-01-01"

        print(f"🔍 Nuevos registros para sincronizar en {nombre_fabrica}:")
        print(nuevos_registros)

        with open("a_enviar.json", "w", encoding="utf-8") as f:
            json.dump(nuevos_registros, f, ensure_ascii=False, indent=2)

        # Sincronizar con Glide
        try:
            result = subprocess.run(
                ["node", "glide_node_client/index.js", "bulk"],
                capture_output=True, text=True
            )
            print(result.stdout)
            if result.stderr:
                print("⚠️ STDERR:", result.stderr)
            errores = []
        except Exception as e:
            print(f"❌ Error al ejecutar Node.js para sincronizar con Glide: {e}")
            errores = [str(e)]

        # Guardar enviados después de sincronizar
        if nuevos_registros:
            guardar_enviados(enviados + nuevos_registros, enviados_path)

        # Guardar log
        fecha = datetime.now().strftime("%Y-%m-%d")
        log_path = guardar_log(nombre_fabrica, fecha, export_df, errores)

        # Enviar correo con el log
        destinatario = os.getenv("EMAIL_TO")
        enviar_email(nombre_fabrica, log_path, destinatario)

    except pyodbc.Error as e:
        print(f"❌ Error con la base de datos de {nombre_fabrica}: {e}")
    except Exception as e:
        print(f"❌ Error inesperado en {nombre_fabrica}: {e}")