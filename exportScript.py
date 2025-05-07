import pyodbc
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import subprocess
import os
import json

username = os.getenv("CRONOS_USERNAME")
password = os.getenv("CRONOS_PASSWORD")
db_path  = r"/mnt/cronos/BASE DE DATOS/CronosXXI.mdb"
conn_str = r"DRIVER={MDBTools};" + fr"DBQ={db_path};"


try:
    if not os.path.ismount("/mnt/cronos"):
        subprocess.run([
        "sudo", "mount", "-t", "cifs",
        "//192.168.0.9/CRONOS", "/mnt/cronos",
        f"-o,username={username},password={password},vers=3.0"
        ], check=True)
        print("Recurso compartido montado correctamente.")
    else:
        print("El recurso compartido ya está montado.")
except subprocess.CalledProcessError as e:
    print(f"Error al montar el recurso compartido: {e}")
    exit()


try:
    with pyodbc.connect(conn_str) as conn:
        novedades = pd.read_sql("SELECT * FROM Novedades", conn)
        rel_novedad_persona = pd.read_sql("SELECT * FROM Rel_Novedad_Persona", conn)
        personas = pd.read_sql("SELECT * FROM Personas", conn)
        justificaciones = pd.read_sql("SELECT * FROM justificaciones", conn)

        df = pd.merge(novedades, rel_novedad_persona, on="idNovedad")
        df = pd.merge(df, personas, on="idPersona")
        df = pd.merge(df, justificaciones, left_on="nov_justificacion", right_on="IdJustificacion")

        df["ID_Creador"] = "JUM-2833"
        df["ID_Empleado"] = "JUM-" + df["per_legajo"].astype(str).str[-4:]

        df.rename(columns={
            "jus_descripcion": "Novedad",
            "nov_Desde": "Fecha_inicio",
            "nov_Hasta": "Fecha_fin",
            "nov_DesdeStr": "Fecha_str"
        }, inplace=True)

        df["Fecha_str"] = pd.to_datetime(df["Fecha_str"].astype(str), format="%Y%m%d")

        hoy = datetime.now()
        inicio = hoy - timedelta(days=7)
        fin = hoy + timedelta(days=7)
        df = df[(df["Fecha_str"] >= inicio) & (df["Fecha_str"] <= fin)]

        export_df = df[["ID_Creador", "Novedad", "ID_Empleado", "Fecha_inicio", "Fecha_fin", "Fecha_str"]].copy()

        
        fecha = datetime.now().strftime("%Y-%m-%d")
        output_path = f"./cronos_export_{fecha}.csv"
        if export_df.empty:
            print("No se encontraron novedades para sincronizar.")
            exit()
        export_df.to_csv(output_path, index=False, encoding="utf-8-sig")

        
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

        
        for campo in ["fechaInicio", "fechaFin", "fechaCreaciN"]:
            export_df[campo] = pd.to_datetime(export_df[campo]).dt.strftime("%Y-%m-%d")

       
        errores = 0
        for row in export_df.to_dict(orient="records"):
            comando = [
                "node",
                "./glide_node_client/index.js",  
                json.dumps(row)
            ]
            try:
                result = subprocess.run(comando, capture_output=True, text=True)
                print(result.stdout)
                if "success" not in result.stdout:
                    errores += 1
            except Exception as e:
                print(f"Error al ejecutar Node.js: {e}")
                errores += 1

        
        log_path = f"./log_{fecha}.txt"
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"Fecha de sincronización: {fecha}\n")
            f.write(f"Archivo exportado: {output_path}\n")
            f.write(f"Total registros: {len(export_df)}\n")
            f.write(f"Errores: {errores}\n")

        print("Sincronización finalizada.")
        print(f"Log generado en: {log_path}")

except pyodbc.Error as e:
    print(f"Error con la base de datos: {e}")
except Exception as e:
    print(f"Error inesperado: {e}")