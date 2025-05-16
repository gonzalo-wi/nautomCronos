import json
import os

def cargar_enviados(enviados_path):
    if os.path.exists(enviados_path):
        with open(enviados_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def limpiar_fechas(diccionario):
    for k, v in diccionario.items():
        if isinstance(v, (date, datetime, pd.Timestamp)):
            diccionario[k] = v.strftime('%Y-%m-%d')
    return diccionario

def guardar_enviados(enviados, enviados_path):
    def serializar(obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return obj  # cualquier otro tipo queda igual

    with open(enviados_path, "w", encoding="utf-8") as f:
        json.dump(enviados, f, ensure_ascii=False, indent=4, default=serializar)

def guardar_log(nombre_fabrica, fecha, export_df, errores):
    log_path = f"./log_{nombre_fabrica}_{fecha}.txt"
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"Fecha de sincronización: {fecha}\n")
        f.write(f"Total registros: {len(export_df)}\n")
        f.write(f"Errores: {errores}\n")
    print(f"📝 Log guardado en: {log_path}")
    return log_path