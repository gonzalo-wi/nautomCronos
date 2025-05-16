import subprocess
import json
import os

def sincronizar_con_glide(nuevos_registros, enviados, enviados_path):
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

    return errores