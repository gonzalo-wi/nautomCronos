import os
from db_utils import procesar_cronos
from dotenv import load_dotenv


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

   
    procesar_cronos(
        nombre_fabrica="JUM",
        ruta_mdb="/mnt/jum/BASE DE DATOS/CronosXXI.mdb",
        creador_id="JUM-2833",
        prefijo_legajo="JUM-"
    )

    procesar_cronos(
        nombre_fabrica="NAFA",
        ruta_mdb="/mnt/nafa/BASE DE DATOS/CronosXXI.mdb",
        creador_id="NAF-2833",
        prefijo_legajo="NAF-"
    )