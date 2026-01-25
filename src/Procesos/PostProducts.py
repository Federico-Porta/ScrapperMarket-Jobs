import json
import os
import requests
import time
import shutil

# =========================================================
# CONFIGURACIÓN GENERAL
# =========================================================

# URL de la API donde se envían los productos
# En este caso es un endpoint local, pero en cloud
# normalmente será una URL pública o privada
API_URL = "http://localhost:8080/api/products/import"

# Clave de seguridad para autenticar la petición
# Se envía en el header X-API-KEY
API_KEY = "clave_secreta_optify"

# BASE_DIR:
# Ruta absoluta de la carpeta donde está este script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# JSON_DIR:
# Carpeta donde están los archivos JSON con productos
# Se asume esta estructura:
#   Jobs/JsonProducts/*.json
JSON_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "Jobs", "JsonProducts"))

# BATCH_DIR:
# Carpeta donde se guardan SOLO los batches que fallan
# Sirve para reintentos o auditoría
BATCH_DIR = os.path.join(BASE_DIR, "Batches")

# BATCH_SIZE:
# Cantidad de productos enviados por request
# Evita sobrecargar la API
BATCH_SIZE = 100

# SLEEP_SECONDS:
# Pausa entre cada envío de batch
# Evita saturar el servidor
SLEEP_SECONDS = 0.5

# HEADERS:
# Cabeceras HTTP que se envían en cada request
# - Content-Type: indica que se envía JSON
# - X-API-KEY: autenticación
HEADERS = {
    "Content-Type": "application/json",
    "X-API-KEY": API_KEY
}

# =========================================================
# FUNCIÓN: limpiar_carpeta
# =========================================================
def limpiar_carpeta(path):
    """
    Elimina completamente una carpeta y la vuelve a crear.
    Se usa para empezar cada ejecución desde cero,
    evitando mezclar batches viejos con nuevos.
    """
    if os.path.exists(path):
        shutil.rmtree(path)  # borra todo el contenido
    os.makedirs(path, exist_ok=True)  # crea la carpeta nuevamente


# =========================================================
# FUNCIÓN: cargar_jsons
# =========================================================
def cargar_jsons(carpeta):
    """
    Lee TODOS los archivos .json de una carpeta.
    Cada archivo debe contener una LISTA de productos.
    Devuelve una lista única con todos los productos.
    """

    productos = []

    # Recorre todos los archivos de la carpeta
    for archivo in os.listdir(carpeta):

        # Ignora cualquier archivo que no sea .json
        if not archivo.lower().endswith(".json"):
            continue

        ruta = os.path.join(carpeta, archivo)
        print(f"📂 Leyendo {archivo}...")

        try:
            # Abre el archivo JSON
            with open(ruta, "r", encoding="utf-8") as f:
                data = json.load(f)

                # Si el contenido es una lista → se agrega
                if isinstance(data, list):
                    productos.extend(data)
                else:
                    # Si no es una lista, se ignora el archivo
                    print(f"⚠️ {archivo} no es una lista, se ignora")

        except Exception as e:
            # Error de lectura o JSON inválido
            print(f"❌ Error leyendo {archivo}: {e}")

    return productos


# =========================================================
# FUNCIÓN: enviar_batch
# =========================================================
def enviar_batch(batch, numero):
    """
    Envía un batch de productos a la API.
    Si falla:
    - guarda el batch en disco
    - devuelve False
    """

    try:
        # Envío HTTP POST a la API
        res = requests.post(
            API_URL,
            headers=HEADERS,
            json=batch,
            timeout=30
        )

        # Si la API responde OK
        if res.status_code in (200, 201):
            print(f"✅ Batch {numero} enviado correctamente ({len(batch)} productos)")
            return True
        else:
            # Error de la API (400, 500, etc)
            print(f"❌ Batch {numero} falló | Status: {res.status_code}")
            print(res.text)

            # Guardamos SOLO los batches fallidos
            batch_file = os.path.join(BATCH_DIR, f"batch_{numero}.json")
            with open(batch_file, "w", encoding="utf-8") as f:
                json.dump(batch, f, ensure_ascii=False, indent=4)

            return False

    except Exception as e:
        # Error de red, timeout, API caída, etc
        print(f"🔥 Error enviando batch {numero}: {e}")

        # Guardamos el batch para no perder información
        batch_file = os.path.join(BATCH_DIR, f"batch_{numero}.json")
        with open(batch_file, "w", encoding="utf-8") as f:
            json.dump(batch, f, ensure_ascii=False, indent=4)

        return False


# =========================================================
# FUNCIÓN PRINCIPAL
# =========================================================
def main():
    print("🚀 Iniciando procesamiento de JSONs...")

    # Limpia la carpeta de batches al inicio
    limpiar_carpeta(BATCH_DIR)

    # Carga todos los productos desde los JSON
    productos = cargar_jsons(JSON_DIR)
    total = len(productos)

    # Si no hay productos, se corta el proceso
    if total == 0:
        print("❌ No se encontraron productos")
        return

    print(f"📦 Total de productos cargados: {total}")

    batch_num = 1
    enviados = 0
    fallidos = 0

    # Recorre los productos de a BATCH_SIZE
    for i in range(0, total, BATCH_SIZE):
        batch = productos[i:i + BATCH_SIZE]

        exito = enviar_batch(batch, batch_num)

        if exito:
            enviados += len(batch)
        else:
            fallidos += len(batch)

        batch_num += 1

        # Pausa entre envíos
        time.sleep(SLEEP_SECONDS)

    # Resumen final
    print("\n📊 Resumen:")
    print(f"   - Totales: {total}")
    print(f"   - Válidos enviados: {enviados}")
    print(f"   - Fallidos: {fallidos}")
    print(f"   - Archivos de batches fallidos guardados en: {BATCH_DIR}")
    print("✨ Proceso finalizado")


# =========================================================
# PUNTO DE ENTRADA
# =========================================================
if __name__ == "__main__":
    main()
