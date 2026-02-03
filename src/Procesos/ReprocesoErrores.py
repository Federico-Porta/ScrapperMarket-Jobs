import json
import os
import requests
import time
import shutil

# =========================================================
# CONFIGURACIÓN GENERAL
# =========================================================

# URL de la API donde se envían los productos
# valída si está en la nube y también por si lo estamos ejecutando en local
API_URL = os.getenv(
    "API_URL",
    "http://localhost:8080/api/products/import"
)

# Clave de seguridad para autenticar la petición
# Se envía en el header X-API-KEY o lo obtiene de la variable de entorno
API_KEY = os.getenv("API_KEY", "clave_secreta_optify")

# BASE_DIR:
# Ruta absoluta de la carpeta donde está ubicado este script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# BATCH_DIR:
# Carpeta donde están los archivos JSON de batches fallidos
# Cada archivo contiene una LISTA de productos
BATCH_DIR = os.path.join(BASE_DIR, "batches")

# ERROR_DIR:
# Carpeta donde se guardan los productos que siguen fallando
# luego de intentar reprocesarlos uno por uno
ERROR_DIR = os.path.join(BASE_DIR, "batches_errores")

# SLEEP_SECONDS:
# Pausa entre el envío de cada producto individual
# Evita saturar la API
SLEEP_SECONDS = 0.2

# HEADERS:
# Headers HTTP enviados en cada request
# - Content-Type: formato JSON
# - X-API-KEY: autenticación
HEADERS = {
    "Content-Type": "application/json",
    "X-API-KEY": API_KEY
}

# =========================================================
# FUNCIÓN: asegurar_carpeta
# =========================================================
def asegurar_carpeta(path):
    """
    Garantiza que una carpeta exista.
    Si no existe, la crea.
    Si existe, no hace nada.
    """
    os.makedirs(path, exist_ok=True)


# =========================================================
# FUNCIÓN: enviar_producto
# =========================================================
def enviar_producto(producto):
    """
    Envía UN solo producto a la API.

    IMPORTANTE:
    - La API espera una LISTA de productos,
      por eso se envía [producto] y no producto solo.
    - Devuelve True si el envío fue exitoso.
    """

    try:
        res = requests.post(
            API_URL,
            headers=HEADERS,
            json=[producto],   # 👈 siempre se envía como lista
            timeout=15
        )

        # Si la API responde 200 o 201 → OK
        return res.status_code in (200, 201)

    except Exception:
        # Error de red, timeout, API caída, etc
        return False


# =========================================================
# FUNCIÓN: procesar_batch
# =========================================================
def procesar_batch(path_batch):
    """
    Reprocesa un archivo batch completo.
    Lee el JSON, intenta enviar cada producto individualmente
    y guarda solo los productos que siguen fallando.
    """

    # Obtiene el nombre del archivo sin la ruta
    nombre = os.path.basename(path_batch)
    print(f"\n🔁 Procesando {nombre}...")

    # Carga los productos del archivo JSON
    with open(path_batch, "r", encoding="utf-8") as f:
        productos = json.load(f)

    # Lista donde se guardan los productos con error
    errores = []

    # Recorre los productos uno por uno
    for idx, producto in enumerate(productos, start=1):

        ok = enviar_producto(producto)

        if ok:
            print(f"   ✔ Producto {idx} OK")
        else:
            print(f"   ❌ Producto {idx} ERROR")
            errores.append(producto)

        # Pausa entre productos
        time.sleep(SLEEP_SECONDS)

    # Si hubo errores, se guardan en un nuevo archivo
    if errores:
        error_file = os.path.join(
            ERROR_DIR,
            nombre.replace(".json", "_errores.json")
        )

        with open(error_file, "w", encoding="utf-8") as f:
            json.dump(errores, f, ensure_ascii=False, indent=4)

        print(f"💾 Guardados {len(errores)} productos erróneos")
    else:
        print("🎉 Batch completamente procesado sin errores")

    # El batch original se elimina SIEMPRE
    # (ya fue procesado exitosamente o no)
    os.remove(path_batch)


# =========================================================
# FUNCIÓN PRINCIPAL
# =========================================================
def main():
    # Se asegura que la carpeta de errores exista
    asegurar_carpeta(ERROR_DIR)

    # Obtiene todos los archivos .json dentro de la carpeta batches
    batches = sorted(
        f for f in os.listdir(BATCH_DIR)
        if f.lower().endswith(".json")
    )

    # Si no hay batches pendientes, termina el proceso
    if not batches:
        print("✅ No hay batches pendientes")
        return

    print(f"📦 Batches a reprocesar: {len(batches)}")

    # Procesa cada batch uno por uno
    for batch_file in batches:
        procesar_batch(os.path.join(BATCH_DIR, batch_file))

    print("\n✨ Reproceso finalizado")


# =========================================================
# PUNTO DE ENTRADA
# =========================================================
# Este bloque asegura que el script se ejecute
# solo cuando se corre directamente
if __name__ == "__main__":
    main()
