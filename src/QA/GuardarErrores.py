import json
import os
import requests
import time
import shutil

# ---------------- CONFIG ----------------
API_URL = "http://localhost:8080/api/products/import"
API_KEY = "clave_secreta_optify"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "Jobs", "JsonProductos"))
BATCH_DIR = os.path.join(BASE_DIR, "../Procesos/Batches")  # Carpeta para guardar batches fallidos

BATCH_SIZE = 100
SLEEP_SECONDS = 0.5

HEADERS = {
    "Content-Type": "application/json",
    "X-API-KEY": API_KEY
}

# ----------------------------------------

def limpiar_carpeta(path):
    """Elimina todo el contenido de la carpeta para empezar limpio."""
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)

def cargar_jsons(carpeta):
    productos = []
    for archivo in os.listdir(carpeta):
        if not archivo.lower().endswith(".json"):
            continue

        ruta = os.path.join(carpeta, archivo)
        print(f"📂 Leyendo {archivo}...")

        try:
            with open(ruta, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    productos.extend(data)
                else:
                    print(f"⚠️ {archivo} no es una lista, se ignora")
        except Exception as e:
            print(f"❌ Error leyendo {archivo}: {e}")

    return productos

def enviar_batch(batch, numero):
    try:
        res = requests.post(API_URL, headers=HEADERS, json=batch, timeout=30)

        if res.status_code in (200, 201):
            print(f"✅ Batch {numero} enviado correctamente ({len(batch)} productos)")
            return True
        else:
            print(f"❌ Batch {numero} falló | Status: {res.status_code}")
            print(res.text)
            # Guardamos solo los batch fallidos
            batch_file = os.path.join(BATCH_DIR, f"batch_{numero}.json")
            with open(batch_file, "w", encoding="utf-8") as f:
                json.dump(batch, f, ensure_ascii=False, indent=4)
            return False

    except Exception as e:
        print(f"🔥 Error enviando batch {numero}: {e}")
        batch_file = os.path.join(BATCH_DIR, f"batch_{numero}.json")
        with open(batch_file, "w", encoding="utf-8") as f:
            json.dump(batch, f, ensure_ascii=False, indent=4)
        return False

def main():
    print("🚀 Iniciando procesamiento de JSONs...")

    # Limpiamos la carpeta de batches al inicio
    limpiar_carpeta(BATCH_DIR)

    productos = cargar_jsons(JSON_DIR)
    total = len(productos)

    if total == 0:
        print("❌ No se encontraron productos")
        return

    print(f"📦 Total de productos cargados: {total}")

    batch_num = 1
    enviados = 0
    fallidos = 0

    for i in range(0, total, BATCH_SIZE):
        batch = productos[i:i + BATCH_SIZE]
        exito = enviar_batch(batch, batch_num)
        if exito:
            enviados += len(batch)
        else:
            fallidos += len(batch)

        batch_num += 1
        time.sleep(SLEEP_SECONDS)

    print("\n📊 Resumen:")
    print(f"   - Totales: {total}")
    print(f"   - Válidos enviados: {enviados}")
    print(f"   - Fallidos: {fallidos}")
    print(f"   - Archivos de batches fallidos guardados en: {BATCH_DIR}")
    print("✨ Proceso finalizado")

if __name__ == "__main__":
    main()
