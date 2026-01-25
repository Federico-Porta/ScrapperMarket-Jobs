import json
import os
import requests
import time
import shutil

# ---------------- CONFIG ----------------
API_URL = "http://localhost:8080/api/products/import"
API_KEY = "clave_secreta_optify"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BATCH_DIR = os.path.join(BASE_DIR, "batches")
ERROR_DIR = os.path.join(BASE_DIR, "batches_errores")

SLEEP_SECONDS = 0.2

HEADERS = {
    "Content-Type": "application/json",
    "X-API-KEY": API_KEY
}
# ----------------------------------------

def asegurar_carpeta(path):
    os.makedirs(path, exist_ok=True)

def enviar_producto(producto):
    try:
        res = requests.post(
            API_URL,
            headers=HEADERS,
            json=[producto],   # 👈 la API espera lista
            timeout=15
        )
        return res.status_code in (200, 201)
    except Exception:
        return False

def procesar_batch(path_batch):
    nombre = os.path.basename(path_batch)
    print(f"\n🔁 Procesando {nombre}...")

    with open(path_batch, "r", encoding="utf-8") as f:
        productos = json.load(f)

    errores = []

    for idx, producto in enumerate(productos, start=1):
        ok = enviar_producto(producto)
        if ok:
            print(f"   ✔ Producto {idx} OK")
        else:
            print(f"   ❌ Producto {idx} ERROR")
            errores.append(producto)

        time.sleep(SLEEP_SECONDS)

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

    # Eliminamos el batch original siempre
    os.remove(path_batch)

def main():
    asegurar_carpeta(ERROR_DIR)

    batches = sorted(
        f for f in os.listdir(BATCH_DIR)
        if f.lower().endswith(".json")
    )

    if not batches:
        print("✅ No hay batches pendientes")
        return

    print(f"📦 Batches a reprocesar: {len(batches)}")

    for batch_file in batches:
        procesar_batch(os.path.join(BATCH_DIR, batch_file))

    print("\n✨ Reproceso finalizado")

if __name__ == "__main__":
    main()
