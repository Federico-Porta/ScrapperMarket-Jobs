import json
import os
import requests
import time

# ---------------- CONFIG ----------------

API_URL = "http://localhost:8080/api/products/import"
API_KEY = "clave_secreta_optify"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CARPETA_JSONS = BASE_DIR

BATCH_SIZE = 100
SLEEP_SECONDS = 0.5

HEADERS = {
    "Content-Type": "application/json",
    "X-API-KEY": API_KEY
}

BATCH_OK_DIR = os.path.join(BASE_DIR, "../Jobs/batches_ok")
BATCH_ERROR_DIR = os.path.join(BASE_DIR, "../Jobs/batches_error")

os.makedirs(BATCH_OK_DIR, exist_ok=True)
os.makedirs(BATCH_ERROR_DIR, exist_ok=True)

# ----------------------------------------


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
        except Exception as e:
            print(f"❌ Error leyendo {archivo}: {e}")

    return productos


def validar_producto(p):
    errores = []

    if not p.get("productName"):
        errores.append("SIN_NOMBRE")

    if p.get("productPrice") is None:
        errores.append("SIN_PRECIO")

    if not p.get("categoryName"):
        errores.append("SIN_CATEGORIA")

    return errores


def enviar_batch(batch, numero, errores_batch):
    batch_file_name = f"batch_{numero:03d}.json"

    try:
        res = requests.post(API_URL, headers=HEADERS, json=batch, timeout=30)

        if res.status_code in (200, 201):
            print(f"✅ Batch {numero} OK ({len(batch)})")

            with open(os.path.join(BATCH_OK_DIR, batch_file_name), "w", encoding="utf-8") as f:
                json.dump(batch, f, ensure_ascii=False, indent=4)

            return True

        else:
            print(f"❌ Batch {numero} ERROR {res.status_code}")

            with open(os.path.join(BATCH_ERROR_DIR, batch_file_name), "w", encoding="utf-8") as f:
                json.dump(batch, f, ensure_ascii=False, indent=4)

            errores_batch.append({
                "batch": numero,
                "status": res.status_code,
                "response": res.text
            })

            return False

    except Exception as e:
        print(f"🔥 Batch {numero} EXCEPTION: {e}")

        with open(os.path.join(BATCH_ERROR_DIR, batch_file_name), "w", encoding="utf-8") as f:
            json.dump(batch, f, ensure_ascii=False, indent=4)

        errores_batch.append({
            "batch": numero,
            "status": "EXCEPTION",
            "response": str(e)
        })

        return False


def main():
    print("🚀 Iniciando importación en modo DEBUG")

    productos = cargar_jsons(CARPETA_JSONS)
    print(f"📦 Productos cargados: {len(productos)}")

    # Validación previa
    validos = []
    for p in productos:
        if not validar_producto(p):
            validos.append(p)

    print(f"✅ Productos válidos: {len(validos)}")

    errores_batch = []
    batch_num = 1

    for i in range(0, len(validos), BATCH_SIZE):
        batch = validos[i:i + BATCH_SIZE]
        enviar_batch(batch, batch_num, errores_batch)
        batch_num += 1
        time.sleep(SLEEP_SECONDS)

    if errores_batch:
        with open("batch_errors_log.json", "w", encoding="utf-8") as f:
            json.dump(errores_batch, f, ensure_ascii=False, indent=4)

    print("\n✨ Proceso finalizado")
    print(f"📊 Batches con error: {len(errores_batch)}")


if __name__ == "__main__":
    main()
