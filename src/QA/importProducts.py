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
                else:
                    print(f"⚠️ {archivo} no es una lista, se ignora")

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


def enviar_batch(batch, numero, fallidos_api):
    try:
        res = requests.post(API_URL, headers=HEADERS, json=batch, timeout=30)

        if res.status_code in (200, 201):
            print(f"✅ Batch {numero} enviado OK ({len(batch)} productos)")
            return True
        else:
            print(f"❌ Batch {numero} falló | Status: {res.status_code}")
            print(res.text)
            fallidos_api.extend(batch)
            return False

    except Exception as e:
        print(f"🔥 Error enviando batch {numero}: {e}")
        fallidos_api.extend(batch)
        return False


def main():
    print("🚀 Iniciando procesamiento de JSONs...")

    productos = cargar_jsons(CARPETA_JSONS)
    total = len(productos)

    if total == 0:
        print("❌ No se encontraron productos")
        return

    print(f"📦 Total de productos cargados: {total}")

    # --- VALIDACIÓN ---
    validos = []
    invalidos = []

    for p in productos:
        errores = validar_producto(p)
        if errores:
            p["_errores"] = errores
            invalidos.append(p)
        else:
            validos.append(p)

    print(f"✅ Productos válidos: {len(validos)}")
    print(f"❌ Productos inválidos: {len(invalidos)}")

    # Guardar inválidos
    if invalidos:
        with open("productos_invalidos.json", "w", encoding="utf-8") as f:
            json.dump(invalidos, f, ensure_ascii=False, indent=4)

    # --- ENVÍO ---
    fallidos_api = []
    batch_num = 1

    for i in range(0, len(validos), BATCH_SIZE):
        batch = validos[i:i + BATCH_SIZE]
        enviar_batch(batch, batch_num, fallidos_api)
        batch_num += 1
        time.sleep(SLEEP_SECONDS)

    # Guardar fallidos de API
    if fallidos_api:
        with open("productos_api_fallidos.json", "w", encoding="utf-8") as f:
            json.dump(fallidos_api, f, ensure_ascii=False, indent=4)

    print("\n✨ Proceso finalizado")
    print(f"📊 Resumen:")
    print(f"   - Totales: {total}")
    print(f"   - Válidos enviados: {len(validos)}")
    print(f"   - Inválidos: {len(invalidos)}")
    print(f"   - Fallidos API: {len(fallidos_api)}")


if __name__ == "__main__":
    main()
