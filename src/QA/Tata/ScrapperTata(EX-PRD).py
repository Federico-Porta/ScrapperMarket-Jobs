import requests
import json
import time
import os
from concurrent.futures import ThreadPoolExecutor

# =========================================================
# CONFIGURACIÓN GENERAL DEL SCRAPER TATA
# =========================================================

TATA_RUT = "210003270017"
MAX_WORKERS = 10

CATEGORIAS = [
    "Almacen",
    "Frescos",
    "Congelados",
    "Limpieza",
    "Bebidas",
    "Perfumeria"
]

# =========================================================
# CONFIGURACIÓN DE RUTAS Y ARCHIVOS
# =========================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../Jobs", "JsonProducts"))
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "productos_tata.json")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =========================================================
# FUNCIÓN: extraer_categoria
# =========================================================
def extraer_categoria(categoria_slug):

    productos_categoria = []
    page_size = 50
    after = 0

    print(f"🚀 [Hilo Iniciado] Extrayendo: {categoria_slug}")

    while True:
        url = "https://www.tata.com.uy/api/graphql"

        variables = {
            "first": page_size,
            "after": str(after),
            "sort": "score_desc",
            "term": "",
            "selectedFacets": [
                {"key": "category-1", "value": categoria_slug},
                {"key": "channel", "value": "{\"salesChannel\":\"4\",\"regionId\":\"U1cjdGF0YXV5bW9udGV2aWRlbw==\"}"},
                {"key": "locale", "value": "es-UY"}
            ]
        }

        params = {
            "operationName": "ProductsQuery",
            "variables": json.dumps(variables)
        }

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }

        try:
            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=20
            )

            data = response.json()

            search_data = data.get('data', {}).get('search', {})
            if not search_data:
                break

            edges = search_data.get('products', {}).get('edges', [])
            total_count = search_data.get('products', {}).get('pageInfo', {}).get('totalCount', 0)

            if not edges:
                break

            for edge in edges:
                node = edge.get('node', {})
                offers = node.get('offers', {}).get('offers', [{}])[0]

                # ✅ CAMBIO REAL: la URL viene en "link"
                link = node.get("slug")
                product_url = f"https://www.tata.com.uy/{link}/p" if link else None

                productos_categoria.append({
                    "idWeb": node.get('gtin'),
                    "productName": node.get('name'),
                    "productDescription": node.get('name'),
                    "productBrand": node.get('brand', {}).get('name'),
                    "productPrice": offers.get('price'),
                    "moneda": node.get('offers', {}).get('priceCurrency', 'UYU'),
                    "storeRut": TATA_RUT,
                    "urlProduct": product_url,
                    "productImageUrl": node.get('image', [{}])[0].get('url'),
                    "categoryName": categoria_slug.replace("-", " ").capitalize()
                })

            if len(productos_categoria) >= total_count:
                break

            after += page_size
            time.sleep(0.2)

        except Exception as e:
            print(f"❌ Error en {categoria_slug}: {e}")
            break

    print(f"✅ [Hilo Finalizado] {categoria_slug}: {len(productos_categoria)} items.")
    return productos_categoria

# =========================================================
# FUNCIÓN PRINCIPAL
# =========================================================
def ejecutar_scrapper_masivo():

    todos_los_productos = []
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        resultados = list(executor.map(extraer_categoria, CATEGORIAS))

        for lista in resultados:
            todos_los_productos.extend(lista)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(todos_los_productos, f, ensure_ascii=False, indent=4)

    total_time = time.time() - start_time

    print(f"\n--- SCRAPING COMPLETADO EN {total_time:.2f} SEGUNDOS ---")
    print(f"📦 Total de productos recolectados: {len(todos_los_productos)}")
    print(f"📂 Archivo generado: {OUTPUT_FILE}")

# =========================================================
# PUNTO DE ENTRADA
# =========================================================
if __name__ == "__main__":
    ejecutar_scrapper_masivo()
