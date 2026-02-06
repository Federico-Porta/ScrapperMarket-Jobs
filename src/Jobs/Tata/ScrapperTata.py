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

CATEGORIAS = {
    "Almacen": [
        "Desayuno",
        "snacks",
        "aceites-y-aderezos",
        "conservas",
        "arroz-harina-y-legumbres",
        "sopas-caldos-y-pure",
        "golosinas-y-chocolates",
        "panificados",
        "pastas-y-salsas",
        "cigarros"
    ],
    "Frescos": [],
    "Congelados": [],
    "Limpieza": [],
    "Bebidas": [],
    "Perfumeria": []
}

# =========================================================
# CONFIGURACIÓN DE RUTAS Y ARCHIVOS
# =========================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# JOBS_DIR:
# Carpeta superior donde se guardan los JSON de salida
JOBS_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))

# JSON_DIR:
# Carpeta donde se escribirá el archivo final
JSON_DIR = os.path.join(JOBS_DIR, "JsonProducts")

# Se asegura que exista la carpeta
os.makedirs(JSON_DIR, exist_ok=True)

# Archivo final con todos los productos
OUTPUT_JSON = os.path.join(JSON_DIR, "productos_tata.json")



# =========================================================
# FUNCIÓN: extraer_categoria
# =========================================================
def extraer_categoria(categoria_padre, subcategoria_slug=None):

    productos_categoria = []
    page_size = 50
    after = 0

    nombre_log = (
        f"{categoria_padre} → {subcategoria_slug}"
        if subcategoria_slug else categoria_padre
    )

    print(f"🚀 [Hilo Iniciado] Extrayendo: {nombre_log}")

    while True:
        url = "https://www.tata.com.uy/api/graphql"

        selected_facets = [
            {"key": "channel", "value": "{\"salesChannel\":\"4\",\"regionId\":\"U1cjdGF0YXV5bW9udGV2aWRlbw==\"}"},
            {"key": "locale", "value": "es-UY"}
        ]

        if subcategoria_slug:
            selected_facets.insert(0, {"key": "category-2", "value": subcategoria_slug})
        else:
            selected_facets.insert(0, {"key": "category-1", "value": categoria_padre})

        variables = {
            "first": page_size,
            "after": str(after),
            "sort": "score_desc",
            "term": "",
            "selectedFacets": selected_facets
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

                link = node.get("slug")
                product_url = f"https://www.tata.com.uy/{link}/p" if link else None

                productos_categoria.append({
                    "idWeb": int(node['gtin']) if node.get('gtin') else None,
                    "productName": node.get('name'),
                    "productDescription": node.get('name'),
                    "productBrand": node.get('brand', {}).get('name'),
                    "productPrice": offers.get('price'),
                    "moneda": node.get('offers', {}).get('priceCurrency', 'UYU'),
                    "storeRut": TATA_RUT,
                    "urlProduct": product_url,
                    "productImageUrl": node.get('image', [{}])[0].get('url'),
                    "categoryName": categoria_padre   # 👈 SIEMPRE categoría padre
                })

            if len(productos_categoria) >= total_count:
                break

            after += page_size
            time.sleep(0.2)

        except Exception as e:
            print(f"❌ Error en {nombre_log}: {e}")
            break

    print(f"✅ [Hilo Finalizado] {nombre_log}: {len(productos_categoria)} items.")
    return productos_categoria

# =========================================================
# FUNCIÓN: DEDUPLICAR POR GTIN
# =========================================================
def deduplicar_productos(productos):
    productos_unicos = {}
    for producto in productos:
        gtin = producto.get("idWeb")
        if gtin:
            productos_unicos[gtin] = producto
    return list(productos_unicos.values())

# =========================================================
# FUNCIÓN PRINCIPAL
# =========================================================
def ejecutar_scrapper_masivo():

    todos_los_productos = []
    start_time = time.time()

    futures = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for categoria, subcategorias in CATEGORIAS.items():

            if subcategorias:
                for sub in subcategorias:
                    futures.append(
                        executor.submit(extraer_categoria, categoria, sub)
                    )
            else:
                futures.append(
                    executor.submit(extraer_categoria, categoria)
                )

        for future in futures:
            todos_los_productos.extend(future.result())

    # 🔥 DEDUPLICADO FINAL
    total_antes = len(todos_los_productos)
    todos_los_productos = deduplicar_productos(todos_los_productos)
    total_despues = len(todos_los_productos)

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(todos_los_productos, f, ensure_ascii=False, indent=4)

    total_time = time.time() - start_time

    print(f"\n--- SCRAPING COMPLETADO EN {total_time:.2f} SEGUNDOS ---")
    print(f"📦 Productos antes deduplicar: {total_antes}")
    print(f"📦 Productos finales únicos: {total_despues}")
    print(f"📂 Archivo generado: {OUTPUT_JSON}")

# =========================================================
# PUNTO DE ENTRADA
# =========================================================
if __name__ == "__main__":
    ejecutar_scrapper_masivo()
