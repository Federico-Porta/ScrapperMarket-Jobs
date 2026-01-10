import requests
import json
import time
from concurrent.futures import ThreadPoolExecutor

# Configuración
TATA_RUT = "210003270017"
MAX_WORKERS = 4  # Los 4 hilos que pediste
CATEGORIAS = [
    "almacen", "bebidas", "congelados", "frescos", "limpieza", "perfumeria",
    "bebes", "ferreteria", "mascotas", "hogar-y-bazar", "textil"
]

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

        params = {"operationName": "ProductsQuery", "variables": json.dumps(variables)}
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

        try:
            response = requests.get(url, params=params, headers=headers, timeout=20)
            data = response.json()

            search_data = data.get('data', {}).get('search', {})
            if not search_data: break

            edges = search_data.get('products', {}).get('edges', [])
            total_count = search_data.get('products', {}).get('pageInfo', {}).get('totalCount', 0)

            if not edges: break

            for edge in edges:
                node = edge.get('node', {})
                offers = node.get('offers', {}).get('offers', [{}])[0]

                # Mapeo al formato solicitado
                productos_categoria.append({
                    "productEan": node.get('gtin'),
                    "productGtin": node.get('gtin'),
                    "sku": node.get('sku'),
                    "productName": node.get('name'),
                    "productDescription": node.get('name'),
                    "productBrand": node.get('brand', {}).get('name'),
                    "productPrice": offers.get('price'),
                    "moneda": node.get('offers', {}).get('priceCurrency', 'UYU'),
                    "storeRut": TATA_RUT,
                    "productImageUrl": node.get('image', [{}])[0].get('url'),
                    "categoryName": categoria_slug.replace("-", " ").capitalize()
                })

            if len(productos_categoria) >= total_count:
                break

            after += page_size
            # Delay mínimo para no saturar la conexión compartida de los hilos
            time.sleep(0.2)

        except Exception as e:
            print(f"❌ Error en {categoria_slug}: {e}")
            break

    print(f"✅ [Hilo Finalizado] {categoria_slug}: {len(productos_categoria)} items.")
    return productos_categoria

def ejecutar_scrapper_masivo():
    todos_los_productos = []

    start_time = time.time()

    # Uso de ThreadPoolExecutor para los 4 hilos
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Mapea la función a la lista de categorías
        resultados = list(executor.map(extraer_categoria, CATEGORIAS))

        # Unificar listas
        for lista in resultados:
            todos_los_productos.extend(lista)

    # Guardar resultado final
    with open('full_market_tata.json', 'w', encoding='utf-8') as f:
        json.dump(todos_los_productos, f, ensure_ascii=False, indent=4)

    total_time = time.time() - start_time
    print(f"\n--- SCRAPING COMPLETADO EN {total_time:.2f} SEGUNDOS ---")
    print(f"📦 Total de productos recolectados: {len(todos_los_productos)}")
    print(f"📂 Archivo generado: full_market_tata.json")

if __name__ == "__main__":
    ejecutar_scrapper_masivo()