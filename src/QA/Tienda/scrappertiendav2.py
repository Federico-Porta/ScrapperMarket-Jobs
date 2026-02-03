import cloudscraper
from bs4 import BeautifulSoup
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

# =========================================================
# CONFIGURACIÓN
# =========================================================

BASE_URL = "https://www.tiendainglesa.com.uy"
RUT_FIJO = 210094030014
CATEGORIAS_PERMITIDAS = [
    "Almacen", "Frescos", "Congelados", "Limpieza",
    "Bebidas", "Perfumeria", "Bebes", "Papeleria", "Ferreteria"
]

MAX_WORKERS_CATEGORIAS = 5  # Número de hilos para categorías
MAX_WORKERS_DETALLES = 10  # Número de hilos para productos
MAX_PAGINAS = 100  # Límite de páginas por categoría

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_DIR = os.path.join(BASE_DIR, "JsonProducts")
os.makedirs(JSON_DIR, exist_ok=True)

OUTPUT_JSON = os.path.join(JSON_DIR, "productos_tienda_inglesa.json")
LOG_DESCARTES = os.path.join(JSON_DIR, "productos_descartados.json")

# =========================================================
# ESTADO GLOBAL
# =========================================================

scraper = cloudscraper.create_scraper()
productos_map = {}
productos_descartados = []

map_lock = Lock()
descartes_lock = Lock()

# =========================================================
# HELPERS
# =========================================================

def log_descartado(url, motivo, categoria):
    with descartes_lock:
        productos_descartados.append({
            "url": url,
            "motivo": motivo,
            "categoria": categoria
        })

def limpiar_url_producto(url):
    """Evita duplicados eliminando parámetros innecesarios"""
    if not url:
        return None
    if '?' in url:
        base, query = url.split('?', 1)
        query = query.split('&', 1)[0].split(',', 1)[0]
        return f"{base}?{query}"
    return url

# =========================================================
# OBTENER CATEGORÍAS
# =========================================================

def get_categories():
    print("🔍 Descargando página principal para categorías...")
    start = time.time()
    try:
        res = scraper.get(f"{BASE_URL}/supermercado/", timeout=15)
        res.raise_for_status()
    except Exception as e:
        print(f"❌ Error descargando página principal: {e}")
        return []

    print(f"✅ Página principal descargada en {time.time() - start:.2f}s")

    import re
    match = re.search(
        r'"W0006W00180002vLEVEL1SDTOPTIONS_DESKTOP":\[(.*?)\]',
        res.text, re.DOTALL
    )
    if not match:
        print("❌ No se encontró el bloque de categorías")
        return []

    data = json.loads("[" + match.group(1) + "]")
    categorias = [
        {"nombre": c["text"], "url": BASE_URL + c["url"]}
        for c in data if c["text"] in CATEGORIAS_PERMITIDAS
    ]
    print(f"✅ Categorías permitidas seleccionadas: {len(categorias)}")
    return categorias

# =========================================================
# SCRAPING DE PRODUCTOS POR CATEGORÍA
# =========================================================

def scrape_category(cat):
    nombre_cat = cat["nombre"]
    url_cat = cat["url"]
    print(f"📦 Scrapeando categoría '{nombre_cat}'")

    for page in range(MAX_PAGINAS):
        pag_url = f"{url_cat}?page={page}"
        try:
            res = scraper.get(pag_url, timeout=20)
            res.raise_for_status()
        except Exception as e:
            print(f"⚠️ Error descargando '{nombre_cat}', página {page}: {e}")
            break

        soup = BeautifulSoup(res.text, "html.parser")
        cards = soup.select("div.card-product")
        if not cards:
            if page == 0:
                print(f"⚠️ No se encontraron productos en '{nombre_cat}'")
            break  # fin de productos

        with map_lock:
            for card in cards:
                # URL producto
                link_tag = card.select_one("a.card-product-link")
                if not link_tag or not link_tag.get("href"):
                    continue
                url = BASE_URL + link_tag.get("href")
                url = limpiar_url_producto(url)

                # Nombre
                nombre_tag = card.select_one("span.card-product-name")
                nombre = nombre_tag.get_text(strip=True) if nombre_tag else None

                # Precio
                precio_tag = card.select_one("span.price-final")
                precio = precio_tag.get_text(strip=True).replace("$", "").replace(",", ".") if precio_tag else None
                try:
                    precio = float(precio) if precio else None
                except:
                    precio = None

                # Imagen
                img_tag = card.select_one("img.card-product-image")
                imagen = img_tag.get("src") if img_tag else None

                # ProductID (desde data-productid)
                product_id = card.get("data-productid")

                # Guardamos
                productos_map[url] = {
                    "nombre": nombre,
                    "price": precio,
                    "image": imagen,
                    "productId": product_id,
                    "categoria": nombre_cat
                }

        print(f"📄 '{nombre_cat}' página {page} → productos detectados: {len(cards)}")
        time.sleep(0.1)  # pequeño delay

# =========================================================
# VALIDACIÓN Y EXTRACCIÓN FINAL
# =========================================================

def extract_and_validate(url, info):
    if not all([info.get("nombre"), info.get("price"), info.get("productId"), info.get("image")]):
        log_descartado(url, "FALTAN_DATOS_OBLIGATORIOS", info.get("categoria"))
        return None
    return {
        "productId": info["productId"],
        "productName": info["nombre"],
        "productPrice": float(info["price"]),
        "productImageUrl": info["image"],
        "categoryName": info["categoria"],
        "storeRut": RUT_FIJO,
        "urlProduct": url
    }

# =========================================================
# MAIN
# =========================================================

def main():
    print("🚀 Iniciando scraper Tienda Inglesa...")
    categorias = get_categories()
    if not categorias:
        print("❌ No hay categorías. Saliendo.")
        return

    # Scraping categorías
    with ThreadPoolExecutor(MAX_WORKERS_CATEGORIAS) as ex:
        ex.map(scrape_category, categorias)

    print(f"📦 Productos detectados (sin filtrar): {len(productos_map)}")

    # Extracción y validación
    resultados = []
    with ThreadPoolExecutor(MAX_WORKERS_DETALLES) as ex:
        futures = [ex.submit(extract_and_validate, url, info) for url, info in productos_map.items()]
        for i, f in enumerate(futures, 1):
            r = f.result()
            if r:
                resultados.append(r)
            if i % 50 == 0 or i == 1:
                print(f"🔄 Procesados {i}/{len(futures)} | Guardados: {len(resultados)}")

    # Guardado final
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2)
    with open(LOG_DESCARTES, "w", encoding="utf-8") as f:
        json.dump(productos_descartados, f, ensure_ascii=False, indent=2)

    print("\n✅ Scraping finalizado")
    print(f"Guardados: {len(resultados)} | Descartados: {len(productos_descartados)}")

if __name__ == "__main__":
    main()
