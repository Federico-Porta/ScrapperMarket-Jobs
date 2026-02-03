import cloudscraper
from bs4 import BeautifulSoup
import re
import json
import sys
import time
import random
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

# =========================================================
# CONFIGURACIÓN GENERAL – TIENDA INGLESA
# =========================================================

RUT_FIJO = 210094030014
SUPERMERCADO_NOMBRE = "Tienda Inglesa"
BASE_URL = "https://www.tiendainglesa.com.uy"

MAX_WORKERS_CATEGORIAS = 10
MAX_WORKERS_DETALLES = 15

# =========================================================
# CATEGORÍAS PERMITIDAS
# =========================================================

CATEGORIAS_PERMITIDAS = [
    "Almacen", "Almacén",
    "Frescos",
    "Congelados",
    "Limpieza",
    "Bebidas",
    "Perfumeria", "Perfumería",
    "Bebes",
    "Papeleria","Papelería",
    "Ferreteria","Ferretería"
]

# =========================================================
# RUTAS
# =========================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JOBS_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
JSON_DIR = os.path.join(JOBS_DIR, "JsonProducts")
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
# FUNCIONES AUXILIARES
# =========================================================

def limpiar_url_producto(url):
    if not url or url == "N/A":
        return None
    if '?' in url:
        base, query = url.split('?', 1)
        query = query.split('&', 1)[0].split(',', 1)[0]
        return f"{base}?{query}"
    return url


def obtener_estado_paginacion(soup):
    breadcrumb_tag = soup.select_one("div#TXTBREADCRUMB")
    if breadcrumb_tag:
        text = breadcrumb_tag.get_text()
        match = re.search(r'\((\d+)\s*-\s*(\d+)\s*de\s*(\d+)\)', text)
        if match:
            return int(match.group(1)), int(match.group(2)), int(match.group(3))
    return 0, 0, 0


def log_descartado(url, motivo, info_basica):
    with descartes_lock:
        productos_descartados.append({
            "url": url,
            "motivo": motivo,
            "categoria": next(iter(info_basica["categorias"]))
        })

# =========================================================
# FASE 1: CATEGORÍAS (FILTRADAS)
# =========================================================

def get_categories():
    print("🔍 Buscando categorías permitidas...")
    try:
        res = scraper.get(f"{BASE_URL}/supermercado/", timeout=15)

        pattern = re.compile(
            r'"W0006W00180002vLEVEL1SDTOPTIONS_DESKTOP":\[(.*?)\]',
            re.DOTALL
        )

        match = pattern.search(res.text)
        if not match:
            print("⚠️ No se encontró el bloque de categorías")
            return []

        data = json.loads("[" + match.group(1) + "]")

        categorias_filtradas = []

        for c in data:
            nombre = c.get("text", "").strip()
            if nombre in CATEGORIAS_PERMITIDAS and c.get("url"):
                categorias_filtradas.append({
                    "nombre": nombre,
                    "url": BASE_URL + c["url"]
                })

        print(f"✅ Categorías seleccionadas: {len(categorias_filtradas)}")
        return categorias_filtradas

    except Exception as e:
        print(f"❌ Error obteniendo categorías: {e}")
        return []

# =========================================================
# FASE 2: PRODUCTOS POR CATEGORÍA
# =========================================================

def scrape_category_products(cat):
    nombre_cat = cat["nombre"]
    base_url = cat["url"]

    try:
        url_parts = base_url.split('/')
        category_id = url_parts[-1].split('?')[0]
        category_path = url_parts[-2]

        search_pattern = f"busqueda?0,0,*%3A*,{category_id},0,0,,,false,,,,"
        api_path = f"{BASE_URL}/supermercado/categoria/{category_path}/{search_pattern}"
    except:
        return

    page = 0
    while True:
        try:
            res = scraper.get(f"{api_path}{page}", timeout=20)
            soup = BeautifulSoup(res.text, "html.parser")

            inicio, fin, total = obtener_estado_paginacion(soup)

            product_links = soup.select("span.card-product-name")
            if not product_links:
                break

            with map_lock:
                for span in product_links:
                    link_tag = span.find_parent('a')
                    if not link_tag or not link_tag.get("href"):
                        continue

                    raw_url = BASE_URL + link_tag.get("href")
                    url_limpia = limpiar_url_producto(raw_url)

                    if url_limpia not in productos_map:
                        productos_map[url_limpia] = {
                            "nombre_lista": span.get_text(strip=True),
                            "categorias": {nombre_cat}
                        }
                    else:
                        productos_map[url_limpia]["categorias"].add(nombre_cat)

            if fin >= total or total == 0:
                break

            page += 1
            time.sleep(0.2)

        except:
            break

# =========================================================
# FASE 3: DETALLE DE PRODUCTO (PRODUCTID)
# =========================================================

def extract_product_detail(url, info_basica):
    time.sleep(random.uniform(0.3, 0.8))

    try:
        res = scraper.get(url, timeout=30)
        soup = BeautifulSoup(res.text, "html.parser")

        script_tag = soup.find("script", {"type": "application/ld+json"})
        if not script_tag:
            log_descartado(url, "SIN_SCHEMA", info_basica)
            return None

        data = json.loads(script_tag.string)

        if isinstance(data, list):
            p = next((i for i in data if i.get("@type") == "Product"), {})
        else:
            p = data if data.get("@type") == "Product" else {}

        price = p.get("offers", {}).get("price")

        # ⛔ ÚNICO MOTIVO DE DESCARTE
        if not price:
            log_descartado(url, "SIN_PRECIO", info_basica)
            return None

        product_id = p.get("productId")

        if not product_id:
            log_descartado(url, "SIN_PRODUCT_ID", info_basica)
            return None

        return {
            "productId": product_id,
            "productName": p.get("name") or info_basica["nombre_lista"],
            "productDescription": (p.get("description") or "").replace("\n", " ").strip(),
            "productBrand": p.get("brand", {}).get("name")
            if isinstance(p.get("brand"), dict)
            else p.get("brand"),
            "productPrice": float(price),
            "moneda": p.get("offers", {}).get("priceCurrency", "UYU"),
            "storeRut": RUT_FIJO,
            "productImageUrl": p.get("image")[0]
            if isinstance(p.get("image"), list)
            else p.get("image"),
            "urlProduct": f"{BASE_URL}/p.producto?{product_id}",
            "categoryName": next(iter(info_basica["categorias"]))
        }

    except Exception as e:
        log_descartado(url, f"ERROR:{str(e)[:80]}", info_basica)
        return None

# =========================================================
# MAIN
# =========================================================

def main():
    start_time = time.time()

    categorias = get_categories()
    if not categorias:
        print("❌ No se encontraron categorías")
        return

    print(f"🚀 Escaneando {len(categorias)} categorías...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_CATEGORIAS) as executor:
        executor.map(scrape_category_products, categorias)

    print(f"📦 Productos únicos detectados: {len(productos_map)}")

    resultados = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_DETALLES) as executor:
        futures = [
            executor.submit(extract_product_detail, url, info)
            for url, info in productos_map.items()
        ]

        for i, future in enumerate(futures, 1):
            res = future.result()
            if res:
                resultados.append(res)

            sys.stdout.write(
                f"\rProgreso: {i}/{len(futures)} | Guardados: {len(resultados)}"
            )
            sys.stdout.flush()

    print(f"\n💾 Guardando productos en {OUTPUT_JSON}")
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(resultados, f, ensure_ascii=False, indent=4)

    print(f"🧾 Guardando descartes en {LOG_DESCARTES}")
    with open(LOG_DESCARTES, "w", encoding="utf-8") as f:
        json.dump(productos_descartados, f, ensure_ascii=False, indent=4)

    print(f"✨ Finalizado en {(time.time() - start_time)/60:.2f} minutos")
    print(f"📄 Productos guardados: {len(resultados)}")
    print(f"❌ Productos descartados: {len(productos_descartados)}")

# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":
    main()
