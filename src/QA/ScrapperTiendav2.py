import cloudscraper
from bs4 import BeautifulSoup
import re
import json
import sys
import time
import random
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

# --- CONFIGURACIÓN ---
RUT_FIJO = 210094030014
SUPERMERCADO_NOMBRE = "Tienda Inglesa"
BASE_URL = "https://www.tiendainglesa.com.uy"

MAX_WORKERS_CATEGORIAS = 7
MAX_WORKERS_DETALLES = 15
OUTPUT_JSON = "PRODUCTOS_SIN_GTIN.json"

# --- ESTADO GLOBAL ---
scraper = cloudscraper.create_scraper()
productos_map = {}
map_lock = Lock()

# --- FUNCIONES AUXILIARES ---

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

# --- FASE 1: OBTENER CATEGORÍAS ---

def get_categories():
    print("🔍 Buscando categorías...")
    try:
        res = scraper.get(f"{BASE_URL}/supermercado/", timeout=15)
        pattern = re.compile(
            r'"W0006W00180002vLEVEL1SDTOPTIONS_DESKTOP":\[(.*?)\]',
            re.DOTALL
        )
        match = pattern.search(res.text)
        if not match:
            return []

        data = json.loads("[" + match.group(1) + "]")
        return [{"nombre": c["text"], "url": BASE_URL + c["url"]}
                for c in data if c.get("url")]
    except Exception as e:
        print(f"❌ Error categorías: {e}")
        return []

# --- FASE 2: LISTAR PRODUCTOS ---

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
                    if not url_limpia:
                        continue

                    if url_limpia not in productos_map:
                        nombre = span.get_text(strip=True)
                        if not nombre:
                            continue

                        productos_map[url_limpia] = {
                            "nombre_lista": nombre,
                            "categoria": nombre_cat
                        }

            if fin >= total or total == 0:
                break

            page += 1
            time.sleep(0.3)
        except:
            break

# --- FASE 3: DETALLE DE PRODUCTO (PRECIO OBLIGATORIO) ---

def extract_product_detail(url, info_basica):
    time.sleep(random.uniform(2, 4))
    try:
        res = scraper.get(url, timeout=50)
        soup = BeautifulSoup(res.text, "html.parser")

        script_tag = soup.find("script", {"type": "application/ld+json"})
        if not script_tag:
            return None  # ❌ sin JSON no hay precio confiable

        try:
            data = json.loads(script_tag.string)
        except:
            return None

        if isinstance(data, list):
            p = next((i for i in data if i.get("@type") == "Product"), {})
        else:
            p = data if data.get("@type") == "Product" else {}

        # --- PRECIO (OBLIGATORIO) ---
        try:
            price = float(p.get("offers", {}).get("price"))
        except:
            return None  # ❌ sin precio se descarta

        product = {
            "productEan": None,
            "productGtin": None,
            "sku": None,
            "productName": p.get("name") or info_basica["nombre_lista"],
            "productDescripcion": p.get("description", "").replace("\n", " ").strip(),
            "productBrand": None,
            "productPrice": price,
            "moneda": p.get("offers", {}).get("priceCurrency", "UYU"),
            "storeRut": RUT_FIJO,
            "productImageUrl": None,
            "categoryName": info_basica["categoria"]
        }

        gtin = p.get("gtin13") or p.get("gtin")
        if gtin:
            product["productGtin"] = gtin
            product["productEan"] = gtin[-13:]

        product["sku"] = str(
            p.get("sku") or p.get("offers", {}).get("sku") or ""
        )

        brand = p.get("brand")
        if isinstance(brand, dict):
            product["productBrand"] = brand.get("name")
        else:
            product["productBrand"] = brand

        image = p.get("image")
        if isinstance(image, list) and image:
            product["productImageUrl"] = image[0]
        elif isinstance(image, str):
            product["productImageUrl"] = image

        return product

    except Exception as e:
        print(f"❌ Error en {url}: {e}")
        return None

# --- ORQUESTADOR ---

def main():
    start_time = time.time()

    categorias_lista = get_categories()
    if not categorias_lista:
        print("❌ Sin categorías. Abortando.")
        return

    print(f"🚀 Escaneando {len(categorias_lista)} categorías...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_CATEGORIAS) as executor:
        executor.map(scrape_category_products, categorias_lista)

    total_unicos = len(productos_map)
    print(f"📦 Productos detectados: {total_unicos}")

    resultados_finales = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_DETALLES) as executor:
        futures = [
            executor.submit(extract_product_detail, url, info)
            for url, info in productos_map.items()
        ]

        count = 0
        for f in futures:
            res = f.result()
            count += 1
            if res:
                resultados_finales.append(res)

            sys.stdout.write(
                f"\rProgreso: {count}/{total_unicos} | Guardados: {len(resultados_finales)}"
            )
            sys.stdout.flush()

    print(f"\n💾 Guardando en {OUTPUT_JSON}...")
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(resultados_finales, f, ensure_ascii=False, indent=4)

    end_time = time.time()
    print(f"\n✨ Listo en {((end_time - start_time)/60):.2f} minutos")
    print(f"📄 Total guardados: {len(resultados_finales)}")

if __name__ == "__main__":
    main()
