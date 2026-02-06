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

# RUT fijo del comercio Tienda Inglesa
RUT_FIJO = 210094030014

# Nombre del supermercado (informativo)
SUPERMERCADO_NOMBRE = "Tienda Inglesa"

# URL base del sitio
BASE_URL = "https://www.tiendainglesa.com.uy"

# Cantidad de hilos para escanear categorías
MAX_WORKERS_CATEGORIAS = 10

# Cantidad de hilos para extraer detalle de productos
MAX_WORKERS_DETALLES = 15

# =========================================================
# CONFIGURACIÓN DE RUTAS
# =========================================================

# BASE_DIR:
# Carpeta donde está ubicado este script
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
OUTPUT_JSON = os.path.join(JSON_DIR, "productos_tienda_inglesa.json")

# =========================================================
# ESTADO GLOBAL (COMPARTIDO ENTRE HILOS)
# =========================================================

# cloudscraper evita bloqueos tipo Cloudflare
scraper = cloudscraper.create_scraper()

# Diccionario global:
# key   → URL del producto
# value → info básica (nombre + categorías)
productos_map = {}

# Lock para evitar conflictos entre hilos
map_lock = Lock()

# =========================================================
# FUNCIONES AUXILIARES
# =========================================================

def limpiar_url_producto(url):
    """
    Limpia parámetros innecesarios de la URL del producto.
    Sirve para evitar duplicados del mismo producto.
    """
    if not url or url == "N/A":
        return None

    if '?' in url:
        base, query = url.split('?', 1)
        query = query.split('&', 1)[0].split(',', 1)[0]
        return f"{base}?{query}"

    return url


def obtener_estado_paginacion(soup):
    """
    Extrae información de paginación desde el breadcrumb.
    Devuelve:
    - primer producto
    - último producto
    - total de productos
    """
    breadcrumb_tag = soup.select_one("div#TXTBREADCRUMB")
    if breadcrumb_tag:
        text = breadcrumb_tag.get_text()
        match = re.search(r'\((\d+)\s*-\s*(\d+)\s*de\s*(\d+)\)', text)
        if match:
            return int(match.group(1)), int(match.group(2)), int(match.group(3))
    return 0, 0, 0

# =========================================================
# FASE 1: OBTENCIÓN DE CATEGORÍAS
# =========================================================

def get_categories():
    """
    Obtiene las categorías principales del supermercado
    analizando un bloque interno del HTML.
    """
    print("🔍 Buscando categorías principales...")
    try:
        res = scraper.get(f"{BASE_URL}/supermercado/", timeout=15)

        # Regex que busca el bloque JS donde están las categorías
        pattern = re.compile(
            r'"W0006W00180002vLEVEL1SDTOPTIONS_DESKTOP":\[(.*?)\]',
            re.DOTALL
        )

        match = pattern.search(res.text)
        if not match:
            print("⚠️ No se encontró el bloque de categorías")
            return []

        data = json.loads("[" + match.group(1) + "]")

        # Devuelve lista de categorías con nombre y URL
        return [
            {"nombre": c["text"], "url": BASE_URL + c["url"]}
            for c in data if c.get("url")
        ]

    except Exception as e:
        print(f"❌ Error obteniendo categorías: {e}")
        return []

# =========================================================
# FASE 2: LISTADO DE PRODUCTOS POR CATEGORÍA
# =========================================================

def scrape_category_products(cat):
    """
    Recorre una categoría completa y obtiene las URLs
    de todos los productos que contiene.
    """
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

            # Bloque crítico protegido por lock
            with map_lock:
                for span in product_links:
                    link_tag = span.find_parent('a')
                    if not link_tag or not link_tag.get("href"):
                        continue

                    raw_url = BASE_URL + link_tag.get("href")
                    url_limpia = limpiar_url_producto(raw_url)

                    # Si el producto es nuevo, se agrega
                    if url_limpia not in productos_map:
                        productos_map[url_limpia] = {
                            "nombre_lista": span.get_text(strip=True),
                            "categorias": {nombre_cat}
                        }
                    else:
                        # Si ya existe, se suma la categoría
                        productos_map[url_limpia]["categorias"].add(nombre_cat)

            if fin >= total or total == 0:
                break

            page += 1
            time.sleep(0.3)

        except:
            break

# =========================================================
# FASE 3: DETALLE DE PRODUCTO
# =========================================================

def extract_product_detail(url, info_basica):
    """
    Entra a la página del producto y extrae
    la información detallada desde Schema.org.
    """
    # Delay aleatorio para evitar bloqueos
    time.sleep(random.uniform(1.5, 3))

    try:
        res = scraper.get(url, timeout=40)
        soup = BeautifulSoup(res.text, "html.parser")

        script_tag = soup.find("script", {"type": "application/ld+json"})
        if not script_tag:
            return None

        data = json.loads(script_tag.string)

        if isinstance(data, list):
            p = next((i for i in data if i.get("@type") == "Product"), {})
        else:
            p = data if data.get("@type") == "Product" else {}

        gtin = p.get("gtin13") or p.get("gtin")
        price = p.get("offers", {}).get("price")

        if not gtin or not price:
            return None

        return {
            "idWeb": int(p.get("productId")),
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
            "urlProduct":  f"{BASE_URL}/p.producto?"+p.get("productId"),
            "categoryName": next(iter(info_basica["categorias"]))
        }

    except:
        return None

# =========================================================
# FUNCIÓN PRINCIPAL
# =========================================================

def main():
    start_time = time.time()

    # Fase 1: categorías
    categorias = get_categories()
    if not categorias:
        print("❌ No se encontraron categorías")
        return

    print(f"🚀 Escaneando {len(categorias)} categorías...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_CATEGORIAS) as executor:
        executor.map(scrape_category_products, categorias)

    print(f"📦 Productos únicos detectados: {len(productos_map)}")

    # Fase 2: detalle de productos
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

            # Barra de progreso en consola
            sys.stdout.write(
                f"\rProgreso: {i}/{len(futures)} | Guardados: {len(resultados)}"
            )
            sys.stdout.flush()

    # Guardado final
    print(f"\n💾 Guardando archivo en {OUTPUT_JSON}")
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(resultados, f, ensure_ascii=False, indent=4)

    print(f"✨ Finalizado en {(time.time() - start_time)/60:.2f} minutos")
    print(f"📄 Productos guardados: {len(resultados)}")

# =========================================================
# PUNTO DE ENTRADA
# =========================================================
if __name__ == "__main__":
    main()
