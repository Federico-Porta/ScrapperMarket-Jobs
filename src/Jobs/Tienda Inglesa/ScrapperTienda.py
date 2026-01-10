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
RUT_FIJO = 210214220011
SUPERMERCADO_NOMBRE = "Tienda Inglesa"
BASE_URL = "https://www.tiendainglesa.com.uy"

# Ajustes de velocidad
MAX_WORKERS_CATEGORIAS = 5  # Hilos para navegar por las listas de productos
MAX_WORKERS_DETALLES = 10    # Hilos para entrar a cada producto (Deep Scraping)
OUTPUT_JSON = "resultado_final_productos.json"

# --- ESTADO GLOBAL ---
scraper = cloudscraper.create_scraper()
# productos_map guardará: { "url_limpia": {"nombre_lista": str, "categorias": set()} }
productos_map = {}
map_lock = Lock()

# --- FUNCIONES AUXILIARES ---

def limpiar_url_producto(url):
    """Elimina parámetros innecesarios de la URL para evitar duplicados por tracking."""
    if not url or url == "N/A": return None
    if '?' in url:
        base, query = url.split('?', 1)
        # Nos quedamos solo con el primer parámetro (usualmente el ID del producto)
        query = query.split('&', 1)[0].split(',', 1)[0]
        return f"{base}?{query}"
    return url

def obtener_estado_paginacion(soup):
    """Extrae el conteo de productos para saber cuándo dejar de paginar."""
    breadcrumb_tag = soup.select_one("div#TXTBREADCRUMB")
    if breadcrumb_tag:
        text = breadcrumb_tag.get_text()
        match = re.search(r'\((\d+)\s*-\s*(\d+)\s*de\s*(\d+)\)', text)
        if match:
            return int(match.group(1)), int(match.group(2)), int(match.group(3))
    return 0, 0, 0

# --- FASE 1: OBTENER CATEGORÍAS ---

def get_categories():
    print(f"🔍 Buscando categorías principales...")
    try:
        res = scraper.get(f"{BASE_URL}/supermercado/", timeout=15)
        # El sitio guarda el menú en un objeto JSON dentro de un script
        pattern = re.compile(r'"W0006W00180002vLEVEL1SDTOPTIONS_DESKTOP":\[(.*?)\]', re.DOTALL)
        match = pattern.search(res.text)
        if not match:
            print("⚠️ No se encontró el bloque de categorías. Revisa el selector.")
            return []

        data = json.loads("[" + match.group(1) + "]")
        return [{"nombre": c["text"], "url": BASE_URL + c["url"]} for c in data if c.get("url")]
    except Exception as e:
        print(f"❌ Error obteniendo categorías: {e}")
        return []

# --- FASE 2: LISTAR PRODUCTOS (MAPEO 1 A MUCHOS) ---

def scrape_category_products(cat):
    """Navega por las páginas de una categoría y llena el mapa de productos."""
    nombre_cat = cat["nombre"]
    base_url = cat["url"]

    try:
        # Construcción de la URL de búsqueda que permite paginación
        url_parts = base_url.split('/')
        category_id = url_parts[-1].split('?')[0]
        category_path = url_parts[-2]
        search_pattern = f"busqueda?0,0,*%3A*,{category_id},0,0,,,false,,,,"
        api_path = f"{BASE_URL}/supermercado/categoria/{category_path}/{search_pattern}"
    except: return

    page = 0
    while True:
        try:
            res = scraper.get(f"{api_path}{page}", timeout=20)
            soup = BeautifulSoup(res.text, "html.parser")
            inicio, fin, total = obtener_estado_paginacion(soup)

            product_links = soup.select("span.card-product-name")
            if not product_links: break

            with map_lock:
                for span in product_links:
                    link_tag = span.find_parent('a')
                    if link_tag and link_tag.get("href"):
                        raw_url = BASE_URL + link_tag.get("href")
                        url_limpia = limpiar_url_producto(raw_url)

                        if url_limpia not in productos_map:
                            productos_map[url_limpia] = {
                                "nombre_lista": span.get_text(strip=True),
                                "categorias": {nombre_cat} # Usamos set para evitar duplicados de cat
                            }
                        else:
                            productos_map[url_limpia]["categorias"].add(nombre_cat)

            if fin >= total or total == 0: break
            page += 1
            time.sleep(0.3) # Respeto al servidor
        except: break

# --- FASE 3: EXTRAER DETALLE PROFUNDO ---

def extract_product_detail(url, info_basica):
    """Entra a la ficha del producto y extrae el JSON-LD (Schema.org)."""
    time.sleep(random.uniform(2, 4))
    try:
        res = scraper.get(url, timeout=50)
        soup = BeautifulSoup(res.text, "html.parser")
        script_tag = soup.find("script", {"type": "application/ld+json"})

        if not script_tag: return None

        data = json.loads(script_tag.string)
        # A veces el JSON-LD es una lista, a veces un objeto directo
        if isinstance(data, list):
            p = next((item for item in data if item.get("@type") == "Product"), {})
        else:
            p = data if data.get("@type") == "Product" else {}

        gtin = p.get("gtin13") or p.get("gtin")
        if not gtin: return None

        # Construcción del objeto según tu formato pedido
        return {
            "ean": gtin[-13:] if gtin else None,
            "gtin": gtin,
            "sku": str(p.get("sku") or p.get("offers", {}).get("sku", "")),
            "name": p.get("name") or info_basica["nombre_lista"],
            "descripcion": p.get("description", "").replace('\n', ' ').strip(),
            "brand": p.get("brand", {}).get("name") if isinstance(p.get("brand"), dict) else p.get("brand"),
            "precio": float(p.get("offers", {}).get("price", 0)),
            "moneda": p.get("offers", {}).get("priceCurrency", "UYU"),
            "rut": RUT_FIJO,
            "imageUrl": p.get("image")[0] if isinstance(p.get("image"), list) else p.get("image"),
            "categorias": list(info_basica["categorias"]) # Convertimos set a list para JSON
        }
    except:
        return None

# --- ORQUESTADOR ---

def main():
    start_time = time.time()

    # 1. Obtener todas las categorías
    categorias_lista = get_categories()
    if not categorias_lista:
        print("❌ No se pudieron obtener categorías. Abortando.")
        return

    # 2. Mapear productos y sus categorías (Multihilo)
    print(f"🚀 Fase 1: Escaneando {len(categorias_lista)} categorías para listar productos...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_CATEGORIAS) as executor:
        executor.map(scrape_category_products, categorias_lista)

    total_unicos = len(productos_map)
    print(f"📦 Fase 2: Se detectaron {total_unicos} productos únicos. Extrayendo detalles detallados...")

    # 3. Extraer detalles uno por uno (Multihilo)
    resultados_finales = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_DETALLES) as executor:
        future_to_url = {executor.submit(extract_product_detail, url, info): url
                         for url, info in productos_map.items()}

        count = 0
        for future in future_to_url:
            res = future.result()
            count += 1
            if res:
                resultados_finales.append(res)

            # Barra de progreso simple
            sys.stdout.write(f"\rProgreso: {count}/{total_unicos} | Exitosos: {len(resultados_finales)}")
            sys.stdout.flush()

    # 4. Guardar resultado en JSON
    print(f"\n💾 Guardando resultados en {OUTPUT_JSON}...")
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(resultados_finales, f, ensure_ascii=False, indent=4)

    end_time = time.time()
    print(f"\n✨ ¡Listo! Proceso completado en {((end_time - start_time)/60):.2f} minutos.")
    print(f"📄 Total de productos únicos guardados: {len(resultados_finales)}")

if __name__ == "__main__":
    main()