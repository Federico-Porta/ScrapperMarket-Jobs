import cloudscraper
from bs4 import BeautifulSoup
import json
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================================================
# CONFIGURACIÓN GENERAL DEL SCRAPER GÉANT
# =========================================================

# RUT de la tienda Géant
# Se usa para identificar el comercio en los productos
GEANT_RUT = 213458920015

# URL base del sitio web de Géant
BASE_URL = "https://www.geant.com.uy"

# Cantidad máxima de hilos para descargar detalles de productos
# Más hilos = más velocidad, pero más carga al sitio
MAX_WORKERS = 15

# Categorías del sitio que se van a recorrer
# Cada categoría se consulta vía API interna de Géant
CATEGORIAS = [
    "Almacen",
    "Frescos",
    "Congelados",
    "Limpieza",
    "Bebidas",
    "Perfumeria",
    "Bebes",
    "Papeleria",
    "Ferreteria"
]

# cloudscraper:
# Se usa en lugar de requests para evitar bloqueos tipo Cloudflare
scraper = cloudscraper.create_scraper()

# =========================================================
# CONFIGURACIÓN DE RUTAS Y ARCHIVOS
# =========================================================

# BASE_DIR:
# Carpeta donde está ubicado este script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# JOBS_DIR:
# Carpeta padre donde se guardan los resultados de los scrapers
JOBS_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))

# JSON_DIR:
# Carpeta donde se guardará el archivo JSON final
JSON_DIR = os.path.join(JOBS_DIR, "JsonProducts")

# Crea la carpeta si no existe
os.makedirs(JSON_DIR, exist_ok=True)

# OUTPUT_JSON:
# Ruta completa del archivo final con todos los productos
OUTPUT_JSON = os.path.join(JSON_DIR, "productos_geant.json")

# =========================================================
# FUNCIÓN: extraer_detalle_producto
# =========================================================
def extraer_detalle_producto(url_relativa, nombre_categoria):
    """
    Entra a la página de un producto individual de Géant
    y extrae la información desde el JSON de Schema.org.

    Devuelve un diccionario con los datos del producto
    o None si ocurre algún error.
    """

    # Construye la URL completa del producto
    url_completa = BASE_URL + url_relativa

    try:
        # Descarga el HTML de la página del producto
        res = scraper.get(url_completa, timeout=15)

        # Parsea el HTML
        soup = BeautifulSoup(res.text, "html.parser")

        # Busca el script JSON-LD donde está la info estructurada
        script_tag = soup.find("script", {"type": "application/ld+json"})
        if not script_tag:
            return None

        # Carga el JSON embebido en la página
        data = json.loads(script_tag.string)

        # A veces el JSON viene como lista
        # Buscamos el objeto cuyo @type sea "Product"
        if isinstance(data, list):
            p = next((i for i in data if i.get("@type") == "Product"), {})
        else:
            p = data

        # Obtiene la información de precios
        oferta = p.get("offers", {})

        product_url = p.get("url") or p.get("@id")

    # Algunos productos tienen múltiples ofertas
        if "offers" in oferta and isinstance(oferta["offers"], list):
            precio_final = oferta["offers"][0].get("price")
            moneda = oferta["offers"][0].get("priceCurrency")
        else:
            precio_final = oferta.get("lowPrice") or oferta.get("price")
            moneda = oferta.get("priceCurrency")

        # Si no hay precio, se descarta el producto
        if not precio_final:
            return None

        # Devuelve el producto en formato estándar
        return {
            "idWeb": int(p['gtin']) if p.get('gtin') else None,
            "productName": p.get("name"),
            "productDescription": p.get("description", "").replace("\n", " ").strip(),
            "productBrand": p.get("brand", {}).get("name")
            if isinstance(p.get("brand"), dict)
            else p.get("brand"),
            "productPrice": float(precio_final),
            "moneda": moneda or "UYU",
            "storeRut": GEANT_RUT,
            "urlProduct": product_url,
            "productImageUrl": p.get("image"),
            "categoryName": nombre_categoria.capitalize()
        }

    except:
        # Cualquier error (timeout, parsing, JSON inválido, etc)
        return None


# =========================================================
# FUNCIÓN: obtener_todas_las_urls
# =========================================================
def obtener_todas_las_urls(categoria):
    """
    Consulta la API interna de Géant para una categoría
    y obtiene las URLs de todos los productos.

    Devuelve una lista de tuplas:
    (url_producto, categoria)
    """

    urls_encontradas = []

    # Parámetros de paginación
    _from = 0
    _to = 49

    while True:
        # Endpoint interno de búsqueda de productos
        api_url = f"{BASE_URL}/api/catalog_system/pub/products/search/{categoria}"
        params = {"_from": _from, "_to": _to}

        try:
            res = scraper.get(api_url, params=params, timeout=10)
            items = res.json()

            # Si no hay productos, se termina
            if not items:
                break

            # Extrae las URLs relativas de cada producto
            for item in items:
                if item.get("linkText"):
                    urls_encontradas.append((f"/{item['linkText']}/p", categoria))

            # Si vinieron menos de 50, no hay más páginas
            if len(items) < 50:
                break

            # Avanza la paginación
            _from += 50
            _to += 50

        except:
            # Error de red o API
            break

    return urls_encontradas


# =========================================================
# FUNCIÓN PRINCIPAL DEL SCRAPER
# =========================================================
def ejecutar_scrapper_geant():
    print(f"--- INICIANDO SCRAPER GÉANT ---")
    start_time = time.time()

    todas_las_urls = []

    # -----------------------------------------------------
    # FASE 1: OBTENCIÓN DE URLs DE PRODUCTOS
    # -----------------------------------------------------
    print(f"🔍 Escaneando categorías: {CATEGORIAS}...")

    # Se usan 3 hilos para recorrer categorías en paralelo
    with ThreadPoolExecutor(max_workers=3) as executor:
        resultados = executor.map(obtener_todas_las_urls, CATEGORIAS)

        for lista in resultados:
            todas_las_urls.extend(lista)

    total_encontrados = len(todas_las_urls)
    print(f"📦 Total de productos encontrados: {total_encontrados}")

    # -----------------------------------------------------
    # FASE 2: EXTRACCIÓN DE DETALLES DE PRODUCTOS
    # -----------------------------------------------------
    total_resultados = []
    print(f"🚀 Extrayendo detalles con {MAX_WORKERS} hilos...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(extraer_detalle_producto, url, cat)
            for url, cat in todas_las_urls
        ]

        # Procesa resultados a medida que terminan
        for i, f in enumerate(as_completed(futures), 1):
            res = f.result()
            if res:
                total_resultados.append(res)

            # Log de progreso cada 100 productos
            if i % 100 == 0 or i == total_encontrados:
                print(f"⏳ Progreso: {i}/{total_encontrados} procesados...")

    # -----------------------------------------------------
    # GUARDADO DEL ARCHIVO FINAL
    # -----------------------------------------------------
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(total_resultados, f, ensure_ascii=False, indent=4)

    duracion = (time.time() - start_time) / 60

    print(f"\n✅ GÉANT FINALIZADO EN {duracion:.2f} MINUTOS")
    print(f"📄 Archivo generado: {OUTPUT_JSON}")
    print(f"📊 Total guardados: {len(total_resultados)} productos.")


# =========================================================
# PUNTO DE ENTRADA
# =========================================================
if __name__ == "__main__":
    ejecutar_scrapper_geant()
