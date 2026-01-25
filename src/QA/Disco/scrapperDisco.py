import cloudscraper
from bs4 import BeautifulSoup
import json
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURACIÓN GÉANT ---
disco_RUT = 213458920015
BASE_URL = "https://www.disco.com.uy"
MAX_WORKERS = 15

CATEGORIAS = ["Almacen",
              "Frescos",
              "Congelados",
              "Limpieza",
              "Bebidas",
              "Perfumeria",
              "Bebes",
              "Papeleria",
              "Ferreteria"]

scraper = cloudscraper.create_scraper()

# -------- RUTAS --------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JOBS_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
JSON_DIR = os.path.join(JOBS_DIR, "JsonProducts")
os.makedirs(JSON_DIR, exist_ok=True)

OUTPUT_JSON = os.path.join(JSON_DIR, "productos_disco.json")
# ----------------------


def extraer_detalle_producto(url_relativa, nombre_categoria):
    """Extrae el JSON de Schema.org de la página del producto en Géant"""
    url_completa = BASE_URL + url_relativa
    try:
        res = scraper.get(url_completa, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")

        script_tag = soup.find("script", {"type": "application/ld+json"})
        if not script_tag:
            return None

        data = json.loads(script_tag.string)
        if isinstance(data, list):
            p = next((i for i in data if i.get("@type") == "Product"), {})
        else:
            p = data

        oferta = p.get("offers", {})
        if "offers" in oferta and isinstance(oferta["offers"], list):
            precio_final = oferta["offers"][0].get("price")
            moneda = oferta["offers"][0].get("priceCurrency")
        else:
            precio_final = oferta.get("lowPrice") or oferta.get("price")
            moneda = oferta.get("priceCurrency")

        if not precio_final:
            return None

        return {
            "idWeb": p.get("gtin") or p.get("sku"),
            "productName": p.get("name"),
            "productDescription": p.get("description", "").replace("\n", " ").strip(),
            "productBrand": p.get("brand", {}).get("name")
            if isinstance(p.get("brand"), dict)
            else p.get("brand"),
            "productPrice": float(precio_final),
            "moneda": moneda or "UYU",
            "storeRut": disco_RUT,
            "productImageUrl": p.get("image"),
            "categoryName": nombre_categoria.capitalize()
        }
    except:
        return None


def obtener_todas_las_urls(categoria):
    """Obtiene todas las URLs de productos de Géant por categoría"""
    urls_encontradas = []
    _from = 0
    _to = 49

    while True:
        api_url = f"{BASE_URL}/api/catalog_system/pub/products/search/{categoria}"
        params = {"_from": _from, "_to": _to}

        try:
            res = scraper.get(api_url, params=params, timeout=10)
            items = res.json()
            if not items:
                break

            for item in items:
                if item.get("linkText"):
                    urls_encontradas.append((f"/{item['linkText']}/p", categoria))

            if len(items) < 50:
                break

            _from += 50
            _to += 50
        except:
            break

    return urls_encontradas


def ejecutar_scrapper_disco():
    print(f"--- INICIANDO SCRAPER GÉANT ---")
    start_time = time.time()
    todas_las_urls = []

    # Fase 1: URLs
    print(f"🔍 Escaneando categorías: {CATEGORIAS}...")
    with ThreadPoolExecutor(max_workers=3) as executor:
        resultados = executor.map(obtener_todas_las_urls, CATEGORIAS)
        for lista in resultados:
            todas_las_urls.extend(lista)

    total_encontrados = len(todas_las_urls)
    print(f"📦 Total de productos encontrados: {total_encontrados}")

    # Fase 2: Detalles
    total_resultados = []
    print(f"🚀 Extrayendo detalles con {MAX_WORKERS} hilos...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(extraer_detalle_producto, url, cat)
            for url, cat in todas_las_urls
        ]

        for i, f in enumerate(as_completed(futures), 1):
            res = f.result()
            if res:
                total_resultados.append(res)

            if i % 100 == 0 or i == total_encontrados:
                print(f"⏳ Progreso: {i}/{total_encontrados} procesados...")

    # Guardar JSON final
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(total_resultados, f, ensure_ascii=False, indent=4)

    duracion = (time.time() - start_time) / 60
    print(f"\n✅ GÉANT FINALIZADO EN {duracion:.2f} MINUTOS")
    print(f"📄 Archivo generado: {OUTPUT_JSON}")
    print(f"📊 Total guardados: {len(total_resultados)} productos.")


if __name__ == "__main__":
    ejecutar_scrapper_disco()
