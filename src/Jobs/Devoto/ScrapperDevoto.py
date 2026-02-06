from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup
import time
import json
import os

# =========================================================
# CONFIGURACIÓN GENERAL
# =========================================================
DISCO_RUT = 210297450018
BASE_URL = "www.devoto.com.uy"

CATEGORIAS = {
    "almacen": "https://www.devoto.com.uy/products/category/almacen/10",
    "frescos": "https://www.devoto.com.uy/products/category/frescos/14",
    "congelados": "https://www.devoto.com.uy/products/category/congelados/1412",
    "bebidas": "https://www.devoto.com.uy/products/category/bebidas/11",
    "limpieza": "https://www.devoto.com.uy/products/category/limpieza/1210",
    "perfumeria": "https://www.devoto.com.uy/products/category/perfumeria/1211"
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# JOBS_DIR:
# Carpeta superior donde se guardan los JSON de salida
JOBS_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))

# JSON_DIR:
# Carpeta donde se escribirá el archivo final
JSON_DIR = os.path.join(JOBS_DIR, "JsonProducts")

# Se asegura que exista la carpeta
os.makedirs(JSON_DIR, exist_ok=True)


# OUTPUT_JSON:
# Ruta completa del archivo final con todos los productos
OUTPUT_JSON = os.path.join(JSON_DIR, "productos_devoto.json")

# =========================================================
# SELENIUM SETUP
# =========================================================
options = Options()
options.add_argument("--headless")
options.add_argument("--window-size=1920,1080")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")

driver = webdriver.Chrome(options=options)

# =========================================================
# FUNCIÓN: SCROLL INFINITO
# =========================================================
def scroll_hasta_el_final():
    last_count = 0
    same_count_times = 0

    while True:
        items = driver.find_elements(By.CSS_SELECTOR, "div.product-item")
        current_count = len(items)

        print(f"   ⏳ Productos cargados: {current_count}")

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1.5)

        if current_count == last_count:
            same_count_times += 1
        else:
            same_count_times = 0

        if same_count_times >= 3:
            break

        last_count = current_count

# =========================================================
# FUNCIÓN: EXTRAER PRODUCTOS
# =========================================================
def extraer_productos_categoria(nombre_categoria, url):
    print(f"🔍 {nombre_categoria.upper()} – cargando productos...")
    driver.get(url)
    time.sleep(3)

    scroll_hasta_el_final()

    soup = BeautifulSoup(driver.page_source, "html.parser")
    productos = []

    items = soup.select("div.product-item")
    print(f"📦 {nombre_categoria}: {len(items)} productos encontrados")

    for item in items:
        try:
            link = item.select_one("h3 a")["href"]
            nombre = item.select_one("h3 a").text.strip()
            precio = item.select_one("span.val").text.strip()
            marca_tag = item.select_one("div.prod-cats a")
            marca = marca_tag.text.strip() if marca_tag else None
            img_tag = item.select_one("figure img")
            img = img_tag["src"] if img_tag else None

            productos.append({
                "idWeb": int(link.split("/")[-1]),
                "productName": nombre,
                "productDescription": "",
                "productBrand": marca,
                "productPrice": float(precio.replace(".", "").replace(",", ".")),
                "moneda": "UYU",
                "storeRut": DISCO_RUT,
                "urlProduct": BASE_URL + link,
                "productImageUrl": img,
                "categoryName": nombre_categoria.capitalize()
            })
        except:
            continue

    return productos

# =========================================================
# MAIN
# =========================================================
def ejecutar_scraper_disco():
    inicio = time.time()
    todos = []

    for cat, url in CATEGORIAS.items():
        productos = extraer_productos_categoria(cat, url)
        todos.extend(productos)

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(todos, f, ensure_ascii=False, indent=4)

    driver.quit()

    duracion = (time.time() - inicio) / 60
    print("\n✅ DISCO FINALIZADO")
    print(f"⏱️ Tiempo total: {duracion:.2f} minutos")
    print(f"📄 Archivo: {OUTPUT_JSON}")
    print(f"📊 Total productos: {len(todos)}")

# =========================================================
# ENTRY POINT
# =========================================================
if __name__ == "__main__":
    ejecutar_scraper_disco()
