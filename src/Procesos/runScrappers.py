import os
import subprocess
import sys
from datetime import datetime

# =========================================================
# CONFIGURACIÓN GENERAL
# =========================================================

# BASE_DIR:
# Obtiene la carpeta donde está ubicado ESTE archivo .py
# Ejemplo:
#   /home/usuario/proyecto/orquestador.py
# BASE_DIR será:
#   /home/usuario/proyecto
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# JOBS_DIR:
# Apunta a la carpeta "Jobs", que está un nivel arriba del script
# y luego dentro de la carpeta "Jobs"
#
# ..  -> subir un nivel en el árbol de carpetas
JOBS_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "Jobs"))

# JSON_DIR:
# Carpeta específica donde se guardan los JSON de productos
# (en este script no se usa directamente, pero queda definida)
JSON_DIR = os.path.join(JOBS_DIR, "JsonProductos")

# PYTHON_EXECUTABLE:
# Guarda la ruta EXACTA del Python que está ejecutando este script.
# Esto es muy importante en servidores/cloud porque:
# - evita usar otro Python distinto
# - evita problemas de versiones
PYTHON_EXECUTABLE = sys.executable

# =========================================================
# FUNCIÓN: ejecutar_scrapper
# =========================================================
def ejecutar_scrapper(scrapper_dir):
    """
    Esta función:
    - recibe una carpeta de scrapper
    - busca el primer archivo .py dentro
    - lo ejecuta como si fuera:
        python archivo.py
    - mide el tiempo de ejecución
    - devuelve True si salió bien, False si falló
    """

    # Busca todos los archivos .py dentro del directorio del scrapper
    py_files = [
        f for f in os.listdir(scrapper_dir)
        if f.endswith(".py")
    ]

    # Si no hay ningún archivo .py, no hay nada que ejecutar
    if not py_files:
        print(f"⚠️ No se encontró ningún .py en {scrapper_dir}")
        return False

    # Toma el primer archivo .py encontrado
    script_path = os.path.join(scrapper_dir, py_files[0])

    print(f"\n🚀 Ejecutando scrapper: {script_path}")

    # Guarda el momento exacto en que empieza
    inicio = datetime.now()

    try:
        # Ejecuta el scrapper como un proceso externo
        # Es equivalente a correrlo desde la terminal
        result = subprocess.run(
            [PYTHON_EXECUTABLE, script_path],
            stdout=sys.stdout,   # muestra la salida normal en pantalla
            stderr=sys.stderr,   # muestra errores en pantalla
            check=False          # NO lanza excepción si falla
        )

        # Guarda el momento en que termina
        fin = datetime.now()

        # Calcula duración total en segundos
        duracion = (fin - inicio).total_seconds()

        # Si el código de salida es 0 → ejecución correcta
        if result.returncode == 0:
            print(f"✅ Finalizado correctamente ({duracion:.2f}s)")
            return True
        else:
            print(f"❌ Finalizó con errores (code={result.returncode}) ({duracion:.2f}s)")
            return False

    except Exception as e:
        # Cualquier error inesperado (permiso, ruta, python, etc)
        print(f"🔥 Error ejecutando {script_path}: {e}")
        return False


# =========================================================
# FUNCIÓN PRINCIPAL
# =========================================================
def main():
    print("🧠 ORQUESTADOR DE SCRAPPERS")
    print(f"📂 Jobs: {JOBS_DIR}")

    # Verifica que la carpeta Jobs exista
    if not os.path.exists(JOBS_DIR):
        print("❌ Carpeta Jobs no encontrada")
        return

    # Busca todas las subcarpetas dentro de Jobs
    # Cada subcarpeta representa un scrapper
    # EXCLUYE la carpeta "JsonProductos"
    scrappers = [
        d for d in os.listdir(JOBS_DIR)
        if os.path.isdir(os.path.join(JOBS_DIR, d))
           and d.lower() != "jsonproductos"
    ]

    # Si no hay scrappers, no hay nada que ejecutar
    if not scrappers:
        print("⚠️ No se encontraron scrappers para ejecutar")
        return

    print(f"🔍 Scrappers detectados: {scrappers}")

    # Diccionario para guardar resultados finales
    # Ejemplo:
    # {
    #   "scrapper1": True,
    #   "scrapper2": False
    # }
    resultados = {}

    # Ejecuta cada scrapper uno por uno
    for scrapper in scrappers:
        scrapper_path = os.path.join(JOBS_DIR, scrapper)
        ok = ejecutar_scrapper(scrapper_path)
        resultados[scrapper] = ok

    # Muestra resumen final
    print("\n📊 RESUMEN FINAL")
    for scrapper, ok in resultados.items():
        estado = "OK" if ok else "ERROR"
        print(f" - {scrapper}: {estado}")

    print("\n🏁 Orquestación finalizada")


# =========================================================
# PUNTO DE ENTRADA DEL SCRIPT
# =========================================================
# Esto asegura que:
# - main() solo se ejecute cuando este archivo
#   se corre directamente
# - no se ejecute si es importado desde otro archivo
if __name__ == "__main__":
    main()
