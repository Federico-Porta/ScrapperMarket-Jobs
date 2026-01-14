import os
import subprocess
import sys
from datetime import datetime

# ---------------- CONFIG ----------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

JOBS_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "Jobs"))
JSON_DIR = os.path.join(JOBS_DIR, "JsonProductos")

PYTHON_EXECUTABLE = sys.executable  # usa el mismo python que ejecuta este script

# ----------------------------------------


def ejecutar_scrapper(scrapper_dir):
    """
    Busca y ejecuta el primer archivo .py dentro de la carpeta del scrapper
    """
    py_files = [
        f for f in os.listdir(scrapper_dir)
        if f.endswith(".py")
    ]

    if not py_files:
        print(f"⚠️ No se encontró ningún .py en {scrapper_dir}")
        return False

    script_path = os.path.join(scrapper_dir, py_files[0])

    print(f"\n🚀 Ejecutando scrapper: {script_path}")

    inicio = datetime.now()

    try:
        result = subprocess.run(
            [PYTHON_EXECUTABLE, script_path],
            stdout=sys.stdout,
            stderr=sys.stderr,
            check=False
        )

        fin = datetime.now()
        duracion = (fin - inicio).total_seconds()

        if result.returncode == 0:
            print(f"✅ Finalizado correctamente ({duracion:.2f}s)")
            return True
        else:
            print(f"❌ Finalizó con errores (code={result.returncode}) ({duracion:.2f}s)")
            return False

    except Exception as e:
        print(f"🔥 Error ejecutando {script_path}: {e}")
        return False


def main():
    print("🧠 ORQUESTADOR DE SCRAPPERS")
    print(f"📂 Jobs: {JOBS_DIR}")

    if not os.path.exists(JOBS_DIR):
        print("❌ Carpeta Jobs no encontrada")
        return

    scrappers = [
        d for d in os.listdir(JOBS_DIR)
        if os.path.isdir(os.path.join(JOBS_DIR, d))
           and d.lower() != "jsonproductos"
    ]

    if not scrappers:
        print("⚠️ No se encontraron scrappers para ejecutar")
        return

    print(f"🔍 Scrappers detectados: {scrappers}")

    resultados = {}

    for scrapper in scrappers:
        scrapper_path = os.path.join(JOBS_DIR, scrapper)
        ok = ejecutar_scrapper(scrapper_path)
        resultados[scrapper] = ok

    print("\n📊 RESUMEN FINAL")
    for scrapper, ok in resultados.items():
        estado = "OK" if ok else "ERROR"
        print(f" - {scrapper}: {estado}")

    print("\n🏁 Orquestación finalizada")


if __name__ == "__main__":
    main()
