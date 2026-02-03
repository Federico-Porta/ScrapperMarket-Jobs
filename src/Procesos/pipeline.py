import subprocess
import sys
import os
from datetime import datetime

# ================= CONFIGURACIÓN =================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON_EXECUTABLE = sys.executable  # Usa el mismo Python del entorno

SCRIPTS = [
    {
        "name": "RUN SCRAPPERS",
        "file": "runScrappers.py",
        "critical": False   # Si falla, se corta todo
    },
    {
        "name": "POST PRODUCTS",
        "file": "PostProducts.py",
        "critical": False  # Si falla, igual seguimos
    },
    {
        "name": "REPROCESO ERRORES",
        "file": "ReprocesoErrores.py",
        "critical": False
    }
]

# =================================================


def ejecutar_script(script):
    """
    Ejecuta un script Python y devuelve True si finaliza correctamente.
    """
    script_path = os.path.join(BASE_DIR, script["file"])

    print(f"\n🚀 Iniciando: {script['name']}")
    print(f"📄 Archivo: {script_path}")

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
            print(f"✅ {script['name']} finalizado correctamente ({duracion:.2f}s)")
            return True
        else:
            print(f"❌ {script['name']} finalizó con errores (code={result.returncode}) ({duracion:.2f}s)")
            return False

    except Exception as e:
        print(f"🔥 Error ejecutando {script['name']}: {e}")
        return False


def main():
    print("🧠 PIPELINE DE SCRAPING + IMPORTACIÓN")
    print(f"📅 Inicio: {datetime.now()}")
    print("=" * 50)

    resultados = {}

    for script in SCRIPTS:
        ok = ejecutar_script(script)
        resultados[script["name"]] = ok

        if not ok and script["critical"]:
            print("\n⛔ PIPELINE DETENIDO")
            print(f"El proceso crítico '{script['name']}' falló.")
            break

    print("\n" + "=" * 50)
    print("📊 RESUMEN DEL PIPELINE")

    for nombre, ok in resultados.items():
        estado = "OK" if ok else "ERROR"
        print(f" - {nombre}: {estado}")

    print(f"\n🏁 Fin del pipeline: {datetime.now()}")


if __name__ == "__main__":
    main()
