# 1. Usar una imagen oficial de Python
FROM python:3.10-slim

# 2. Evitar que Python genere archivos .pyc y permitir que los logs se vean en tiempo real
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# 3. Crear una carpeta de trabajo dentro del contenedor
WORKDIR /app

# 4. Copiar el archivo de dependencias e instalarlas
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copiar todo el contenido de tu carpeta local a la carpeta /app del contenedor
COPY . .

# 6. Comando para ejecutar tu script principal (cambia 'main.py' por tu archivo)
# cuando se suba tenemos que modificar esto para el pipeline o un juego de pruebas anterior (lo que hablamos de un post de 3 productos)
CMD ["python", "src/Jobs/Devoto/ScrapperDevoto.py"]
#CMD ["python", "src/Procesos/pipelina.py"]
#CMD ["python", "src/Procesos/pipelina.py"]