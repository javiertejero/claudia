# Usamos una imagen ligera de Python
FROM python:3.14-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Instalamos uv directamente desde su imagen oficial
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copiamos los archivos de dependencias (si tienes pyproject.toml)
COPY pyproject.toml ./

# Instalamos dependencias del sistema requeridas por WeasyPrint/md2pdf
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpango-1.0-0 \
    libpangoft2-1.0-0 \
    libharfbuzz0b \
    libjpeg-dev \
    libopenjp2-7-dev \
    shared-mime-info \
    && rm -rf /var/lib/apt/lists/*

# Instalamos las dependencias a nivel de sistema dentro del contenedor
RUN uv pip install --system aiosqlite "fastapi[standard]" "md2pdf[cli]"

# Copiamos el código fuente (main.py, index.html, etc.)
COPY . .

# Generamos automáticamente los PDFs a partir de los archivos Markdown
RUN md2pdf --input manual_usuario.md --output static/manual_usuario.pdf
RUN md2pdf --input manual_admin.md --output static/manual_admin.pdf

# Exponemos el puerto de FastAPI
EXPOSE 8000

# Comando para arrancar el servidor optimizado para producción
CMD ["fastapi", "run", "main.py", "--host", "0.0.0.0", "--port", "8000"]
