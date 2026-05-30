# Usamos una imagen ligera de Python
FROM python:3.14-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Instalamos uv directamente desde su imagen oficial
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copiamos los archivos de dependencias (si tienes pyproject.toml)
COPY pyproject.toml ./

# Instalamos las dependencias a nivel de sistema dentro del contenedor
RUN uv pip install --system aiosqlite "fastapi[standard]"

# Copiamos el código fuente (main.py, index.html, etc.)
COPY . .

# Exponemos el puerto de FastAPI
EXPOSE 8000

# Comando para arrancar el servidor optimizado para producción
CMD ["fastapi", "run", "main.py", "--host", "0.0.0.0", "--port", "8000"]
