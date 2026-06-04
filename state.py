import os
import uuid
import asyncio
import logging

logger = logging.getLogger(__name__)

# For k6 stress testing (e.g. RATE_LIMIT=False DISABLE_IDENTITY_CHECKS=True uv run --frozen --env-file .env fastapi run main.py )
DISABLE_IDENTITY_CHECKS = os.getenv("DISABLE_IDENTITY_CHECKS", "False") != "False"
RATE_LIMIT = os.getenv("RATE_LIMIT", "True") != "False"

DB_FILE = "data/reservas.db"
MAX_ACTIVE_USERS = 2
SESSION_TIMEOUT = 30  # segundos para expiración de sesión de usuario
active_user_tasks = {}  # Para guardar el cronómetro de cada usuario
active_user_expires = {}  # Guarda el timestamp absoluto en el que expira

ADMIN_SECRET = os.getenv("CLAUDIA_SECRET", str(uuid.uuid4()))
logger.debug("Admin secret: visit http://127.0.0.1:8000/admin/%s", ADMIN_SECRET)
admin_connections = set()

SYSTEM_SEED = None
VALID_COMBINATIONS = set()
NUM_VALID_COMBINATIONS = 300

# Estado en memoria
active_connections = {}  # {client_id: websocket}
active_users_names = {}  # {client_id: "Nombre Apellido"}
virtuales_procesados = 0
waiting_queue = []
queue_lock = asyncio.Lock()
active_users = set()
ASIENTOS_POR_FILA = 20
FILAS = 12
