import aiosqlite
import asyncio
import csv
import html
import io
import json
import logging
import math
import os
import time
import uuid
import hashlib
import secrets
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.responses import StreamingResponse
from uvicorn.protocols.utils import ClientDisconnected

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

DB_FILE = "data/reservas.db"
MAX_ACTIVE_USERS = 2
SESSION_TIMEOUT = 30  # segundos para expiración de sesión de usuario
active_user_tasks = {}  # Para guardar el cronómetro de cada usuario
active_user_expires = {}  # Guarda el timestamp absoluto en el que expira

ADMIN_SECRET = os.getenv("CLAUDIA_SECRET", str(uuid.uuid4()))
logger.debug("Admin secret: visit http://127.0.0.1:8000/admin/%s", ADMIN_SECRET)
admin_connections = set()

# Listas estáticas para la generación de duplas de usuario
ANIMALS = [
    "tigre", "leon", "perro", "gato", "raton", "elefante", "jirafa", "mono", "oso", "lobo",
    "zorro", "liebre", "tortuga", "buho", "aguila", "delfin", "tiburon", "ballena", "pulpo", "cangrejo",
    "cebra", "cocodrilo", "koala", "panda", "camello", "caballo", "oveja", "cabra", "gallina", "pato",
    "bisonte", "hipopotamo", "rinoceronte", "leopardo", "pantera", "guepardo", "ciervo", "alce", "ardilla", "castor"
]

ADJECTIVES = [
    "miedica", "valiente", "rapido", "lento", "grande", "pequeño", "alegre", "triste", "astuto", "torpe",
    "fuerte", "debil", "manso", "feroz", "tranquilo", "inquieto", "timido", "audaz", "fiel", "perezoso",
    "curioso", "travieso", "amigable", "solitario", "generoso", "tacaño", "paciente", "impaciente", "educado", "grosero",
    "limpio", "sucio", "ordenado", "desordenado", "inteligente", "despistado", "orgulloso", "humilde", "agradecido", "chistoso"
]

SYSTEM_SEED = None
VALID_COMBINATIONS = set()
NUM_VALID_COMBINATIONS = 300

# Rate Limiting por IP
ip_blocks = {}  # { ip: {"failures": int, "blocked_until": float} }

def generate_valid_combinations(seed: str) -> set[str]:
    all_combos = [f"{a}_{adj}" for a in ANIMALS for adj in ADJECTIVES]
    # Hashing determinista para evitar dependencia en random.Random interno de Python
    all_combos.sort(key=lambda c: hashlib.sha256(f"{c}:{seed}".encode()).hexdigest())
    return set(all_combos[:NUM_VALID_COMBINATIONS])

def get_block_remaining(ip: str) -> int:
    if ip not in ip_blocks:
        return 0
    remaining = ip_blocks[ip]["blocked_until"] - time.time()
    return max(0, int(remaining))

def register_failed_attempt(ip: str):
    now = time.time()
    if ip not in ip_blocks:
        ip_blocks[ip] = {"failures": 0, "blocked_until": 0}
    ip_blocks[ip]["failures"] += 1
    failures = ip_blocks[ip]["failures"]
    if failures == 1:
        duration = 60
    elif failures == 2:
        duration = 120
    else:
        duration = 180
    ip_blocks[ip]["blocked_until"] = now + duration
    logger.warning(f"IP {ip} falló validación. Fallos acumulados: {failures}. Bloqueada por {duration}s.")

def register_successful_attempt(ip: str):
    if ip in ip_blocks:
        del ip_blocks[ip]
        logger.info(f"IP {ip} validada exitosamente. Penalización reseteada.")

def normalize_combination(client_id: str) -> str:
    cleaned = client_id.strip().lower()
    cleaned = cleaned.replace(" ", "_").replace("-", "_")
    parts = [p.strip() for p in cleaned.split("_") if p.strip()]
    if len(parts) == 2:
        return f"{parts[0]}_{parts[1]}"
    return cleaned

def get_client_ip(websocket: WebSocket) -> str:
    forwarded = websocket.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if websocket.client:
        return websocket.client.host
    return "127.0.0.1"


# Estado en memoria
active_connections = {}  # {client_id: websocket}
active_users_names = {}  # {client_id: "Nombre Apellido"}
virtuales_procesados = 0
waiting_queue = []
queue_lock = asyncio.Lock()
active_users = set()
ASIENTOS_POR_FILA = 20
FILAS = 12


async def init_db():
    global waiting_queue, SYSTEM_SEED, VALID_COMBINATIONS
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS seats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                seat_number INTEGER,
                session_time TEXT,
                status TEXT DEFAULT 'free',
                owner_id TEXT,
                owner_name TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS queue (
                client_id TEXT PRIMARY KEY,
                position INTEGER
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        
        # Cargar o generar la semilla persistida únicamente en la base de datos
        async with db.execute("SELECT value FROM settings WHERE key = 'seed'") as cursor:
            row = await cursor.fetchone()
            if row is None:
                SYSTEM_SEED = secrets.token_hex(16)
                await db.execute("INSERT INTO settings (key, value) VALUES ('seed', ?)", (SYSTEM_SEED,))
                await db.commit()
                logger.info(f"Semilla generada y persistida en base de datos: {SYSTEM_SEED}")
            else:
                SYSTEM_SEED = row[0]
                logger.info("Semilla cargada con éxito desde base de datos.")

        # Generar las combinaciones de usuarios válidas deterministamente usando la semilla
        VALID_COMBINATIONS = generate_valid_combinations(SYSTEM_SEED)

        # Cargar cola persistida
        async with db.execute("SELECT client_id FROM queue ORDER BY position") as cursor:
            rows = await cursor.fetchall()
            waiting_queue = [r[0] for r in rows]

        # Inicializar 50 butacas por cada una de las 3 sesiones
        async with db.execute("SELECT COUNT(*) FROM seats") as cursor:
            count = await cursor.fetchone()
            if count[0] == 0:
                for session in ["11h", "12:45h", "18h"]:
                    # Filas 1 a 11 (20 asientos) -> IDs 1 a 220
                    # Fila 12 (23 asientos, sin pasillo) -> IDs 221 a 243
                    await db.executemany(
                        "INSERT INTO seats (seat_number, session_time, status) VALUES (?, ?, ?)",
                        [
                            (i, session, "free")
                            for i in range(1, ASIENTOS_POR_FILA * FILAS + 1 + 3)
                        ],
                    )
                await db.commit()
        await db.commit()


async def broadcast_queue_positions():
    """Envía la posición en la cola a todos los clientes encolados."""
    async with queue_lock:
        for i, client_id in enumerate(waiting_queue):
            if client_id in active_connections:
                try:
                    await active_connections[client_id].send_text(
                        json.dumps({"type": "status", "status": "queued", "position": i + 1})
                    )
                except Exception as e:
                    logger.error("Error enviando posición de cola a %s: %s", client_id, e)


async def cleanup_waiting_queue():
    """Elimina de la waiting_queue a clientes que no tienen conexión activa."""
    while True:
        await asyncio.sleep(10)  # Ejecutar cada x segundos
        logger.info("Trying to queue_lock... ")
        # Crear una nueva lista solo con los clientes que sí tienen conexión activa.
        # Esto elimina eficazmente a los clientes "basura" de la cola.
        cleaned_queue = [
            client_id
            for client_id in waiting_queue
            if client_id in active_connections
        ]
        if len(cleaned_queue) < len(waiting_queue):
            logger.info(
                "[CLEANUP] Eliminados %d clientes inactivos de la cola.",
                len(waiting_queue) - len(cleaned_queue),
            )
            waiting_queue[:] = cleaned_queue  # Actualiza la cola en memoria
            await sync_queue_to_db()  # Sincroniza la cola limpia con la base de datos
        else:
            logger.info("No hay clientes desconectados, queue: %s", waiting_queue)

        # Limpiar de active_users a clientes que ya no tienen conexión activa (evita que se queden bloqueados slots activos)
        cleaned_active_users = {
            client_id
            for client_id in active_users
            if client_id in active_connections
        }
        logger.info("Stats: %d cleaned_active_users, %d active_users, %d active_connections, %d waiting_queue", len(cleaned_active_users), len(active_users), len(active_connections), len(waiting_queue))
        if len(cleaned_active_users) < len(active_users):
            logger.info(
                "[CLEANUP] Eliminados %d usuarios activos inactivos.",
                len(active_users) - len(cleaned_active_users),
            )
            # Liberar las reservas en estado "reserving" de estos usuarios inactivos
            for client_id in active_users - cleaned_active_users:
                try:
                    async with aiosqlite.connect(DB_FILE) as db:
                        await db.execute(
                            """
                            UPDATE seats 
                            SET status = "free", owner_id = NULL, owner_name = NULL 
                            WHERE owner_id = ? AND status = "reserving"
                        """,
                            (client_id,),
                        )
                        await db.commit()
                    if client_id in active_user_tasks:
                        active_user_tasks[client_id].cancel()
                        del active_user_tasks[client_id]
                except Exception as e:
                    logger.error("Error limpiando reservas de usuario inactivo %s: %s", client_id, e)
            
            active_users.clear()
            active_users.update(cleaned_active_users)
            await broadcast_seats()

        await process_queue()
        
        # Enviamos las posiciones actualizadas de la cola a todos
        await broadcast_queue_positions()
        await broadcast_admin_stats()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    # Iniciar la tarea de limpieza en segundo plano
    cleanup_task = asyncio.create_task(cleanup_waiting_queue())
    try:
        yield
    finally:
        # Cancelar la tarea de limpieza cuando la aplicación se detiene
        cleanup_task.cancel()
        # 2. Catch the CancelledError gracefully
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass


app = FastAPI(lifespan=lifespan)


async def expire_user_session(client_id: str):
    """Espera el tiempo límite y expulsa al usuario si sigue conectado."""
    try:
        # Calculamos cuánto tiempo real le queda
        expires_at = active_user_expires.get(client_id, time.time() + SESSION_TIMEOUT)
        remaining = expires_at - time.time()

        if remaining > 0:
            await asyncio.sleep(remaining)

        # Al despertar, verificamos si realmente se ha agotado y sigue activo
        if client_id in active_connections and client_id in active_users:
            if time.time() >= active_user_expires.get(client_id, 0):
                await active_connections[client_id].send_text(
                    json.dumps(
                        {
                            "type": "timeout",
                            "message": "Tiempo agotado. Tus butacas seleccionadas han quedado registradas, pero has perdido el turno de edición. Recarga la página si necesitas modificar algo.",
                        }
                    )
                )
                await active_connections[client_id].close()
    except asyncio.CancelledError:
        pass


async def get_all_seats():
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT seat_number, session_time, status, owner_id, owner_name FROM seats"
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]  # Convertimos la fila a diccionario completo


def sanitize_seats(seats: list[dict], client_id: str):
    sanitized_seats = [
        {
            "seat_number": s["seat_number"],
            "session_time": s["session_time"],
            "status": s["status"],
            "is_mine": s["owner_id"] == client_id,  # Solo enviamos true o false
        }
        for s in seats
    ]
    return sanitized_seats


async def broadcast_seats():
    seats = await get_all_seats()

    # 1. Enviar a usuarios normales (sanitizado)
    for client_id in list(active_users):
        if client_id in active_connections:
            sanitized_seats = sanitize_seats(seats, client_id)
            message = json.dumps({"type": "seats_update", "seats": sanitized_seats})
            try:
                await active_connections[client_id].send_text(message)
            except (WebSocketDisconnect, RuntimeError, ClientDisconnected) as exc:
                logger.error("Error while broadcast_seats: %s, removing client_id %s", exc, client_id)
                active_connections.pop(client_id)
                active_users.remove(client_id)

    # 2. Enviar a administradores (datos completos en tiempo real)
    if admin_connections:
        admin_message = json.dumps({"type": "admin_update", "seats": seats})
        for admin_ws in list(admin_connections):
            await admin_ws.send_text(admin_message)


async def sync_queue_to_db():
    """Sincroniza el estado de waiting_queue con la tabla queue en SQLite."""
    async with queue_lock:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute("DELETE FROM queue")
            await db.executemany(
                "INSERT INTO queue (client_id, position) VALUES (?, ?)",
                [(client_id, i) for i, client_id in enumerate(waiting_queue)],
            )
            await db.commit()


async def process_queue():
    global virtuales_procesados
    while len(active_users) < MAX_ACTIVE_USERS and waiting_queue:
        async with queue_lock:
            if not waiting_queue:
                break
            next_client = waiting_queue.pop(0)
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute("DELETE FROM queue")
                await db.executemany(
                    "INSERT INTO queue (client_id, position) VALUES (?, ?)",
                    [(client_id, i) for i, client_id in enumerate(waiting_queue)],
                )
                await db.commit()

        active_users.add(next_client)
        virtuales_procesados += 1
        # INICIAMOS SU CRONÓMETRO EN EL SERVIDOR
        active_user_expires[next_client] = time.time() + SESSION_TIMEOUT
        active_user_tasks[next_client] = asyncio.create_task(
            expire_user_session(next_client)
        )

        if next_client in active_connections:
            ws = active_connections[next_client]
            # Pasamos el tiempo al cliente para que pinte su reloj
            await ws.send_text(
                json.dumps(
                    {"type": "status", "status": "active", "timeout": SESSION_TIMEOUT}
                )
            )
            await broadcast_seats()

    for i, client_id in enumerate(waiting_queue):
        if client_id in active_connections:
            await active_connections[client_id].send_text(
                json.dumps({"type": "status", "status": "queued", "position": i + 1})
            )
    await broadcast_admin_stats()


@app.get("/")
async def get_index():
    return FileResponse("index.html")


@app.get("/api/config")
async def get_config():
    return {
        "animals": ANIMALS,
        "adjectives": ADJECTIVES
    }


@app.websocket("/ws/{client_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    client_id: str,
    secret: str = "",
):
    ip = get_client_ip(websocket)
    is_privileged = secret == ADMIN_SECRET

    if not is_privileged:
        # 1. Comprobar si la IP está bloqueada
        remaining = get_block_remaining(ip)
        if remaining > 0:
            await websocket.accept()
            await websocket.send_text(
                json.dumps(
                    {
                        "type": "error",
                        "message": f"Esta IP está temporalmente bloqueada por intentos fallidos. Inténtalo de nuevo en {remaining} segundos.",
                    }
                )
            )
            await websocket.close()
            return

        # 2. Normalizar y Validar combinación
        normalized_id = normalize_combination(client_id)
        if normalized_id not in VALID_COMBINATIONS:
            register_failed_attempt(ip)
            await websocket.accept()
            await websocket.send_text(
                json.dumps(
                    {
                        "type": "error",
                        "message": "La combinación de Animal y Adjetivo no es una credencial válida.",
                    }
                )
            )
            await websocket.close()
            return

        # Registro exitoso, limpiar fallos de la IP
        register_successful_attempt(ip)
        client_id = normalized_id
    else:
        client_id = normalize_combination(client_id)

    await websocket.accept()
    global virtuales_procesados

    # 3. EXPULSIÓN DE MULTIPESTAÑA
    if client_id in active_connections:
        old_ws = active_connections[client_id]
        try:
            await old_ws.send_text(
                json.dumps(
                    {
                        "type": "duplicate",
                        "message": "Has abierto la página en otra pestaña, esta ha quedado anulada. Disculpas.",
                    }
                )
            )
            await old_ws.close()
        except Exception:
            pass

    active_connections[client_id] = websocket

    nombre_limpio = html.escape(client_id[:50])
    # Distinguimos visualmente en el admin quién viene de la taquilla
    if is_privileged:
        active_users_names[client_id] = f"[TAQ] {nombre_limpio}"
    else:
        active_users_names[client_id] = nombre_limpio

    # 2. GESTIÓN JUSTA DE RECONEXIONES Y HERENCIA DE TIEMPO
    if client_id in active_users:
        # Si ya estaba activo, hereda el tiempo restante
        remaining = int(
            active_user_expires.get(client_id, time.time() + SESSION_TIMEOUT)
            - time.time()
        )
        if remaining < 0:
            remaining = 0

        await websocket.send_text(
            json.dumps({"type": "status", "status": "active", "timeout": remaining})
        )
        seats = await get_all_seats()
        sanitized_seats = sanitize_seats(seats, client_id)
        await websocket.send_text(
            json.dumps({"type": "seats_update", "seats": sanitized_seats})
        )

    elif client_id in waiting_queue:
        # Si ya estaba en la cola, le devolvemos su posición
        pos = waiting_queue.index(client_id) + 1
        await websocket.send_text(
            json.dumps({"type": "status", "status": "queued", "position": pos})
        )

    else:
        # Es un usuario completamente nuevo (o que recarga tras haber perdido el turno)
        if len(active_users) < MAX_ACTIVE_USERS or is_privileged:
            if not is_privileged:
                virtuales_procesados += 1
            # Permitimos entrar si hay hueco OR si es administrador
            active_users.add(client_id)
            active_user_expires[client_id] = time.time() + SESSION_TIMEOUT
            active_user_tasks[client_id] = asyncio.create_task(
                expire_user_session(client_id)
            )

            await websocket.send_text(
                json.dumps(
                    {"type": "status", "status": "active", "timeout": SESSION_TIMEOUT}
                )
            )
            seats = await get_all_seats()
            sanitized_seats = sanitize_seats(seats, client_id)
            await websocket.send_text(
                json.dumps({"type": "seats_update", "seats": sanitized_seats})
            )
        else:
            async with queue_lock:
                waiting_queue.append(client_id)
                async with aiosqlite.connect(DB_FILE) as db:
                    await db.execute("DELETE FROM queue")
                    await db.executemany(
                        "INSERT INTO queue (client_id, position) VALUES (?, ?)",
                        [(client_id, i) for i, client_id in enumerate(waiting_queue)],
                    )
                    await db.commit()
            pos = len(waiting_queue)
            await websocket.send_text(
                json.dumps({"type": "status", "status": "queued", "position": pos})
            )

    await broadcast_admin_stats()

    try:
        while True:
            data = await websocket.receive_text()

            try:
                payload = json.loads(data)
            except json.JSONDecodeError as exc:
                logger.error("Error decoding json %s", exc)
                continue  # Si no es un JSON válido, lo ignoramos
            if payload.get("action") == "ping":
                logger.info("Ping from client %s", client_id)
                continue

            if client_id not in active_users:
                continue

            action = payload.get("action")

            if action == "finalizar":
                async with aiosqlite.connect(DB_FILE) as db:
                    await db.execute(
                        """
                        UPDATE seats 
                        SET status = "reserved" 
                        WHERE owner_id = ? AND status = "reserving"
                    """,
                        (client_id,),
                    )
                    await db.commit()

                await websocket.close()
                if client_id in active_users:
                    active_users.remove(client_id)
                await process_queue()
                await broadcast_seats()
                break

            elif action == "toggle":
                seat_num = payload.get("seat_number")
                sess_time = payload.get("session_time")
                if (
                    not isinstance(seat_num, int)
                    or not isinstance(sess_time, str)
                    or sess_time not in ["11h", "12:45h", "18h"]
                ):
                    logger.warning(
                        "Invalid seat_num (%s) or sess_time (%s)", seat_num, sess_time
                    )
                    continue

                user_full_name = active_users_names.get(client_id, "Desconocido")

                async with aiosqlite.connect(DB_FILE) as db:
                    # Contamos cuántos asientos tiene ya (reserved o reserving)
                    async with db.execute(
                        "SELECT COUNT(*) FROM seats WHERE owner_id = ?", (client_id,)
                    ) as cursor:
                        user_seats_count = (await cursor.fetchone())[0]

                    if user_seats_count < 6:
                        # Intentamos marcar como 'reserving'
                        cursor = await db.execute(
                            """
                            UPDATE seats 
                            SET status = "reserving", owner_id = ?, owner_name = ? 
                            WHERE seat_number = ? AND session_time = ? AND status = "free"
                        """,
                            (client_id, user_full_name, seat_num, sess_time),
                        )

                        # Si no pudo (no estaba libre), vemos si es para liberar uno suyo
                        if cursor.rowcount == 0:
                            await db.execute(
                                """
                                UPDATE seats 
                                SET status = "free", owner_id = NULL, owner_name = NULL 
                                WHERE seat_number = ? AND session_time = ? AND owner_id = ?
                            """,
                                (seat_num, sess_time, client_id),
                            )
                    else:
                        # Si ya tiene 6, solo puede liberar
                        cursor = await db.execute(
                            """
                            UPDATE seats 
                            SET status = "free", owner_id = NULL, owner_name = NULL 
                            WHERE seat_number = ? AND session_time = ? AND owner_id = ?
                        """,
                            (seat_num, sess_time, client_id),
                        )

                        if cursor.rowcount == 0:
                            await websocket.send_text(
                                json.dumps(
                                    {
                                        "type": "error",
                                        "message": "Has alcanzado el límite de 6, tendrás que deseleccionar algún asiento...",
                                    }
                                )
                            )

                    await db.commit()
                await broadcast_seats()

    except (WebSocketDisconnect, RuntimeError, ClientDisconnected) as exc:
        logger.error("Error while processing websocket_endpoint %s", exc)
        if active_connections.get(client_id) == websocket:
            # Si se cierra sin finalizar, liberamos las que estaban en 'reserving'
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute(
                    """
                    UPDATE seats 
                    SET status = "free", owner_id = NULL, owner_name = NULL 
                    WHERE owner_id = ? AND status = "reserving"
                """,
                    (client_id,),
                )
                await db.commit()

            del active_connections[client_id]
            if client_id in active_users:
                active_users.remove(client_id)
            async with queue_lock:
                if client_id in waiting_queue:
                    waiting_queue.remove(client_id)
                    async with aiosqlite.connect(DB_FILE) as db:
                        await db.execute("DELETE FROM queue")
                        await db.executemany(
                            "INSERT INTO queue (client_id, position) VALUES (?, ?)",
                            [(client_id, i) for i, client_id in enumerate(waiting_queue)],
                        )
                        await db.commit()
            await broadcast_seats()
            if client_id in active_user_tasks:
                active_user_tasks[client_id].cancel()
                del active_user_tasks[client_id]

            await process_queue()
            await broadcast_queue_positions()
            await broadcast_admin_stats()


@app.get("/admin")
@app.get("/admin/{secret}")
async def get_admin(secret: str | None = None):
    if secret and secret != ADMIN_SECRET:
        return {"error": "No autorizado"}
    return FileResponse("admin.html")


@app.websocket("/ws/admin/{secret}")
async def admin_websocket(websocket: WebSocket, secret: str):
    if secret != ADMIN_SECRET:
        await websocket.close()
        return

    await websocket.accept()
    admin_connections.add(websocket)

    try:
        # Enviar el estado actual nada más conectar
        seats = await get_all_seats()
        await websocket.send_text(json.dumps({"type": "admin_update", "seats": seats}))
        await broadcast_admin_stats()

        while True:
            # Escuchamos los comandos del administrador
            data = await websocket.receive_text()
            try:
                payload = json.loads(data)
                if payload.get("action") == "reset_counter":
                    global virtuales_procesados
                    virtuales_procesados = 0
                    await broadcast_admin_stats()
                if payload.get("action") == "reset_db":
                    logger.warning(
                        "El administrador ha solicitado el RESETEO TOTAL de la base de datos."
                    )

                    # 1. Borrar tabla completa y recrearla con el init_db() original
                    async with aiosqlite.connect(DB_FILE) as db:
                        await db.execute("DROP TABLE IF EXISTS seats")
                        await db.commit()
                    await init_db()

                    # 2. Expulsar a todos los clientes (Evitar estados corruptos)
                    for cid, ws_conn in list(active_connections.items()):
                        try:
                            await ws_conn.send_text(
                                json.dumps(
                                    {
                                        "type": "timeout",
                                        "message": "El administrador ha reiniciado el sistema. Todas las reservas se han borrado. Por favor, recarga la página para empezar de nuevo.",
                                    }
                                )
                            )
                            await ws_conn.close()
                        except Exception:
                            pass

                    # 3. Limpiar toda la memoria del servidor
                    active_connections.clear()
                    active_users.clear()
                    async with queue_lock:
                        waiting_queue.clear()
                        async with aiosqlite.connect(DB_FILE) as db:
                            await db.execute("DELETE FROM queue")
                            await db.commit()
                    active_users_names.clear()
                    for task in active_user_tasks.values():
                        task.cancel()
                    active_user_tasks.clear()
                    active_user_expires.clear()

                    # 4. Refrescar los paneles admin conectados
                    new_seats = await get_all_seats()
                    for admin_ws in list(admin_connections):
                        try:
                            await admin_ws.send_text(
                                json.dumps({"type": "admin_update", "seats": new_seats})
                            )
                        except Exception:
                            pass
                    await broadcast_admin_stats()

            except json.JSONDecodeError:
                pass

    except WebSocketDisconnect:
        admin_connections.remove(websocket)


async def broadcast_admin_stats():
    """Envía el conteo de usuarios activos y en cola a los administradores."""
    if admin_connections:
        message = json.dumps(
            {
                "type": "admin_stats",
                "active_users": len(active_users),
                "queued_users": len(waiting_queue),
                "virtuales_procesados": virtuales_procesados,
            }
        )
        for admin_ws in list(admin_connections):
            try:
                await admin_ws.send_text(message)
            except Exception:
                pass


@app.get("/admin/{secret}/export.csv")
async def export_csv(secret: str):
    if secret != ADMIN_SECRET:
        return {"error": "No autorizado"}

    output = io.StringIO()
    writer = csv.writer(output)
    mitad = ASIENTOS_POR_FILA // 2
    writer.writerow(["Sesión", "Nombre y Apellidos", "Fila", "Butaca", "ID_Interno_BD"])

    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("""
        SELECT session_time, owner_name, seat_number 
        FROM seats 
        WHERE status = 'reserved'
        ORDER BY session_time, owner_name
        """) as cursor:
            async for session, owner, seat_id in cursor:
                fila = math.ceil(seat_id / ASIENTOS_POR_FILA)
                pos_in_row = (seat_id - 1) % ASIENTOS_POR_FILA

                if seat_id <= 220:
                    fila = math.ceil(seat_id / ASIENTOS_POR_FILA)
                    pos_in_row = (seat_id - 1) % ASIENTOS_POR_FILA
                    if pos_in_row < mitad:
                        butaca = (mitad - pos_in_row) * 2
                    else:
                        butaca = ((pos_in_row - mitad) * 2) + 1
                else:
                    fila = 12
                    fila12_nums = [
                        22,
                        20,
                        18,
                        16,
                        14,
                        12,
                        10,
                        8,
                        6,
                        4,
                        2,
                        1,
                        3,
                        5,
                        7,
                        9,
                        11,
                        13,
                        15,
                        17,
                        19,
                        21,
                        23,
                    ]
                    butaca = fila12_nums[seat_id - 221]

                writer.writerow([session, owner, f"Fila {fila}", butaca, seat_id])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="reservas.csv"'},
    )
