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
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.responses import StreamingResponse

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

DB_FILE = "data/reservas.db"
MAX_ACTIVE_USERS = 2
SESSION_TIMEOUT = 180  # segundos para expiración de sesión de usuario
active_user_tasks = {}  # Para guardar el cronómetro de cada usuario
active_user_expires = {}  # Guarda el timestamp absoluto en el que expira

ADMIN_SECRET = os.getenv("CLAUDIA_SECRET", str(uuid.uuid4()))
logger.debug("Admin secret: visit http://127.0.0.1:8000/admin/%s", ADMIN_SECRET)
admin_connections = set()


# Estado en memoria
active_connections = {}  # {client_id: websocket}
active_users_names = {}  # {client_id: "Nombre Apellido"}
virtuales_procesados = 0
waiting_queue = []
active_users = set()
ASIENTOS_POR_FILA = 20
FILAS = 12


async def init_db():
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


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
            await active_connections[client_id].send_text(message)

    # 2. Enviar a administradores (datos completos en tiempo real)
    if admin_connections:
        admin_message = json.dumps({"type": "admin_update", "seats": seats})
        for admin_ws in list(admin_connections):
            await admin_ws.send_text(admin_message)


async def process_queue():
    global virtuales_procesados
    while len(active_users) < MAX_ACTIVE_USERS and waiting_queue:
        next_client = waiting_queue.pop(0)
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


@app.websocket("/ws/{client_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    client_id: str,
    nombre: str = "",
    apellido: str = "",
    secret: str = "",
):
    await websocket.accept()
    global virtuales_procesados

    # Comprobamos si tiene el pase VIP
    is_privileged = secret == ADMIN_SECRET

    # 1. EXPULSIÓN DE MULTIPESTAÑA
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

    nombre_limpio = html.escape(nombre[:50])
    apellido_limpio = html.escape(apellido[:50])
    # Distinguimos visualmente en el admin quién viene de la taquilla
    if is_privileged:
        active_users_names[client_id] = (
            f"[TAQ] {nombre_limpio} {apellido_limpio}".strip()
        )
    else:
        active_users_names[client_id] = f"{nombre_limpio} {apellido_limpio}".strip()

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
            waiting_queue.append(client_id)
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
                continue

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
                await broadcast_seats()

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

    except WebSocketDisconnect:
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
            await broadcast_seats()

            del active_connections[client_id]
            if client_id in active_users:
                active_users.remove(client_id)
            if client_id in waiting_queue:
                waiting_queue.remove(client_id)

            if client_id in active_user_tasks:
                active_user_tasks[client_id].cancel()
                del active_user_tasks[client_id]

            await process_queue()
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
        seats = await get_all_seats()
        await websocket.send_text(json.dumps({"type": "admin_update", "seats": seats}))
        await broadcast_admin_stats()

        while True:
            data = await websocket.receive_text()
            try:
                payload = json.loads(data)
                if payload.get("action") == "reset_counter":
                    global virtuales_procesados
                    virtuales_procesados = 0
                    await broadcast_admin_stats()
                if payload.get("action") == "reset_db":
                    logger.warning("RESET TOTAL de la base de datos.")
                    async with aiosqlite.connect(DB_FILE) as db:
                        await db.execute("DROP TABLE IF EXISTS seats")
                        await db.commit()
                    await init_db()

                    for cid, ws_conn in list(active_connections.items()):
                        try:
                            await ws_conn.send_text(
                                json.dumps(
                                    {
                                        "type": "timeout",
                                        "message": "El administrador ha reiniciado el sistema. Por favor, recarga.",
                                    }
                                )
                            )
                            await ws_conn.close()
                        except Exception:
                            pass

                    active_connections.clear()
                    active_users.clear()
                    waiting_queue.clear()
                    active_users_names.clear()
                    for task in active_user_tasks.values():
                        task.cancel()
                    active_user_tasks.clear()
                    active_user_expires.clear()

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
