import aiosqlite
import asyncio
import csv
import html
import io
import json
import logging
import math
import os
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
active_user_tasks = {} # Para guardar el cronómetro de cada usuario

ADMIN_SECRET = os.getenv('CLAUDIA_SECRET', str(uuid.uuid4()))
logger.debug("Admin secret: visit http://127.0.0.1:8000/admin/%s", ADMIN_SECRET)
admin_connections = set()


# Estado en memoria
active_connections = {}  # {client_id: websocket}
active_users_names = {}  # {client_id: "Nombre Apellido"}
waiting_queue = []       
active_users = set()     
ASIENTOS_POR_FILA = 20
FILAS = 10

async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS seats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                seat_number INTEGER,
                session_time TEXT,
                status TEXT DEFAULT 'free',
                owner_id TEXT,
                owner_name TEXT
            )
        ''')
        # Inicializar 50 butacas por cada una de las 3 sesiones
        async with db.execute('SELECT COUNT(*) FROM seats') as cursor:
            count = await cursor.fetchone()
            if count[0] == 0:
                for session in ['11h', '12:45h', '18h']:
                    await db.executemany(
                        'INSERT INTO seats (seat_number, session_time, status) VALUES (?, ?, ?)',
                        [(i, session, 'free') for i in range(1, ASIENTOS_POR_FILA * FILAS + 1)]
                    )
        await db.commit()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield

app = FastAPI(lifespan=lifespan)

async def expire_user_session(client_id: str):
    """Espera el tiempo límite y expulsa al usuario si sigue conectado."""
    try:
        await asyncio.sleep(SESSION_TIMEOUT)
        if client_id in active_connections:
            # Le enviamos un mensaje especial antes de cortarle
            await active_connections[client_id].send_text(json.dumps({
                "type": "timeout",
                "message": "Tiempo agotado. Tus butacas seleccionadas han quedado registradas, pero has perdido el turno de edición. Recarga la página si necesitas modificar algo."
            }))
            # Cortamos la conexión. Esto disparará automáticamente el bloque except WebSocketDisconnect
            await active_connections[client_id].close()
    except asyncio.CancelledError:
        # Si el usuario cierra la pestaña antes de los 3 minutos, cancelamos esta tarea en silencio
        pass

async def get_all_seats():
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT seat_number, session_time, status, owner_id, owner_name FROM seats') as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows] # Convertimos la fila a diccionario completo

def sanitize_seats(seats: list[dict], client_id: str):
    sanitized_seats = [
        {
            "seat_number": s["seat_number"],
            "session_time": s["session_time"],
            "status": s["status"],
            "is_mine": s["owner_id"] == client_id # Solo enviamos true o false
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
    while len(active_users) < MAX_ACTIVE_USERS and waiting_queue:
        next_client = waiting_queue.pop(0)
        active_users.add(next_client)

        # INICIAMOS SU CRONÓMETRO EN EL SERVIDOR
        active_user_tasks[next_client] = asyncio.create_task(expire_user_session(next_client))

        if next_client in active_connections:
            ws = active_connections[next_client]
            # Pasamos el tiempo al cliente para que pinte su reloj
            await ws.send_text(json.dumps({"type": "status", "status": "active", "timeout": SESSION_TIMEOUT}))
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
async def websocket_endpoint(websocket: WebSocket, client_id: str, nombre: str = "", apellido: str = ""):
    await websocket.accept()

    # 1. LÓGICA MULTIPESTAÑA: Expulsar conexión antigua si existe
    if client_id in active_connections:
        old_ws = active_connections[client_id]
        try:
            # Avisamos a la pestaña vieja
            await old_ws.send_text(json.dumps({
                "type": "duplicate",
                "message": "Has abierto la página en otra pestaña, esta ha quedado anulada. Disculpas."
            }))
            await old_ws.close()
        except Exception:
            pass

    active_connections[client_id] = websocket
    nombre_limpio = html.escape(nombre[:50])     # Máximo 50 caracteres
    apellido_limpio = html.escape(apellido[:50])
    active_users_names[client_id] = f"{nombre_limpio} {apellido_limpio}".strip()

    if len(active_users) < MAX_ACTIVE_USERS:
        active_users.add(client_id)
        active_user_tasks[client_id] = asyncio.create_task(expire_user_session(client_id))
        await websocket.send_text(json.dumps({"type": "status", "status": "active", "timeout": SESSION_TIMEOUT}))
        seats = await get_all_seats()
        sanitized_seats = sanitize_seats(seats, client_id)
        await websocket.send_text(json.dumps({"type": "seats_update", "seats": sanitized_seats}))
    else:
        if client_id not in waiting_queue and client_id not in active_users:
            waiting_queue.append(client_id)
        pos = waiting_queue.index(client_id) + 1 if client_id in waiting_queue else 0
        await websocket.send_text(json.dumps({"type": "status", "status": "queued", "position": pos}))

    await broadcast_admin_stats()

    try:
        while True:
            data = await websocket.receive_text()
            try:
                payload = json.loads(data)
            except json.JSONDecodeError as exc:
                logger.error("Error decoding json %s", exc)
                continue # Si no es un JSON válido, lo ignoramos

            if client_id in active_users and payload.get("action") == "toggle":
                # Usamos .get() y validamos el tipo para evitar excepciones
                seat_num = payload.get("seat_number")
                sess_time = payload.get("session_time")
                if not isinstance(seat_num, int) or not isinstance(sess_time, str) or sess_time not in ["11h", "12:45h", "18h"]:
                    logger.warning("Invalid seat_num (%s) or sess_time (%s)", seat_num, sess_time)
                    continue # Ignoramos cargas útiles manipuladas
                user_full_name = active_users_names.get(client_id, "Desconocido")
                
                async with aiosqlite.connect(DB_FILE) as db:
                    # 1. Obtenemos cuántos tiene ANTES de intentar reservar
                    async with db.execute('SELECT COUNT(*) FROM seats WHERE owner_id = ?', (client_id,)) as cursor:
                        user_seats_count = (await cursor.fetchone())[0]

                    # Intentamos reservar (Operación Atómica)
                    if user_seats_count < 6:
                        cursor = await db.execute('''
                            UPDATE seats 
                            SET status = "reserved", owner_id = ?, owner_name = ? 
                            WHERE seat_number = ? AND session_time = ? AND status = "free"
                        ''', (client_id, user_full_name, seat_num, sess_time))
                        
                        # Si no pudo reservar (no estaba libre), vemos si intentaba liberar uno suyo
                        if cursor.rowcount == 0:
                            await db.execute('''
                                UPDATE seats 
                                SET status = "free", owner_id = NULL, owner_name = NULL 
                                WHERE seat_number = ? AND session_time = ? AND owner_id = ?
                            ''', (seat_num, sess_time, client_id))
                    else:
                        # Si ya tiene 6, intentamos liberar el asiento (solo funcionará si es suyo)
                        cursor = await db.execute('''
                            UPDATE seats 
                            SET status = "free", owner_id = NULL, owner_name = NULL 
                            WHERE seat_number = ? AND session_time = ? AND owner_id = ?
                        ''', (seat_num, sess_time, client_id))
                        
                        # Si rowcount es 0, el asiento no era suyo. Estaba intentando coger el 7º.
                        if cursor.rowcount == 0:
                            await websocket.send_text(json.dumps({
                                "type": "error",
                                "message": "Has alcanzado el límite de 6, tendrás que deseleccionar algún asiento..."
                            }))

                    await db.commit()
                # Refrescamos la vista para todos los usuarios activos
                await broadcast_seats()
                
    except WebSocketDisconnect:
        # 2. CORRECCIÓN DE CONCURRENCIA: Solo limpiamos si la conexión que muere es la activa
        if active_connections.get(client_id) == websocket:
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

@app.get("/admin/{secret}")
async def get_admin_panel(secret: str):
    if secret != ADMIN_SECRET:
        return {"error": "No autorizado"} # Podrías devolver un 404 para disimular
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
            # Mantenemos la conexión viva
            await websocket.receive_text()
            
    except WebSocketDisconnect:
        admin_connections.remove(websocket)


async def broadcast_admin_stats():
    """Envía el conteo de usuarios activos y en cola a los administradores."""
    if admin_connections:
        message = json.dumps({
            "type": "admin_stats",
            "active_users": len(active_users),
            "queued_users": len(waiting_queue)
        })
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

    writer.writerow(
        ["Sesión", "Nombre y Apellidos", "Fila", "Butaca", "ID_Interno_BD"]
    )


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

                # La misma lógica del estándar de teatro
                if pos_in_row < mitad:
                    butaca = ((mitad - 1 - pos_in_row) * 2) + 1
                else:
                    butaca = ((pos_in_row - mitad) * 2) + 2

                writer.writerow([session, owner, f"Fila {fila}", butaca, seat_id])


    output.seek(0)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={
            "Content-Disposition":
                'attachment; filename="reservas.csv"'
        }
    )