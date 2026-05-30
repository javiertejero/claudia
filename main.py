import json
from contextlib import asynccontextmanager
import aiosqlite
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

DB_FILE = "data/reservas.db"
MAX_ACTIVE_USERS = 2

# Estado en memoria
active_connections = {}  # {client_id: websocket}
active_users_names = {}  # {client_id: "Nombre Apellido"}
waiting_queue = []       
active_users = set()     

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
                        [(i, session, 'free') for i in range(1, 51)]
                    )
        await db.commit()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield

app = FastAPI(lifespan=lifespan)

async def get_all_seats():
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT seat_number, session_time, status, owner_id FROM seats') as cursor:
            rows = await cursor.fetchall()
            return [{"seat_number": r["seat_number"], "session_time": r["session_time"], "status": r["status"], "owner_id": r["owner_id"]} for r in rows]

async def broadcast_seats():
    seats = await get_all_seats()
    message = json.dumps({"type": "seats_update", "seats": seats})
    for client_id in list(active_users):
        if client_id in active_connections:
            await active_connections[client_id].send_text(message)

async def process_queue():
    while len(active_users) < MAX_ACTIVE_USERS and waiting_queue:
        next_client = waiting_queue.pop(0)
        active_users.add(next_client)
        if next_client in active_connections:
            ws = active_connections[next_client]
            await ws.send_text(json.dumps({"type": "status", "status": "active"}))
            await broadcast_seats()
            
    for i, client_id in enumerate(waiting_queue):
        if client_id in active_connections:
            await active_connections[client_id].send_text(
                json.dumps({"type": "status", "status": "queued", "position": i + 1})
            )

@app.get("/")
async def get_index():
    return FileResponse("index.html")

@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str, nombre: str = "", apellido: str = ""):
    await websocket.accept()
    active_connections[client_id] = websocket
    active_users_names[client_id] = f"{nombre} {apellido}".strip()

    if len(active_users) < MAX_ACTIVE_USERS:
        active_users.add(client_id)
        await websocket.send_text(json.dumps({"type": "status", "status": "active"}))
        seats = await get_all_seats()
        await websocket.send_text(json.dumps({"type": "seats_update", "seats": seats}))
    else:
        if client_id not in waiting_queue and client_id not in active_users:
            waiting_queue.append(client_id)
        pos = waiting_queue.index(client_id) + 1 if client_id in waiting_queue else 0
        await websocket.send_text(json.dumps({"type": "status", "status": "queued", "position": pos}))

    try:
        while True:
            data = await websocket.receive_text()
            payload = json.loads(data)
            
            if client_id in active_users and payload.get("action") == "toggle":
                seat_num = payload["seat_number"]
                sess_time = payload["session_time"]
                user_full_name = active_users_names.get(client_id, "Desconocido")
                
                async with aiosqlite.connect(DB_FILE) as db:
                    async with db.execute('SELECT status, owner_id FROM seats WHERE seat_number = ? AND session_time = ?', (seat_num, sess_time)) as cursor:
                        row = await cursor.fetchone()
                        if row:
                            status, owner_id = row
                            
                            if status == 'free':
                                # Comprobar cuántos asientos tiene ya este usuario
                                async with db.execute('SELECT COUNT(*) FROM seats WHERE owner_id = ?', (client_id,)) as count_cursor:
                                    user_seats_count = (await count_cursor.fetchone())[0]
                                
                                if user_seats_count >= 6:
                                    # Si ya tiene 6 o más, enviamos un mensaje de error solo a este usuario
                                    await websocket.send_text(json.dumps({
                                        "type": "error",
                                        "message": "Has alcanzado el límite de 6, tendrás que deseleccionar algún asiento..."
                                    }))
                                else:
                                    # Si tiene menos de 6, permitimos la reserva
                                    await db.execute('UPDATE seats SET status = "reserved", owner_id = ?, owner_name = ? WHERE seat_number = ? AND session_time = ?', (client_id, user_full_name, seat_num, sess_time))
                                    
                            elif status == 'reserved' and owner_id == client_id:
                                # Siempre permitimos liberar un asiento propio
                                await db.execute('UPDATE seats SET status = "free", owner_id = NULL, owner_name = NULL WHERE seat_number = ? AND session_time = ?', (seat_num, sess_time))
                            
                            await db.commit()
                # Refrescamos la vista para todos los usuarios activos
                await broadcast_seats()
                
    except WebSocketDisconnect:
        if client_id in active_connections:
            del active_connections[client_id]
        if client_id in active_users:
            active_users.remove(client_id)
        if client_id in waiting_queue:
            waiting_queue.remove(client_id)
        await process_queue()