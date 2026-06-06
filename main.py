import asyncio
import html
import json
import logging
import signal
import time
from contextlib import asynccontextmanager
from random import shuffle

import aiosqlite
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from uvicorn.protocols.utils import ClientDisconnected

import bootstrap_db
import broadcast
import identity
import rate_limiting
import seats
import state
from admin import admin_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper: touch last_seen for a client in the DB queue row
# ---------------------------------------------------------------------------


async def update_last_seen(client_id: str):
    """Updates the last_seen timestamp for client_id in the queue table."""
    now = time.time()
    async with aiosqlite.connect(state.DB_FILE) as db:
        await db.execute(
            "UPDATE queue SET last_seen = ? WHERE client_id = ?",
            (now, client_id),
        )
        await db.commit()


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


async def cleanup_waiting_queue():
    """Centralized cleanup: removes stale clients from waiting_queue, active_users
    and active_connections based on their last_seen value in the DB.
    A client_id is considered stale when:
        time.time() - last_seen > DISCONNECTION_TIMEOUT_SECONDS
    """
    while True:
        await asyncio.sleep(10)  # run every 10 seconds
        if state.IS_SHUTTING_DOWN:
            logger.info("[CLEANUP] Aplicación apagándose, saliendo del cleanup.")
            return

        if state.MAX_ACTIVE_USERS == 0:
            # El par de timbales no ha abierto; la cola debería estar vacía.
            if state.waiting_queue:
                logger.warning(
                    "[CLEANUP] MAX_ACTIVE_USERS es 0 pero hay %d clientes en cola. Randomizando asignaciones de asientos.",
                    len(state.waiting_queue),
                )
                shuffle(state.waiting_queue)
                await sync_queue_to_db()

        # ---------------------------------------------------------------
        # 1) Fetch last_seen values for all queued clients from the DB
        # ---------------------------------------------------------------
        now = time.time()
        timeout = state.DISCONNECTION_TIMEOUT_SECONDS

        async with aiosqlite.connect(state.DB_FILE) as db:
            async with db.execute("SELECT client_id, last_seen FROM queue") as cursor:
                rows = await cursor.fetchall()

        # Build a dict for quick lookup; clients with NULL last_seen are treated as seen "now"
        last_seen_map = {
            row[0]: (row[1] if row[1] is not None else now) for row in rows
        }

        def is_stale(client_id: str) -> bool:
            ls = last_seen_map.get(client_id, now)
            return (now - ls) > timeout

        # ---------------------------------------------------------------
        # 2) Clean waiting_queue
        # ---------------------------------------------------------------
        stale_in_queue = [cid for cid in state.waiting_queue if is_stale(cid)]
        if stale_in_queue:
            logger.info(
                "[CLEANUP] Eliminando %d clientes inactivos de la cola: %s",
                len(stale_in_queue),
                stale_in_queue,
            )
            for cid in stale_in_queue:
                state.waiting_queue.remove(cid)
            await sync_queue_to_db()
        else:
            logger.info("No hay clientes desconectados, queue: %s", state.waiting_queue)

        # ---------------------------------------------------------------
        # 3) Clean active_users (also check active_connections stale peers)
        # ---------------------------------------------------------------
        # Collect all client_ids that appear in active_users OR active_connections
        # so we can clean up stale entries from both structures.
        all_tracked = state.active_users | set(state.active_connections.keys())
        stale_active = {cid for cid in all_tracked if is_stale(cid)}

        logger.info(
            "Stats: %d active_users, %d active_connections, %d waiting_queue",
            len(state.active_users),
            len(state.active_connections),
            len(state.waiting_queue),
        )

        if stale_active:
            logger.info(
                "[CLEANUP] Eliminando %d clientes inactivos de active_users/connections: %s",
                len(stale_active),
                stale_active,
            )
            for client_id in stale_active:
                # Release any in-progress seat reservations
                try:
                    await seats.release_reserving_seats(client_id)
                except Exception as e:
                    logger.error(
                        "Error limpiando reservas de usuario inactivo %s: %s",
                        client_id,
                        e,
                    )
                # Cancel session-expiry task
                if client_id in state.active_user_tasks:
                    state.active_user_tasks[client_id].cancel()
                    del state.active_user_tasks[client_id]
                # Remove from all state structures
                state.active_users.discard(client_id)
                state.active_connections.pop(client_id, None)
                state.active_users_names.pop(client_id, None)

            await broadcast.broadcast_seats()

        await process_queue()

        # Broadcast updated queue positions and admin stats
        await broadcast.broadcast_queue_positions()
        await broadcast.broadcast_admin_stats()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Capture Uvicorn's original signal handlers
    original_sigint = signal.getsignal(signal.SIGINT)
    original_sigterm = signal.getsignal(signal.SIGTERM)

    def shutdown_interceptor(signum, frame):
        # 2. Flip our flag IMMEDIATELY upon receiving the signal
        state.IS_SHUTTING_DOWN = True
        print(f"\n[Interceptor] Signal {signum} caught! Flag flipped.")

        # 3. Hand control back to Uvicorn so it can gracefully close connections
        if signum == signal.SIGINT and callable(original_sigint):
            original_sigint(signum, frame)
        elif signum == signal.SIGTERM and callable(original_sigterm):
            original_sigterm(signum, frame)

    # 4. Override the signals with our interceptor
    signal.signal(signal.SIGINT, shutdown_interceptor)
    signal.signal(signal.SIGTERM, shutdown_interceptor)

    await bootstrap_db.init_db()
    # Iniciar la tarea de limpieza en segundo plano
    cleanup_task = asyncio.create_task(cleanup_waiting_queue())
    try:
        yield
    finally:
        # Cancelar la tarea de limpieza cuando la aplicación se detiene
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass


app = FastAPI(lifespan=lifespan)
app.include_router(admin_router)


async def expire_user_session(client_id: str):
    """Espera el tiempo límite y expulsa al usuario si sigue conectado."""
    try:
        # Calculamos cuánto tiempo real le queda
        expires_at = state.active_user_expires.get(
            client_id, time.time() + state.SESSION_TIMEOUT
        )
        remaining = expires_at - time.time()

        if remaining > 0:
            await asyncio.sleep(remaining)

        # Al despertar, verificamos si realmente se ha agotado y sigue activo
        if client_id in state.active_connections and client_id in state.active_users:
            if time.time() >= state.active_user_expires.get(client_id, 0):
                await state.active_connections[client_id].send_text(
                    json.dumps(
                        {
                            "type": "timeout",
                            "message": "Tiempo agotado. Tus butacas seleccionadas han quedado registradas, pero has perdido el turno de edición. Recarga la página si necesitas modificar algo.",
                        }
                    )
                )
                await state.active_connections[client_id].close()
    except asyncio.CancelledError:
        pass


async def sync_queue_to_db():
    """Sincroniza el estado de waiting_queue con la tabla queue en SQLite.

    Preserves each client's existing last_seen so that a queue reshuffle
    (e.g. cleanup or shuffle) does not unfairly reset disconnection timers.
    """
    if state.IS_SHUTTING_DOWN:
        logger.warning(
            "[SYNC_QUEUE] Aplicación apagándose, no se sincroniza la cola a la BD."
        )
        return
    now = time.time()
    async with state.queue_lock:
        async with aiosqlite.connect(state.DB_FILE) as db:
            # 1. Backup existing last_seen values before wiping the table
            async with db.execute("SELECT client_id, last_seen FROM queue") as cursor:
                existing = {row[0]: row[1] for row in await cursor.fetchall()}

            await db.execute("DELETE FROM queue")
            await db.executemany(
                "INSERT INTO queue (client_id, position, last_seen) VALUES (?, ?, ?)",
                [
                    # Preserve original last_seen; fall back to now only for brand-new entries
                    (client_id, i, existing.get(client_id, now))
                    for i, client_id in enumerate(state.waiting_queue)
                ],
            )
            await db.commit()


async def process_queue():
    while len(state.active_users) < state.MAX_ACTIVE_USERS and state.waiting_queue:
        async with state.queue_lock:
            if not state.waiting_queue:
                break
            next_client = state.waiting_queue.pop(0)
            async with aiosqlite.connect(state.DB_FILE) as db:
                # Backup existing last_seen values before wiping the table
                async with db.execute(
                    "SELECT client_id, last_seen FROM queue"
                ) as cursor:
                    existing = {row[0]: row[1] for row in await cursor.fetchall()}

                now = time.time()
                await db.execute("DELETE FROM queue")
                await db.executemany(
                    "INSERT INTO queue (client_id, position, last_seen) VALUES (?, ?, ?)",
                    [
                        # Preserve original last_seen; fall back to now only for brand-new entries
                        (client_id, i, existing.get(client_id, now))
                        for i, client_id in enumerate(state.waiting_queue)
                    ],
                )
                await db.commit()

        state.active_users.add(next_client)
        state.virtuales_procesados += 1
        # INICIAMOS SU CRONÓMETRO EN EL SERVIDOR
        state.active_user_expires[next_client] = time.time() + state.SESSION_TIMEOUT
        state.active_user_tasks[next_client] = asyncio.create_task(
            expire_user_session(next_client)
        )

        if next_client in state.active_connections:
            ws = state.active_connections[next_client]
            # Pasamos el tiempo al cliente para que pinte su reloj
            await ws.send_text(
                json.dumps(
                    {
                        "type": "status",
                        "status": "active",
                        "timeout": state.SESSION_TIMEOUT,
                    }
                )
            )
            await broadcast.broadcast_seats()

    for i, client_id in enumerate(state.waiting_queue):
        if client_id in state.active_connections:
            await state.active_connections[client_id].send_text(
                json.dumps({"type": "status", "status": "queued", "position": i + 1})
            )
    await broadcast.broadcast_admin_stats()


@app.get("/")
async def get_index():
    return FileResponse("index.html")


@app.get("/api/config")
async def get_config():
    return {"animals": identity.ANIMALS, "adjectives": identity.ADJECTIVES}


def get_client_ip(websocket: WebSocket) -> str:
    forwarded = websocket.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if websocket.client:
        return websocket.client.host
    return "127.0.0.1"


@app.websocket("/ws/{client_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    client_id: str,
    secret: str = "",
):
    ip = get_client_ip(websocket)
    is_privileged = secret == state.ADMIN_SECRET

    if not is_privileged:
        # 1. Comprobar si la IP está bloqueada
        remaining = rate_limiting.get_block_remaining(ip)
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
        normalized_id = identity.normalize_combination(client_id)
        if (
            not state.DISABLE_IDENTITY_CHECKS
            and normalized_id not in state.VALID_COMBINATIONS
        ):
            rate_limiting.register_failed_attempt(ip)
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
        rate_limiting.register_successful_attempt(ip)
        client_id = normalized_id
    else:
        # Modo privilegiado: normalizar y validar igualmente (sin penalizar al rate limiter)
        normalized_id = identity.normalize_combination(client_id)
        if normalized_id not in state.VALID_COMBINATIONS:
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
        client_id = normalized_id

    await websocket.accept()

    # 3. EXPULSIÓN DE MULTIPESTAÑA
    if client_id in state.active_connections:
        old_ws = state.active_connections[client_id]
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

    state.active_connections[client_id] = websocket

    nombre_limpio = html.escape(client_id[:50])
    # Distinguimos visualmente en el admin quién viene de la taquilla
    if is_privileged:
        state.active_users_names[client_id] = f"[TAQ] {nombre_limpio}"
    else:
        state.active_users_names[client_id] = nombre_limpio

    # 4. GESTIÓN JUSTA DE RECONEXIONES Y HERENCIA DE TIEMPO
    if client_id in state.active_users:
        # Si ya estaba activo, hereda el tiempo restante; actualizamos last_seen
        await update_last_seen(client_id)
        remaining = int(
            state.active_user_expires.get(
                client_id, time.time() + state.SESSION_TIMEOUT
            )
            - time.time()
        )
        if remaining < 0:
            remaining = 0

        await websocket.send_text(
            json.dumps({"type": "status", "status": "active", "timeout": remaining})
        )
        all_s = await seats.get_all_seats()
        sanitized_seats = seats.sanitize_seats(all_s, client_id)
        await websocket.send_text(
            json.dumps({"type": "seats_update", "seats": sanitized_seats})
        )

    elif client_id in state.waiting_queue:
        # Si ya estaba en la cola, actualizamos last_seen y devolvemos su posición
        await update_last_seen(client_id)
        pos = state.waiting_queue.index(client_id) + 1
        await websocket.send_text(
            json.dumps({"type": "status", "status": "queued", "position": pos})
        )

    else:
        # Es un usuario completamente nuevo (o que recarga tras haber perdido el turno)
        if len(state.active_users) < state.MAX_ACTIVE_USERS or is_privileged:
            if not is_privileged:
                state.virtuales_procesados += 1
            # Permitimos entrar si hay hueco OR si es administrador
            state.active_users.add(client_id)
            state.active_user_expires[client_id] = time.time() + state.SESSION_TIMEOUT
            state.active_user_tasks[client_id] = asyncio.create_task(
                expire_user_session(client_id)
            )

            await websocket.send_text(
                json.dumps(
                    {
                        "type": "status",
                        "status": "active",
                        "timeout": state.SESSION_TIMEOUT,
                    }
                )
            )
            all_s = await seats.get_all_seats()
            sanitized_seats = seats.sanitize_seats(all_s, client_id)
            await websocket.send_text(
                json.dumps({"type": "seats_update", "seats": sanitized_seats})
            )
        else:
            async with state.queue_lock:
                state.waiting_queue.append(client_id)
                now = time.time()
                async with aiosqlite.connect(state.DB_FILE) as db:
                    await db.execute("DELETE FROM queue")
                    await db.executemany(
                        "INSERT INTO queue (client_id, position, last_seen) VALUES (?, ?, ?)",
                        [(cid, i, now) for i, cid in enumerate(state.waiting_queue)],
                    )
                    await db.commit()
            pos = len(state.waiting_queue)
            await websocket.send_text(
                json.dumps({"type": "status", "status": "queued", "position": pos})
            )

    await broadcast.broadcast_admin_stats()

    try:
        while True:
            data = await websocket.receive_text()
            # Touch last_seen on every message received
            await update_last_seen(client_id)

            try:
                payload = json.loads(data)
            except json.JSONDecodeError as exc:
                logger.error("Error decoding json %s", exc)
                continue  # Si no es un JSON válido, lo ignoramos
            if payload.get("action") == "ping":
                logger.info("Ping from client %s", client_id)
                continue

            if client_id not in state.active_users:
                continue

            action = payload.get("action")

            if action == "finalizar":
                await seats.reserve_seats(client_id)
                await websocket.close()
                state.active_users.discard(client_id)
                state.active_connections.pop(client_id, None)
                state.active_users_names.pop(client_id, None)
                state.active_user_expires.pop(client_id, None)
                await process_queue()
                await broadcast.broadcast_seats()
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
                        "Invalid seat_num (%s) or sess_time (%s)",
                        seat_num,
                        sess_time,
                    )
                    continue

                user_full_name = state.active_users_names.get(client_id, "Desconocido")
                err_msg = await seats.toggle_seat(
                    client_id, seat_num, sess_time, user_full_name
                )
                if err_msg:
                    await websocket.send_text(
                        json.dumps(
                            {
                                "type": "error",
                                "message": err_msg,
                            }
                        )
                    )
                # Touch last_seen after sending a response
                await update_last_seen(client_id)
                await broadcast.broadcast_seats()

    except (WebSocketDisconnect, RuntimeError, ClientDisconnected) as exc:
        # On disconnect we do NOT remove the client from waiting_queue, active_users,
        # or active_connections immediately. The cleanup_waiting_queue background task
        # will evict stale clients once DISCONNECTION_TIMEOUT_SECONDS has elapsed.
        if state.IS_SHUTTING_DOWN:
            logger.warning("Server is shutting down. Skipping normal disconnect logic.")
            return
        logger.info(
            "[DISCONNECT] Client %s disconnected (%s). Keeping in state until timeout.",
            client_id,
            exc,
        )
        # Only clear the active WebSocket reference if it still points to this socket
        if state.active_connections.get(client_id) == websocket:
            state.active_connections.pop(client_id, None)

        await broadcast.broadcast_admin_stats()
