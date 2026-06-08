import asyncio
import glob
import html
import json
import logging
import os
import signal
import time
from contextlib import asynccontextmanager
from random import shuffle

import aiosqlite
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from uvicorn.protocols.utils import ClientDisconnected

import bootstrap_db
import broadcast
import rate_limiting
import seats
import state
from admin import admin_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

import identity  # noqa: E402

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper: touch last_seen for a client in the DB queue row
# ---------------------------------------------------------------------------


async def update_last_seen(client_id: str):
    """Updates the last_seen timestamp for client_id in the queue table."""
    state.client_last_seen[client_id] = time.time()
    state.queue_dirty = True


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
        logger.info("Active users: %s", state.active_users)
        logger.info("Waiting users: %s", state.waiting_queue)
        for active_user, timeout in state.active_user_expires.items():
            expire_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timeout))
            logger.info(f"User {active_user} expires at {expire_str}")

        if stale_active:
            logger.info(
                "[CLEANUP] Eliminando %d clientes inactivos de active_users/connections: %s",
                len(stale_active),
                stale_active,
            )
            for client_id in stale_active:
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


async def persist_state_task():
    """Vuelca la cola de espera a SQLite en batch cada 3 segundos si ha habido cambios."""
    while not state.IS_SHUTTING_DOWN:
        await asyncio.sleep(3)
        if state.queue_dirty:
            state.queue_dirty = False
            now = time.time()
            async with state.db_write_lock:
                try:
                    async with aiosqlite.connect(state.DB_FILE) as db:
                        await db.execute("PRAGMA journal_mode=WAL;")
                        await db.execute("PRAGMA synchronous=NORMAL;")
                        # Merge with in-memory last_seen
                        async with db.execute(
                            "SELECT client_id, last_seen FROM queue"
                        ) as cursor:
                            existing = {
                                row[0]: row[1] for row in await cursor.fetchall()
                            }
                        existing.update(state.client_last_seen)

                        await db.execute("DELETE FROM queue")
                        await db.executemany(
                            "INSERT INTO queue (client_id, position, last_seen) VALUES (?, ?, ?)",
                            [
                                (client_id, i, existing.get(client_id, now))
                                for i, client_id in enumerate(state.waiting_queue)
                            ],
                        )
                        await db.commit()
                except Exception as e:
                    logger.error("Error persistiendo estado de cola: %s", e)
                    state.queue_dirty = True


async def backup_db_task():
    """Crea una copia de seguridad rotativa cada 60 segundos."""
    while not state.IS_SHUTTING_DOWN:
        await asyncio.sleep(60)
        backup_dir = "data/backups"
        os.makedirs(backup_dir, exist_ok=True)
        now = time.time()
        timestamp = time.strftime("%Y%m%d_%H%M%S", time.localtime(now))
        backup_file = os.path.join(backup_dir, f"reservas_{timestamp}.db")

        async with state.db_write_lock:
            try:
                # Use SQLite's safe backup API
                async with aiosqlite.connect(state.DB_FILE) as db:
                    async with aiosqlite.connect(backup_file) as backup_db:
                        await db.backup(backup_db)
                logger.info("Backup local creado en %s", backup_file)
            except Exception as e:
                logger.error("Error creando backup: %s", e)
                continue

        # Cleanup backups older than 24 hours
        try:
            for f in glob.glob(os.path.join(backup_dir, "reservas_*.db")):
                if now - os.path.getctime(f) > 86400:
                    os.remove(f)
                    logger.info("Backup local antiguo eliminado: %s", f)
        except Exception as e:
            logger.error("Error limpiando backups antiguos: %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Capture Uvicorn's original signal handlers
    try:
        original_sigint = signal.getsignal(signal.SIGINT)
        original_sigterm = signal.getsignal(signal.SIGTERM)

        def shutdown_interceptor(signum, frame):
            # 2. Flip our flag IMMEDIATELY upon receiving the signal
            state.IS_SHUTTING_DOWN = True
            print(f"\\n[Interceptor] Signal {signum} caught! Flag flipped.")

            # 3. Hand control back to Uvicorn so it can gracefully close connections
            if signum == signal.SIGINT and callable(original_sigint):
                original_sigint(signum, frame)
            elif signum == signal.SIGTERM and callable(original_sigterm):
                original_sigterm(signum, frame)

        # 4. Override the signals with our interceptor
        signal.signal(signal.SIGINT, shutdown_interceptor)
        signal.signal(signal.SIGTERM, shutdown_interceptor)
    except ValueError:
        logger.warning(
            "No se pudieron registrar las señales (probablemente ejecutando en un thread de test)."
        )

    await bootstrap_db.init_db()
    # Iniciar la tarea de limpieza en segundo plano
    cleanup_task = asyncio.create_task(cleanup_waiting_queue())
    persist_task = asyncio.create_task(persist_state_task())
    backup_task = asyncio.create_task(backup_db_task())
    try:
        yield
    finally:
        # Cancelar la tarea de limpieza cuando la aplicación se detiene
        cleanup_task.cancel()
        backup_task.cancel()

        # Force a final flush synchronously before exiting
        if state.queue_dirty:
            logger.info("Realizando volcado final de memoria completado a SQLite...")
            try:
                async with aiosqlite.connect(state.DB_FILE) as db:
                    await db.execute("PRAGMA journal_mode=WAL;")
                    await db.execute("DELETE FROM queue")
                    await db.executemany(
                        "INSERT INTO queue (client_id, position, last_seen) VALUES (?, ?, ?)",
                        [
                            (
                                client_id,
                                i,
                                state.client_last_seen.get(client_id, time.time()),
                            )
                            for i, client_id in enumerate(state.waiting_queue)
                        ],
                    )
                    await db.commit()
                logger.info("Volcado final de memoria completado.")
            except Exception as e:
                logger.error("Error en el volcado final: %s", e)

        persist_task.cancel()
        try:
            await asyncio.gather(
                cleanup_task, persist_task, backup_task, return_exceptions=True
            )
        except asyncio.CancelledError:
            pass


app = FastAPI(lifespan=lifespan)
app.include_router(admin_router)


class CacheControlledStaticFiles(StaticFiles):
    def file_response(self, *args, **kwargs) -> FileResponse:
        response = super().file_response(*args, **kwargs)
        response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        return response


app.mount("/static", CacheControlledStaticFiles(directory="static"), name="static")


async def expire_user_session(client_id: str):
    """Espera el tiempo límite y expulsa al usuario si sigue conectado."""
    try:
        # Calculamos cuánto tiempo real le queda
        expires_at = state.active_user_expires.get(
            client_id, time.time() + state.SESSION_TIMEOUT
        )
        remaining = expires_at - time.time()

        if remaining > 0:
            logger.info("User %s will expire after %s", client_id, remaining)
            await asyncio.sleep(remaining)

        # Al despertar, verificamos si realmente se ha agotado y sigue activo
        if client_id in state.active_users:
            if time.time() >= state.active_user_expires.get(client_id, 0):
                logger.warning("Expiring user %s", client_id)
                try:
                    await state.active_connections[client_id].send_text(
                        json.dumps(
                            {
                                "type": "timeout",
                                "message": "Tiempo agotado. Tus butacas seleccionadas han quedado registradas, pero has perdido el turno de edición. Recarga la página si necesitas modificar algo.",
                            }
                        )
                    )
                except Exception as e:
                    logger.error("Error enviando timeout a %s: %s", client_id, e)
                await asyncio.sleep(
                    0.1
                )  # wait to ensure the client can process the timeout message
                try:
                    await state.active_connections[client_id].close()
                except Exception as e:
                    # es probable que esté ya cerrada, suele pasar
                    logger.warning("Error cerrando conexión a %s: %s", client_id, e)
                state.active_user_expires.pop(client_id, None)
                state.active_user_tasks.pop(client_id, None)
                state.active_users.discard(client_id)
                state.active_connections.pop(client_id, None)
                state.active_users_names.pop(client_id, None)
                await broadcast.broadcast_seats()
                await (
                    process_queue()
                )  # trigger process_queue to ensure we let the next user enter
            else:
                logger.warning(
                    "User %s was not disconnected, probably reconnected with extension...",
                    client_id,
                )
    except asyncio.CancelledError:
        logger.exception("Task cancelled for user %s", client_id)


async def sync_queue_to_db():
    """Sincroniza el estado de waiting_queue con la tabla queue en SQLite."""
    if state.IS_SHUTTING_DOWN:
        logger.warning(
            "[SYNC_QUEUE] Aplicación apagándose, no se sincroniza la cola a la BD."
        )
        return
    state.queue_dirty = True


async def process_queue():
    """Procesa la cola de espera y permite entrar a los usuarios."""
    logger.info("Procesando cola de espera...")
    while len(state.active_users) < state.MAX_ACTIVE_USERS and state.waiting_queue:
        async with state.queue_lock:
            if not state.waiting_queue:
                break
            next_client = state.waiting_queue.pop(0)
            state.queue_dirty = True

        state.active_users.add(next_client)
        state.virtuales_procesados += 1
        # INICIAMOS SU CRONÓMETRO EN EL SERVIDOR
        state.active_user_expires[next_client] = time.time() + state.SESSION_TIMEOUT
        state.active_user_tasks[next_client] = asyncio.create_task(
            expire_user_session(next_client)
        )

        if next_client in state.active_connections:
            ws = state.active_connections[next_client]
            # Pasamos el tiempo y cuota al cliente para que pinte su reloj e indicador
            await ws.send_text(
                json.dumps(
                    {
                        "type": "status",
                        "status": "active",
                        "timeout": state.SESSION_TIMEOUT,
                        "quota": state.get_quota(next_client),
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


def get_client_ip(request: Request | WebSocket) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "127.0.0.1"


@app.get("/")
async def get_index():
    return FileResponse("index.html")


@app.get("/transferencia")
async def get_transfer_page():
    return FileResponse("transfer.html")


@app.get("/thanks")
@app.get("/thanks/")
async def get_thanks_page():
    return FileResponse("thanks.html")


@app.get("/gracias")
@app.get("/gracias/")
async def get_gracias_page():
    return FileResponse("agradecimientos.html")


@app.get("/legal")
@app.get("/legal/")
async def get_legal_page():
    return FileResponse("legal.html")


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse("static/favicon.ico")


@app.get("/favicon-32x32.png", include_in_schema=False)
async def favicon_32():
    return FileResponse("static/favicon-32x32.png")


@app.get("/favicon-192x192.png", include_in_schema=False)
async def favicon_192():
    return FileResponse("static/favicon-192x192.png")


@app.get("/apple-touch-icon.png", include_in_schema=False)
async def apple_touch_icon():
    return FileResponse("static/apple-touch-icon.png")


@app.get("/site.webmanifest", include_in_schema=False)
async def webmanifest():
    return FileResponse("static/site.webmanifest")


@app.get("/api/config")
async def get_config():
    return {
        "animals": identity.ANIMALS,
        "adjectives": identity.ADJECTIVES,
        "app_url": state.APP_URL,
        "taquilla_mode": state.TAQUILLA_MODE,
    }


@app.get("/check")
@app.get("/check/")
@app.get("/check/{animal}/{adjetivo}")
async def get_check_page(animal: str | None = None, adjetivo: str | None = None):
    return FileResponse("check.html")


@app.get("/api/check/info")
async def get_check_info(animal: str = "", adjetivo: str = "", token: str = ""):
    if token != state.ADMIN_SECRET:
        return JSONResponse(
            {"error": "No autorizado (Token incorrecto o ausente)"}, status_code=401
        )

    import math

    from identity import normalize_combination

    normalized = normalize_combination(f"{animal}_{adjetivo}")

    if normalized not in state.VALID_COMBINATIONS:
        return JSONResponse(
            {
                "valid": False,
                "reason": "La combinación de Animal y Adjetivo no existe o no es válida.",
            }
        )

    seats_list = []
    already_used_recently = False
    recently_used_time = 0.0

    async with aiosqlite.connect(state.DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT seat_number, session_time, used_at FROM seats WHERE owner_id = ? AND status = 'reserved'",
            (normalized,),
        ) as cursor:
            rows = await cursor.fetchall()
            for r in rows:
                seat_num = r["seat_number"]
                sess = r["session_time"]
                used_at = r["used_at"]

                # Lógica física de butacas
                if seat_num <= 220:
                    fila = math.ceil(seat_num / state.ASIENTOS_POR_FILA)
                    pos_in_row = (seat_num - 1) % state.ASIENTOS_POR_FILA
                    mitad = state.ASIENTOS_POR_FILA // 2
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
                    butaca = fila12_nums[seat_num - 221]

                seats_list.append(
                    {
                        "seat_number": seat_num,
                        "session_time": sess,
                        "fila": fila,
                        "butaca": butaca,
                        "used_at": used_at,
                    }
                )

                if used_at is not None:
                    time_diff = time.time() - used_at
                    if time_diff < 900:  # 15 minutos
                        already_used_recently = True
                        if used_at > recently_used_time:
                            recently_used_time = used_at

    # Ordenar por fila y luego por seat_number (orden físico de izquierda a derecha)
    seats_list.sort(key=lambda x: (x["fila"], x["seat_number"]))

    if not seats_list:
        return JSONResponse(
            {
                "valid": False,
                "reason": "La combinación existe pero no tiene ninguna butaca reservada.",
            }
        )

    return {
        "valid": True,
        "user_id": normalized,
        "seats": seats_list,
        "already_used_recently": already_used_recently,
        "recently_used_time": recently_used_time,
        "server_time": time.time(),
    }


@app.post("/api/check/use")
async def mark_seats_as_used(request: Request):
    body = await request.json()
    animal = body.get("animal", "")
    adjetivo = body.get("adjetivo", "")
    token = body.get("token", "")

    if token != state.ADMIN_SECRET:
        return JSONResponse(
            {"error": "No autorizado (Token incorrecto o ausente)"}, status_code=401
        )

    from identity import normalize_combination

    normalized = normalize_combination(f"{animal}_{adjetivo}")

    if normalized not in state.VALID_COMBINATIONS:
        return JSONResponse({"error": "Identidad inválida"}, status_code=404)

    now = time.time()
    async with aiosqlite.connect(state.DB_FILE) as db:
        cursor = await db.execute(
            "UPDATE seats SET used_at = ? WHERE owner_id = ? AND status = 'reserved'",
            (now, normalized),
        )
        marked_count = cursor.rowcount
        await db.commit()

    return {"ok": True, "marked_count": marked_count, "used_at": now}


@app.get("/api/my-reservations")
async def get_my_reservations(animal: str = "", adjetivo: str = ""):
    import math

    from identity import normalize_combination

    normalized = normalize_combination(f"{animal}_{adjetivo}")
    if normalized not in state.VALID_COMBINATIONS:
        return JSONResponse({"error": "Identidad no válida"}, status_code=404)

    seats_list = []
    async with aiosqlite.connect(state.DB_FILE) as db:
        async with db.execute(
            "SELECT seat_number, session_time FROM seats WHERE owner_id = ? AND status = 'reserved'",
            (normalized,),
        ) as cursor:
            rows = await cursor.fetchall()
            for r in rows:
                seat_num = r[0]
                sess = r[1]

                if seat_num <= 220:
                    fila = math.ceil(seat_num / state.ASIENTOS_POR_FILA)
                    pos_in_row = (seat_num - 1) % state.ASIENTOS_POR_FILA
                    mitad = state.ASIENTOS_POR_FILA // 2
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
                    butaca = fila12_nums[seat_num - 221]

                seats_list.append(
                    {"session_time": sess, "fila": fila, "butaca": butaca}
                )
    return {"seats": seats_list, "user_id": normalized}


@app.get("/api/mi-hash")
async def get_mi_hash(id: str = ""):
    """Devuelve el hash_transferencia del usuario autenticado.
    El frontend lo usa para construir el enlace de WhatsApp.
    """
    from identity import normalize_combination

    normalized = normalize_combination(id)
    if normalized not in state.VALID_COMBINATIONS:
        return JSONResponse({"error": "Identidad no válida"}, status_code=404)
    hash_t = state.USER_TRANSFER_HASHES.get(normalized)
    if not hash_t:
        return JSONResponse({"error": "Hash no disponible"}, status_code=404)
    return {"hash": hash_t}


@app.get("/api/transfer/validate")
async def transfer_validate(request: Request, token: str = "", emisor: str = ""):
    """Valida un token de transferencia y devuelve cuántas butacas puede transferir el emisor.

    - token: hash_transferencia del receptor (quien pide la cuota).
    - emisor: client_id (animal_adjetivo) del emisor (quien tiene la cuota).

    Reglas:
    - El token debe existir en HASH_TO_USER.
    - El emisor debe ser una combinación válida.
    - Emisor != Receptor.
    - max_transferibles = cuota_emisor - total_butacas_reservadas_emisor.
    """
    from identity import normalize_combination

    ip = get_client_ip(request)
    remaining = rate_limiting.get_block_remaining(ip)
    if remaining > 0:
        return JSONResponse(
            {
                "error": f"Esta IP está temporalmente bloqueada por intentos fallidos. Inténtalo de nuevo en {remaining} segundos."
            },
            status_code=429,
        )

    if not token:
        return JSONResponse({"error": "token requerido"}, status_code=400)

    receptor_id = state.HASH_TO_USER.get(token)
    if not receptor_id:
        return JSONResponse(
            {"error": "Enlace de transferencia no válido"}, status_code=404
        )

    normalized_emisor = normalize_combination(emisor)
    if normalized_emisor not in state.VALID_COMBINATIONS:
        rate_limiting.register_failed_attempt(ip)
        return JSONResponse(
            {"error": "Identidad del emisor no válida"}, status_code=404
        )

    rate_limiting.register_successful_attempt(ip)

    if normalized_emisor == receptor_id:
        return JSONResponse(
            {"error": "No puedes transferirte cuota a ti mismo"},
            status_code=422,
        )

    cuota_emisor = state.get_quota(normalized_emisor)

    # Contar butacas ya reservadas por el emisor (en todas las sesiones)
    async with aiosqlite.connect(state.DB_FILE) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM seats WHERE owner_id = ? AND status = 'reserved'",
            (normalized_emisor,),
        ) as cursor:
            row = await cursor.fetchone()
            butacas_reservadas = row[0] if row else 0

    max_transferibles = cuota_emisor - butacas_reservadas
    if max_transferibles <= 0:
        return JSONResponse(
            {
                "error": "No tienes butacas disponibles para transferir",
                "cuota": cuota_emisor,
                "reservadas": butacas_reservadas,
            },
            status_code=422,
        )

    return {
        "ok": True,
        "emisor": normalized_emisor,
        "receptor_confirmado": True,
        "cuota_emisor": cuota_emisor,
        "butacas_reservadas": butacas_reservadas,
        "max_transferibles": max_transferibles,
    }


@app.post("/api/transfer/confirm")
async def transfer_confirm(request: Request):
    """Ejecuta la transferencia de cuota de emisor a receptor.

    Body JSON:
    {
        "token": "<hash_transferencia del receptor>",
        "emisor": "<animal_adjetivo del emisor>",
        "butacas": <int>
    }
    """
    from identity import normalize_combination

    ip = get_client_ip(request)
    remaining = rate_limiting.get_block_remaining(ip)
    if remaining > 0:
        return JSONResponse(
            {
                "error": f"Esta IP está temporalmente bloqueada por intentos fallidos. Inténtalo de nuevo en {remaining} segundos."
            },
            status_code=429,
        )

    body = await request.json()
    token = body.get("token", "")
    emisor_raw = body.get("emisor", "")
    butacas = body.get("butacas", 0)

    if not token:
        return JSONResponse({"error": "token requerido"}, status_code=400)

    receptor_id = state.HASH_TO_USER.get(token)
    if not receptor_id:
        return JSONResponse(
            {"error": "Enlace de transferencia no válido"}, status_code=404
        )

    normalized_emisor = normalize_combination(emisor_raw)
    if normalized_emisor not in state.VALID_COMBINATIONS:
        rate_limiting.register_failed_attempt(ip)
        return JSONResponse(
            {"error": "Identidad del emisor no válida"}, status_code=404
        )

    rate_limiting.register_successful_attempt(ip)

    if normalized_emisor == receptor_id:
        return JSONResponse(
            {"error": "No puedes transferirte cuota a ti mismo"},
            status_code=422,
        )

    if not isinstance(butacas, int) or butacas <= 0:
        return JSONResponse({"error": "Número de butacas inválido"}, status_code=422)

    cuota_emisor = state.get_quota(normalized_emisor)

    # Contar butacas reservadas del emisor para calcular máximo transferible
    async with aiosqlite.connect(state.DB_FILE) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM seats WHERE owner_id = ? AND status = 'reserved'",
            (normalized_emisor,),
        ) as cursor:
            row = await cursor.fetchone()
            butacas_reservadas = row[0] if row else 0

    max_transferibles = cuota_emisor - butacas_reservadas
    if butacas > max_transferibles:
        return JSONResponse(
            {
                "error": f"Solo puedes transferir hasta {max_transferibles} butaca(s)",
                "max_transferibles": max_transferibles,
            },
            status_code=422,
        )

    nueva_cuota_emisor = cuota_emisor - butacas
    cuota_receptor = state.get_quota(receptor_id)
    nueva_cuota_receptor = cuota_receptor + butacas

    # Actualizar BD de forma atómica
    async with aiosqlite.connect(state.DB_FILE) as db:
        await db.execute(
            "UPDATE user_quotas SET quota = ? WHERE user_id = ?",
            (nueva_cuota_emisor, normalized_emisor),
        )
        await db.execute(
            "UPDATE user_quotas SET quota = ? WHERE user_id = ?",
            (nueva_cuota_receptor, receptor_id),
        )
        await db.commit()

    # Actualizar espejo en memoria
    state.USER_QUOTAS[normalized_emisor] = nueva_cuota_emisor
    state.USER_QUOTAS[receptor_id] = nueva_cuota_receptor

    logger.info(
        "Transferencia: %s -> %s, %d butaca(s). Cuotas: %d -> %d / %d -> %d",
        normalized_emisor,
        receptor_id,
        butacas,
        cuota_emisor,
        nueva_cuota_emisor,
        cuota_receptor,
        nueva_cuota_receptor,
    )

    # Notificar al receptor si está conectado por WebSocket
    if receptor_id in state.active_connections:
        try:
            await state.active_connections[receptor_id].send_text(
                json.dumps(
                    {
                        "type": "status",
                        "status": "active"
                        if receptor_id in state.active_users
                        else "queued",
                        "quota": nueva_cuota_receptor,
                        "message": f"¡Has recibido {butacas} butaca(s) adicional(es)!",
                    }
                )
            )
        except Exception:
            pass

    return {
        "ok": True,
        "emisor": normalized_emisor,
        "receptor": receptor_id,
        "butacas_transferidas": butacas,
        "nueva_cuota_emisor": nueva_cuota_emisor,
        "nueva_cuota_receptor": nueva_cuota_receptor,
    }


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
        remaining_login_retries = rate_limiting.get_block_remaining(ip)
        if remaining_login_retries > 0:
            await websocket.accept()
            await websocket.send_text(
                json.dumps(
                    {
                        "type": "error",
                        "message": f"Esta IP está temporalmente bloqueada por intentos fallidos. Inténtalo de nuevo en {remaining_login_retries} segundos.",
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
            json.dumps(
                {
                    "type": "status",
                    "status": "active",
                    "timeout": remaining,
                    "quota": state.get_quota(client_id),
                }
            )
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
                        "quota": state.get_quota(client_id),
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
