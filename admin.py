import csv
import io
import json
import logging
import math

import aiosqlite
from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

import state
from bootstrap_db import TOTAL_CUOTAS, init_db, save_state_to_db
from broadcast import broadcast_admin_stats
from seats import get_all_seats

logger = logging.getLogger(__name__)

admin_router = APIRouter()


@admin_router.get("/admin")
@admin_router.get("/admin/{secret}")
async def get_admin(secret: str | None = None):
    if secret and secret != state.ADMIN_SECRET:
        return {"error": "No autorizado"}
    return FileResponse("admin.html")


@admin_router.websocket("/ws/admin/{secret}")
async def admin_websocket(websocket: WebSocket, secret: str):
    if secret != state.ADMIN_SECRET:
        await websocket.close()
        return

    await websocket.accept()
    state.admin_connections.add(websocket)

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
                    state.virtuales_procesados = 0
                    await broadcast_admin_stats()
                if payload.get("action") == "set_max_users":
                    value = payload.get("value")
                    if isinstance(value, int) and 0 <= value <= 10:
                        state.MAX_ACTIVE_USERS = value
                        logger.info(
                            "MAX_ACTIVE_USERS actualizado a %d por el administrador.",
                            value,
                        )
                        await save_state_to_db("MAX_ACTIVE_USERS", value)
                        await broadcast_admin_stats()
                if payload.get("action") == "reset_db":
                    logger.warning(
                        "El administrador ha solicitado el RESETEO TOTAL de la base de datos."
                    )

                    # 1. Borrar tabla completa y recrearla con el init_db() original
                    async with aiosqlite.connect(state.DB_FILE) as db:
                        await db.execute("DROP TABLE IF EXISTS seats")
                        await db.commit()
                    await init_db()

                    # 2. Expulsar a todos los clientes (Evitar estados corruptos)
                    for cid, ws_conn in list(state.active_connections.items()):
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
                    state.active_connections.clear()
                    state.active_users.clear()
                    async with state.queue_lock:
                        state.waiting_queue.clear()
                        async with aiosqlite.connect(state.DB_FILE) as db:
                            await db.execute("DROP TABLE IF EXISTS queue")
                            await db.commit()
                    state.active_users_names.clear()
                    for task in list(state.active_user_tasks.values()):
                        task.cancel()
                    state.active_user_tasks.clear()
                    state.active_user_expires.clear()

                    # 4. Refrescar los paneles admin conectados
                    new_seats = await get_all_seats()
                    for admin_ws in list(state.admin_connections):
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
        state.admin_connections.remove(websocket)


@admin_router.get("/admin/{secret}/combinations")
async def list_combinations(secret: str):
    if secret != state.ADMIN_SECRET:
        return {"error": "No autorizado"}
    result = [
        {"combo": combo, "quota": state.USER_QUOTAS.get(combo, 0)}
        for combo in sorted(state.VALID_COMBINATIONS)
    ]
    return {"combinations": result}


@admin_router.get("/admin/{secret}/export.csv")
async def export_csv(secret: str):
    if secret != state.ADMIN_SECRET:
        return {"error": "No autorizado"}

    output = io.StringIO()
    writer = csv.writer(output)
    mitad = state.ASIENTOS_POR_FILA // 2
    writer.writerow(["Sesión", "Nombre y Apellidos", "Fila", "Butaca", "ID_Interno_BD"])

    async with aiosqlite.connect(state.DB_FILE) as db:
        async with db.execute("""
        SELECT session_time, owner_name, seat_number 
        FROM seats 
        WHERE status = 'reserved'
        ORDER BY session_time, owner_name
        """) as cursor:
            async for session, owner, seat_id in cursor:
                fila = math.ceil(seat_id / state.ASIENTOS_POR_FILA)
                pos_in_row = (seat_id - 1) % state.ASIENTOS_POR_FILA

                if seat_id <= 220:
                    fila = math.ceil(seat_id / state.ASIENTOS_POR_FILA)
                    pos_in_row = (seat_id - 1) % state.ASIENTOS_POR_FILA
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


@admin_router.get("/admin/{secret}/quotas.csv")
async def download_quotas(secret: str):
    """Descarga las cuotas/derechos de todos los usuarios en formato CSV."""
    if secret != state.ADMIN_SECRET:
        return {"error": "No autorizado"}

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["user_id", "quota"])
    for combo in sorted(state.VALID_COMBINATIONS):
        writer.writerow([combo, state.USER_QUOTAS.get(combo, 0)])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="cuotas.csv"'},
    )


@admin_router.post("/admin/{secret}/quotas.csv")
async def upload_quotas(secret: str, request: Request):
    """Carga cuotas desde un CSV. Valida que la suma total == TOTAL_CUOTAS (729).
    Si es válido, reemplaza la tabla user_quotas y actualiza el espejo en memoria."""
    if secret != state.ADMIN_SECRET:
        return JSONResponse({"error": "No autorizado"}, status_code=401)

    body = await request.body()
    try:
        text = body.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        new_quotas: dict[str, int] = {}
        for row in reader:
            user_id = row.get("user_id", "").strip()
            try:
                quota = int(row.get("quota", "0").strip())
            except ValueError:
                return JSONResponse(
                    {
                        "error": f"Cuota inválida para usuario '{user_id}'. Debe ser un número entero."
                    },
                    status_code=422,
                )
            if quota < 0 or quota > 6:
                return JSONResponse(
                    {
                        "error": f"Cuota fuera de rango para '{user_id}': {quota}. Debe estar entre 0 y 6."
                    },
                    status_code=422,
                )
            if user_id not in state.VALID_COMBINATIONS:
                return JSONResponse(
                    {"error": f"Usuario desconocido en CSV: '{user_id}'."},
                    status_code=422,
                )
            new_quotas[user_id] = quota

        # Rellenar con 0 los usuarios válidos no presentes en el CSV
        for combo in state.VALID_COMBINATIONS:
            if combo not in new_quotas:
                new_quotas[combo] = 0

        total = sum(new_quotas.values())
        if total != TOTAL_CUOTAS:
            return JSONResponse(
                {
                    "error": (
                        f"La suma de cuotas es {total}, pero debe ser exactamente {TOTAL_CUOTAS}. "
                        f"Diferencia: {total - TOTAL_CUOTAS:+d}."
                    )
                },
                status_code=422,
            )

        # Actualizar base de datos
        async with aiosqlite.connect(state.DB_FILE) as db:
            await db.execute("DELETE FROM user_quotas")
            await db.executemany(
                "INSERT INTO user_quotas (user_id, quota) VALUES (?, ?)",
                list(new_quotas.items()),
            )
            await db.commit()

        # Actualizar espejo en memoria
        state.USER_QUOTAS = new_quotas
        logger.info("Cuotas actualizadas por admin. Suma total: %d", total)

        return JSONResponse({"ok": True, "total": total, "usuarios": len(new_quotas)})

    except Exception as e:
        logger.error("Error procesando CSV de cuotas: %s", e)
        return JSONResponse({"error": f"Error procesando CSV: {e}"}, status_code=500)
