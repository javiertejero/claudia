import json
import logging
from fastapi import WebSocketDisconnect
from uvicorn.protocols.utils import ClientDisconnected
import state
from seats import get_all_seats, sanitize_seats

logger = logging.getLogger(__name__)


async def broadcast_seats():
    seats = await get_all_seats()

    # 1. Enviar a usuarios normales (sanitizado)
    for client_id in list(state.active_users):
        if client_id in state.active_connections:
            sanitized_seats = sanitize_seats(seats, client_id)
            message = json.dumps({"type": "seats_update", "seats": sanitized_seats})
            try:
                await state.active_connections[client_id].send_text(message)
            except (WebSocketDisconnect, RuntimeError, ClientDisconnected) as exc:
                logger.error(
                    "Error while broadcast_seats: %s, removing client_id %s",
                    exc,
                    client_id,
                )
                state.active_connections.pop(client_id, None)
                state.active_users.discard(client_id)

    # 2. Enviar a administradores (datos completos en tiempo real)
    if state.admin_connections:
        admin_message = json.dumps({"type": "admin_update", "seats": seats})
        for admin_ws in list(state.admin_connections):
            try:
                await admin_ws.send_text(admin_message)
            except Exception as e:
                logger.error("Error sending seats update to admin: %s", e)


async def broadcast_queue_positions():
    """Envía la posición en la cola a todos los clientes encolados."""
    async with state.queue_lock:
        for i, client_id in enumerate(state.waiting_queue):
            if client_id in state.active_connections:
                try:
                    await state.active_connections[client_id].send_text(
                        json.dumps(
                            {"type": "status", "status": "queued", "position": i + 1}
                        )
                    )
                except Exception as e:
                    logger.error(
                        "Error enviando posición de cola a %s: %s", client_id, e
                    )


async def broadcast_admin_stats():
    """Envía el conteo de usuarios activos y en cola a los administradores."""
    if state.admin_connections:
        message = json.dumps(
            {
                "type": "admin_stats",
                "active_users": len(state.active_users),
                "queued_users": len(state.waiting_queue),
                "virtuales_procesados": state.virtuales_procesados,
                "max_active_users": state.MAX_ACTIVE_USERS,
            }
        )
        for admin_ws in list(state.admin_connections):
            try:
                await admin_ws.send_text(message)
            except Exception:
                pass
