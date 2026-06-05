import aiosqlite

import state


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


async def get_all_seats():
    async with aiosqlite.connect(state.DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT seat_number, session_time, status, owner_id, owner_name FROM seats"
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]  # Convertimos la fila a diccionario completo


async def release_reserving_seats(client_id: str):
    async with aiosqlite.connect(state.DB_FILE) as db:
        await db.execute(
            """
            UPDATE seats 
            SET status = "free", owner_id = NULL, owner_name = NULL 
            WHERE owner_id = ? AND status = "reserving"
        """,
            (client_id,),
        )
        await db.commit()


async def reserve_seats(client_id: str):
    async with aiosqlite.connect(state.DB_FILE) as db:
        await db.execute(
            """
            UPDATE seats 
            SET status = "reserved" 
            WHERE owner_id = ? AND status = "reserving"
        """,
            (client_id,),
        )
        await db.commit()


async def toggle_seat(
    client_id: str, seat_num: int, sess_time: str, user_full_name: str
) -> str | None:
    # Retorna mensaje de error si se alcanza el límite de 6 y no se puede reservar más, de lo contrario None.
    async with aiosqlite.connect(state.DB_FILE) as db:
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
                return "Has alcanzado el límite de 6, tendrás que deseleccionar algún asiento..."
        await db.commit()
    return None
