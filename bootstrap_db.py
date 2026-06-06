import logging
import os
import secrets

import aiosqlite

import state
from identity import generate_valid_combinations

logger = logging.getLogger(__name__)


async def init_db():
    os.makedirs(os.path.dirname(state.DB_FILE), exist_ok=True)
    async with aiosqlite.connect(state.DB_FILE) as db:
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
                position INTEGER,
                last_seen REAL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        # Cargar o generar la semilla persistida únicamente en la base de datos
        async with db.execute(
            "SELECT value FROM settings WHERE key = 'seed'"
        ) as cursor:
            row = await cursor.fetchone()
            if row is None:
                state.SYSTEM_SEED = secrets.token_hex(16)
                await db.execute(
                    "INSERT INTO settings (key, value) VALUES ('seed', ?)",
                    (state.SYSTEM_SEED,),
                )
                await db.commit()
                logger.info(
                    f"Semilla generada y persistida en base de datos: {state.SYSTEM_SEED}"
                )
            else:
                state.SYSTEM_SEED = row[0]
                logger.info("Semilla cargada con éxito desde base de datos.")

        # Cargar o generar la semilla persistida únicamente en la base de datos
        async with db.execute(
            "SELECT value FROM settings WHERE key = 'MAX_ACTIVE_USERS'"
        ) as cursor:
            row = await cursor.fetchone()
            if row is None:
                state.MAX_ACTIVE_USERS = 2  # by default we start with 2
                await db.execute(
                    "INSERT INTO settings (key, value) VALUES ('MAX_ACTIVE_USERS', ?)",
                    (state.MAX_ACTIVE_USERS,),
                )
                await db.commit()
                logger.info(
                    f"MAX_ACTIVE_USERS generado y persistido en base de datos: {state.MAX_ACTIVE_USERS}"
                )
            else:
                state.MAX_ACTIVE_USERS = int(row[0])
                logger.info("MAX_ACTIVE_USERS cargado con éxito desde base de datos.")

        # Generar las combinaciones de usuarios válidas deterministamente usando la semilla
        state.VALID_COMBINATIONS = generate_valid_combinations(
            state.SYSTEM_SEED, state.NUM_VALID_COMBINATIONS
        )
        logger.info("Length of Valid Combinations: %s", len(state.VALID_COMBINATIONS))
        logger.info(
            "First 5 valid combinations: %s", sorted(list(state.VALID_COMBINATIONS))[:5]
        )

        # Cargar cola persistida
        async with db.execute(
            "SELECT client_id FROM queue ORDER BY position"
        ) as cursor:
            rows = await cursor.fetchall()
            state.waiting_queue = [r[0] for r in rows]

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
                            for i in range(
                                1, state.ASIENTOS_POR_FILA * state.FILAS + 1 + 3
                            )
                        ],
                    )
                await db.commit()
        await db.commit()


async def save_state_to_db(key: str, value: int):
    async with aiosqlite.connect(state.DB_FILE) as db:
        await db.execute(
            f"UPDATE settings SET value = ? WHERE key = '{key}'",
            (value,),
        )
        await db.commit()
        logger.info(f"State {key} saved to database: {value}")
