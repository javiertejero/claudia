import logging
import os
import secrets

import aiosqlite

import state
from identity import generate_valid_combinations

logger = logging.getLogger(__name__)

# Total de cuotas que deben sumar todos los usuarios (243 asientos × 3 sesiones)
TOTAL_CUOTAS = (
    state.ASIENTOS_POR_FILA * state.FILAS + 3
) * 3  # (243 asientos/sesión) × 3 sesiones = 729


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
                owner_name TEXT,
                used_at REAL
            )
        """)
        # Migración de la tabla seats para incluir used_at
        async with db.execute("PRAGMA table_info(seats)") as cursor:
            columns = [r[1] for r in await cursor.fetchall()]
        if "used_at" not in columns:
            await db.execute("ALTER TABLE seats ADD COLUMN used_at REAL")
            await db.commit()
            logger.info("Columna used_at añadida a la tabla seats.")
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
        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_quotas (
                user_id TEXT PRIMARY KEY,
                quota INTEGER DEFAULT 0,
                hash_transferencia TEXT UNIQUE
            )
        """)

        # Migración lazy: si falta la columna hash_transferencia, borrar y recrear la tabla.
        # (El sistema no está en producción, no es necesario preservar datos.)
        async with db.execute("PRAGMA table_info(user_quotas)") as cursor:
            columns = [row[1] for row in await cursor.fetchall()]
        if "hash_transferencia" not in columns:
            await db.execute("DROP TABLE IF EXISTS user_quotas")
            await db.execute("""
                CREATE TABLE user_quotas (
                    user_id TEXT PRIMARY KEY,
                    quota INTEGER DEFAULT 0,
                    hash_transferencia TEXT UNIQUE
                )
            """)
            await db.commit()
            logger.info("user_quotas recreada con columna hash_transferencia.")

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

        # Cargar o generar MAX_ACTIVE_USERS persistido en la base de datos
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

        # Cargar o generar taquilla_mode persistido en la base de datos
        async with db.execute(
            "SELECT value FROM settings WHERE key = 'taquilla_mode'"
        ) as cursor:
            row = await cursor.fetchone()
            if row is None:
                state.TAQUILLA_MODE = False
                await db.execute(
                    "INSERT INTO settings (key, value) VALUES ('taquilla_mode', '0')"
                )
                await db.commit()
                logger.info("taquilla_mode inicializado en 0 (Desactivado).")
            else:
                state.TAQUILLA_MODE = row[0] == "1"
                logger.info(f"taquilla_mode cargado con éxito: {state.TAQUILLA_MODE}")

        # Generar las combinaciones de usuarios válidas deterministamente usando la semilla
        state.VALID_COMBINATIONS = generate_valid_combinations(
            state.SYSTEM_SEED, state.NUM_VALID_COMBINATIONS
        )
        logger.info("Length of Valid Combinations: %s", len(state.VALID_COMBINATIONS))
        logger.info(
            "First 5 valid combinations: %s", sorted(list(state.VALID_COMBINATIONS))[:5]
        )

        # Poblar user_quotas si está vacía
        async with db.execute("SELECT COUNT(*) FROM user_quotas") as cursor:
            quota_count = (await cursor.fetchone())[0]

        if quota_count == 0:
            # Distribución: primeros usuarios reciben 6 cuotas hasta llegar a 729.
            # 121 usuarios × 6 = 726, luego 1 usuario × 3 = 729, el resto = 0.
            sorted_combos = sorted(state.VALID_COMBINATIONS)
            rows_to_insert = []
            remaining = TOTAL_CUOTAS  # 729
            for combo in sorted_combos:
                hash_t = secrets.token_urlsafe(16)
                if remaining >= 6:
                    rows_to_insert.append((combo, 6, hash_t))
                    remaining -= 6
                elif remaining > 0:
                    rows_to_insert.append((combo, remaining, hash_t))
                    remaining = 0
                else:
                    rows_to_insert.append((combo, 0, hash_t))

            await db.executemany(
                "INSERT INTO user_quotas (user_id, quota, hash_transferencia) VALUES (?, ?, ?)",
                rows_to_insert,
            )
            await db.commit()
            logger.info(
                "user_quotas inicializada: %d filas, suma total = %d",
                len(rows_to_insert),
                sum(r[1] for r in rows_to_insert),
            )

        # Generar hashes para filas que tengan NULL (usuarios pre-existentes sin hash)
        async with db.execute(
            "SELECT user_id FROM user_quotas WHERE hash_transferencia IS NULL"
        ) as cursor:
            sin_hash = [row[0] for row in await cursor.fetchall()]
        for uid in sin_hash:
            new_hash = secrets.token_urlsafe(16)
            await db.execute(
                "UPDATE user_quotas SET hash_transferencia = ? WHERE user_id = ?",
                (new_hash, uid),
            )
        if sin_hash:
            await db.commit()
            logger.info(
                "Hashes de transferencia generados para %d usuarios.", len(sin_hash)
            )

        # Cargar cuotas y hashes en memoria
        async with db.execute(
            "SELECT user_id, quota, hash_transferencia FROM user_quotas"
        ) as cursor:
            rows = await cursor.fetchall()
            state.USER_QUOTAS = {r[0]: r[1] for r in rows}
            state.USER_TRANSFER_HASHES = {r[0]: r[2] for r in rows if r[2] is not None}
            state.HASH_TO_USER = {r[2]: r[0] for r in rows if r[2] is not None}
        logger.info(
            "USER_QUOTAS cargado en memoria: %d entradas", len(state.USER_QUOTAS)
        )
        logger.info(
            "USER_TRANSFER_HASHES cargado en memoria: %d entradas",
            len(state.USER_TRANSFER_HASHES),
        )

        # Cargar cola persistida
        async with db.execute(
            "SELECT client_id FROM queue ORDER BY position"
        ) as cursor:
            rows = await cursor.fetchall()
            state.waiting_queue = [r[0] for r in rows]

        # Inicializar butacas por cada una de las 3 sesiones
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
