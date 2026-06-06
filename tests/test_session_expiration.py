import os
import time
from unittest.mock import AsyncMock, patch

import aiosqlite
import pytest

import seats
import state
from bootstrap_db import init_db
from main import expire_user_session


@pytest.fixture(autouse=True)
def reset_state():
    original_db = state.DB_FILE
    state.DB_FILE = "tests/test_session_temp.db"

    state.USER_QUOTAS.clear()
    state.VALID_COMBINATIONS.clear()
    state.active_connections.clear()
    state.active_users.clear()
    state.active_users_names.clear()
    state.waiting_queue = []

    yield

    state.DB_FILE = original_db
    state.USER_QUOTAS.clear()
    state.VALID_COMBINATIONS.clear()
    state.active_connections.clear()
    state.active_users.clear()
    state.active_users_names.clear()
    state.waiting_queue = []

    if os.path.exists("tests/test_session_temp.db"):
        try:
            os.remove("tests/test_session_temp.db")
        except Exception:
            pass


@pytest.mark.anyio
async def test_expire_user_session_releases_reserving_seats():
    # 1. Initialize database and add a user with some reserving seats
    await init_db()

    client_id = "delfin_valiente"
    state.VALID_COMBINATIONS.add(client_id)
    state.USER_QUOTAS[client_id] = 6
    state.active_users.add(client_id)
    state.active_users_names[client_id] = "Delfin Valiente"

    # Toggle a seat to put it in 'reserving' status
    err = await seats.toggle_seat(client_id, 1, "11h", "Delfin Valiente")
    assert err is None

    # Verify the seat is reserving in the DB
    async with aiosqlite.connect(state.DB_FILE) as db:
        async with db.execute(
            "SELECT status, owner_id FROM seats WHERE seat_number = 1 AND session_time = '11h'"
        ) as cursor:
            row = await cursor.fetchone()
            assert row is not None
            assert row[0] == "reserving"
            assert row[1] == client_id

    # 2. Mock WebSocket connection
    mock_ws = AsyncMock()
    state.active_connections[client_id] = mock_ws

    # Set the expiration time in the past so expire_user_session doesn't sleep
    state.active_user_expires[client_id] = time.time() - 10

    # Mock broadcast_seats so we can assert it was called
    with patch(
        "broadcast.broadcast_seats", new_callable=AsyncMock
    ) as mock_broadcast_seats:
        # 3. Call the expire_user_session function
        await expire_user_session(client_id)

        # Verify broadcast was called
        mock_broadcast_seats.assert_called_once()

    # 4. Verify user was removed from all states
    assert client_id not in state.active_users
    assert client_id not in state.active_connections
    assert client_id not in state.active_users_names

    # 5. Verify the seat was released (changed back to free) in the DB
    async with aiosqlite.connect(state.DB_FILE) as db:
        async with db.execute(
            "SELECT status, owner_id, owner_name FROM seats WHERE seat_number = 1 AND session_time = '11h'"
        ) as cursor:
            row = await cursor.fetchone()
            assert row is not None
            assert row[0] == "free"
            assert row[1] is None
            assert row[2] is None
