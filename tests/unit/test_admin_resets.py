import json
import os

import aiosqlite
import anyio
import pytest
from fastapi.testclient import TestClient

import seats
import state
from bootstrap_db import init_db
from main import app


@pytest.fixture(autouse=True)
def reset_state():
    original_db = state.DB_FILE
    state.DB_FILE = "tests/unit/test_resets_temp.db"

    state.USER_QUOTAS.clear()
    state.VALID_COMBINATIONS.clear()
    state.active_connections.clear()
    state.active_users.clear()
    state.active_users_names.clear()
    state.waiting_queue = []
    state.admin_connections.clear()
    state.SYSTEM_SEED = None

    # Ensure clean DB initially
    if os.path.exists("tests/unit/test_resets_temp.db"):
        try:
            os.remove("tests/unit/test_resets_temp.db")
        except Exception:
            pass

    # Run init_db synchronously using anyio
    anyio.run(init_db)

    yield

    state.DB_FILE = original_db
    state.USER_QUOTAS.clear()
    state.VALID_COMBINATIONS.clear()
    state.active_connections.clear()
    state.active_users.clear()
    state.active_users_names.clear()
    state.waiting_queue = []
    state.admin_connections.clear()
    state.SYSTEM_SEED = None

    if os.path.exists("tests/unit/test_resets_temp.db"):
        try:
            os.remove("tests/unit/test_resets_temp.db")
        except Exception:
            pass


def test_admin_websocket_reset_reservations():
    # 1. Arrange: Add user and reserve some seats
    client_id = "loba_astuta"
    state.VALID_COMBINATIONS.add(client_id)
    state.USER_QUOTAS[client_id] = 6

    client = TestClient(app)

    async def create_reservation():
        await seats.toggle_seat(client_id, 1, "11h", "Loba Astuta")
        await seats.toggle_seat(client_id, 2, "11h", "Loba Astuta")

    anyio.run(create_reservation)

    # Confirm they are reserved
    async def get_reserved_seats():
        async with aiosqlite.connect(state.DB_FILE) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM seats WHERE status = 'reserved'"
            ) as cursor:
                return (await cursor.fetchone())[0]

    assert anyio.run(get_reserved_seats) == 2

    # 2. Act: Send reset_reservations via websocket
    original_secret = state.ADMIN_SECRET
    state.ADMIN_SECRET = "supersecret"
    try:
        with client.websocket_connect("/ws/admin/supersecret") as websocket:
            # Read first 2 messages on connection (admin_update, admin_stats)
            initial_types = []
            for _ in range(2):
                msg = json.loads(websocket.receive_text())
                initial_types.append(msg["type"])
            assert "admin_update" in initial_types
            assert "admin_stats" in initial_types

            # Send reset_reservations action
            websocket.send_text(json.dumps({"action": "reset_reservations"}))

            # Receive the update from reset_reservations
            msg = json.loads(websocket.receive_text())
            assert msg["type"] == "admin_update"

            # Assert all seats are free now in DB
            assert anyio.run(get_reserved_seats) == 0

            # Assert user quota and validations are preserved
            assert state.get_quota(client_id) == 6
            assert client_id in state.VALID_COMBINATIONS
    finally:
        state.ADMIN_SECRET = original_secret


def test_admin_websocket_reset_db():
    # 1. Arrange: Add user, queue position, active connections, and reserve seats
    client_id = "loba_astuta"
    state.VALID_COMBINATIONS.add(client_id)
    state.USER_QUOTAS[client_id] = 6
    state.active_users.add(client_id)
    state.waiting_queue.append("other_user")

    client = TestClient(app)

    # Make reservation
    async def create_reservation():
        await seats.toggle_seat(client_id, 1, "11h", "Loba Astuta")

    anyio.run(create_reservation)

    async def get_reserved_seats():
        async with aiosqlite.connect(state.DB_FILE) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM seats WHERE status = 'reserved'"
            ) as cursor:
                return (await cursor.fetchone())[0]

    assert anyio.run(get_reserved_seats) == 1

    # Mock an active user WS connection so reset_db can eject it
    from unittest.mock import AsyncMock

    mock_ws = AsyncMock()
    state.active_connections[client_id] = mock_ws

    # 2. Act: Send reset_db via admin websocket
    original_secret = state.ADMIN_SECRET
    state.ADMIN_SECRET = "supersecret"
    try:
        with client.websocket_connect("/ws/admin/supersecret") as websocket:
            # Read first 2 messages on connection (admin_update, admin_stats)
            initial_types = []
            for _ in range(2):
                msg = json.loads(websocket.receive_text())
                initial_types.append(msg["type"])
            assert "admin_update" in initial_types
            assert "admin_stats" in initial_types

            websocket.send_text(json.dumps({"action": "reset_db"}))

            # Let's wait a bit for async tasks to process DB initialization and connections closure
            import time

            time.sleep(0.5)

            # Assert queue, user state, and reservations are cleared
            assert len(state.active_users) == 0
            assert len(state.waiting_queue) == 0
            assert anyio.run(get_reserved_seats) == 0

            # Verify that client was notified of timeout/reset
            assert mock_ws.send_text.called
            sent_message = mock_ws.send_text.call_args[0][0]
            assert "El administrador ha reiniciado el sistema" in sent_message
            assert mock_ws.close.called
    finally:
        state.ADMIN_SECRET = original_secret
