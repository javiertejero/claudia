import asyncio

import aiosqlite
import pytest
from fastapi.testclient import TestClient

import bootstrap_db
import state
from main import app


@pytest.fixture(autouse=True)
def setup_db():
    # Ejecutar init_db de forma síncrona para evitar advertencias de pytest
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(bootstrap_db.init_db())

    # Guardar secretos originales
    orig_secret = state.ADMIN_SECRET
    state.ADMIN_SECRET = "secretotestado"
    state.VALID_COMBINATIONS.add("loba_astuta")

    yield

    state.ADMIN_SECRET = orig_secret
    state.VALID_COMBINATIONS.discard("loba_astuta")
    loop.close()


def test_get_check_page():
    client = TestClient(app)
    response = client.get("/check")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]


def test_api_check_info_unauthorized():
    client = TestClient(app)
    response = client.get("/api/check/info?animal=loba&adjetivo=astuta&token=mal_token")
    assert response.status_code == 401
    assert "error" in response.json()


def test_api_check_info_invalid_combination():
    client = TestClient(app)
    response = client.get(
        "/api/check/info?animal=dragon&adjetivo=volador&token=secretotestado"
    )
    assert response.status_code == 200
    data = response.json()
    assert data["valid"] is False
    assert "no existe o no es válida" in data["reason"]


def test_api_check_info_valid_combination_no_seats():
    client = TestClient(app)

    # Limpiar asientos
    async def clear_seats():
        async with aiosqlite.connect(state.DB_FILE) as db:
            await db.execute(
                "UPDATE seats SET status = 'free', owner_id = NULL, owner_name = NULL WHERE owner_id = ?",
                ("loba_astuta",),
            )
            await db.commit()

    asyncio.run(clear_seats())

    response = client.get(
        "/api/check/info?animal=loba&adjetivo=astuta&token=secretotestado"
    )
    assert response.status_code == 200
    data = response.json()
    assert data["valid"] is False
    assert "no tiene ninguna butaca reservada" in data["reason"]


def test_api_check_info_valid_combination_with_seats_and_marking():
    client = TestClient(app)

    # Reservar una butaca (ej: asiento 5 en sesión 11h)
    async def reserve_seat():
        async with aiosqlite.connect(state.DB_FILE) as db:
            await db.execute(
                "UPDATE seats SET status = 'reserved', owner_id = 'loba_astuta', owner_name = 'Loba Astuta', used_at = NULL "
                "WHERE seat_number = 5 AND session_time = '11h'"
            )
            await db.commit()

    asyncio.run(reserve_seat())

    # 1. Consultar info
    response = client.get(
        "/api/check/info?animal=loba&adjetivo=astuta&token=secretotestado"
    )
    assert response.status_code == 200
    data = response.json()
    assert data["valid"] is True
    assert data["user_id"] == "loba_astuta"
    assert len(data["seats"]) == 1
    assert data["seats"][0]["seat_number"] == 5
    assert data["seats"][0]["fila"] == 1
    assert data["seats"][0]["butaca"] == 12  # (10 - 4) * 2 = 12
    assert data["seats"][0]["used_at"] is None
    assert data["already_used_recently"] is False

    # 2. Marcar como utilizada con token inválido
    response_mark_bad = client.post(
        "/api/check/use",
        json={"animal": "loba", "adjetivo": "astuta", "token": "mal_token"},
    )
    assert response_mark_bad.status_code == 401

    # 3. Marcar como utilizada con token válido
    response_mark = client.post(
        "/api/check/use",
        json={"animal": "loba", "adjetivo": "astuta", "token": "secretotestado"},
    )
    assert response_mark.status_code == 200
    assert response_mark.json()["ok"] is True
    assert response_mark.json()["marked_count"] == 1

    # 4. Volver a consultar info para verificar que está utilizada
    response_after = client.get(
        "/api/check/info?animal=loba&adjetivo=astuta&token=secretotestado"
    )
    assert response_after.status_code == 200
    data_after = response_after.json()
    assert data_after["valid"] is True
    assert data_after["seats"][0]["used_at"] is not None
    assert data_after["already_used_recently"] is True
    assert data_after["recently_used_time"] > 0


def test_get_my_reservations_invalid_combination():
    client = TestClient(app)
    response = client.get("/api/my-reservations?animal=dragon&adjetivo=volador")
    assert response.status_code == 404
    assert "error" in response.json()


def test_get_my_reservations_valid_combination():
    client = TestClient(app)

    # Reservar una butaca (ej: asiento 10 en sesión 18h)
    async def reserve_seat():
        async with aiosqlite.connect(state.DB_FILE) as db:
            await db.execute(
                "UPDATE seats SET status = 'free', owner_id = NULL, owner_name = NULL WHERE owner_id = ?",
                ("loba_astuta",),
            )
            await db.execute(
                "UPDATE seats SET status = 'reserved', owner_id = 'loba_astuta', owner_name = 'Loba Astuta', used_at = NULL "
                "WHERE seat_number = 10 AND session_time = '18h'"
            )
            await db.commit()

    asyncio.run(reserve_seat())

    response = client.get("/api/my-reservations?animal=loba&adjetivo=astuta")
    assert response.status_code == 200
    data = response.json()
    assert "seats" in data
    assert data["user_id"] == "loba_astuta"
    assert len(data["seats"]) == 1
    assert data["seats"][0]["session_time"] == "18h"
    assert data["seats"][0]["fila"] == 1
    assert data["seats"][0]["butaca"] == 2  # (10 - 9) * 2 = 2
