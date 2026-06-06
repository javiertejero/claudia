import os

import pytest

import seats
import state
from bootstrap_db import init_db


@pytest.fixture(autouse=True)
def reset_state():
    original_disable = state.DISABLE_IDENTITY_CHECKS
    original_quotas = state.USER_QUOTAS.copy()
    original_db = state.DB_FILE
    state.USER_QUOTAS.clear()
    state.DB_FILE = "tests/test_temp.db"
    yield
    state.DISABLE_IDENTITY_CHECKS = original_disable
    state.USER_QUOTAS = original_quotas
    state.DB_FILE = original_db
    if os.path.exists("tests/test_temp.db"):
        try:
            os.remove("tests/test_temp.db")
        except Exception:
            pass


def test_get_quota_disable_identity_checks_true():
    state.DISABLE_IDENTITY_CHECKS = True
    state.USER_QUOTAS["valid_combo"] = 3

    assert state.get_quota("valid_combo") == 6
    assert state.get_quota("bot_1_0") == 6
    assert state.get_quota("random_user") == 6


def test_get_quota_disable_identity_checks_false():
    state.DISABLE_IDENTITY_CHECKS = False
    state.USER_QUOTAS["valid_combo"] = 3

    assert state.get_quota("valid_combo") == 3
    assert state.get_quota("bot_1_0") == 0
    assert state.get_quota("random_user") == 0


@pytest.mark.anyio
async def test_toggle_seat_respects_disable_identity_checks_quota():
    state.DISABLE_IDENTITY_CHECKS = True
    await init_db()

    # Check that a bot can toggle seats up to 6 times
    client_id = "bot_1_0"
    user_name = "Usuario1"

    # Toggle 6 different seats successfully
    for seat_num in range(1, 7):
        err = await seats.toggle_seat(client_id, seat_num, "11h", user_name)
        assert err is None

    # Toggling the 7th seat should fail with quota error
    err = await seats.toggle_seat(client_id, 7, "11h", user_name)
    assert err is not None
    assert "Has alcanzado tu límite de 6 asiento" in err
