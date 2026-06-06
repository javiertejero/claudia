import json
from unittest.mock import AsyncMock

import pytest

import state
from broadcast import broadcast_admin_stats


@pytest.fixture(autouse=True)
def reset_state():
    """Resetea el estado global antes de cada test."""
    state.USER_QUOTAS.clear()
    state.VALID_COMBINATIONS.clear()
    state.active_connections.clear()
    state.active_users.clear()
    state.active_users_names.clear()
    state.waiting_queue = []
    state.admin_connections.clear()
    state.virtuales_procesados = 0
    yield
    state.USER_QUOTAS.clear()
    state.VALID_COMBINATIONS.clear()
    state.active_connections.clear()
    state.active_users.clear()
    state.active_users_names.clear()
    state.waiting_queue = []
    state.admin_connections.clear()
    state.virtuales_procesados = 0


@pytest.mark.anyio
async def test_broadcast_admin_stats_sends_user_lists():
    # 1. Preparar datos de usuarios activos y en cola
    state.active_users.add("tigre_artista")
    state.active_users_names["tigre_artista"] = "Tigre Artista"

    state.waiting_queue.append("gato_valiente")
    state.active_users_names["gato_valiente"] = "[TAQ] Gato Valiente"

    # 2. Configurar una conexión de administrador falsa (Mock WS)
    mock_admin_ws = AsyncMock()
    state.admin_connections.add(mock_admin_ws)

    # 3. Invocar la función bajo prueba
    await broadcast_admin_stats()

    # 4. Verificar que el administrador recibió el mensaje con las identidades
    assert mock_admin_ws.send_text.called
    sent_msg = mock_admin_ws.send_text.call_args[0][0]
    payload = json.loads(sent_msg)

    assert payload["type"] == "admin_stats"
    assert payload["active_users"] == 1
    assert payload["queued_users"] == 1

    # Verificar lista de activos
    active_list = payload["active_users_list"]
    assert len(active_list) == 1
    assert active_list[0]["id"] == "tigre_artista"
    assert active_list[0]["name"] == "Tigre Artista"

    # Verificar lista de la cola
    queued_list = payload["queued_users_list"]
    assert len(queued_list) == 1
    assert queued_list[0]["id"] == "gato_valiente"
    assert queued_list[0]["name"] == "[TAQ] Gato Valiente"
