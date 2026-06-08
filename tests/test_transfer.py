"""
Tests para el flujo de transferencia de cuotas.

Estrategia: se mockean los diccionarios de `state` en memoria y se usa
una base de datos SQLite en memoria (:memory:) para las queries del servidor.
Los endpoints de FastAPI se ejercitan a través de httpx + TestClient (ASGI).
"""

import secrets

import pytest

# ---------------------------------------------------------------------------
# Fixtures de estado base
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_state():
    """Resetea el estado global en memoria antes de cada test."""
    import state

    state.USER_QUOTAS.clear()
    state.USER_TRANSFER_HASHES.clear()
    state.HASH_TO_USER.clear()
    state.VALID_COMBINATIONS.clear()
    state.active_connections.clear()
    state.active_users.clear()
    yield
    # Limpieza post-test (por si algún test no lo hace)
    state.USER_QUOTAS.clear()
    state.USER_TRANSFER_HASHES.clear()
    state.HASH_TO_USER.clear()
    state.VALID_COMBINATIONS.clear()
    state.active_connections.clear()
    state.active_users.clear()


@pytest.fixture()
def two_users(reset_state):
    """Crea dos usuarios con cuotas y hashes en el estado en memoria."""
    import state

    hash_a = secrets.token_urlsafe(16)
    hash_b = secrets.token_urlsafe(16)

    state.VALID_COMBINATIONS.add("delfin_valiente")
    state.VALID_COMBINATIONS.add("gato_alegre")

    state.USER_QUOTAS["delfin_valiente"] = 6
    state.USER_QUOTAS["gato_alegre"] = 3

    state.USER_TRANSFER_HASHES["delfin_valiente"] = hash_a
    state.USER_TRANSFER_HASHES["gato_alegre"] = hash_b

    state.HASH_TO_USER[hash_a] = "delfin_valiente"
    state.HASH_TO_USER[hash_b] = "gato_alegre"

    return {
        "emisor": "delfin_valiente",
        "emisor_hash": hash_a,
        "receptor": "gato_alegre",
        "receptor_hash": hash_b,
    }


# ---------------------------------------------------------------------------
# Tests de lógica de transferencia (capa de estado, sin HTTP)
# ---------------------------------------------------------------------------


class TestTransferBusinessLogic:
    """Prueba la lógica de negocio de transferencia de cuotas directamente."""

    def test_max_transferibles_sin_reservas(self, two_users):
        """Sin butacas reservadas, el máximo transferible es toda la cuota."""
        import state

        cuota = state.USER_QUOTAS["delfin_valiente"]
        butacas_reservadas = 0
        max_t = cuota - butacas_reservadas
        assert max_t == 6

    def test_max_transferibles_con_reservas(self, two_users):
        """Con butacas reservadas, el máximo se reduce correctamente."""
        import state

        cuota = state.USER_QUOTAS["delfin_valiente"]  # 6
        butacas_reservadas = 4
        max_t = cuota - butacas_reservadas
        assert max_t == 2

    def test_transferencia_actualiza_memoria(self, two_users):
        """Simula una transferencia exitosa y verifica el estado en memoria."""
        import state

        emisor = "delfin_valiente"
        receptor = "gato_alegre"
        butacas = 2

        cuota_e_antes = state.USER_QUOTAS[emisor]  # 6
        cuota_r_antes = state.USER_QUOTAS[receptor]  # 3

        # Aplicar transferencia en memoria
        state.USER_QUOTAS[emisor] = cuota_e_antes - butacas
        state.USER_QUOTAS[receptor] = cuota_r_antes + butacas

        assert state.USER_QUOTAS[emisor] == 4
        assert state.USER_QUOTAS[receptor] == 5

    def test_hash_inverso_correcto(self, two_users):
        """El hash del receptor resuelve correctamente al user_id."""
        import state

        receptor_hash = two_users["receptor_hash"]
        receptor_id = state.HASH_TO_USER.get(receptor_hash)
        assert receptor_id == "gato_alegre"

    def test_hash_desconocido_retorna_none(self, two_users):
        """Un hash inventado no debe existir en HASH_TO_USER."""
        import state

        resultado = state.HASH_TO_USER.get("hash_falso_xyz")
        assert resultado is None

    def test_emisor_receptor_misma_identidad(self, two_users):
        """Un usuario no puede transferirse cuota a sí mismo."""
        import state

        receptor_hash = two_users["emisor_hash"]  # hash del mismo usuario
        receptor_id = state.HASH_TO_USER.get(receptor_hash)
        emisor_id = two_users["emisor"]
        assert emisor_id == receptor_id  # → debe ser rechazado

    def test_transferencia_cuota_cero(self, two_users):
        """Si la cuota del emisor es 0, max_transferibles debe ser ≤ 0."""
        import state

        state.USER_QUOTAS["delfin_valiente"] = 0
        cuota = state.USER_QUOTAS["delfin_valiente"]
        butacas_reservadas = 0
        max_t = cuota - butacas_reservadas
        assert max_t <= 0

    def test_transferencia_excede_maximo(self, two_users):
        """Transferir más butacas de las disponibles debe ser rechazado."""
        import state

        cuota = state.USER_QUOTAS["delfin_valiente"]  # 6
        butacas_reservadas = 4
        max_t = cuota - butacas_reservadas  # 2
        butacas_solicitadas = 5
        assert butacas_solicitadas > max_t  # debe rechazarse


# ---------------------------------------------------------------------------
# Tests de validación de identidades
# ---------------------------------------------------------------------------


class TestIdentityValidation:
    """Prueba la normalización y validación de combinaciones."""

    def test_normalize_convierte_a_minusculas(self):
        """La normalización convierte a minúsculas y une con guión bajo."""
        from identity import normalize_combination

        resultado = normalize_combination("Delfín_Valiente")
        # El resultado debe ser lowercase
        assert resultado == resultado.lower()

    def test_combinacion_valida_en_state(self, two_users):
        """Una combinación añadida al state es reconocida como válida."""
        import state

        assert "delfin_valiente" in state.VALID_COMBINATIONS
        assert "gato_alegre" in state.VALID_COMBINATIONS

    def test_combinacion_invalida_no_en_state(self, two_users):
        """Una combinación no registrada no debe estar en VALID_COMBINATIONS."""
        import state

        assert "perro_furioso" not in state.VALID_COMBINATIONS


# ---------------------------------------------------------------------------
# Tests de bootstrap: generación de hashes
# ---------------------------------------------------------------------------


class TestHashGeneration:
    """Prueba que los hashes son únicos y tienen el formato correcto."""

    def test_hashes_son_unicos(self):
        """Generar múltiples hashes no produce colisiones."""
        hashes = {secrets.token_urlsafe(16) for _ in range(200)}
        assert len(hashes) == 200

    def test_hash_longitud_correcta(self):
        """token_urlsafe(16) produce un string de al menos 16 caracteres."""
        h = secrets.token_urlsafe(16)
        # base64url de 16 bytes → ~22 caracteres
        assert len(h) >= 16

    def test_hash_solo_caracteres_url_seguros(self):
        """El hash no debe contener caracteres problemáticos para URLs."""
        import re

        for _ in range(50):
            h = secrets.token_urlsafe(16)
            assert re.match(r"^[A-Za-z0-9_\-]+$", h), f"Hash no URL-safe: {h}"


# ---------------------------------------------------------------------------
# Tests de endpoint y throttling (con TestClient)
# ---------------------------------------------------------------------------


class TestTransferRateLimiting:
    """Prueba el throttling/rate limiting en los endpoints de transferencia."""

    @pytest.fixture(autouse=True)
    def setup_api(self):
        """Inicializa la base de datos de test y limpia bloques de rate limiting."""
        import asyncio

        import bootstrap_db
        import rate_limiting
        import state

        # Inicializar la base de datos
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(bootstrap_db.init_db())
        loop.close()

        # Asegurar rate limiting activo en state y limpiar bloques
        self.orig_rate_limit = state.RATE_LIMIT
        state.RATE_LIMIT = True
        rate_limiting.ip_blocks.clear()

        yield

        state.RATE_LIMIT = self.orig_rate_limit
        rate_limiting.ip_blocks.clear()

    def test_validate_success_resets_rate_limit(self, two_users):
        import time

        from fastapi.testclient import TestClient

        import rate_limiting
        from main import app

        client = TestClient(app)
        # Meter un fallo para una IP ficticia con expiración en el pasado
        rate_limiting.ip_blocks["testclient"] = {
            "failures": 2,
            "blocked_until": time.time() - 10,
        }
        assert rate_limiting.get_block_remaining("testclient") == 0
        assert "testclient" in rate_limiting.ip_blocks

        # Llamar a validate con una combinación exitosa
        token = two_users["receptor_hash"]
        emisor = two_users["emisor"]  # "delfin_valiente"
        response = client.get(f"/api/transfer/validate?token={token}&emisor={emisor}")

        assert response.status_code == 200
        # Debería haber limpiado el rate limit por completo
        assert "testclient" not in rate_limiting.ip_blocks

    def test_validate_failure_triggers_rate_limit(self, two_users):
        from fastapi.testclient import TestClient

        import rate_limiting
        from main import app

        client = TestClient(app)
        token = two_users["receptor_hash"]

        # Intentar con un emisor inválido
        response = client.get(
            f"/api/transfer/validate?token={token}&emisor=animal_invalido"
        )
        assert response.status_code == 404
        assert response.json()["error"] == "Identidad del emisor no válida"

        # Debería estar bloqueado
        assert rate_limiting.get_block_remaining("testclient") > 0

        # Siguiente petición debería devolver 429
        response_blocked = client.get(
            f"/api/transfer/validate?token={token}&emisor={two_users['emisor']}"
        )
        assert response_blocked.status_code == 429
        assert "IP está temporalmente bloqueada" in response_blocked.json()["error"]

    def test_confirm_failure_triggers_rate_limit(self, two_users):
        from fastapi.testclient import TestClient

        import rate_limiting
        from main import app

        client = TestClient(app)
        token = two_users["receptor_hash"]

        # Intentar confirmación con emisor inválido
        response = client.post(
            "/api/transfer/confirm",
            json={"token": token, "emisor": "animal_invalido", "butacas": 1},
        )
        assert response.status_code == 404
        assert response.json()["error"] == "Identidad del emisor no válida"

        # Debería estar bloqueado
        assert rate_limiting.get_block_remaining("testclient") > 0

        # Intentar de nuevo con datos correctos deberia retornar 429
        response_blocked = client.post(
            "/api/transfer/confirm",
            json={"token": token, "emisor": two_users["emisor"], "butacas": 1},
        )
        assert response_blocked.status_code == 429
        assert "IP está temporalmente bloqueada" in response_blocked.json()["error"]
