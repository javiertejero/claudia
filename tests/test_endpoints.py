from fastapi.testclient import TestClient

import state
from main import app


def test_api_config_returns_app_url():
    original_app_url = state.APP_URL
    state.APP_URL = "http://test-url.local"
    try:
        client = TestClient(app)
        response = client.get("/api/config")
        assert response.status_code == 200
        data = response.json()
        assert "app_url" in data
        assert data["app_url"] == "http://test-url.local"
    finally:
        state.APP_URL = original_app_url


def test_admin_combinations_returns_app_url():
    original_app_url = state.APP_URL
    state.APP_URL = "http://test-url-combinations.local"
    original_admin_secret = state.ADMIN_SECRET
    state.ADMIN_SECRET = "testsecret123"
    state.VALID_COMBINATIONS.add("tigresa_valiente")
    try:
        client = TestClient(app)
        # Unauthorized check
        response_unauth = client.get("/admin/wrongsecret/combinations")
        assert response_unauth.status_code == 200
        assert response_unauth.json() == {"error": "No autorizado"}

        # Authorized check
        response_auth = client.get("/admin/testsecret123/combinations")
        assert response_auth.status_code == 200
        data = response_auth.json()
        assert "app_url" in data
        assert data["app_url"] == "http://test-url-combinations.local"
        assert "combinations" in data
    finally:
        state.APP_URL = original_app_url
        state.ADMIN_SECRET = original_admin_secret
        state.VALID_COMBINATIONS.discard("tigresa_valiente")


def test_thanks_endpoint_returns_agradecimientos_html():
    client = TestClient(app)
    response = client.get("/thanks")
    assert response.status_code == 200
    assert "Agradecimientos" in response.text
    # Test with trailing slash as well
    response_slash = client.get("/thanks/")
    assert response_slash.status_code == 200
    assert "Agradecimientos" in response_slash.text
