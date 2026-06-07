import asyncio
import os
import threading
import time

import pytest
import uvicorn

import bootstrap_db
import state
from main import app


class UvicornTestServer(uvicorn.Server):
    def install_signal_handlers(self):
        pass

    def run_in_thread(self):
        self.thread = threading.Thread(target=self.run)
        self.thread.start()
        while not self.started:
            time.sleep(0.01)

    def stop(self):
        self.should_exit = True
        self.thread.join()


@pytest.fixture(scope="session")
def test_server():
    # Setup test DB
    test_db = "reservas_test_e2e.db"
    if os.path.exists(test_db):
        os.remove(test_db)

    # Save original state
    orig_db = state.DB_FILE
    orig_max = state.MAX_ACTIVE_USERS
    orig_secret = state.ADMIN_SECRET
    orig_disable_identity = state.DISABLE_IDENTITY_CHECKS

    state.DB_FILE = test_db
    state.MAX_ACTIVE_USERS = 5
    state.ADMIN_SECRET = "secretotestado"
    state.DISABLE_IDENTITY_CHECKS = True
    state.VALID_COMBINATIONS.add("loba_astuta")

    # Bootstrap the DB
    loop = asyncio.new_event_loop()
    loop.run_until_complete(bootstrap_db.init_db())
    loop.close()

    # Start server
    config = uvicorn.Config(app, host="127.0.0.1", port=8001, log_level="info")
    server = UvicornTestServer(config=config)
    server.run_in_thread()

    yield "http://127.0.0.1:8001"

    # Teardown
    server.stop()
    if os.path.exists(test_db):
        try:
            os.remove(test_db)
        except OSError:
            pass

    # Restore original state
    state.DB_FILE = orig_db
    state.MAX_ACTIVE_USERS = orig_max
    state.ADMIN_SECRET = orig_secret
    state.VALID_COMBINATIONS.discard("loba_astuta")
    state.DISABLE_IDENTITY_CHECKS = orig_disable_identity
