from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import state
from main import backup_db_task


@pytest.mark.anyio
async def test_backup_db_task_cleanup():
    # We want to test that backups older than 24 hours (86400 seconds) are deleted,
    # and newer backups are kept.

    backup_files = [
        "data/backups/reservas_20260608_010000.db",  # old file (25 hours old)
        "data/backups/reservas_20260608_020000.db",  # new file (1 hour old)
    ]

    now = 1717830000.0  # Some fixed timestamp

    # File ctimes:
    # 25 hours ago: now - 25 * 3600 = now - 90000
    # 1 hour ago: now - 1 * 3600 = now - 3600
    file_ctime_map = {
        "data/backups/reservas_20260608_010000.db": now - 90000,
        "data/backups/reservas_20260608_020000.db": now - 3600,
    }

    removed_files = []

    def mock_remove(path):
        removed_files.append(path)

    def mock_getctime(path):
        return file_ctime_map.get(path, now)

    original_shutting_down = state.IS_SHUTTING_DOWN
    state.IS_SHUTTING_DOWN = False

    async def mock_sleep(seconds):
        state.IS_SHUTTING_DOWN = True

    with (
        patch("asyncio.sleep", mock_sleep),
        patch("time.time", return_value=now),
        patch("glob.glob", return_value=backup_files),
        patch("os.path.getctime", mock_getctime),
        patch("os.remove", mock_remove),
        patch("os.makedirs"),
        patch("aiosqlite.connect") as mock_connect,
    ):
        # Mock database backup context managers to not do real database work
        mock_db = MagicMock()
        mock_connect.return_value.__aenter__.return_value = mock_db
        mock_db.backup = AsyncMock()

        await backup_db_task()

    assert "data/backups/reservas_20260608_010000.db" in removed_files
    assert "data/backups/reservas_20260608_020000.db" not in removed_files

    state.IS_SHUTTING_DOWN = original_shutting_down
