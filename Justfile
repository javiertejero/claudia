launch:
    uv run --frozen --env-file .env fastapi run main.py

launch_for_testing:
    RATE_LIMIT=False DISABLE_IDENTITY_CHECKS=True uv run --frozen --env-file .env fastapi run main.py

lint:
    uv run --frozen prek run --all-files

test:
    uv run --frozen pytest