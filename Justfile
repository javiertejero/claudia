download_music:
	@[ -f static/valse_gymnopedie.mp3 ] || curl -L -o static/valse_gymnopedie.mp3 "https://incompetech.com/music/royalty-free/mp3-royaltyfree/Valse%20Gymnopedie.mp3"
	@[ -f static/star_wars_theme.mp3 ] || curl -L -o static/star_wars_theme.mp3 "https://s.cdpn.io/1202/Star_Wars_original_opening_crawl_1977.mp3"

launch: download_music
    uv run --frozen --env-file .env fastapi run main.py

launch_for_testing: download_music
    RATE_LIMIT=False DISABLE_IDENTITY_CHECKS=True uv run --frozen --env-file .env fastapi run main.py

lint:
    uv run --frozen prek run --all-files

test:
    uv run --frozen pytest


build_pdf:
    uv tool install md2pdf[cli]
    md2pdf --input manual_admin.md --output static/manual_admin.pdf
    md2pdf --input manual_usuario.md --output static/manual_usuario.pdf


install_playwright:
    uv run playwright install chromium

run_playwright *ARGS: install_playwright
    uv run pytest tests/e2e/test_booking_flow.py -v --headed {{ARGS}}