download_music:
    @[ -f static/valse_gymnopedie.mp3 ] || curl -L -o static/valse_gymnopedie.mp3 "https://incompetech.com/music/royalty-free/mp3-royaltyfree/Valse%20Gymnopedie.mp3"

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