import os
import time
import logging
import state

logger = logging.getLogger(__name__)


# Rate Limiting por IP
ip_blocks = {}  # { ip: {"failures": int, "blocked_until": float} }


def get_block_remaining(ip: str) -> int:
    if ip not in ip_blocks:
        return 0
    remaining = ip_blocks[ip]["blocked_until"] - time.time()
    return max(0, int(remaining))


def register_failed_attempt(ip: str):
    if not state.RATE_LIMIT:
        logger.info(f"Rate limiting desactivado. IP {ip} no bloqueada.")
        return
    now = time.time()
    if ip not in ip_blocks:
        ip_blocks[ip] = {"failures": 0, "blocked_until": 0}
    ip_blocks[ip]["failures"] += 1
    failures = ip_blocks[ip]["failures"]
    if failures == 1:
        duration = 60
    elif failures == 2:
        duration = 120
    else:
        duration = 180
    ip_blocks[ip]["blocked_until"] = now + duration
    logger.warning(
        f"IP {ip} falló validación. Fallos acumulados: {failures}. Bloqueada por {duration}s."
    )


def register_successful_attempt(ip: str):
    if ip in ip_blocks:
        del ip_blocks[ip]
        logger.info(f"IP {ip} validada exitosamente. Penalización reseteada.")
