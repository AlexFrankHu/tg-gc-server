"""Watchdog - monitors event loop health and force-kills the process if stuck."""
import logging
import os
import threading
import time

logger = logging.getLogger(__name__)

# Timestamp of last successful event loop activity
_last_heartbeat: float = time.monotonic()
_lock = threading.Lock()

# Max allowed staleness before force-kill (seconds)
WATCHDOG_TIMEOUT = 180  # 3 minutes
WATCHDOG_CHECK_INTERVAL = 30  # check every 30 seconds


def ping():
    """Called from async tasks to signal the event loop is alive."""
    global _last_heartbeat
    with _lock:
        _last_heartbeat = time.monotonic()


def _watchdog_thread():
    """Background thread that monitors event loop health."""
    logger.info(f"Watchdog started: timeout={WATCHDOG_TIMEOUT}s, check_interval={WATCHDOG_CHECK_INTERVAL}s")
    while True:
        time.sleep(WATCHDOG_CHECK_INTERVAL)
        with _lock:
            elapsed = time.monotonic() - _last_heartbeat
        if elapsed > WATCHDOG_TIMEOUT:
            logger.critical(
                f"Watchdog: event loop unresponsive for {elapsed:.0f}s (>{WATCHDOG_TIMEOUT}s), "
                f"force-killing process!"
            )
            # Flush logs before exit
            logging.shutdown()
            os._exit(1)


def start():
    """Start the watchdog thread (call once at startup)."""
    t = threading.Thread(target=_watchdog_thread, daemon=True, name="watchdog")
    t.start()
    logger.info("Watchdog thread started")
