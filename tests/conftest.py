import subprocess
import shutil
import socket
import time
import pytest


def _is_azurite_running(host: str = "127.0.0.1", port: int = 10000) -> bool:
    """Return True if Azurite is listening on the specified port."""
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def _start_azurite() -> subprocess.Popen | None:
    """Start Azurite using ``npx`` if available and return the process."""
    npx = shutil.which("npx")
    if npx is None:
        return None

    process = subprocess.Popen(
        [npx, "azurite", "--skipApiVersionCheck", "--silent", "--location", "/tmp/azurite"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    for _ in range(30):
        if _is_azurite_running():
            return process
        time.sleep(1)

    process.terminate()
    return None


def pytest_sessionstart(session: pytest.Session) -> None:  # noqa: D401
    """Ensure Azurite is running before the test session starts."""
    if _is_azurite_running():
        session.config.azurite_process = None
        return

    process = _start_azurite()
    session.config.azurite_process = process
    if process is None:
        print("Warning: Azurite could not be started. Tests may fail.")


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:  # noqa: D401
    """Terminate Azurite after the test session if we started it."""
    process = getattr(session.config, "azurite_process", None)
    if process is not None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except Exception:
            process.kill()

