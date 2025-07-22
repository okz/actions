import subprocess
import shutil
import socket
import time
import pytest


def _is_azurite_running(host: str = "127.0.0.1", port: int = 10000) -> bool:
    """Check if Azurite is already running on the given port."""
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def _start_azurite() -> subprocess.Popen | None:
    """Attempt to start Azurite using npx if available."""
    npx = shutil.which("npx")
    if npx is None:
        return None

    # Launch Azurite in silent mode to minimize output
    process = subprocess.Popen(
        [npx, "azurite", "--skipApiVersionCheck", "--silent", "--location", "/tmp/azurite"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # Wait for Azurite to become ready
    for _ in range(30):
        if _is_azurite_running():
            return process
        time.sleep(1)

    process.terminate()
    return None


def pytest_sessionstart(session: pytest.Session) -> None:  # noqa: D401 - Pytest hook
    """Start Azurite before tests if it's not already running."""
    if _is_azurite_running():
        session.config.azurite_process = None
        return

    process = _start_azurite()
    session.config.azurite_process = process
    if process is None:
        print("Warning: Azurite could not be started. Some tests may fail.")


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:  # noqa: D401 - Pytest hook
    """Terminate Azurite after the test session."""
    process = getattr(session.config, "azurite_process", None)
    if process is not None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except Exception:
            process.kill()
