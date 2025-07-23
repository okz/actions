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


@pytest.fixture(autouse=True)
def azurite_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provide default Azurite credentials via environment variables."""
    connection = (
        "DefaultEndpointsProtocol=http;"
        "AccountName=devstoreaccount1;"
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
    )

    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", connection)
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_NAME", "devstoreaccount1")
    monkeypatch.setenv(
        "AZURE_STORAGE_ACCOUNT_KEY",
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
    )
    monkeypatch.setenv("AZURITE_BLOB_STORAGE_URL", "http://127.0.0.1:10000")


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:  # noqa: D401
    """Terminate Azurite after the test session if we started it."""
    process = getattr(session.config, "azurite_process", None)
    if process is not None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except Exception:
            process.kill()

