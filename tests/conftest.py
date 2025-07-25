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


def _docker_image_exists(image: str) -> bool:
    """Check if Docker image exists locally."""
    docker = shutil.which("docker")
    if not docker:
        return False

    try:
        result = subprocess.run(
            [docker, "image", "inspect", image],
            capture_output=True,
            timeout=0.5,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def _command_succeeds(cmd: list[str], timeout: float = 0.5) -> bool:
    """Return True if *cmd* runs & exits with 0 inside *timeout*."""
    try:
        return (
            subprocess.run(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=timeout,
            ).returncode
            == 0
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def _start_azurite() -> subprocess.Popen | None:
    """Start Azurite (preferring Docker) and return the process or None."""
    docker = shutil.which("docker")
    npx = shutil.which("npx")

    # --- Docker (preferred) -------------------------------------------------
    image = "mcr.microsoft.com/azure-storage/azurite"
    if (
        docker
        and _docker_image_exists(image)  # image already present → no pull delay
        and _command_succeeds(
            [docker, "run", "--rm", image, "azurite-blob", "--version"]
        )
    ):
        cmd = [
            docker,
            "run",
            "--rm",
            "-i",
            "-p",
            "10000:10000",
            image,
            "azurite-blob",
            "--skipApiVersionCheck",
            "--silent",
        ]
        return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # --- npx fallback -------------------------------------------------------
    if npx and _command_succeeds([npx, "azurite", "--version"]):
        cmd = [
            npx,
            "azurite",
            "--skipApiVersionCheck",
            "--silent",
            "--location",
            "/tmp/azurite",
        ]
        return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # Nothing suitable
    return None


def pytest_sessionstart(session: pytest.Session) -> None:  # noqa: D401
    """Ensure Azurite is running before tests start."""
    if _is_azurite_running():
        session.config.azurite_process = None
        return

    process = _start_azurite()
    session.config.azurite_process = process

    for _ in range(20):  # ≤2 s total
        if _is_azurite_running():
            return
        time.sleep(0.1)

    if process:
        process.terminate()
        session.config.azurite_process = None
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
            process.wait(timeout=5)
        except Exception:
            process.kill()


