import os
import shutil
import subprocess
from pathlib import Path


def _compose_exec(env_dir: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    base = ["docker", "compose"] if shutil.which("docker") else ["docker-compose"]
    return subprocess.run(
        [*base, *args],
        check=True,
        cwd=str(env_dir),
        text=True,
        capture_output=True,
        timeout=120,
    )


def _get_sftp_container_id(env_dir: Path) -> str:
    proc = _compose_exec(env_dir, ["ps", "-q", "sftp-server"])
    cid = (proc.stdout or "").strip()
    if not cid:
        raise RuntimeError("sftp-server container not running")
    return cid


def _clear_and_populate_sftp(env_dir: Path, inputs_dir: Path) -> None:
    container_id = _get_sftp_container_id(env_dir)

    # Clear target directory inside container (avoid SFTP operations)
    subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            container_id,
            "sh",
            "-lc",
            "rm -rf /home/test/data/* && mkdir -p /home/test/data/doc/cor /home/test/data/doc/Quarterly/Cor",
        ],
        check=True,
        text=True,
        capture_output=True,
        timeout=120,
    )

    # Copy input files into container preserving structure
    # Daily files
    daily_dir = inputs_dir / "cor"
    if daily_dir.exists():
        for p in daily_dir.iterdir():
            if p.is_file():
                subprocess.run(
                    [
                        "docker",
                        "cp",
                        str(p),
                        f"{container_id}:/home/test/data/doc/cor/{p.name}",
                    ],
                    check=True,
                    text=True,
                    capture_output=True,
                    timeout=120,
                )

    # Quarterly archive
    q_dir = inputs_dir / "Quarterly" / "Cor"
    if q_dir.exists():
        for p in q_dir.iterdir():
            if p.is_file():
                subprocess.run(
                    [
                        "docker",
                        "cp",
                        str(p),
                        f"{container_id}:/home/test/data/doc/Quarterly/Cor/{p.name}",
                    ],
                    check=True,
                    text=True,
                    capture_output=True,
                    timeout=120,
                )


def main() -> None:
    # Prepare SFTP container with case inputs
    env_dir = Path(__file__).resolve().parents[2] / "environment"
    inputs_dir = Path(__file__).resolve().parent / "inputs"

    # Ensure environment is up (no-op if already up)
    try:
        _compose_exec(env_dir, ["up", "-d"])
    except Exception:
        # Proceed; container may already be running
        pass

    _clear_and_populate_sftp(env_dir, inputs_dir)

    _ = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")


if __name__ == "__main__":
    main()
