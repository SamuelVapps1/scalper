from __future__ import annotations

import logging
import os
import signal
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Callable, TextIO


def _sanitize_instance_id(instance_id: str) -> str:
    raw = str(instance_id or "").strip()
    if not raw:
        return ""
    safe = "".join(ch for ch in raw if ch.isalnum() or ch in ("-", "_"))
    return safe[:64]


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


class SingleInstanceLock:
    """
    File-based single instance guard.
    - Default lock path: runs/bot.lock
    """

    def __init__(self, runs_dir: Path, instance_id: str = "") -> None:
        self.runs_dir = Path(runs_dir)
        self.instance_id = _sanitize_instance_id(instance_id)
        self.lock_path = self.runs_dir / "bot.lock"
        self._acquired = False

    def acquire(self) -> None:
        self.runs_dir.mkdir(parents=True, exist_ok=True)

        if self.lock_path.exists():
            try:
                payload = self.lock_path.read_text(encoding="utf-8").strip()
                old_pid = int(payload.split("|", 1)[0]) if payload else 0
            except Exception:
                old_pid = 0
            if _pid_is_alive(old_pid):
                raise RuntimeError(f"ALREADY_RUNNING pid={old_pid}")
            # Stale lock: remove and continue.
            try:
                self.lock_path.unlink()
            except OSError:
                pass

        pid = os.getpid()
        started = datetime.now(timezone.utc).isoformat()
        # O_EXCL avoids races when two instances start simultaneously.
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        fd = os.open(str(self.lock_path), flags)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(f"{pid}|{started}\n")
        self._acquired = True

    def release(self) -> None:
        if not self._acquired:
            return
        try:
            if self.lock_path.exists():
                self.lock_path.unlink()
        except OSError:
            pass
        finally:
            self._acquired = False

    def __enter__(self) -> "SingleInstanceLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


def setup_rotating_logging(
    *,
    log_level: str,
    runs_dir: Path,
    instance_id: str = "",
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
) -> Path:
    """
    Configure root logger with:
    - Console stream handler (stdout)
    - Rotating file handler (10MB, 5 backups by default)
    """
    runs_dir = Path(runs_dir)
    runs_dir.mkdir(parents=True, exist_ok=True)

    level = getattr(logging, str(log_level or "INFO").upper(), logging.INFO)
    inst = _sanitize_instance_id(instance_id)
    file_name = "bot.log" if not inst else f"bot_{inst}.log"
    log_path = runs_dir / file_name

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    root = logging.getLogger()
    # Replace previous handlers to avoid duplicate logs.
    for h in list(root.handlers):
        root.removeHandler(h)

    # Windows-safe console fallback for unexpected characters.
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    if hasattr(sys.stderr, "reconfigure"):
        try:
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    class SafeConsoleHandler(logging.StreamHandler):
        def emit(self, record: logging.LogRecord) -> None:
            try:
                super().emit(record)
            except UnicodeEncodeError:
                msg = self.format(record)
                stream: TextIO = self.stream
                safe = msg.encode("ascii", errors="replace").decode("ascii", errors="replace")
                stream.write(safe + self.terminator)
                self.flush()

    console = SafeConsoleHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(formatter)

    rotating = RotatingFileHandler(
        filename=str(log_path),
        maxBytes=int(max_bytes),
        backupCount=int(backup_count),
        encoding="utf-8",
    )
    rotating.setLevel(level)
    rotating.setFormatter(formatter)

    root.setLevel(level)
    root.addHandler(console)
    root.addHandler(rotating)

    return log_path


def install_signal_handlers(on_shutdown: Callable[[str], None]) -> None:
    """
    Install SIGINT/SIGTERM handlers that trigger graceful shutdown callback.
    """

    def _handler(sig_num, _frame):
        try:
            sig_name = signal.Signals(sig_num).name
        except Exception:
            sig_name = f"SIG{sig_num}"
        on_shutdown(sig_name)

    signal.signal(signal.SIGINT, _handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handler)

