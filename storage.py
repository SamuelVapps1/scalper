import csv
from pathlib import Path
from typing import Dict, Any

CSV_PATH = Path("signals_log.csv")
CSV_HEADERS = ["timestamp_utc", "symbol", "setup", "direction", "close", "reason"]


def append_signal(signal: Dict[str, Any]) -> None:
    file_exists = CSV_PATH.exists()

    with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(
            {
                "timestamp_utc": signal.get("timestamp_utc", ""),
                "symbol": signal.get("symbol", ""),
                "setup": signal.get("setup", ""),
                "direction": signal.get("direction", ""),
                "close": signal.get("close", ""),
                "reason": signal.get("reason", ""),
            }
        )
