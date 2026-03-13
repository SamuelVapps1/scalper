from __future__ import annotations

import argparse
import importlib
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str = ""


def _import_module(name: str) -> Tuple[Optional[Any], CheckResult]:
    try:
        module = importlib.import_module(name)
        file = getattr(module, "__file__", "") or ""
        return module, CheckResult(name=f"import:{name}", ok=True, detail=file)
    except Exception as exc:  # pragma: no cover - defensive
        return None, CheckResult(name=f"import:{name}", ok=False, detail=str(exc))


def check_core_imports() -> List[CheckResult]:
    modules = [
        "config",
        "scalper.scanner",
        "scalper.signals",
        "scalper.trade_preview",
        "scalper.paper_engine",
        "scalper.paper_broker",
        "scalper.telegram_format",
        "scalper.bybit",
        "scalper.settings",
    ]
    results: List[CheckResult] = []
    for name in modules:
        _, res = _import_module(name)
        results.append(res)
    return results


def check_settings_parse() -> List[CheckResult]:
    results: List[CheckResult] = []

    cfg, res_cfg = _import_module("config")
    results.append(res_cfg)

    settings_mod, res_settings = _import_module("scalper.settings")
    results.append(res_settings)

    if settings_mod is not None:
        try:
            get_settings = getattr(settings_mod, "get_settings", None)
            settings_obj = get_settings() if callable(get_settings) else None
            if settings_obj is None:
                results.append(
                    CheckResult(
                        name="settings:get_settings",
                        ok=False,
                        detail="get_settings() returned None",
                    )
                )
            else:
                # Minimal structural sanity checks, no behavior changes.
                missing: List[str] = []
                for attr in ("risk", "telegram", "bybit"):
                    if not hasattr(settings_obj, attr):
                        missing.append(attr)
                if missing:
                    results.append(
                        CheckResult(
                            name="settings:structure",
                            ok=False,
                            detail=f"settings missing attributes: {', '.join(missing)}",
                        )
                    )
                else:
                    results.append(
                        CheckResult(
                            name="settings:structure",
                            ok=True,
                            detail="risk/telegram/bybit present",
                        )
                    )
        except Exception as exc:  # pragma: no cover - defensive
            results.append(
                CheckResult(
                    name="settings:get_settings",
                    ok=False,
                    detail=str(exc),
                )
            )

    # Touch a couple of config constants without asserting specific values.
    if cfg is not None:
        try:
            _ = getattr(cfg, "INTERVAL", None)
            _ = getattr(cfg, "LOOKBACK", None)
            results.append(
                CheckResult(
                    name="config:core_constants",
                    ok=True,
                    detail="INTERVAL/LOOKBACK readable",
                )
            )
        except Exception as exc:  # pragma: no cover - defensive
            results.append(
                CheckResult(
                    name="config:core_constants",
                    ok=False,
                    detail=str(exc),
                )
            )

    return results


def check_scanner_paper_path() -> List[CheckResult]:
    results: List[CheckResult] = []
    scanner_mod, res = _import_module("scalper.scanner")
    results.append(res)
    if not res.ok or scanner_mod is None:
        return results

    import inspect

    try:
        run_scan_cycle = getattr(scanner_mod, "run_scan_cycle", None)
        sig = inspect.signature(run_scan_cycle)
        has_paper_mode = "paper_mode" in sig.parameters
        results.append(
            CheckResult(
                name="scanner:run_scan_cycle.paper_mode",
                ok=bool(has_paper_mode),
                detail="paper_mode parameter present" if has_paper_mode else "paper_mode parameter missing",
            )
        )
    except Exception as exc:  # pragma: no cover - defensive
        results.append(
            CheckResult(
                name="scanner:run_scan_cycle.inspect",
                ok=False,
                detail=str(exc),
            )
        )

    try:
        parse_args = getattr(scanner_mod, "parse_args", None)
        results.append(
            CheckResult(
                name="scanner:parse_args.exists",
                ok=callable(parse_args),
                detail="parse_args callable" if callable(parse_args) else "parse_args not found",
            )
        )
    except Exception as exc:  # pragma: no cover - defensive
        results.append(
            CheckResult(
                name="scanner:parse_args.exists",
                ok=False,
                detail=str(exc),
            )
        )
    return results


def run_formatter_smoke_internal() -> List[CheckResult]:
    """
    Call scanner.run_test_telegram_formats(config) directly.
    This only prints to stdout and does not send real Telegram messages.
    """
    results: List[CheckResult] = []
    scanner_mod, res_scanner = _import_module("scalper.scanner")
    results.append(res_scanner)
    cfg_mod, res_cfg = _import_module("config")
    results.append(res_cfg)

    if not (res_scanner.ok and res_cfg.ok and scanner_mod and cfg_mod):
        return results

    try:
        fn = getattr(scanner_mod, "run_test_telegram_formats", None)
        if not callable(fn):
            results.append(
                CheckResult(
                    name="scanner:run_test_telegram_formats",
                    ok=False,
                    detail="function not found",
                )
            )
            return results
        code = int(fn(cfg_mod))
        results.append(
            CheckResult(
                name="scanner:formatter_smoke_internal",
                ok=(code == 0),
                detail=f"exit_code={code}",
            )
        )
    except Exception as exc:  # pragma: no cover - defensive
        results.append(
            CheckResult(
                name="scanner:formatter_smoke_internal",
                ok=False,
                detail=str(exc),
            )
        )
    return results


def run_external_cli_smoke() -> List[CheckResult]:
    """
    Optional: invoke `python -m scalper.scanner --test-telegram-formats`.
    Useful to ensure the CLI entrypoint is still wired correctly.
    """
    cmd = [sys.executable, "-m", "scalper.scanner", "--test-telegram-formats"]
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=120,
        )
        ok = proc.returncode == 0
        detail = f"exit_code={proc.returncode}"
        return [CheckResult(name="cli:scanner_test_telegram_formats", ok=ok, detail=detail)]
    except Exception as exc:  # pragma: no cover - defensive
        return [
            CheckResult(
                name="cli:scanner_test_telegram_formats",
                ok=False,
                detail=str(exc),
            )
        ]


def scan_legacy_conflicts() -> List[CheckResult]:
    """
    Scan for merge markers and duplicate runtime-critical modules under the scalper package.
    This does not modify anything; it only reports potential legacy-path dangers.
    """
    results: List[CheckResult] = []
    try:
        scalper_pkg, res_pkg = _import_module("scalper")
        results.append(res_pkg)
        if not res_pkg.ok or scalper_pkg is None:
            return results
        pkg_root = Path(scalper_pkg.__file__).resolve().parent
    except Exception as exc:  # pragma: no cover - defensive
        results.append(
            CheckResult(
                name="scan_legacy_conflicts:pkg_root",
                ok=False,
                detail=str(exc),
            )
        )
        return results

    critical_names = {"scanner.py", "paper_engine.py", "paper_broker.py", "settings.py", "config.py"}
    conflict_markers = ("<<<<<<< HEAD", ">>>>>>>")
    duplicate_map: Dict[str, List[Path]] = {}
    conflict_files: List[Path] = []

    for path in pkg_root.rglob("*.py"):
        if path.name in critical_names:
            duplicate_map.setdefault(path.name, []).append(path)
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            if any(marker in text for marker in conflict_markers):
                conflict_files.append(path)

    for name, paths in duplicate_map.items():
        if len(paths) > 1:
            results.append(
                CheckResult(
                    name=f"duplicates:{name}",
                    ok=False,
                    detail="; ".join(str(p) for p in paths),
                )
            )
        else:
            results.append(
                CheckResult(
                    name=f"duplicates:{name}",
                    ok=True,
                    detail=str(paths[0]),
                )
            )

    if conflict_files:
        results.append(
            CheckResult(
                name="merge_conflicts:critical_files",
                ok=False,
                detail="; ".join(str(p) for p in conflict_files),
            )
        )
    else:
        results.append(
            CheckResult(
                name="merge_conflicts:critical_files",
                ok=True,
                detail="none detected",
            )
        )
    return results


def run_all_checks(external_cli: bool = False) -> Tuple[int, List[CheckResult]]:
    all_results: List[CheckResult] = []
    all_results.extend(check_core_imports())
    all_results.extend(check_settings_parse())
    all_results.extend(check_scanner_paper_path())
    all_results.extend(run_formatter_smoke_internal())
    all_results.extend(scan_legacy_conflicts())
    if external_cli:
        all_results.extend(run_external_cli_smoke())

    # Failure criteria: any core import failure, settings failure, or formatter smoke failure.
    failed: List[CheckResult] = [
        r
        for r in all_results
        if not r.ok
        and (
            r.name.startswith("import:")
            or r.name.startswith("settings:")
            or r.name.startswith("config:")
            or "formatter_smoke" in r.name
            or r.name.startswith("scanner:run_scan_cycle")
        )
    ]
    exit_code = 0 if not failed else 1
    return exit_code, all_results


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Scalper runtime validation harness (non-intrusive safety checks)."
    )
    parser.add_argument(
        "--external-cli",
        action="store_true",
        help="Also run `python -m scalper.scanner --test-telegram-formats` as a smoke test.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print all check results, not just failures.",
    )
    args = parser.parse_args(argv)

    exit_code, results = run_all_checks(external_cli=bool(args.external_cli))

    def _fmt(res: CheckResult) -> str:
        status = "OK" if res.ok else "FAIL"
        return f"[{status}] {res.name}: {res.detail}"

    if args.verbose or exit_code != 0:
        for res in results:
            if args.verbose or not res.ok:
                print(_fmt(res))
    else:
        # Summarize only.
        failed = [r for r in results if not r.ok]
        print(f"Checks passed={len(results) - len(failed)} failed={len(failed)}")
        for res in failed:
            print(_fmt(res))

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

