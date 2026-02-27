from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse

from scalper.settings import _ENV_BOOTSTRAP_STATE, get_settings
from storage import (
    compute_paper_kpis,
    get_block_stats_last_24h,
    get_last_bias_json,
    get_last_block_reason,
    get_last_scan_ts,
    get_near_misses,
    get_recent_risk_events,
    get_recent_signals,
    get_recent_trade_intents,
    get_selected_watchlist,
    get_signals_since,
    get_symbols_v3,
    get_watchlist_transparency,
    load_paper_state,
)

VERSION = "dry-run-dashboard-1.1"
settings = get_settings()
_STARTED_AT = time.time()


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _format_ts_human(ts: int | None) -> str:
    if ts is None:
        return "-"
    try:
        dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return "-"
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def _today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _summary_payload() -> Dict[str, Any]:
    now_ts = int(time.time())
    last_scan_ts = get_last_scan_ts()
    signals_last_24h = get_signals_since(now_ts - 24 * 60 * 60)
    state = load_paper_state()
    open_positions = list(state.get("open_positions", []) or [])
    closed_trades = list(state.get("closed_trades", []) or [])
    today_iso = _today_iso()
    wins_today = 0
    losses_today = 0
    for t in closed_trades:
        if not isinstance(t, dict):
            continue
        close_ts = str(t.get("close_ts", t.get("ts", "")) or "")
        if not close_ts:
            continue
        try:
            close_date = close_ts[:10] if len(close_ts) >= 10 else ""
        except Exception:
            close_date = ""
        if close_date != today_iso:
            continue
        pnl = float(t.get("pnl_usdt", 0.0) or 0.0)
        if pnl > 0:
            wins_today += 1
        elif pnl < 0:
            losses_today += 1
    open_positions_rows = [
        {
            "symbol": str(p.get("symbol", "") or ""),
            "direction": str(p.get("side", p.get("direction", "")) or ""),
            "entry": float(p.get("entry_price", 0.0) or 0.0),
            "sl": float(p.get("sl_price", 0.0) or 0.0),
            "tp": float(p.get("tp_price", 0.0) or 0.0),
            "unrealized": "-",
        }
        for p in open_positions
        if isinstance(p, dict)
    ]
    watchlist_symbols: List[str] = []
    watchlist_mode = "static"
    stored_symbols, stored_mode = get_selected_watchlist()
    if stored_symbols:
        watchlist_symbols = list(stored_symbols)
        watchlist_mode = stored_mode
    else:
        try:
            from watchlist import get_watchlist

            import config as legacy_config

            watchlist_symbols, watchlist_mode = get_watchlist(legacy_config, bybit_client=None, logger=None)
        except Exception:
            watchlist_symbols = list(settings.risk.watchlist or [])
            watchlist_mode = str(settings.risk.watchlist_mode).strip().lower()
    trans = get_watchlist_transparency()
    selected_from_trans = trans.get("selected_symbols") or []
    selected_symbols = selected_from_trans if selected_from_trans else watchlist_symbols
    bias_rows = get_last_bias_json() or []
    bias_by_symbol = {
        str(item.get("symbol", "") or "").upper(): item
        for item in bias_rows
        if isinstance(item, dict) and str(item.get("symbol", "")).strip()
    }
    symbols_v3 = get_symbols_v3() or {}
    per_symbol: List[Dict[str, Any]] = []
    for sym in selected_symbols:
        symbol = str(sym or "").upper()
        bias_info = bias_by_symbol.get(symbol, {})
        v3_wrap = symbols_v3.get(symbol, {}) if isinstance(symbols_v3, dict) else {}
        v3 = (v3_wrap.get("v3", {}) if isinstance(v3_wrap, dict) else {}) or {}
        v3_status = {
            "ok": bool(v3.get("ok", False)),
            "side": v3.get("side"),
            "reason": str(v3.get("reason", "") or ""),
            "breakout_level": v3.get("breakout_level"),
        }
        skip_reasons: List[str] = []
        bias_reason = str(bias_info.get("reason", "") or "")
        if bias_reason:
            skip_reasons.append(bias_reason)
        if v3_status["reason"] and not v3_status["ok"]:
            skip_reasons.append(v3_status["reason"])
        per_symbol.append(
            {
                "symbol": symbol,
                "bias": str(bias_info.get("bias", "") or ""),
                "v3": v3_status,
                "skip_reasons": skip_reasons,
            }
        )
    return {
        "last_scan_ts": _format_ts_human(last_scan_ts),
        "scan_seconds": int(settings.risk.scan_seconds),
        "watchlist_count": len(watchlist_symbols),
        "watchlist_mode": watchlist_mode,
        "watchlist_source": trans.get("watchlist_source", "static"),
        "watchlist_updated_ts": _format_ts_human(trans.get("watchlist_updated_ts")) if trans.get("watchlist_updated_ts") else "-",
        "watchlist_cached_until_ts": _format_ts_human(trans.get("watchlist_cached_until_ts")) if trans.get("watchlist_cached_until_ts") else "-",
        "watchlist_candidates_count": trans.get("watchlist_candidates_count"),
        "watchlist": selected_symbols,
        "watchlist_symbols": watchlist_symbols,
        "selected_symbols": selected_symbols,
        "signals_last_24h": int(signals_last_24h),
        "pnl_today": float(state.get("daily_pnl_sim", 0.0) or 0.0),
        "open_positions_count": len(open_positions),
        "wins_today": wins_today,
        "losses_today": losses_today,
        "open_positions": open_positions_rows,
        "bias": bias_rows[:10],
        "symbols": symbols_v3,
        "per_symbol": per_symbol,
        "near_misses": get_near_misses(),
        "block_stats": get_block_stats_last_24h(),
        "kpi": compute_paper_kpis(
            list(state.get("closed_trades", []) or []),
            paper_equity_usdt=float(settings.risk.paper_equity_usdt),
        ),
    }


def _derive_last_block_reason(state: Dict[str, Any]) -> str:
    risk_events = list(state.get("risk_events", []) or [])
    for event in reversed(risk_events):
        if not isinstance(event, dict):
            continue
        status = str(event.get("status", "") or "").strip().upper()
        if status == "TRIGGERED":
            reason = str(event.get("reason") or event.get("type") or "").strip()
            if reason:
                return reason

    trade_intents = list(state.get("trade_intents", []) or [])
    for intent in reversed(trade_intents):
        if not isinstance(intent, dict):
            continue
        block_reason = str(intent.get("block_reason", "") or "").strip()
        if block_reason:
            return block_reason

    # Legacy fallback to older state key used before RiskEvents/TradeIntents existed.
    return str(get_last_block_reason() or "-")


def _risk_payload() -> Dict[str, Any]:
    state = load_paper_state()
    open_positions = list(state.get("open_positions", []) or [])
    open_position_simulated = len(open_positions) > 0
    position_mode = str(settings.risk.position_mode or "global").lower().strip()
    max_concurrent_positions = max(0, int(settings.risk.max_concurrent_positions))
    return {
        "kill_switch": bool(_env_int("KILL_SWITCH", 0)),
        "max_trades_day": _env_int("MAX_TRADES_DAY", 999),
        "cooldown_after_loss_minutes": _env_int("COOLDOWN_AFTER_LOSS_MIN", 0),
        "open_position_simulated": open_position_simulated,
        "last_block_reason": _derive_last_block_reason(state),
        "position_mode": position_mode,
        "max_concurrent_positions": max_concurrent_positions,
        "open_positions_count": len(open_positions),
    }


def _health_payload() -> Dict[str, Any]:
    last_scan_ts = get_last_scan_ts()
    uptime_s = max(0.0, time.time() - _STARTED_AT)
    try:
        _ = load_paper_state()
        storage_ok = True
    except Exception:
        storage_ok = False
    bybit_latency_ms = None
    try:
        from bybit import get_last_api_latency_ms

        bybit_latency_ms = get_last_api_latency_ms()
    except Exception:
        bybit_latency_ms = None
    env_state = _ENV_BOOTSTRAP_STATE
    return {
        "ok": True,
        "uptime": int(uptime_s),
        "last_scan_ts": _format_ts_human(last_scan_ts),
        "last_scan_ts_epoch": int(last_scan_ts) if last_scan_ts is not None else None,
        "bybit_latency_ms": bybit_latency_ms,
        "storage_ok": storage_ok,
        "telegram_config_ok": bool(settings.telegram.bot_token and settings.telegram.chat_id),
        "dotenv_loaded": bool(env_state.get("dotenv_loaded", False)),
        "version": VERSION,
    }


def _index_html() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Scalper Bot Dashboard (DRY RUN)</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; background: #0f172a; color: #e2e8f0; }
    h1 { margin: 0 0 12px 0; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px; margin-bottom: 14px; }
    .card { background: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 10px; }
    .label { color: #93c5fd; font-size: 12px; text-transform: uppercase; }
    .value { font-size: 18px; margin-top: 6px; word-break: break-word; }
    table { width: 100%; border-collapse: collapse; background: #111827; border: 1px solid #374151; margin-bottom: 16px; }
    th, td { border-bottom: 1px solid #374151; padding: 8px; text-align: left; font-size: 13px; }
    th { color: #93c5fd; }
    .muted { color: #94a3b8; font-size: 12px; margin: 8px 0 12px 0; }
  </style>
</head>
<body>
  <h1>Scalper Bot Dashboard (DRY RUN)</h1>
  <div class="muted">Auto-refresh: every 5 minutes</div>

  <h2>Summary</h2>
  <div class="grid">
    <div class="card"><div class="label">last_scan_ts</div><div class="value" id="last_scan_ts">-</div></div>
    <div class="card"><div class="label">scan_seconds</div><div class="value" id="scan_seconds">-</div></div>
    <div class="card"><div class="label">watchlist_count</div><div class="value" id="watchlist_count">-</div></div>
    <div class="card"><div class="label">watchlist_mode</div><div class="value" id="watchlist_mode">-</div></div>
    <div class="card"><div class="label">watchlist_source</div><div class="value" id="watchlist_source">-</div></div>
    <div class="card"><div class="label">watchlist_updated_ts</div><div class="value" id="watchlist_updated_ts">-</div></div>
    <div class="card"><div class="label">watchlist_cached_until_ts</div><div class="value" id="watchlist_cached_until_ts">-</div></div>
    <div class="card"><div class="label">watchlist_candidates_count</div><div class="value" id="watchlist_candidates_count">-</div></div>
    <div class="card"><div class="label">watchlist_symbols</div><div class="value" id="watchlist_symbols">-</div></div>
    <div class="card"><div class="label">signals_last_24h</div><div class="value" id="signals_last_24h">-</div></div>
    <div class="card"><div class="label">pnl_today</div><div class="value" id="pnl_today">-</div></div>
    <div class="card"><div class="label">open_positions_count</div><div class="value" id="open_positions_count">-</div></div>
    <div class="card"><div class="label">wins_today</div><div class="value" id="wins_today">-</div></div>
    <div class="card"><div class="label">losses_today</div><div class="value" id="losses_today">-</div></div>
  </div>

  <h2>Open Positions</h2>
  <table>
    <thead>
      <tr>
        <th>symbol</th>
        <th>direction</th>
        <th>entry</th>
        <th>sl</th>
        <th>tp</th>
        <th>unrealized</th>
      </tr>
    </thead>
    <tbody id="open_positions_table"></tbody>
  </table>

  <h2>Near Misses (Strategy V1, Top 3)</h2>
  <table>
    <thead>
      <tr>
        <th>symbol</th>
        <th>setup_hint</th>
        <th>dist_atr</th>
        <th>bias</th>
      </tr>
    </thead>
    <tbody id="near_misses_table"></tbody>
  </table>

  <h2>4H Bias (Top 10)</h2>
  <table>
    <thead>
      <tr>
        <th>symbol</th>
        <th>bias</th>
        <th>slope10</th>
        <th>dist_pct</th>
        <th>reason</th>
      </tr>
    </thead>
    <tbody id="bias_table"></tbody>
  </table>

  <h2>Paper KPIs</h2>
  <div class="grid">
    <div class="card"><div class="label">expectancy_R</div><div class="value" id="kpi_expectancy_R">-</div></div>
    <div class="card"><div class="label">winrate</div><div class="value" id="kpi_winrate">-</div></div>
    <div class="card"><div class="label">profit_factor</div><div class="value" id="kpi_profit_factor">-</div></div>
    <div class="card"><div class="label">max_dd_usdt</div><div class="value" id="kpi_max_dd_usdt">-</div></div>
    <div class="card"><div class="label">trades_total</div><div class="value" id="kpi_trades_total">-</div></div>
    <div class="card"><div class="label">wins</div><div class="value" id="kpi_wins">-</div></div>
    <div class="card"><div class="label">losses</div><div class="value" id="kpi_losses">-</div></div>
    <div class="card"><div class="label">avg_win_R</div><div class="value" id="kpi_avg_win_R">-</div></div>
    <div class="card"><div class="label">avg_loss_R</div><div class="value" id="kpi_avg_loss_R">-</div></div>
  </div>
  <h3>KPIs by Setup</h3>
  <table>
    <thead>
      <tr>
        <th>setup</th>
        <th>expectancy_R</th>
        <th>avg_win_R</th>
        <th>avg_loss_R</th>
        <th>winrate</th>
        <th>profit_factor</th>
        <th>trades</th>
        <th>wins</th>
        <th>losses</th>
      </tr>
    </thead>
    <tbody id="kpi_by_setup_table"></tbody>
  </table>

  <h2>Block Reasons (Last 24h)</h2>
  <table>
    <thead>
      <tr>
        <th>block_reason</th>
        <th>count</th>
      </tr>
    </thead>
    <tbody id="block_stats_table"></tbody>
  </table>

  <h2>Risk</h2>
  <div class="grid">
    <div class="card"><div class="label">kill_switch</div><div class="value" id="kill_switch">-</div></div>
    <div class="card"><div class="label">max_trades_day</div><div class="value" id="max_trades_day">-</div></div>
    <div class="card"><div class="label">cooldown_after_loss_minutes</div><div class="value" id="cooldown_after_loss_minutes">-</div></div>
    <div class="card"><div class="label">open_position_simulated</div><div class="value" id="open_position_simulated">-</div></div>
    <div class="card"><div class="label">last_block_reason</div><div class="value" id="last_block_reason">-</div></div>
    <div class="card"><div class="label">position_mode</div><div class="value" id="position_mode">-</div></div>
    <div class="card"><div class="label">max_concurrent_positions</div><div class="value" id="max_concurrent_positions">-</div></div>
    <div class="card"><div class="label">open_positions_count</div><div class="value" id="risk_open_positions_count">-</div></div>
  </div>

  <h2>Recent Signals</h2>
  <table>
    <thead>
      <tr>
        <th>ts</th>
        <th>symbol</th>
        <th>setup</th>
        <th>direction</th>
        <th>score</th>
        <th>timeframe</th>
        <th>notes</th>
      </tr>
    </thead>
    <tbody id="signals_table"></tbody>
  </table>

  <h2>Recent Trade Intents</h2>
  <table>
    <thead>
      <tr>
        <th>ts</th>
        <th>symbol</th>
        <th>setup</th>
        <th>direction</th>
        <th>timeframe</th>
        <th>risk_verdict</th>
        <th>block_reason</th>
      </tr>
    </thead>
    <tbody id="intents_table"></tbody>
  </table>

  <h2>Recent Risk Events</h2>
  <table>
    <thead>
      <tr>
        <th>ts</th>
        <th>type</th>
        <th>status</th>
        <th>reason</th>
      </tr>
    </thead>
    <tbody id="risk_events_table"></tbody>
  </table>

  <script>
    function setText(id, value) {
      const el = document.getElementById(id);
      if (!el) return;
      el.textContent = String(value ?? '-');
    }

    function setSummary(data) {
      setText('last_scan_ts', data.last_scan_ts);
      setText('scan_seconds', data.scan_seconds);
      setText('watchlist_count', data.watchlist_count);
      setText('watchlist_mode', data.watchlist_mode ?? '-');
      setText('watchlist_source', data.watchlist_source ?? '-');
      setText('watchlist_updated_ts', data.watchlist_updated_ts ?? '-');
      setText('watchlist_cached_until_ts', data.watchlist_cached_until_ts ?? '-');
      setText('watchlist_candidates_count', data.watchlist_candidates_count != null ? data.watchlist_candidates_count : '-');
      setText('watchlist_symbols', Array.isArray(data.watchlist_symbols) ? data.watchlist_symbols.join(', ') : '-');
      setText('signals_last_24h', data.signals_last_24h);
      setText('pnl_today', data.pnl_today != null ? data.pnl_today : '-');
      setText('open_positions_count', data.open_positions_count != null ? data.open_positions_count : '-');
      setText('wins_today', data.wins_today != null ? data.wins_today : '-');
      setText('losses_today', data.losses_today != null ? data.losses_today : '-');
      const risk = data.risk || {};
      setText('kill_switch', risk.kill_switch);
      setText('max_trades_day', risk.max_trades_day);
      setText('cooldown_after_loss_minutes', risk.cooldown_after_loss_minutes);
      setText('open_position_simulated', risk.open_position_simulated);
      setText('last_block_reason', risk.last_block_reason);
      setText('position_mode', risk.position_mode);
      setText('max_concurrent_positions', risk.max_concurrent_positions);
      setText('risk_open_positions_count', risk.open_positions_count);
      renderBiasTable(Array.isArray(data.bias) ? data.bias : []);
      renderNearMissesTable(Array.isArray(data.near_misses) ? data.near_misses : []);
      renderBlockStatsTable(Array.isArray(data.block_stats) ? data.block_stats : []);
      renderOpenPositions(Array.isArray(data.open_positions) ? data.open_positions : []);
      renderKpi(data.kpi);
    }

    function renderKpi(kpiData) {
      if (!kpiData || typeof kpiData !== 'object') return;
      const kpi = kpiData.kpi || {};
      const fmt = (v) => (v != null && v !== '' ? String(v) : '-');
      const pct = (v) => (typeof v === 'number' ? (v * 100).toFixed(1) + '%' : fmt(v));
      setText('kpi_expectancy_R', fmt(kpi.expectancy_R));
      setText('kpi_winrate', pct(kpi.winrate));
      setText('kpi_profit_factor', fmt(kpi.profit_factor));
      setText('kpi_max_dd_usdt', fmt(kpi.max_dd_usdt));
      setText('kpi_trades_total', fmt(kpi.trades_total));
      setText('kpi_wins', fmt(kpi.wins));
      setText('kpi_losses', fmt(kpi.losses));
      setText('kpi_avg_win_R', fmt(kpi.avg_win_R));
      setText('kpi_avg_loss_R', fmt(kpi.avg_loss_R));
      const bySetup = kpiData.kpi_by_setup || {};
      const tbody = document.getElementById('kpi_by_setup_table');
      if (!tbody) return;
      tbody.innerHTML = '';
      for (const [setup, s] of Object.entries(bySetup)) {
        const tr = document.createElement('tr');
        const cols = [
          setup,
          s.expectancy_R != null ? s.expectancy_R : '-',
          s.avg_win_R != null ? s.avg_win_R : '-',
          s.avg_loss_R != null ? s.avg_loss_R : '-',
          typeof s.winrate === 'number' ? (s.winrate * 100).toFixed(1) + '%' : '-',
          s.profit_factor != null ? s.profit_factor : '-',
          s.trades_total != null ? s.trades_total : '-',
          s.wins != null ? s.wins : '-',
          s.losses != null ? s.losses : '-'
        ];
        for (const v of cols) {
          const td = document.createElement('td');
          td.textContent = String(v ?? '-');
          tr.appendChild(td);
        }
        tbody.appendChild(tr);
      }
    }

    function renderBlockStatsTable(items) {
      const tbody = document.getElementById('block_stats_table');
      if (!tbody) return;
      tbody.innerHTML = '';
      for (const item of items) {
        const tr = document.createElement('tr');
        const cols = [item.block_reason, item.count];
        for (const v of cols) {
          const td = document.createElement('td');
          td.textContent = String(v ?? '-');
          tr.appendChild(td);
        }
        tbody.appendChild(tr);
      }
    }

    function renderNearMissesTable(items) {
      const tbody = document.getElementById('near_misses_table');
      if (!tbody) return;
      tbody.innerHTML = '';
      for (const item of items) {
        const tr = document.createElement('tr');
        const cols = [item.symbol, item.setup_hint, item.dist_atr, item.bias];
        for (const v of cols) {
          const td = document.createElement('td');
          td.textContent = String(v ?? '-');
          tr.appendChild(td);
        }
        tbody.appendChild(tr);
      }
    }

    function renderBiasTable(items) {
      const tbody = document.getElementById('bias_table');
      if (!tbody) return;
      tbody.innerHTML = '';
      for (const item of items) {
        const tr = document.createElement('tr');
        const slope = item.slope10 != null ? Number(item.slope10).toFixed(6) : '-';
        const dist = item.dist_pct != null ? Number(item.dist_pct).toFixed(2) + '%' : '-';
        const cols = [item.symbol, item.bias, slope, dist, item.reason || '-'];
        for (const v of cols) {
          const td = document.createElement('td');
          td.textContent = String(v ?? '-');
          tr.appendChild(td);
        }
        tbody.appendChild(tr);
      }
    }

    function renderOpenPositions(items) {
      const tbody = document.getElementById('open_positions_table');
      if (!tbody) return;
      tbody.innerHTML = '';
      for (const item of items) {
        const tr = document.createElement('tr');
        const cols = [
          item.symbol,
          item.direction,
          item.entry,
          item.sl,
          item.tp,
          item.unrealized != null ? item.unrealized : '-'
        ];
        for (const v of cols) {
          const td = document.createElement('td');
          td.textContent = String(v ?? '-');
          tr.appendChild(td);
        }
        tbody.appendChild(tr);
      }
    }

    function renderSignals(items) {
      const tbody = document.getElementById('signals_table');
      if (!tbody) return;
      tbody.innerHTML = '';
      for (const item of items) {
        const tr = document.createElement('tr');
        const cols = [
          item.ts,
          item.symbol,
          item.setup,
          item.direction,
          item.score,
          item.timeframe,
          item.notes
        ];
        for (const v of cols) {
          const td = document.createElement('td');
          td.textContent = String(v ?? '-');
          tr.appendChild(td);
        }
        tbody.appendChild(tr);
      }
    }

    function renderIntents(items) {
      const tbody = document.getElementById('intents_table');
      if (!tbody) return;
      tbody.innerHTML = '';
      for (const item of items) {
        const tr = document.createElement('tr');
        const cols = [
          item.ts,
          item.symbol,
          item.setup,
          item.direction,
          item.timeframe,
          item.risk_verdict,
          item.block_reason
        ];
        for (const v of cols) {
          const td = document.createElement('td');
          td.textContent = String(v ?? '-');
          tr.appendChild(td);
        }
        tbody.appendChild(tr);
      }
    }

    function renderRiskEvents(items) {
      const tbody = document.getElementById('risk_events_table');
      if (!tbody) return;
      tbody.innerHTML = '';
      for (const item of items) {
        const tr = document.createElement('tr');
        const cols = [item.ts, item.type, item.status, item.reason];
        for (const v of cols) {
          const td = document.createElement('td');
          td.textContent = String(v ?? '-');
          tr.appendChild(td);
        }
        tbody.appendChild(tr);
      }
    }

    async function refresh() {
      try {
        const [summaryResp, signalsResp, intentsResp, riskEventsResp] = await Promise.all([
          fetch('/api/summary', { cache: 'no-store' }),
          fetch('/api/signals?limit=50', { cache: 'no-store' }),
          fetch('/api/intents?limit=50', { cache: 'no-store' }),
          fetch('/api/risk-events?limit=50', { cache: 'no-store' })
        ]);
        if (!summaryResp.ok || !signalsResp.ok || !intentsResp.ok || !riskEventsResp.ok) return;

        const summary = await summaryResp.json();
        const signals = await signalsResp.json();
        const intents = await intentsResp.json();
        const riskEvents = await riskEventsResp.json();

        setSummary(summary);
        renderSignals(Array.isArray(signals) ? signals : []);
        renderIntents(Array.isArray(intents) ? intents : []);
        renderRiskEvents(Array.isArray(riskEvents) ? riskEvents : []);
      } catch (e) {
        // Keep dashboard resilient if a transient fetch error occurs.
      }
    }

    refresh();
    setInterval(refresh, 300000); // 5 min
  </script>
</body>
</html>
"""


def _map_intent_row(intent: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "ts": str(intent.get("ts") or intent.get("timestamp_utc") or ""),
        "symbol": str(intent.get("symbol") or ""),
        "setup": str(intent.get("setup") or intent.get("strategy") or ""),
        "direction": str(intent.get("direction") or intent.get("side") or ""),
        "timeframe": str(intent.get("timeframe") or intent.get("interval") or ""),
        "risk_verdict": str(intent.get("risk_verdict") or intent.get("status") or ""),
        "block_reason": str(intent.get("block_reason") or ""),
    }


def _map_risk_event_row(event: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "ts": str(event.get("ts") or event.get("timestamp_utc") or ""),
        "type": str(event.get("type") or ""),
        "status": str(event.get("status") or ""),
        "reason": str(event.get("reason") or ""),
    }


def create_app() -> FastAPI:
    app = FastAPI(title="Scalper DRY-RUN Dashboard")

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        return HTMLResponse(content=_index_html())

    @app.get("/api/health")
    def api_health() -> Dict[str, Any]:
        return _health_payload()

    @app.get("/api/signals")
    def api_signals(limit: int = Query(default=50, ge=1, le=500)) -> List[Dict[str, Any]]:
        return get_recent_signals(limit=limit)

    @app.get("/api/intents")
    def api_intents(limit: int = Query(default=50, ge=1, le=500)) -> List[Dict[str, Any]]:
        intents = get_recent_trade_intents(limit=limit)
        return [_map_intent_row(i) for i in intents]

    @app.get("/api/risk-events")
    def api_risk_events(limit: int = Query(default=50, ge=1, le=500)) -> List[Dict[str, Any]]:
        events = get_recent_risk_events(limit=limit)
        return [_map_risk_event_row(e) for e in events]

    @app.get("/api/summary")
    def api_summary() -> Dict[str, Any]:
        summary = _summary_payload()
        summary["risk"] = _risk_payload()
        return summary

    return app


def run_dashboard_server(host: str, port: int, log_level: str = "info") -> None:
    import uvicorn

    app = create_app()
    uvicorn.run(app, host=host, port=int(port), log_level=str(log_level).lower())


if __name__ == "__main__":
    run_dashboard_server(
        host=settings.dashboard.host,
        port=int(settings.dashboard.port),
        log_level="info",
    )
