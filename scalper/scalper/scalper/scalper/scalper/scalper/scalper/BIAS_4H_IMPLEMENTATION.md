# STRICT 4H Bias Gate – Implementation Plan (DRY RUN)

## Overview

Add a hard 4H bias gate using existing MTF candles/indicators. Bias is computed from EMA200 slope and close vs EMA200 on 4H. Only LONG or SHORT intents matching the bias are allowed.

---

## 1. mtf.py – Compute EMA200 slope and bias

### 1a. Extend `_compute_snapshot_for_tf` to include `ema200_slope_10` for 4H

```diff
--- a/mtf.py
+++ b/mtf.py
@@ -98,6 +98,7 @@ def _compute_snapshot_for_tf(
     result: Dict[str, Any] = {
         "ema20": 0.0,
         "ema50": 0.0,
         "ema200": 0.0,
+        "ema200_slope_10": None,
         "atr14": 0.0,
         "close": 0.0,
         "high": 0.0,
@@ -131,6 +132,11 @@ def _compute_snapshot_for_tf(
     result["ema20"] = ema20_list[-1] if ema20_list else 0.0
     result["ema50"] = ema50_list[-1] if ema50_list else 0.0
     result["ema200"] = ema200_list[-1] if ema200_list else 0.0
+    if len(ema200_list) >= 11:
+        result["ema200_slope_10"] = ema200_list[-1] - ema200_list[-11]
+    else:
+        result["ema200_slope_10"] = None
     result["atr14"] = atr_list[-1] if atr_list and atr_list[-1] is not None else 0.0
 
     return result
```

### 1b. Add `compute_4h_bias` function

```diff
--- a/mtf.py
+++ b/mtf.py
@@ -162,6 +162,52 @@ def _tf_label(tf_min: int) -> str:
     return f"{tf_min}m"
 
 
+def compute_4h_bias(symbol: str, snapshot_4h: Dict[str, Any]) -> Dict[str, Any]:
+    """
+    Compute STRICT 4H bias from snapshot.
+    Returns: {symbol, bias, slope_10, dist_pct, close, ema200, ts, reason}
+    bias: "LONG" | "SHORT" | "NONE"
+    """
+    ts = str(snapshot_4h.get("ts", "") or "")
+    close = float(snapshot_4h.get("close", 0.0) or 0.0)
+    ema200 = float(snapshot_4h.get("ema200", 0.0) or 0.0)
+    slope_raw = snapshot_4h.get("ema200_slope_10")
+
+    if slope_raw is None:
+        return {
+            "symbol": symbol,
+            "bias": "NONE",
+            "slope_10": None,
+            "dist_pct": 0.0,
+            "close": close,
+            "ema200": ema200,
+            "ts": ts,
+            "reason": "INSUFFICIENT_4H_DATA",
+        }
+
+    slope_10 = float(slope_raw)
+    dist_pct = ((close - ema200) / max(ema200, 1e-10)) * 100.0 if ema200 else 0.0
+
+    if close > ema200 and slope_10 > 0:
+        bias = "LONG"
+        reason = ""
+    elif close < ema200 and slope_10 < 0:
+        bias = "SHORT"
+        reason = ""
+    else:
+        bias = "NONE"
+        reason = "NEUTRAL"
+
+    return {
+        "symbol": symbol,
+        "bias": bias,
+        "slope_10": slope_10,
+        "dist_pct": dist_pct,
+        "close": close,
+        "ema200": ema200,
+        "ts": ts,
+        "reason": reason,
+    }
+
+
 def log_mtf_ready(symbol: str, snapshot: Dict[int, Dict[str, Any]], logger: Optional[logging.Logger] = None) -> None:
```

---

## 2. storage.py – Persist bias summary

```diff
--- a/storage.py
+++ b/storage.py
@@ -310,6 +310,24 @@ def get_selected_watchlist() -> tuple:
         return [], "static"
+
+def set_last_bias_json(bias_list: List[Dict[str, Any]]) -> None:
+    """Persist per-scan bias map for dashboard. No secrets."""
+    sqlite_store.kv_set("last_bias_json", json.dumps(bias_list or [], ensure_ascii=True))
+
+
+def get_last_bias_json() -> List[Dict[str, Any]]:
+    """Return last bias list for /api/summary."""
+    raw = sqlite_store.kv_get("last_bias_json")
+    if not raw:
+        return []
+    try:
+        parsed = json.loads(raw)
+        return list(parsed) if isinstance(parsed, list) else []
+    except Exception:
+        return []
```

---

## 3. bot.py – Bias gate and persistence

### 3a. Add import and bias list collection

```diff
--- a/bot.py
+++ b/bot.py
@@ -544,7 +544,7 @@ def run_scan_cycle(
     from mtf import build_mtf_snapshot, log_mtf_ready
+    from mtf import build_mtf_snapshot, compute_4h_bias, log_mtf_ready
+    from storage import set_last_bias_json
@@ -548,6 +548,7 @@ def run_scan_cycle(
     snapshot_by_symbol: Dict[str, Dict[int, Dict[str, Any]]] = {}
+    bias_list: List[Dict[str, Any]] = []
+    tf_bias = int(getattr(__import__("config"), "TF_BIAS", 240))
```

### 3b. Compute bias per symbol and apply gate before `evaluate_symbol_intents`

Insert after `log_mtf_ready` and before the `if symbol in watchlist_set:` block. Replace the block that starts with `if symbol in watchlist_set:`:

```diff
--- a/bot.py
+++ b/bot.py
             if snap:
                 log_mtf_ready(symbol, snap, logger=logging.getLogger(__name__))
 
+            # STRICT 4H bias gate
+            snap_4h = (snap or {}).get(tf_bias, {}) if snap else {}
+            bias_info = compute_4h_bias(symbol, snap_4h)
+            bias_list.append(dict(bias_info))
+            logging.debug(
+                "BIAS4H symbol=%s bias=%s slope10=%s dist=%.2f%%",
+                symbol,
+                bias_info.get("bias", "NONE"),
+                bias_info.get("slope_10") if bias_info.get("slope_10") is not None else "n/a",
+                float(bias_info.get("dist_pct", 0.0)),
+            )
+
             # DRY RUN only: public market data endpoint.
             candles = fetch_klines(symbol=symbol, interval=interval, limit=lookback)
@@ -660,7 +678,25 @@ def run_scan_cycle(
                     save_paper_state(merged_state)
 
             if symbol in watchlist_set:
+                # Hard gate: skip signal generation if bias is NONE
+                if (bias_info.get("bias") or "NONE") == "NONE":
+                    symbol_context["bias"] = bias_info
+                    symbol_context["candidates_before"] = []
+                    symbol_context["final_intents"] = []
+                    symbol_context["early_intents"] = []
+                    symbol_context["debug_why_none"] = dict(
+                        symbol_context.get("debug_why_none", {}),
+                        **{"BIAS_4H": bias_info.get("reason", "NONE")},
+                    )
+                    run_context["symbols"].append(symbol_context)
+                    continue
+
                 active_profile = str(threshold_profile or "A").strip().upper()
                 evaluated = evaluate_symbol_intents(
```

### 3c. Filter intents by bias (LONG/SHORT)

Add `allowed_bias` and filter `candidates_before` after assigning from evaluated. Filter `detected` before the `for signal in detected:` loop:

```diff
--- a/bot.py
+++ b/bot.py
+                allowed_bias = bias_info.get("bias") or "NONE"
+                symbol_context["bias"] = bias_info
+                if allowed_bias in ("LONG", "SHORT"):
+                    symbol_context["candidates_before"] = [
+                        c for c in symbol_context.get("candidates_before", [])
+                        if str(c.get("side", c.get("direction", ""))).upper() == allowed_bias
+                    ]
                 ...
                 detected = list(evaluated.get("final_intents", []) or [])
+                if allowed_bias in ("LONG", "SHORT"):
+                    detected = [
+                        d for d in detected
+                        if str(d.get("side", d.get("direction", ""))).upper() == allowed_bias
+                    ]
                 for signal in detected:
```

### 3d. Persist bias list at end of scan cycle

Find the end of the `for symbol in symbols_to_process:` loop (before the final `return run_context` or similar). Add:

```diff
--- a/bot.py
+++ b/bot.py
         # ... end of for symbol in symbols_to_process
 
+    set_last_bias_json(bias_list)
     return run_context
```

---

## 4. dashboard_server.py – API and UI

### 4a. Add bias to summary payload

```diff
--- a/dashboard_server.py
+++ b/dashboard_server.py
-from storage import (
+from storage import (
     get_last_block_reason,
     get_last_scan_ts,
+    get_last_bias_json,
     get_recent_risk_events,
     ...
 )
@@ -103,6 +104,7 @@ def _summary_payload() -> Dict[str, Any]:
         "signals_last_24h": int(signals_last_24h),
         "pnl_today": float(state.get("daily_pnl_sim", 0.0) or 0.0),
         ...
         "open_positions": open_positions_rows,
+        "bias": (get_last_bias_json() or [])[:10],
     }
```

### 4b. Add bias table to HTML and JS

In `_index_html()`, after the Open Positions table and before Risk:

```diff
--- a/dashboard_server.py
+++ b/dashboard_server.py
   </table>
 
+  <h2>4H Bias (Top 10)</h2>
+  <table>
+    <thead>
+      <tr>
+        <th>symbol</th>
+        <th>bias</th>
+        <th>slope_10</th>
+        <th>dist_pct</th>
+      </tr>
+    </thead>
+    <tbody id="bias_table"></tbody>
+  </table>
+
   <h2>Risk</h2>
```

In the `setSummary` function, add:

```diff
--- a/dashboard_server.py
+++ b/dashboard_server.py
       setText('losses_today', data.losses_today != null ? data.losses_today : '-');
       const risk = data.risk || {};
+      renderBiasTable(Array.isArray(data.bias) ? data.bias : []);
       ...
```

Add new function before `renderOpenPositions`:

```diff
--- a/dashboard_server.py
+++ b/dashboard_server.py
+    function renderBiasTable(items) {
+      const tbody = document.getElementById('bias_table');
+      if (!tbody) return;
+      tbody.innerHTML = '';
+      for (const item of items) {
+        const tr = document.createElement('tr');
+        const slope = item.slope_10 != null ? Number(item.slope_10).toFixed(6) : '-';
+        const dist = item.dist_pct != null ? Number(item.dist_pct).toFixed(2) + '%' : '-';
+        const cols = [item.symbol, item.bias, slope, dist];
+        for (const v of cols) {
+          const td = document.createElement('td');
+          td.textContent = String(v ?? '-');
+          tr.appendChild(td);
+        }
+        tbody.appendChild(tr);
+      }
+    }
+
     function renderOpenPositions(items) {
```

Update the `setSummary` call to pass bias:

```diff
--- a/dashboard_server.py
+++ b/dashboard_server.py
-      renderOpenPositions(Array.isArray(data.open_positions) ? data.open_positions : []);
+      renderBiasTable(Array.isArray(data.bias) ? data.bias : []);
+      renderOpenPositions(Array.isArray(data.open_positions) ? data.open_positions : []);
```

---

## 5. Early intents bias filter

Early intents also need to respect bias. In the early_enabled block, filter `symbol_context["early_intents"]`:

```diff
--- a/bot.py
+++ b/bot.py
                         early_intents = evaluate_early_intents_from_5m(...)
                         symbol_context["early_intents"] = list(early_intents)
+                        if allowed_bias in ("LONG", "SHORT"):
+                            symbol_context["early_intents"] = [
+                                e for e in symbol_context["early_intents"]
+                                if str(e.get("side", e.get("direction", ""))).upper() == allowed_bias
+                            ]
```

---

## Test Commands

```powershell
# 1. Run a single scan with DEBUG logging to see BIAS4H lines
$env:LOG_LEVEL="DEBUG"; python -c "
import logging
logging.basicConfig(level=logging.DEBUG)
import config
from watchlist import get_watchlist
from bot import run_scan_cycle, resolve_watchlist
from risk_autopilot import RiskAutopilot
wl, mode = get_watchlist(config, None, None)
risk = RiskAutopilot(config)
run_scan_cycle(
    watchlist=wl[:3],
    watchlist_mode=mode,
    interval=str(config.INTERVAL),
    lookback=config.LOOKBACK,
    telegram_token='',
    telegram_chat_id='',
    risk_autopilot=risk,
    notify_blocked_telegram=False,
    always_notify_intents=False,
    signal_debug=False,
    early_enabled=False,
    early_tf='5',
    early_lookback_5m=60,
    early_min_conf=0.35,
    early_require_15m_context=True,
    early_max_alerts_per_symbol_per_15m=2,
    threshold_profile='A',
    telegram_format='compact',
    telegram_max_chars_compact=900,
    telegram_max_chars_verbose=2500,
)
"

# 2. Verify bias persisted
python -c "
from storage import get_last_bias_json
import json
print(json.dumps(get_last_bias_json(), indent=2))
"

# 3. Start dashboard and check /api/summary
python dashboard_server.py
# In another terminal: curl http://127.0.0.1:8000/api/summary | python -m json.tool
```

---

## Summary of Files Changed

| File | Changes |
|------|---------|
| `mtf.py` | Add `ema200_slope_10` to snapshot; add `compute_4h_bias()` |
| `storage.py` | Add `set_last_bias_json`, `get_last_bias_json` |
| `bot.py` | Compute bias per symbol; gate NONE (skip); filter LONG/SHORT; persist bias list; filter early intents |
| `dashboard_server.py` | Add `bias` to summary; add bias table in HTML + `renderBiasTable` |

---

## Edge Cases

- **INSUFFICIENT_4H_DATA**: When `len(ema200_list) < 11`, bias = NONE, reason = INSUFFICIENT_4H_DATA.
- **Symbol not in watchlist but in open_positions**: Bias is still computed and logged; gate only applies when `symbol in watchlist_set`.
- **MTF fetch failure**: `snap` is empty; `bias_info` stays `{}`; `bias_info.get("bias") == "NONE"` is False (because key missing). To be safe, treat missing bias as NONE: `if (bias_info.get("bias") or "NONE") == "NONE":`

Recommended guard:

```python
if (bias_info.get("bias") or "NONE") == "NONE":
    # skip signal generation
```
