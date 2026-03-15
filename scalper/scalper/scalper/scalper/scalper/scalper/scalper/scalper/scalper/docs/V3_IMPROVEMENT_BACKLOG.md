# V3 Trend-Continuation Breakout — Improvement Backlog

Prioritized list for A/B testing via replay. Each item: Hypothesis, Change, Param/Toggle, Expected metric movement, Failure mode. Implementable in ≤1 day.

---

## 1. USE_5M_CONFIRM (toggle 5m close confirmation)

| Field | Value |
|-------|-------|
| **Hypothesis** | Requiring 5m close beyond breakout filters false breakouts; disabling may capture more true breakouts that retrace before 5m closes. |
| **Change** | Set `USE_5M_CONFIRM=0` to skip 5m confirmation. |
| **Param** | `USE_5M_CONFIRM` (bool, default 1) |
| **Expected** | PF ↑, trades/day ↑; winrate may ↓ |
| **Failure** | More false breakouts → winrate ↓, PF ↓ |

---

## 2. DONCHIAN_N_15M (lookback length)

| Field | Value |
|-------|-------|
| **Hypothesis** | Shorter Donchian (e.g. 14) catches faster breakouts; longer (e.g. 30) reduces noise. |
| **Change** | Override `DONCHIAN_N_15M` (14, 20, 24, 30). |
| **Param** | `DONCHIAN_N_15M` (int, default 20) |
| **Expected** | 14: trades ↑, avgR may ↓; 30: trades ↓, avgR ↑ |
| **Failure** | 14: too many whipsaws; 30: miss valid breakouts |

---

## 3. BODY_ATR_15M (minimum breakout body size)

| Field | Value |
|-------|-------|
| **Hypothesis** | Higher body/ATR ratio filters weak breakouts; lower allows more entries. |
| **Change** | Override `BODY_ATR_15M` (0.15, 0.25, 0.35). |
| **Param** | `BODY_ATR_15M` (float, default 0.25) |
| **Expected** | 0.35: trades ↓, winrate ↑; 0.15: trades ↑, winrate ↓ |
| **Failure** | 0.35: miss good setups; 0.15: more false breakouts |

---

## 4. TREND_SEP_ATR_1H (1H trend strength)

| Field | Value |
|-------|-------|
| **Hypothesis** | Looser trend separation (e.g. 0.5) allows more trades in weaker trends; tighter (1.0) filters to strong trends only. |
| **Change** | Override `TREND_SEP_ATR_1H` (0.5, 0.8, 1.0). |
| **Param** | `TREND_SEP_ATR_1H` (float, default 0.8) |
| **Expected** | 0.5: trades ↑, PF may ↓; 1.0: trades ↓, PF ↑ |
| **Failure** | 0.5: weak-trend whipsaws; 1.0: too few signals |

---

## 5. ATR_REGIME_MIN_PCTL (volatility filter)

| Field | Value |
|-------|-------|
| **Hypothesis** | Trading only when 15m ATR is above Nth percentile of recent ATR reduces low-volatility chop. |
| **Change** | Add param `ATR_REGIME_MIN_PCTL` (0–100). When >0, compute ATR percentile over last 100 bars; block if below threshold. |
| **Param** | `ATR_REGIME_MIN_PCTL` (int, default 0 = off) |
| **Expected** | 50: trades ↓, winrate ↑, maxDD ↓ |
| **Failure** | Miss valid breakouts in quiet regimes |

---

## 6. BREAKOUT_BUFFER_ATR (buffer above/below Donchian)

| Field | Value |
|-------|-------|
| **Hypothesis** | Requiring close beyond Donchian + buffer reduces marginal breakouts. |
| **Change** | Add `BREAKOUT_BUFFER_ATR` (float, default 0). When >0, require close > donch_high + buffer*ATR (LONG) or < donch_low - buffer*ATR (SHORT). |
| **Param** | `BREAKOUT_BUFFER_ATR` (float, default 0) |
| **Expected** | 0.05–0.15: trades ↓, winrate ↑ |
| **Failure** | Miss breakouts that barely clear level |

---

## 7. SLOPE_MIN_4H (minimum 4H EMA200 slope)

| Field | Value |
|-------|-------|
| **Hypothesis** | Requiring stronger 4H slope filters sideways/weak trends. |
| **Change** | Add `SLOPE_MIN_4H` (float, default 0). When >0, require abs(ema200_slope_10) >= slope_min (e.g. 0.001). |
| **Param** | `SLOPE_MIN_4H` (float, default 0) |
| **Expected** | 0.001: trades ↓, PF ↑ |
| **Failure** | Over-filter in slow trends |

---

## 8. BODY_MAX_ATR (reject exhaustion bars)

| Field | Value |
|-------|-------|
| **Hypothesis** | Very large bodies (e.g. >1.5*ATR) may indicate exhaustion; skip these breakouts. |
| **Change** | Add `BODY_MAX_ATR` (float, default 0 = off). When >0, block if body > body_max * atr15m. |
| **Param** | `BODY_MAX_ATR` (float, default 0) |
| **Expected** | 1.5: trades ↓, winrate ↑ |
| **Failure** | Reject strong momentum breakouts |

---

## 9. DONCHIAN_LOOKBACK_OFFSET (exclude last bar from range)

| Field | Value |
|-------|-------|
| **Hypothesis** | Excluding the current bar from Donchian range avoids using the breakout bar in the level. |
| **Change** | Add `DONCHIAN_EXCLUDE_CURRENT` (bool, default 0). When 1, use highs[i15-donchian_n:i15] (exclude i15). |
| **Param** | `DONCHIAN_EXCLUDE_CURRENT` (bool, default 0) |
| **Expected** | Slightly different breakout levels; PF may ↑ |
| **Failure** | Level too loose, more false triggers |

---

## 10. 5M_CONFIRM_BAR_OFFSET (use prior 5m bar)

| Field | Value |
|-------|-------|
| **Hypothesis** | Using the 5m bar that ends before 15m close may reduce look-ahead bias. |
| **Change** | Add `5M_CONFIRM_USE_PRIOR` (bool, default 0). When 1, use j5-1 if j5 aligns with 15m close. |
| **Param** | `5M_CONFIRM_USE_PRIOR` (bool, default 0) |
| **Expected** | Fewer triggers, possibly cleaner |
| **Failure** | Miss valid 5m confirmations |

---

## 11. TREND_SEP_ASYMMETRIC (different LONG vs SHORT threshold)

| Field | Value |
|-------|-------|
| **Hypothesis** | LONG and SHORT may need different trend-strength thresholds. |
| **Change** | Add `TREND_SEP_LONG` and `TREND_SEP_SHORT` (float). When set, override TREND_SEP_ATR_1H per side. |
| **Param** | `TREND_SEP_LONG`, `TREND_SEP_SHORT` (float, default use TREND_SEP_ATR_1H) |
| **Expected** | Better tuning per direction |
| **Failure** | Over-optimization, poor OOS |

---

## 12. CLOSE_BEYOND_ATR (minimum close distance from level)

| Field | Value |
|-------|-------|
| **Hypothesis** | Requiring close beyond level by at least X*ATR filters marginal breaks. |
| **Change** | Add `CLOSE_BEYOND_ATR` (float, default 0). When >0, require (close - level) >= X*atr for LONG. |
| **Param** | `CLOSE_BEYOND_ATR` (float, default 0) |
| **Expected** | trades ↓, winrate ↑ |
| **Failure** | Miss breakouts that barely clear |

---

## 13. BIAS_SLOPE_ALIGN (4H slope must match bias sign)

| Field | Value |
|-------|-------|
| **Hypothesis** | Already enforced (close>ema200 + slope>0 → LONG). Tighten by requiring slope magnitude. |
| **Change** | Add `BIAS_SLOPE_MIN` (float, default 0). When >0, require abs(slope10) >= value. |
| **Param** | `BIAS_SLOPE_MIN` (float, default 0) |
| **Expected** | trades ↓, PF ↑ |
| **Failure** | Over-filter in slow trends |

---

## 14. DONCHIAN_MIN_RANGE_ATR (minimum range size)

| Field | Value |
|-------|-------|
| **Hypothesis** | Very tight Donchian ranges may be noise; require minimum range in ATR terms. |
| **Change** | Add `DONCHIAN_MIN_RANGE_ATR` (float, default 0). When >0, block if (donch_high-donch_low)/atr < threshold. |
| **Param** | `DONCHIAN_MIN_RANGE_ATR` (float, default 0) |
| **Expected** | trades ↓, winrate ↑ |
| **Failure** | Miss breakouts from tight ranges |

---

## 15. 5M_CLOSE_TOLERANCE_ATR (allow 5m close within tolerance)

| Field | Value |
|-------|-------|
| **Hypothesis** | Strict 5m > level may be too tight; allow close within X*ATR of level. |
| **Change** | Add `5M_CLOSE_TOLERANCE_ATR` (float, default 0). When >0, confirm if close within tolerance of level (LONG: close >= level - tol*atr). |
| **Param** | `5M_CLOSE_TOLERANCE_ATR` (float, default 0) |
| **Expected** | trades ↑, winrate may ↓ |
| **Failure** | Accept weak 5m confirmations |
