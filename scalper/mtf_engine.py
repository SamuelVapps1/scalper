"""
Shared MTF snapshot engine.

Builds O(1) snapshots at trigger timestamps using precomputed index mapping.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List


def build_ts_index_map(trigger_ts: List[int], target_ts: List[int]) -> List[int]:
    """Map each trigger ts to latest target index with target_ts[idx] <= trigger_ts[i]."""
    if not trigger_ts:
        return []
    if not target_ts:
        return [-1] * len(trigger_ts)
    out = [-1] * len(trigger_ts)
    j = 0
    for i, t in enumerate(trigger_ts):
        while j + 1 < len(target_ts) and target_ts[j + 1] <= t:
            j += 1
        out[i] = j if target_ts[j] <= t else -1
    return out


def _snapshot_from_frame_at_idx(frame: Dict[str, List[Any]], idx: int) -> Dict[str, Any]:
    ema20_v = frame["ema20"][idx]
    ema50_v = frame["ema50"][idx]
    ema200_v = frame["ema200"][idx]
    ema200_prev10_v = frame["ema200_prev10"][idx]
    atr14_v = frame["atr14"][idx]
    ts_ms = frame["ts"][idx]
    return {
        "ema20": float(ema20_v) if ema20_v is not None else 0.0,
        "ema50": float(ema50_v) if ema50_v is not None else 0.0,
        "ema200": float(ema200_v) if ema200_v is not None else 0.0,
        "ema200_prev10": float(ema200_prev10_v) if ema200_prev10_v is not None else None,
        "ema200_slope_10": (
            float(ema200_v) - float(ema200_prev10_v)
            if ema200_v is not None and ema200_prev10_v is not None
            else None
        ),
        "atr14": float(atr14_v) if atr14_v is not None else 0.0,
        "open": float(frame["open"][idx]),
        "close": float(frame["close"][idx]),
        "high": float(frame["high"][idx]),
        "low": float(frame["low"][idx]),
        "ts": datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat(),
    }


def build_snapshot_at_trigger(
    frames_by_tf: Dict[int, Dict[str, List[Any]]],
    trigger_to_tf_index: Dict[int, List[int]],
    trigger_idx: int,
    tfs: List[int],
) -> Dict[int, Dict[str, Any]]:
    """O(1) snapshot lookup per TF using precomputed trigger->TF index maps."""
    snapshot: Dict[int, Dict[str, Any]] = {}
    for tf_min in tfs:
        frame = frames_by_tf.get(tf_min)
        idx_map = trigger_to_tf_index.get(tf_min)
        if not frame or not idx_map or trigger_idx >= len(idx_map):
            continue
        idx = idx_map[trigger_idx]
        if idx < 0 or idx >= len(frame["ts"]):
            continue
        snapshot[tf_min] = _snapshot_from_frame_at_idx(frame, idx)
    return snapshot


def build_snapshot_at_index(frame: Dict[str, List[Any]], idx: int) -> Dict[str, Any]:
    """Build snapshot for a direct index in a precomputed frame."""
    if not frame or idx < 0 or idx >= len(frame.get("ts", [])):
        return {}
    return _snapshot_from_frame_at_idx(frame, idx)

