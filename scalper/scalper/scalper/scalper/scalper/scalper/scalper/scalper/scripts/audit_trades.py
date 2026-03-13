# scripts/audit_trades.py
import glob
import os
import sys

try:
    import pandas as pd
except ImportError:
    print("Missing dependency: pandas. Install with: python -m pip install pandas")
    sys.exit(1)

def newest(pattern: str) -> str:
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"No files match: {pattern}")
    return max(files, key=os.path.getmtime)

def main():
    path = newest("data/replay_trades_*.csv")
    df = pd.read_csv(path)

    print("CSV:", path)
    print("Rows:", len(df))
    print("\nexit_reason counts:")
    if "exit_reason" in df.columns:
        print(df["exit_reason"].value_counts(dropna=False).head(20).to_string())
    else:
        print("  (missing column exit_reason)")

    # Risk sanity
    if "risk_usdt" in df.columns:
        risk0 = int((df["risk_usdt"] == 0).sum())
        riskneg = int((df["risk_usdt"] < 0).sum())
        risknan = int(df["risk_usdt"].isna().sum())
        print("\nRisk sanity:")
        print("risk_usdt==0:", risk0)
        print("risk_usdt<0 :", riskneg)
        print("risk_usdt NaN:", risknan)
    else:
        print("\nRisk sanity: (missing column risk_usdt)")

    # R multiple sanity
    if "r_multiple" in df.columns:
        rnan = int(df["r_multiple"].isna().sum())
        r0 = int((df["r_multiple"] == 0).sum())
        print("\nR multiple sanity:")
        print("r_multiple NaN:", rnan)
        print("r_multiple==0:", r0)
        print(df["r_multiple"].describe().to_string())
    else:
        print("\nR multiple sanity: (missing column r_multiple)")

    # PnL sign
    if "pnl_usdt" in df.columns:
        wins = int((df["pnl_usdt"] > 0).sum())
        loss = int((df["pnl_usdt"] < 0).sum())
        flat = int((df["pnl_usdt"] == 0).sum())
        print("\nPnL sign:", {"wins": wins, "losses": loss, "flat": flat})
    else:
        print("\nPnL sign: (missing column pnl_usdt)")

    # Sample: pnl>0 but r_multiple==0
    if set(["pnl_usdt", "r_multiple"]).issubset(df.columns):
        sample = df[(df["pnl_usdt"] > 0) & (df["r_multiple"] == 0)].head(10)
        print("\nSample pnl_usdt>0 but r_multiple==0 (first 10):")
        if len(sample) == 0:
            print("  (none)")
        else:
            print(sample.to_string(index=False))
    print("\nDone.")

if __name__ == "__main__":
    main()
