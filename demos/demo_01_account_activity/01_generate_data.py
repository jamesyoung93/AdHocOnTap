"""
Demo 1 — generate synthetic 'account activity' data.

Fully synthetic.  No PHI, PII, NPIs, drug names, diagnoses, or any
real-world identifiers.  Designed to exercise the analyzer's strengths:
entity detection, time-grain detection, panel completeness, low-cardinality
groupings, and time-series stats.

Outputs three CSVs into ./data/:
  account_activity.csv  — ~50k rows × 6 cols (the panel table)
  account_master.csv    — ~5k rows × 6 cols  (entity attributes)
  region_targets.csv    — 15 rows × 4 cols   (small reference table)
"""
from __future__ import annotations
from pathlib import Path

import numpy as np
import pandas as pd

OUT_DIR = Path(__file__).parent / "data"
OUT_DIR.mkdir(exist_ok=True)

SEED        = 20260409
N_ACCOUNTS  = 5000
QUARTERS    = ["2024Q1", "2024Q2", "2024Q3", "2024Q4",
               "2025Q1", "2025Q2", "2025Q3", "2025Q4",
               "2026Q1", "2026Q2"]
REGIONS     = ["R1", "R2", "R3", "R4", "R5"]
CATEGORIES  = ["Category A", "Category B", "Category C"]
TIERS       = ["Bronze", "Silver", "Gold", "Platinum"]


def main():
    rng = np.random.default_rng(SEED)

    # ── account_master ──
    account_ids = [f"ACCOUNT_{i:05d}" for i in range(1, N_ACCOUNTS + 1)]
    account_master = pd.DataFrame({
        "account_id":     account_ids,
        "region_code":    rng.choice(REGIONS,    size=N_ACCOUNTS, p=[0.30, 0.25, 0.20, 0.15, 0.10]),
        "category":       rng.choice(CATEGORIES, size=N_ACCOUNTS, p=[0.50, 0.30, 0.20]),
        "tier":           rng.choice(TIERS,      size=N_ACCOUNTS, p=[0.40, 0.35, 0.18, 0.07]),
        "onboarded_year": rng.integers(2018, 2025, size=N_ACCOUNTS),
        "active_flag":    rng.choice([True, False], size=N_ACCOUNTS, p=[0.85, 0.15]),
    })

    # ── region_targets (5 regions × 3 categories = 15 rows) ──
    rows = []
    for r in REGIONS:
        for cat in CATEGORIES:
            rows.append({
                "region_code":                     r,
                "category":                        cat,
                "target_engagements_per_quarter":  int(rng.integers(40, 120)),
                "min_response_rate_pct":           round(float(rng.uniform(20, 45)), 1),
            })
    region_targets = pd.DataFrame(rows)

    # ── account_activity panel (intentionally incomplete) ──
    activity_rows = []
    for _, acc in account_master.iterrows():
        # how many quarters this account appears in (tier-driven)
        if   acc["tier"] == "Platinum": n_q = int(rng.integers(8, len(QUARTERS) + 1))
        elif acc["tier"] == "Gold":     n_q = int(rng.integers(6, len(QUARTERS) + 1))
        elif acc["tier"] == "Silver":   n_q = int(rng.integers(4, len(QUARTERS) - 1))
        else:                            n_q = int(rng.integers(2, len(QUARTERS) - 2))

        if not acc["active_flag"]:
            n_q = max(1, n_q // 2)

        # contiguous quarters from a random start
        start = int(rng.integers(0, max(1, len(QUARTERS) - n_q + 1)))
        active_quarters = QUARTERS[start : start + n_q]

        base = {"Bronze": 25, "Silver": 50, "Gold": 90, "Platinum": 140}[acc["tier"]]
        for q in active_quarters:
            engagements = max(0, int(rng.normal(base, base * 0.30)))
            outreach    = max(0, int(engagements + rng.normal(0, 5)))
            response    = round(float(np.clip(rng.normal(35, 10), 5, 95)), 1)
            satisfaction = round(float(np.clip(rng.normal(7.5, 1.2), 1, 10)), 1)
            activity_rows.append({
                "account_id":         acc["account_id"],
                "quarter":            q,
                "engagements":        engagements,
                "outreach_completed": outreach,
                "response_rate_pct":  response,
                "satisfaction_score": satisfaction,
            })

    account_activity = pd.DataFrame(activity_rows)

    # ── save ──
    account_master.to_csv(OUT_DIR / "account_master.csv",     index=False)
    region_targets.to_csv(OUT_DIR / "region_targets.csv",     index=False)
    account_activity.to_csv(OUT_DIR / "account_activity.csv", index=False)

    print(f"[ok] generated demo data in {OUT_DIR.resolve()}")
    print(f"     account_master.csv     {len(account_master):>7,} rows × {len(account_master.columns)} cols")
    print(f"     region_targets.csv     {len(region_targets):>7,} rows × {len(region_targets.columns)} cols")
    print(f"     account_activity.csv   {len(account_activity):>7,} rows × {len(account_activity.columns)} cols")


if __name__ == "__main__":
    main()
