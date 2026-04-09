"""
Demo 2 — generate synthetic retail store sales data.

Fully synthetic.  Designed to exercise the analyzer's strengths on a
classic store × week × category panel — strong time-series story,
clear regional comparisons, and groupings that make for good slides.

Outputs three CSVs into ./data/:
  store_week_sales.csv  — ~75k rows × 9 cols (the panel)
  store_master.csv      — 1500 rows × 7 cols
  category_master.csv   — 8 rows × 4 cols
"""
from __future__ import annotations
from pathlib import Path
from datetime import date, timedelta

import numpy as np
import pandas as pd

OUT_DIR = Path(__file__).parent / "data"
OUT_DIR.mkdir(exist_ok=True)

SEED      = 20260409
N_STORES  = 1500
N_WEEKS   = 50  # ~1 year of weeks

REGIONS         = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
STORE_FORMATS   = ["Express", "Standard", "Supercenter"]
CATEGORIES      = [
    ("CAT01", "Grocery",     1.00),
    ("CAT02", "Beverages",   0.85),
    ("CAT03", "Snacks",      0.95),
    ("CAT04", "Household",   0.70),
    ("CAT05", "Personal Care", 0.65),
    ("CAT06", "Apparel",     0.40),
    ("CAT07", "Electronics", 0.25),
    ("CAT08", "Seasonal",    0.55),
]


def main():
    rng = np.random.default_rng(SEED)

    # ── store master ──
    store_ids = [f"STORE_{i:04d}" for i in range(1, N_STORES + 1)]
    store_master = pd.DataFrame({
        "store_id":     store_ids,
        "region":       rng.choice(REGIONS, size=N_STORES, p=[0.22, 0.22, 0.22, 0.18, 0.16]),
        "store_format": rng.choice(STORE_FORMATS, size=N_STORES, p=[0.30, 0.55, 0.15]),
        "square_feet":  rng.integers(8_000, 200_000, size=N_STORES),
        "opened_year":  rng.integers(1995, 2024, size=N_STORES),
        "remodel_year": rng.choice(list(range(2018, 2026)) + [None]*5, size=N_STORES),
        "active_flag":  rng.choice([True, False], size=N_STORES, p=[0.93, 0.07]),
    })

    # ── category master ──
    category_master = pd.DataFrame([
        {"category_id": cid, "category_name": name,
         "basket_attach_rate": rate,
         "margin_class": ("high" if rate < 0.5 else ("mid" if rate < 0.85 else "low"))}
        for cid, name, rate in CATEGORIES
    ])

    # ── week list ──
    start_date = date(2025, 1, 6)  # a Monday
    weeks = [(start_date + timedelta(weeks=w)).isoformat() for w in range(N_WEEKS)]

    # ── store_week_sales panel ──
    rows = []
    for _, store in store_master.iterrows():
        # bigger formats see more revenue
        format_mult = {"Express": 0.4, "Standard": 1.0, "Supercenter": 2.5}[store["store_format"]]
        size_mult   = (store["square_feet"] / 60_000.0) ** 0.6

        # most stores are present every week, but some recently-opened ones
        # only appear partway through (panel-completeness story)
        if store["opened_year"] >= 2024:
            n_w = int(rng.integers(N_WEEKS // 2, N_WEEKS + 1))
            start = N_WEEKS - n_w
            store_weeks = weeks[start:]
        elif not store["active_flag"]:
            n_w = int(rng.integers(1, N_WEEKS // 2))
            start = int(rng.integers(0, N_WEEKS - n_w + 1))
            store_weeks = weeks[start : start + n_w]
        else:
            store_weeks = weeks

        for w in store_weeks:
            for cid, cname, attach in CATEGORIES:
                base_rev = 25_000 * format_mult * size_mult * attach
                # seasonality: bump in spring + late-year
                week_idx  = weeks.index(w)
                seasonal  = 1.0 + 0.20 * np.sin(2 * np.pi * week_idx / N_WEEKS)
                noise     = rng.normal(1.0, 0.12)
                revenue   = max(0.0, base_rev * seasonal * noise)
                units     = max(0, int(revenue / max(rng.normal(7.5, 2.0), 1.0)))
                customers = max(0, int(units / max(rng.normal(2.8, 0.8), 1.0)))
                rows.append({
                    "store_id":      store["store_id"],
                    "category_id":   cid,
                    "week_start":    w,
                    "revenue":       round(revenue, 2),
                    "units_sold":    units,
                    "customer_count": customers,
                    "avg_basket":    round(revenue / customers, 2) if customers else 0.0,
                    "promo_flag":    bool(rng.choice([True, False], p=[0.20, 0.80])),
                    "stockouts":     int(rng.poisson(0.5)),
                })

    store_week_sales = pd.DataFrame(rows)

    # ── inject some QC issues so the data_qc step has things to find ──
    # (the video shows the QC catching real issues; flip these off for a clean run)
    INJECT_QC_ISSUES = True
    if INJECT_QC_ISSUES:
        # 1. null primary keys (BLOCKER) — wipe a few store_ids on the master
        null_idxs = rng.choice(len(store_master), size=3, replace=False)
        store_master.loc[null_idxs, "store_id"] = None

        # 2. duplicate primary keys (WARNING) — clone two existing rows
        dup_rows = store_master.iloc[[10, 200]].copy()
        store_master = pd.concat([store_master, dup_rows], ignore_index=True)

        # 3. mixed-case categoricals (AI-assisted finding) — make some lowercase
        store_master.loc[rng.choice(len(store_master), size=20, replace=False), "store_format"] = "express"

        # 4. orphan FK on the sales side — sneak in a fake store_id that
        #    doesn't exist in the master
        orphan_rows = store_week_sales.iloc[:5].copy()
        orphan_rows["store_id"] = "STORE_GHOST"
        store_week_sales = pd.concat([store_week_sales, orphan_rows], ignore_index=True)

    # ── save ──
    store_master.to_csv(OUT_DIR / "store_master.csv",         index=False)
    category_master.to_csv(OUT_DIR / "category_master.csv",   index=False)
    store_week_sales.to_csv(OUT_DIR / "store_week_sales.csv", index=False)

    print(f"[ok] generated demo data in {OUT_DIR.resolve()}")
    print(f"     store_master.csv      {len(store_master):>7,} rows × {len(store_master.columns)} cols")
    print(f"     category_master.csv   {len(category_master):>7,} rows × {len(category_master.columns)} cols")
    print(f"     store_week_sales.csv  {len(store_week_sales):>7,} rows × {len(store_week_sales.columns)} cols")


if __name__ == "__main__":
    main()
