"""
Local pandas-based AdHocOnTap analyzer.

A slimmed-down port of the main `ad_hoc_analyzer.py` Databricks notebook
that runs anywhere with pandas + an LLM API key.  No Spark, no Databricks,
no /dbfs paths.

For production at scale on Databricks Delta tables, use the main notebook.
For local demos, Colab, or laptop runs, use this module.

Public API:
    profile_table(df, label)        -> dict
    analyze_completeness(df, ...)   -> dict
    infer_columns(profiles, llm)    -> dict
    generate_code_cells(...)        -> list[dict]
    insights_summary(...)           -> list[dict]
    analyze(tables, prompt, llm)    -> dict   <-- main entry point
"""
from __future__ import annotations
import json
import re
from typing import Any, Callable, Optional

import numpy as np
import pandas as pd


# ─────────────────────────────────────────────────────────────────
# Entity / time / grouping detection (mirrors the Databricks notebook)
# ─────────────────────────────────────────────────────────────────

_ENTITY_PATTERNS = [
    (r'(?i)\bnpi\b', 8),
    (r'(?i)provider.?id', 6), (r'(?i)customer.?id', 6),
    (r'(?i)account.?id', 6),  (r'(?i)store.?id', 6),
    (r'(?i)member.?id', 6),   (r'(?i)patient.?id', 6),
    (r'(?i)hcp.?id', 6),      (r'(?i)entity.?id', 5),
    (r'(?i)user.?id', 5),     (r'(?i)product.?id', 5),
    (r'(?i)^id$', 4),         (r'(?i)_id$', 3),
    (r'(?i)_key$', 3),        (r'(?i)_uid$', 3),
]

def detect_entities(columns_info, row_count):
    """Score each column as a potential entity/key identifier."""
    candidates = []
    for c in columns_info:
        score = 0
        for pat, pts in _ENTITY_PATTERNS:
            if re.search(pat, c["name"]):
                score += pts
                break
        if row_count and c["distinct"] > row_count * 0.01:
            score += 2
        if row_count and c["distinct"] > row_count * 0.3:
            score += 2
        if c["null_pct"] < 1:
            score += 1
        if c["dtype_kind"] in ("string", "integer"):
            score += 1
        if score >= 4:
            candidates.append({"col": c["name"], "score": score})
    candidates.sort(key=lambda x: -x["score"])
    return candidates


_TIME_NAME_PATTERNS = [
    r'(?i)date', r'(?i)month', r'(?i)quarter', r'(?i)qtr',
    r'(?i)year', r'(?i)period', r'(?i)week', r'(?i)_dt$',
    r'(?i)timestamp',
]

def detect_time_columns(df, columns_info):
    """Find time-dimension columns, including disguised string formats."""
    results = []
    for c in columns_info:
        score = 0
        for pat in _TIME_NAME_PATTERNS:
            if re.search(pat, c["name"]):
                score += 3
                break
        if c["dtype_kind"] == "datetime":
            score += 5
        sample_vals = c.get("top_values", [])[:5]
        sv_str = [str(v) for v, _ in sample_vals] if sample_vals else []
        if any(re.match(r'^\d{6}$', v) for v in sv_str):
            score += 5  # YYYYMM
        elif any(re.match(r'^\d{4}-\d{2}', v) for v in sv_str):
            score += 5  # YYYY-MM-DD or YYYY-MM
        elif any(re.match(r'^\d{4}Q\d$', v) for v in sv_str):
            score += 5  # YYYYQN
        elif any(re.match(r'^\d{4}$', v) for v in sv_str):
            score += 4
        if c["dtype_kind"] == "integer" and c.get("min") is not None:
            mn = c.get("min")
            if isinstance(mn, (int, float)):
                if 1990 <= mn <= 2030 and c["distinct"] < 30:
                    score += 4   # probably year
                elif 190000 <= mn <= 203012:
                    score += 5   # YYYYMM int
        if score >= 4:
            results.append({"col": c["name"], "score": score,
                            "dtype_kind": c["dtype_kind"]})
    results.sort(key=lambda x: -x["score"])
    return results


def detect_granularity(df, time_col):
    """daily / weekly / monthly / quarterly / yearly / unknown."""
    series = df[time_col].dropna().drop_duplicates().sort_values()
    if len(series) < 2:
        return "single_value"

    if pd.api.types.is_datetime64_any_dtype(series):
        gaps = series.diff().dropna().dt.days
        if gaps.empty:
            return "unknown"
        med = float(gaps.median())
        if med <= 1:                    return "daily"
        if 6 <= med <= 8:               return "weekly"
        if 27 <= med <= 32:             return "monthly"
        if 85 <= med <= 95:             return "quarterly"
        if 360 <= med <= 370:           return "yearly"
        return f"irregular(median_gap={int(med)}d)"

    str_vals = series.astype(str).tolist()
    sample = str_vals[:50]
    if all(re.match(r'^\d{6}$', v) for v in sample):
        return "monthly(YYYYMM)"
    if all(re.match(r'^\d{4}Q\d$', v) for v in sample):
        return "quarterly(YYYYQN)"
    if all(re.match(r'^\d{4}$', v) for v in sample):
        return "yearly(YYYY)"
    if all(re.match(r'^\d{4}-\d{2}-\d{2}', v) for v in sample):
        try:
            dates = pd.to_datetime(sample)
            gaps = pd.Series(dates).diff().dt.days.dropna()
            med = float(gaps.median())
            if med <= 1:           return "daily"
            if 6 <= med <= 8:      return "weekly"
            if 27 <= med <= 32:    return "monthly"
            if 85 <= med <= 95:    return "quarterly"
            if 360 <= med <= 370:  return "yearly"
            return f"irregular(median_gap={int(med)}d)"
        except Exception:
            pass
    return "unknown"


def detect_groupings(columns_info, row_count):
    """Low-cardinality categoricals (2-50 distinct, <30% null)."""
    results = []
    for c in columns_info:
        if c["dtype_kind"] not in ("string", "boolean"):
            continue
        if c["distinct"] < 2 or c["distinct"] > 50:
            continue
        if c["null_pct"] > 30:
            continue
        results.append({"col": c["name"], "distinct": c["distinct"],
                        "top_values": c.get("top_values", [])[:5]})
    results.sort(key=lambda x: x["distinct"])
    return results


# ─────────────────────────────────────────────────────────────────
# Panel completeness
# ─────────────────────────────────────────────────────────────────

def analyze_completeness(df: pd.DataFrame, entity_col: str, time_col: str) -> dict:
    """Compute the entity x time panel structure."""
    pair = df[[entity_col, time_col]].dropna().drop_duplicates()
    n_entities = int(pair[entity_col].nunique())
    n_periods  = int(pair[time_col].nunique())
    actual     = int(len(pair))
    expected   = n_entities * n_periods
    fill       = round(actual / expected * 100, 2) if expected else 0.0

    ent_counts = pair.groupby(entity_col)[time_col].nunique()
    fully_complete = int((ent_counts == n_periods).sum())

    per_period = (
        pair.groupby(time_col)[entity_col].nunique()
            .reset_index()
            .rename(columns={entity_col: "entity_count"})
            .sort_values(time_col)
            .to_dict(orient="records")
    )

    period_list = sorted(pair[time_col].unique().tolist(), key=str)

    return {
        "entity_col": entity_col,
        "time_col":   time_col,
        "n_entities": n_entities,
        "n_periods":  n_periods,
        "period_range": [str(period_list[0]), str(period_list[-1])] if period_list else [],
        "periods":    [str(p) for p in period_list],
        "expected_cells": expected,
        "actual_cells":   actual,
        "fill_rate_pct":  fill,
        "entities_fully_complete":     fully_complete,
        "entities_fully_complete_pct": round(fully_complete / n_entities * 100, 1) if n_entities else 0,
        "entity_period_stats": {
            "min":    int(ent_counts.min()),
            "max":    int(ent_counts.max()),
            "mean":   round(float(ent_counts.mean()), 1),
            "median": int(ent_counts.median()),
        },
        "per_period_entity_counts": per_period,
    }


def time_summary_stats(df: pd.DataFrame, time_col: str,
                       metric_cols: list, entity_col: Optional[str] = None) -> list:
    """Per-period sum/mean/min/max/std for each metric column."""
    metric_cols = [m for m in metric_cols if m in df.columns]
    if not metric_cols:
        return []
    aggs = {m: ['sum', 'mean', 'min', 'max', 'std'] for m in metric_cols}
    grouped = df.groupby(time_col).agg(aggs).reset_index()
    grouped.columns = [
        '_'.join(c).strip('_') if isinstance(c, tuple) else c
        for c in grouped.columns
    ]
    if entity_col and entity_col in df.columns:
        ent = (df.groupby(time_col)[entity_col].nunique()
                 .reset_index().rename(columns={entity_col: "entity_count"}))
        grouped = grouped.merge(ent, on=time_col, how="left")
    return grouped.sort_values(time_col).to_dict(orient="records")


# ─────────────────────────────────────────────────────────────────
# Profile a single table
# ─────────────────────────────────────────────────────────────────

def profile_table(df: pd.DataFrame, label: str,
                  source: str = "DataFrame",
                  max_cols: int = 60, top_k: int = 10) -> dict:
    """Profile one pandas DataFrame: schema, stats, structural detection."""
    if len(df.columns) > max_cols:
        print(f"    !  {len(df.columns)} cols — profiling first {max_cols}")
        df = df[df.columns[:max_cols]]

    row_count = len(df)
    if row_count == 0:
        return {"label": label, "source": source, "row_count": 0,
                "col_count": len(df.columns), "columns": [],
                "sample_rows": [], "sample_display": "",
                "entities": [], "time_cols": [], "groupings": [],
                "completeness": None, "grain": None, "time_stats": None}

    columns_info = []
    for col in df.columns:
        s = df[col]
        null_count = int(s.isnull().sum())
        info = {
            "name":       col,
            "dtype":      str(s.dtype),
            "null_count": null_count,
            "null_pct":   round(null_count / row_count * 100, 2),
            "distinct":   int(s.nunique(dropna=True)),
        }

        if pd.api.types.is_bool_dtype(s):
            info["dtype_kind"] = "boolean"
        elif pd.api.types.is_integer_dtype(s):
            info["dtype_kind"] = "integer"
            non_null = s.dropna()
            if len(non_null):
                info["min"]    = float(non_null.min())
                info["max"]    = float(non_null.max())
                info["mean"]   = round(float(non_null.mean()), 4)
                info["stddev"] = round(float(non_null.std()), 4) if len(non_null) > 1 else 0
                pcts = non_null.quantile([0.01, 0.25, 0.5, 0.75, 0.99]).tolist()
                info.update({"p01": pcts[0], "p25": pcts[1], "p50": pcts[2],
                             "p75": pcts[3], "p99": pcts[4]})
        elif pd.api.types.is_numeric_dtype(s):
            info["dtype_kind"] = "numeric"
            non_null = s.dropna()
            if len(non_null):
                info["min"]    = float(non_null.min())
                info["max"]    = float(non_null.max())
                info["mean"]   = round(float(non_null.mean()), 4)
                info["stddev"] = round(float(non_null.std()), 4) if len(non_null) > 1 else 0
                pcts = non_null.quantile([0.01, 0.25, 0.5, 0.75, 0.99]).tolist()
                info.update({"p01": pcts[0], "p25": pcts[1], "p50": pcts[2],
                             "p75": pcts[3], "p99": pcts[4]})
        elif pd.api.types.is_datetime64_any_dtype(s):
            info["dtype_kind"] = "datetime"
            non_null = s.dropna()
            if len(non_null):
                info["min"] = str(non_null.min())
                info["max"] = str(non_null.max())
        else:
            info["dtype_kind"] = "string"
            non_null = s.dropna().astype(str)
            if len(non_null):
                lengths = non_null.str.len()
                info["min_length"] = int(lengths.min())
                info["max_length"] = int(lengths.max())
                info["avg_length"] = round(float(lengths.mean()), 1)
                top = non_null.value_counts().head(top_k)
                info["top_values"] = [(k, int(v)) for k, v in top.items()]

        # also surface top values for very low-cardinality numerics (e.g. year)
        if info["dtype_kind"] in ("integer", "numeric") and info["distinct"] < 50 and info["distinct"] > 0:
            top = s.dropna().value_counts().head(top_k)
            info["top_values"] = [
                (float(k) if isinstance(k, (int, float, np.number)) else k, int(v))
                for k, v in top.items()
            ]

        columns_info.append(info)

    sample_pd = df.head(20).reset_index(drop=True)
    sample_rows = sample_pd.to_dict(orient="records")
    sample_display = sample_pd.head(10).to_string(index=False, max_cols=12)

    # ── structural detection ──
    entities  = detect_entities(columns_info, row_count)
    time_cols = detect_time_columns(df, columns_info)
    groupings = detect_groupings(columns_info, row_count)

    grain = None
    completeness = None
    time_stats = None
    if time_cols:
        tc = time_cols[0]["col"]
        # remove the detected time column from groupings (it's not a category)
        groupings = [g for g in groupings if g["col"] != tc]
        grain = detect_granularity(df, tc)
        if entities:
            completeness = analyze_completeness(df, entities[0]["col"], tc)
            # On dimension/master tables an "entity" sits in exactly one
            # "period" (e.g. opened_year), so completeness is meaningless.
            # Suppress it to keep downstream consumers honest.
            eps = (completeness or {}).get("entity_period_stats") or {}
            if (eps.get("mean") or 0) < 1.5:
                completeness = None
        numeric_names = [c["name"] for c in columns_info
                         if c["dtype_kind"] in ("numeric", "integer")
                         and c["name"] != tc]
        if numeric_names:
            time_stats = time_summary_stats(
                df, tc, numeric_names[:8],
                entity_col=entities[0]["col"] if entities else None)

    return {
        "label":       label,
        "source":      source,
        "row_count":   row_count,
        "col_count":   len(df.columns),
        "columns":     columns_info,
        "sample_rows": sample_rows,
        "sample_display": sample_display,
        "entities":    entities,
        "time_cols":   time_cols,
        "grain":       grain,
        "groupings":   groupings,
        "completeness": completeness,
        "time_stats":  time_stats,
    }


def format_profile_for_llm(p: dict) -> str:
    """Compact text summary for LLM prompts."""
    lines = [
        f"TABLE: {p['label']}  ({p['row_count']:,} rows x {p['col_count']} cols)",
        f"SOURCE: {p['source']}",
    ]
    if p.get("entities"):
        lines.append(f"ENTITY KEY(S): {', '.join(e['col'] for e in p['entities'][:3])}")
    if p.get("time_cols"):
        lines.append(f"TIME COLUMN: {p['time_cols'][0]['col']}  GRAIN: {p.get('grain','?')}")
    if p.get("groupings"):
        lines.append(f"GROUPING COLS: {', '.join(g['col'] for g in p['groupings'][:5])}")
    if p.get("completeness"):
        c = p["completeness"]
        lines.append(f"PANEL: {c['n_entities']:,} entities x {c['n_periods']} periods, "
                     f"fill={c['fill_rate_pct']}%, "
                     f"{c['entities_fully_complete_pct']}% fully complete")

    lines.append("\nCOLUMNS:")
    for c in p["columns"]:
        parts = [f"  {c['name']:<28s} {c['dtype']:<14s}"]
        parts.append(f"nulls={c['null_count']:,}({c['null_pct']}%)")
        parts.append(f"distinct={c['distinct']:,}")
        if c["dtype_kind"] in ("numeric", "integer") and "min" in c:
            parts.append(f"min={c.get('min')} max={c.get('max')} mean={c.get('mean')}")
        elif c["dtype_kind"] == "string":
            tv = c.get("top_values", [])[:3]
            if tv:
                parts.append("top=[" + ", ".join(f'"{v}"({n:,})' for v, n in tv) + "]")
        lines.append("  ".join(parts))

    lines.append(f"\nSAMPLE (10 rows):\n{p['sample_display'][:1200]}")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────
# LLM-driven steps
# ─────────────────────────────────────────────────────────────────

def _parse_json(text: str, fallback):
    text = text.strip()
    text = re.sub(r"^```[\w]*\n?", "", text)
    text = re.sub(r"\n?```$", "", text)
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        for pat in [r'\{.*\}', r'\[.*\]']:
            m = re.search(pat, text, re.DOTALL)
            if m:
                try:
                    return json.loads(m.group())
                except json.JSONDecodeError:
                    continue
    return fallback


def infer_columns(profiles: dict, llm_call: Callable) -> dict:
    """Use the LLM to infer what each column means.  Returns dict keyed by 'label.col'."""
    col_list = []
    for p in profiles.values():
        for c in p["columns"]:
            sample = ""
            if c.get("top_values"):
                sample = str([v for v, _ in c["top_values"][:5]])
            elif c.get("min") is not None:
                sample = f"min={c.get('min')} max={c.get('max')} mean={c.get('mean')}"
            col_list.append(f"  {p['label']}.{c['name']}  type={c['dtype']}  "
                            f"distinct={c['distinct']}  sample={sample}")

    prompt = f"""Given these table columns with sample values, infer what each one
represents.  Be specific about business meaning.

COLUMNS:
{chr(10).join(col_list)}

For EACH column output:
- table: which table
- col: column name
- meaning: what it represents (e.g. "Total Sales Revenue")
- display_label: clean label for chart axes (e.g. "Revenue")
- role: one of entity_id | time | metric | grouping | attribute | unknown

Output ONLY a JSON array of objects."""

    resp = llm_call("You are a senior data analyst. Output ONLY valid JSON.", prompt)
    parsed = _parse_json(resp, [])
    if isinstance(parsed, dict):
        # some LLMs wrap the array in {"columns": [...]}
        for v in parsed.values():
            if isinstance(v, list):
                parsed = v
                break
        else:
            parsed = []

    result = {}
    for entry in parsed or []:
        if not isinstance(entry, dict):
            continue
        key = f"{entry.get('table','')}.{entry.get('col','')}"
        result[key] = entry
    return result


def generate_code_cells(user_prompt: str, profiles: dict, col_map: dict,
                        llm_call: Callable) -> list:
    """LLM plans + writes pandas+matplotlib code cells for the user's request."""
    profiles_text = "\n\n".join(format_profile_for_llm(p) for p in profiles.values())
    col_map_text  = json.dumps(list(col_map.values()), indent=1, default=str)[:3000]

    plan_prompt = f"""The user wants analyses of their data.  Plan specific visualizations.

USER REQUEST:
{user_prompt}

DATA PROFILES:
{profiles_text}

COLUMN MEANINGS:
{col_map_text}

Each table is available as a pandas DataFrame named after its label
(e.g. `activity`, `accounts`, `targets`).  Generate self-contained
pandas + matplotlib code for each visualization.  Each cell must:
  - import pandas, matplotlib.pyplot, numpy as needed
  - reference the dataframes by their label name
  - end with plt.show()
  - have professional styling (titles, axis labels, legends if multi-series)

1. Break the user request into 3-5 specific visualizations.
2. Add 1-2 SUGGESTIONS the user didn't ask for that are valuable.

Output JSON ONLY:
{{"cells": [
  {{
    "title": "...",
    "purpose": "one-line description",
    "chart_type": "line|bar|histogram|heatmap|scatter|box",
    "code": "import pandas as pd\\nimport matplotlib.pyplot as plt\\n... full code ...\\nplt.show()"
  }}
]}}"""

    resp = llm_call("You are a senior data analyst. Output ONLY valid JSON.", plan_prompt, max_tokens=6000)
    plan = _parse_json(resp, {"cells": []})
    if isinstance(plan, list):
        plan = {"cells": plan}
    cells = plan.get("cells", []) if isinstance(plan, dict) else []
    return [c for c in cells if isinstance(c, dict)]


def insights_summary(user_prompt: str, profiles: dict, col_map: dict,
                     llm_call: Callable) -> list:
    """Distill 3-5 'what to investigate next' bullets for the deck."""
    profiles_text = "\n\n".join(format_profile_for_llm(p) for p in profiles.values())[:6000]

    prompt = f"""Given this data and the user's request, suggest 3-5 high-value
next-step investigations a senior analyst would prioritize.  Be specific and
data-driven (mention column names, expected effect sizes).

USER REQUEST:
{user_prompt}

DATA:
{profiles_text}

For each suggestion, give:
- lead: short bold phrase (4-8 words)
- detail: one-sentence explanation that names columns / metrics

Output JSON ONLY: {{"suggestions": [{{"lead": "...", "detail": "..."}}]}}"""

    resp = llm_call("You are a senior data analyst. Output ONLY valid JSON.", prompt)
    parsed = _parse_json(resp, {"suggestions": []})
    if isinstance(parsed, list):
        parsed = {"suggestions": parsed}
    suggestions = parsed.get("suggestions", []) if isinstance(parsed, dict) else []
    return [s for s in suggestions if isinstance(s, dict)]


# ─────────────────────────────────────────────────────────────────
# Main pipeline
# ─────────────────────────────────────────────────────────────────

def analyze(tables: dict, user_prompt: str = "",
            llm_call: Optional[Callable] = None) -> dict:
    """Run the full local pipeline.

    tables      : {label: pandas_df}
    user_prompt : natural-language description of what to analyze
    llm_call    : (system, user, max_tokens=None) -> str   (use llm_client.get_call_llm_fn)

    Returns dict with: profiles, col_map, code_cells, insights, user_prompt
    """
    print("[Phase 1] Profiling tables...")
    profiles = {}
    for label, df in tables.items():
        print(f"  -> {label} ...", end=" ", flush=True)
        p = profile_table(df, label)
        profiles[label] = p
        print(f"{p['row_count']:,} rows x {p['col_count']} cols")
        if p.get("entities"):
            print(f"     entity: {p['entities'][0]['col']}")
        if p.get("time_cols"):
            print(f"     time:   {p['time_cols'][0]['col']}  grain={p.get('grain','?')}")
        if p.get("completeness"):
            c = p["completeness"]
            print(f"     panel:  {c['n_entities']:,} x {c['n_periods']}  fill={c['fill_rate_pct']}%")
        if p.get("groupings"):
            print(f"     groupings: {', '.join(g['col'] for g in p['groupings'][:4])}")

    col_map = {}
    code_cells = []
    insights = []

    if llm_call:
        print("\n[Phase 2] Inferring column meanings via LLM...")
        col_map = infer_columns(profiles, llm_call)
        print(f"  -> {len(col_map)} columns mapped")
        for k, v in list(col_map.items())[:5]:
            print(f"     {k:<30s} -> {(v.get('meaning') or '?')[:55]}")

        if user_prompt.strip():
            print("\n[Phase 3] Generating reproducible code cells...")
            code_cells = generate_code_cells(user_prompt, profiles, col_map, llm_call)
            print(f"  -> {len(code_cells)} code cells generated")
            for i, c in enumerate(code_cells):
                print(f"     {i+1}. {c.get('title','(untitled)')}")

            print("\n[Phase 4] Distilling deck-ready insights...")
            insights = insights_summary(user_prompt, profiles, col_map, llm_call)
            print(f"  -> {len(insights)} insight suggestions")
    else:
        print("\n!  No LLM client provided -- skipping inference, code-gen, and insights")

    return {
        "profiles":    profiles,
        "col_map":     col_map,
        "code_cells":  code_cells,
        "insights":    insights,
        "user_prompt": user_prompt,
    }
