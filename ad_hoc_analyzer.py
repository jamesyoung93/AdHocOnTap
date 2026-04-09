# Databricks notebook source
# ============================================================
#  Ad Hoc Analyzer v2 — Smart Profiling & Code Generation
# ============================================================
#  Point it at tables + PDFs.  It detects entity keys (NPI etc.),
#  time grain, panel completeness, and what columns mean.  You
#  describe what you want in plain English; it hands back
#  deterministic, reproducible Python cells you run to produce
#  slide-ready figures.
#
#  QUICK START:
#    1. Edit Cell 1 (tables, PDFs, prompt)
#    2. Run All
#    3. Copy generated code cells into new cells and run them
#
#  The generated code uses spark.sql() + matplotlib — nothing
#  outside standard Databricks. Every query has ORDER BY so
#  results are deterministic.
# ============================================================


# COMMAND ----------
# CELL 1: USER CONFIG
# ============================================================
# ✏️  THIS IS THE ONLY CELL YOU EDIT.
# ============================================================

# %pip install PyPDF2
# dbutils.library.restartPython()

PROJECT_NAME = "Prescriber Panel Analysis"

# ============================================================
# ✏️  TABLE INPUTS — Unity Catalog paths or DataFrames.
#     Keys become the SQL view names you reference in prompts.
# ============================================================
TABLE_INPUTS = {
    "rx_claims":   "catalog.schema.rx_claims_2024",
    "targets":     "catalog.schema.hcp_targets_q4",
    # "custom":    my_dataframe,
}

# ============================================================
# ✏️  PDF INPUTS — reference docs.  Used to:
#       • infer what your columns mean (TRx, NRx, spec codes)
#       • extract benchmark numbers for sanity checks
#       • understand what chart types / ranges are expected
# ============================================================
import PyPDF2

PDF_INPUTS = {
    # "iqvia":    PyPDF2.PdfReader("/dbfs/FileStore/pdf_sources/IQVIA_2024.pdf"),
    # "zs":      PyPDF2.PdfReader("/dbfs/FileStore/pdf_sources/ZS_Biopharma_2025.pdf"),
}

# ============================================================
# ✏️  USER PROMPT — describe what you want in plain English.
#     Be specific: mention columns, groupings, comparisons.
#     The system delivers what you ask for PLUS suggestions.
# ============================================================
USER_PROMPT = """
Show me monthly TRx trends for the top 20 prescribers, broken down by product.
Also show a completeness heatmap of the panel over time, and compare
my total TRx to the industry benchmarks from the PDF.
"""

# ============================================================
# ✏️  SANITY CHECKS — explicit comparisons (optional).
#     Leave empty to rely on auto-detection from PDFs.
# ============================================================
SANITY_CHECKS = [
    # {"label": "Total TRx 2024", "sql": "SELECT SUM(trx) FROM rx_claims",
    #  "expected_range": [800_000, 1_200_000]},
]

# ============================================================
# ✏️  SETTINGS
# ============================================================
LLM_ENDPOINT_NAME = "databricks-qwen3-next-80b-a3b-instruct"
MAX_OUTPUT_TOKENS  = 4096
TEMPERATURE        = 0.2
MAX_PROFILE_COLS   = 60
TOP_K_VALUES       = 10

# ── Profiling speed knobs ──────────────────────────────────
# For tables larger than SAMPLE_FOR_STATS rows, percentiles and
# top-value queries run against a cached random sample instead
# of the full table.  Batch stats (null/distinct/min/max/mean/std)
# still run over the full table in a single pass.
SAMPLE_FOR_STATS     = 500_000
PROFILE_PARALLELISM  = 8      # driver threads for per-column top-value queries

# ── Context cache ──────────────────────────────────────────
# Profiles + column inferences are cached per (table_address, schema)
# so repeat runs against the same table skip the expensive profiling
# and LLM column-inference steps entirely.  Delete the dir or set
# FORCE_REBUILD_CONTEXT=True to invalidate.
CONTEXT_CACHE_DIR       = "/dbfs/FileStore/ad_hoc_analyzer_cache"
FORCE_REBUILD_CONTEXT   = False


# COMMAND ----------
# CELL 2: Imports
# COMMAND ----------

import json, os, re, time, hashlib, html as html_mod, textwrap
from datetime import datetime
from typing import List, Dict, Any, Optional
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import functions as F, Window
from pyspark.sql.types import (
    StringType, IntegerType, LongType, ShortType, ByteType,
    FloatType, DoubleType, DecimalType,
    DateType, TimestampType, BooleanType,
    ArrayType, MapType, StructType,
)
import warnings
warnings.filterwarnings("ignore")

_NUM = (IntegerType, LongType, ShortType, ByteType, FloatType, DoubleType, DecimalType)
_TIME = (DateType, TimestampType)


# COMMAND ----------
# CELL 3: LLM Client
# COMMAND ----------

from openai import OpenAI
from databricks.sdk import WorkspaceClient

LLM_CLIENT = WorkspaceClient().serving_endpoints.get_open_ai_client()

def call_llm(system_prompt: str, user_prompt: str, max_tokens=None) -> str:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore",
            message="PydanticSerializationUnexpectedValue")
        resp = LLM_CLIENT.chat.completions.create(
            model=LLM_ENDPOINT_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt},
            ],
            max_tokens=max_tokens or MAX_OUTPUT_TOKENS,
            temperature=TEMPERATURE,
        )
    return resp.choices[0].message.content

def strip_fences(text):
    text = text.strip()
    text = re.sub(r"^```[\w]*\n?", "", text)
    text = re.sub(r"\n?```$", "", text)
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL)
    return text.strip()

def parse_json(response, fallback=None):
    cleaned = strip_fences(response)
    result = None
    try:
        result = json.loads(cleaned)
    except json.JSONDecodeError:
        for pat in [r'\{.*\}', r'\[.*\]']:
            m = re.search(pat, cleaned, re.DOTALL)
            if m:
                try: result = json.loads(m.group()); break
                except json.JSONDecodeError: continue
    if result is None:
        if fallback is not None: return fallback
        raise json.JSONDecodeError("No JSON found", cleaned, 0)
    # if caller expects a dict but LLM returned a list, wrap it
    if isinstance(fallback, dict) and isinstance(result, list):
        first_key = next(iter(fallback), "data")
        result = {first_key: result}
    return result


# COMMAND ----------
# CELL 4: PDF + text utilities
# COMMAND ----------

def extract_pdf_text(reader, label=""):
    pages = []
    for i, page in enumerate(reader.pages):
        text = page.extract_text() or ""
        pages.append(f"--- PAGE {i+1} ---\n{text}")
    return "\n\n".join(pages)

def chunk_text(text, size=3000):
    paras = text.split("\n\n")
    chunks, cur = [], ""
    for p in paras:
        if len(cur) + len(p) > size and cur:
            chunks.append(cur.strip()); cur = p
        else:
            cur += "\n\n" + p
    if cur.strip(): chunks.append(cur.strip())
    return chunks

def relevant_chunks(chunks, keywords, top_k=5):
    kw = [k.lower() for k in keywords]
    scored = sorted(
        [(sum(1 for k in kw if k in c.lower()), c) for c in chunks],
        key=lambda x: -x[0])
    sel = [c for s, c in scored[:top_k] if s > 0]
    return "\n---\n".join(sel) if sel else chunks[0] if chunks else ""

def pdf_summary(chunks, n=400):
    return "\n".join(
        f"[{i+1}] {c[:n].replace(chr(10),' ').strip()}..."
        for i, c in enumerate(chunks[:50]))


# COMMAND ----------
# CELL 4b: Context cache
# ============================================================
#  Persists profiles + column inferences per (table_address, schema)
#  so repeat runs reuse the expensive profiling and LLM column
#  inference work.  Cache invalidates automatically if the table's
#  schema changes; force a rebuild via FORCE_REBUILD_CONTEXT or
#  clear_context_cache().
# ============================================================

def _context_cache_key(source, schema):
    h = hashlib.sha256()
    h.update(str(source).encode("utf-8"))
    h.update(schema.json().encode("utf-8"))
    return h.hexdigest()[:16]

def _cache_path(cache_dir, key):
    return os.path.join(cache_dir, f"{key}.json")

def load_context(cache_dir, source, schema):
    """Return cached {profile, col_map_entries} dict, or None."""
    if not cache_dir:
        return None
    key = _context_cache_key(source, schema)
    path = _cache_path(cache_dir, key)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"    ⚠️  cache read failed ({e}) — will rebuild")
        return None

def save_context(cache_dir, source, schema, profile, col_map_entries):
    """Write a cache entry for one table."""
    if not cache_dir:
        return
    try:
        os.makedirs(cache_dir, exist_ok=True)
        key = _context_cache_key(source, schema)
        payload = {
            "version": 1,
            "cached_at": datetime.utcnow().isoformat(),
            "source": str(source),
            "schema_hash": key,
            "profile": profile,
            "col_map_entries": col_map_entries,
        }
        with open(_cache_path(cache_dir, key), "w") as f:
            json.dump(payload, f, default=str)
    except Exception as e:
        print(f"    ⚠️  cache write failed: {e}")

def clear_context_cache(cache_dir=None):
    """Wipe all cached profiles. Call after a table is rewritten in place."""
    d = cache_dir or CONTEXT_CACHE_DIR
    if not os.path.exists(d):
        print(f"  (no cache at {d})"); return
    removed = 0
    for fn in os.listdir(d):
        if fn.endswith(".json"):
            os.remove(os.path.join(d, fn)); removed += 1
    print(f"  cleared {removed} cache entries from {d}")

def list_context_cache(cache_dir=None):
    """List cached tables and when they were profiled."""
    d = cache_dir or CONTEXT_CACHE_DIR
    if not os.path.exists(d):
        print(f"  (no cache at {d})"); return []
    entries = []
    for fn in sorted(os.listdir(d)):
        if not fn.endswith(".json"):
            continue
        try:
            with open(os.path.join(d, fn)) as f:
                p = json.load(f)
            entries.append({
                "source":    p.get("source"),
                "cached_at": p.get("cached_at"),
                "cols":      (p.get("profile") or {}).get("col_count"),
                "rows":      (p.get("profile") or {}).get("row_count"),
            })
        except Exception:
            pass
    for e in entries:
        print(f"  {e['cached_at']}  {e['source']}  "
              f"({e['rows']:,} rows × {e['cols']} cols)")
    return entries


# COMMAND ----------
# CELL 5: Smart profiler
# ============================================================
#  Detects: entity keys (NPI, IDs), time columns & grain,
#  data grain (entity × period), panel completeness,
#  secondary groupings, and time-aware summary stats.
# ============================================================

def _rnd(v, n=4):
    return round(v, n) if v is not None else None

# ── Entity / key column detection ──

_ENTITY_PATTERNS = [
    (r'(?i)\bnpi\b', 8),
    (r'(?i)provider.?id', 6), (r'(?i)prescriber.?id', 6),
    (r'(?i)patient.?id', 6),  (r'(?i)member.?id', 6),
    (r'(?i)hcp.?id', 6),      (r'(?i)hco.?id', 6),
    (r'(?i)customer.?id', 5),  (r'(?i)account.?id', 5),
    (r'(?i)physician', 5),     (r'(?i)claim.?id', 4),
    (r'(?i)^id$', 4),          (r'(?i)_id$', 3),
    (r'(?i)_key$', 3),
]

def detect_entities(columns, row_count):
    """Score each column as a potential entity/key identifier."""
    candidates = []
    for c in columns:
        score = 0
        for pat, pts in _ENTITY_PATTERNS:
            if re.search(pat, c["name"]):
                score += pts; break
        # high cardinality relative to rows → likely an entity
        if row_count and c["distinct"] > row_count * 0.01:
            score += 2
        if row_count and c["distinct"] > row_count * 0.3:
            score += 2
        # low null rate
        if c["null_pct"] < 1:
            score += 1
        # reasonable type
        if c["type"] in ("StringType()", "LongType()", "IntegerType()"):
            score += 1
        if score >= 4:
            candidates.append({"col": c["name"], "score": score})
    candidates.sort(key=lambda x: -x["score"])
    return candidates


# ── Time column detection & granularity ──

_TIME_NAME_PATTERNS = [
    r'(?i)date', r'(?i)month', r'(?i)quarter', r'(?i)qtr',
    r'(?i)year', r'(?i)period', r'(?i)week', r'(?i)_dt$',
    r'(?i)_tm$', r'(?i)timestamp',
]

def detect_time_columns(df, columns):
    """Find time-dimension columns, including disguised ones (YYYYMM strings, etc.)."""
    results = []
    for c in columns:
        score = 0
        for pat in _TIME_NAME_PATTERNS:
            if re.search(pat, c["name"]):
                score += 3; break
        if c["category"] == "temporal":
            score += 5
        # string column that looks like YYYYMM or YYYY-MM
        if c["category"] == "string" and c.get("top_values"):
            sample_vals = [str(v) for v, _ in c["top_values"][:5]]
            if any(re.match(r'^\d{6}$', v) for v in sample_vals):
                score += 5  # YYYYMM
            elif any(re.match(r'^\d{4}-\d{2}', v) for v in sample_vals):
                score += 5  # YYYY-MM-DD or YYYY-MM
            elif any(re.match(r'^\d{4}$', v) for v in sample_vals):
                score += 4  # YYYY
        # integer column in year range
        if c["category"] == "numeric" and c.get("min") is not None:
            if 1990 <= (c.get("min") or 0) <= 2030 and c["distinct"] < 30:
                score += 4  # probably year
            elif 190000 <= (c.get("min") or 0) <= 203012:
                score += 5  # YYYYMM as integer
        if score >= 4:
            results.append({"col": c["name"], "score": score,
                            "type": c["type"], "category": c["category"]})
    results.sort(key=lambda x: -x["score"])
    return results


def detect_granularity(df, time_col, col_type_category):
    """Determine if time column is daily, weekly, monthly, quarterly, yearly."""
    try:
        sample = (df.select(time_col).distinct()
                    .orderBy(time_col).limit(200)
                    .toPandas())
        vals = sorted(sample[time_col].dropna().tolist())
        if len(vals) < 2:
            return "single_value"

        # for native date/timestamp — compute day gaps
        if col_type_category == "temporal":
            import pandas as pd
            vals = pd.to_datetime(vals)
            gaps = [(vals[i+1] - vals[i]).days for i in range(len(vals)-1)]
            med = sorted(gaps)[len(gaps)//2]
            if med <= 1:   return "daily"
            if 6 <= med <= 8:   return "weekly"
            if 27 <= med <= 32: return "monthly"
            if 85 <= med <= 95: return "quarterly"
            if 360 <= med <= 370: return "yearly"
            return f"irregular(median_gap={med}d)"

        # for string/int YYYYMM, YYYY, etc.
        str_vals = [str(v) for v in vals]
        if all(re.match(r'^\d{6}$', v) for v in str_vals):
            return "monthly(YYYYMM)"
        if all(re.match(r'^\d{4}$', v) for v in str_vals):
            return "yearly(YYYY)"
        if all(re.match(r'^\d{4}-\d{2}-\d{2}', v) for v in str_vals):
            import pandas as pd
            dates = pd.to_datetime(str_vals)
            gaps = [(dates[i+1] - dates[i]).days for i in range(len(dates)-1)]
            med = sorted(gaps)[len(gaps)//2]
            if med <= 1: return "daily"
            if 27 <= med <= 32: return "monthly"
            if 85 <= med <= 95: return "quarterly"
            return f"irregular(median_gap={med}d)"
        return "unknown"
    except Exception:
        return "unknown"


# ── Grouping column detection ──

def detect_groupings(columns, row_count):
    """Find low-cardinality categorical columns suitable for GROUP BY / color."""
    results = []
    for c in columns:
        if c["category"] not in ("string", "boolean"):
            continue
        if c["distinct"] < 2 or c["distinct"] > 50:
            continue
        if c["null_pct"] > 30:
            continue
        results.append({"col": c["name"], "distinct": c["distinct"],
                        "top_values": c.get("top_values", [])[:5]})
    results.sort(key=lambda x: x["distinct"])
    return results


# ── Panel completeness ──

def analyze_completeness(spark_session, df, entity_col, time_col, label):
    """How complete is the entity × time panel?

    Speed: project down to distinct (entity, time) pairs once and cache.
    All subsequent counts/groupBy run against this much smaller table.
    """
    pair = (df.select(entity_col, time_col)
              .where(F.col(entity_col).isNotNull() & F.col(time_col).isNotNull())
              .distinct()
              .cache())
    try:
        actual = pair.count()  # materializes the cache

        period_rows = pair.select(time_col).distinct().orderBy(time_col).collect()
        period_list = [str(r[0]) for r in period_rows]
        n_periods   = len(period_list)

        n_entities  = pair.select(entity_col).distinct().count()

        expected = n_entities * n_periods
        fill     = round(actual / expected * 100, 2) if expected else 0

        # per-entity period stats + fully-complete count in ONE pass
        ent_stats = (pair.groupBy(entity_col)
                         .agg(F.count("*").alias("periods_present"))
                         .agg(
                             F.min("periods_present").alias("min"),
                             F.max("periods_present").alias("max"),
                             F.mean("periods_present").alias("mean"),
                             F.expr("percentile_approx(periods_present, 0.5)").alias("median"),
                             F.sum(F.when(F.col("periods_present") == n_periods, 1)
                                    .otherwise(0)).alias("fully_complete"),
                         )
                         .collect()[0])

        fully_complete = ent_stats["fully_complete"] or 0

        # per-period entity counts (cached pair → tiny table)
        per_period = (pair.groupBy(time_col)
                          .agg(F.count("*").alias("entity_count"))
                          .orderBy(time_col)
                          .toPandas().to_dict(orient="records"))

        return {
            "entity_col": entity_col, "time_col": time_col,
            "n_entities": n_entities, "n_periods": n_periods,
            "period_range": [period_list[0], period_list[-1]] if period_list else [],
            "periods": period_list,
            "expected_cells": expected, "actual_cells": actual,
            "fill_rate_pct": fill,
            "entities_fully_complete": fully_complete,
            "entities_fully_complete_pct": round(fully_complete / n_entities * 100, 1) if n_entities else 0,
            "entity_period_stats": {
                "min": ent_stats["min"], "max": ent_stats["max"],
                "mean": _rnd(ent_stats["mean"], 1), "median": ent_stats["median"],
            },
            "per_period_entity_counts": per_period,
        }
    finally:
        try:
            pair.unpersist()
        except Exception:
            pass


# ── Time-aware summary stats ──

def time_summary_stats(spark_session, df, time_col, metric_cols, entity_col=None):
    """Per-period aggregates for numeric columns — shows how metrics evolve."""
    aggs = [F.count("*").alias("row_count")]
    if entity_col:
        aggs.append(F.countDistinct(entity_col).alias("entity_count"))
    for m in metric_cols:
        aggs += [
            F.sum(m).alias(f"{m}__sum"),
            F.mean(m).alias(f"{m}__mean"),
            F.min(m).alias(f"{m}__min"),
            F.max(m).alias(f"{m}__max"),
            F.stddev(m).alias(f"{m}__std"),
        ]
    result = (df.groupBy(time_col).agg(*aggs)
                .orderBy(time_col)
                .toPandas())
    return result.to_dict(orient="records")


# ── Main profiler ──

def profile_table(spark_session, source, label):
    """Full smart profile: schema, stats, entity/time/grain/completeness.

    Speed notes:
      • Batch stats run in ONE pass over the full table.  We use
        approx_count_distinct (HLL) instead of exact countDistinct
        — same single pass, much cheaper on wide tables.
      • Percentiles and top-values run against a CACHED SAMPLE of
        the table (configurable via SAMPLE_FOR_STATS).  Sample is
        unpersisted before we return.
      • All approxQuantile calls are batched into ONE Spark job.
      • Per-column top-value queries run concurrently from the
        driver via a thread pool — N queries become roughly N/k.
    """
    if isinstance(source, str):
        df = spark_session.table(source); src = source
    else:
        df = source; src = "DataFrame"
    df.createOrReplaceTempView(label)

    fields = df.schema.fields
    if len(fields) > MAX_PROFILE_COLS:
        print(f"    ⚠️  {len(fields)} cols — profiling first {MAX_PROFILE_COLS}")
        fields = fields[:MAX_PROFILE_COLS]
        df = df.select(*[f.name for f in fields])

    row_count = df.count()
    if row_count == 0:
        return {"label": label, "source": src, "row_count": 0,
                "col_count": len(fields), "columns": [], "sample_rows": [],
                "entities": [], "time_cols": [], "groupings": [],
                "completeness": None, "grain": None, "time_stats": None}

    # ── batch stats: ONE pass over full table (approx distincts) ──
    exprs = []
    for i, f in enumerate(fields):
        cn = f.name
        exprs += [
            F.count(F.when(F.col(cn).isNull(), True)).alias(f"_{i}_n"),
            F.approx_count_distinct(cn, rsd=0.05).alias(f"_{i}_d"),
        ]
        if isinstance(f.dataType, _NUM):
            exprs += [F.min(cn).alias(f"_{i}_mi"), F.max(cn).alias(f"_{i}_mx"),
                      F.mean(cn).alias(f"_{i}_mu"), F.stddev(cn).alias(f"_{i}_sd")]
        elif isinstance(f.dataType, _TIME):
            exprs += [F.min(cn).cast("string").alias(f"_{i}_mi"),
                      F.max(cn).cast("string").alias(f"_{i}_mx")]
        elif isinstance(f.dataType, StringType):
            exprs += [F.min(F.length(cn)).alias(f"_{i}_lmi"),
                      F.max(F.length(cn)).alias(f"_{i}_lmx"),
                      F.mean(F.length(cn)).alias(f"_{i}_lmu")]

    batch = df.select(*exprs).collect()[0]

    columns = []
    numeric_names = []
    string_names  = []
    for i, f in enumerate(fields):
        info = {"name": f.name, "type": str(f.dataType), "nullable": f.nullable,
                "null_count": batch[f"_{i}_n"] or 0,
                "null_pct": round((batch[f"_{i}_n"] or 0) / row_count * 100, 2),
                "distinct": batch[f"_{i}_d"] or 0}
        if isinstance(f.dataType, _NUM):
            info["category"] = "numeric"
            info.update({"min": batch[f"_{i}_mi"], "max": batch[f"_{i}_mx"],
                         "mean": _rnd(batch[f"_{i}_mu"]), "stddev": _rnd(batch[f"_{i}_sd"])})
            numeric_names.append(f.name)
        elif isinstance(f.dataType, _TIME):
            info["category"] = "temporal"
            info.update({"min": batch[f"_{i}_mi"], "max": batch[f"_{i}_mx"]})
        elif isinstance(f.dataType, StringType):
            info["category"] = "string"
            info.update({"min_length": batch[f"_{i}_lmi"], "max_length": batch[f"_{i}_lmx"],
                         "avg_length": _rnd(batch[f"_{i}_lmu"], 1)})
            string_names.append(f.name)
        elif isinstance(f.dataType, BooleanType):
            info["category"] = "boolean"
        else:
            info["category"] = "complex"
        columns.append(info)

    # ── cache a working sample for the per-column heavy work ──
    if row_count > SAMPLE_FOR_STATS:
        frac = min(1.0, SAMPLE_FOR_STATS / row_count)
        df_stats = df.sample(fraction=frac, seed=42).cache()
        sample_size = df_stats.count()
        print(f"    (sampled {sample_size:,}/{row_count:,} rows for stats)")
    else:
        df_stats = df.cache()
        df_stats.count()

    try:
        # ── percentiles: ONE call for all numeric columns ──
        if numeric_names:
            try:
                all_pcts = df_stats.stat.approxQuantile(
                    numeric_names, [0.01, 0.25, 0.5, 0.75, 0.99], 0.01)
                for name, pcts in zip(numeric_names, all_pcts):
                    if pcts and len(pcts) == 5:
                        ci = next(c for c in columns if c["name"] == name)
                        ci.update({"p01": pcts[0], "p25": pcts[1], "p50": pcts[2],
                                   "p75": pcts[3], "p99": pcts[4]})
            except Exception:
                pass

        # ── top values for strings: parallel queries from driver ──
        if string_names:
            def _top_values_for(name):
                try:
                    top = (df_stats.groupBy(name).count()
                             .orderBy(F.desc("count")).limit(TOP_K_VALUES).collect())
                    return name, [(r[name], r["count"]) for r in top]
                except Exception:
                    return name, []

            workers = max(1, min(PROFILE_PARALLELISM, len(string_names)))
            with ThreadPoolExecutor(max_workers=workers) as pool:
                for name, tv in pool.map(_top_values_for, string_names):
                    if tv:
                        ci = next(c for c in columns if c["name"] == name)
                        ci["top_values"] = tv

        # sample rows
        sample_pd = df_stats.limit(20).toPandas()

        # ── structural detection ──
        entities  = detect_entities(columns, row_count)
        time_cols = detect_time_columns(df_stats, columns)
        groupings = detect_groupings(columns, row_count)

        # granularity + completeness (if we found both entity and time)
        grain = None
        completeness = None
        time_stats = None
        if time_cols:
            tc = time_cols[0]
            tc_info = next(c for c in columns if c["name"] == tc["col"])
            grain = detect_granularity(df, tc["col"], tc_info["category"])
            if entities:
                ec = entities[0]["col"]
                completeness = analyze_completeness(spark_session, df, ec, tc["col"], label)
            if numeric_names:
                metric_subset = numeric_names[:8]  # cap to avoid huge queries
                time_stats = time_summary_stats(
                    spark_session, df, tc["col"], metric_subset,
                    entity_col=entities[0]["col"] if entities else None)

        return {
            "label": label, "source": src,
            "row_count": row_count, "col_count": len(fields),
            "columns": columns,
            "sample_rows": sample_pd.head(20).to_dict(orient="records"),
            "sample_display": sample_pd.head(10).to_string(index=False, max_cols=12),
            "entities": entities,
            "time_cols": time_cols,
            "grain": grain,
            "groupings": groupings,
            "completeness": completeness,
            "time_stats": time_stats,
        }
    finally:
        try:
            df_stats.unpersist()
        except Exception:
            pass


def format_profile_for_llm(p):
    """Compact text summary for LLM prompts — includes structural findings."""
    lines = [
        f"TABLE: {p['label']}  ({p['row_count']:,} rows × {p['col_count']} cols)",
        f"SOURCE: {p['source']}",
    ]
    if p.get("entities"):
        lines.append(f"ENTITY KEY(S): {', '.join(e['col'] for e in p['entities'][:3])}")
    if p.get("time_cols"):
        tc = p["time_cols"][0]
        lines.append(f"TIME COLUMN: {tc['col']}  GRAIN: {p.get('grain','?')}")
    if p.get("groupings"):
        lines.append(f"GROUPING COLS: {', '.join(g['col'] for g in p['groupings'][:5])}")
    if p.get("completeness"):
        c = p["completeness"]
        lines.append(f"PANEL: {c['n_entities']:,} entities × {c['n_periods']} periods, "
                     f"fill={c['fill_rate_pct']}%, "
                     f"{c['entities_fully_complete_pct']}% fully complete")
        lines.append(f"PERIOD RANGE: {c['period_range'][0]} .. {c['period_range'][1]}"
                     if c.get("period_range") else "")

    lines.append("\nCOLUMNS:")
    for c in p["columns"]:
        parts = [f"  {c['name']:<30s} {c['type']:<15s}"]
        parts.append(f"nulls={c['null_count']:,}({c['null_pct']}%)")
        parts.append(f"distinct={c['distinct']:,}")
        if c["category"] == "numeric":
            parts.append(f"min={c.get('min')} max={c.get('max')} "
                         f"mean={c.get('mean')} std={c.get('stddev')}")
            if c.get("p50") is not None:
                parts.append(f"[p25={c['p25']} p50={c['p50']} p75={c['p75']}]")
        elif c["category"] == "string":
            tv = c.get("top_values", [])[:3]
            if tv: parts.append("top=[" + ", ".join(f'"{v}"({n:,})' for v, n in tv) + "]")
        elif c["category"] == "temporal":
            parts.append(f"{c.get('min')} .. {c.get('max')}")
        lines.append("  ".join(parts))

    if p.get("time_stats"):
        lines.append(f"\nTIME-SERIES SUMMARY (by {p['time_cols'][0]['col']}, first 6 periods):")
        for row in p["time_stats"][:6]:
            lines.append(f"  {row}")

    lines.append(f"\nSAMPLE (5 rows):\n{p['sample_display'][:1200]}")
    return "\n".join(lines)


# COMMAND ----------
# CELL 6: Column inference via LLM + PDFs
# COMMAND ----------

def infer_columns(profiles, pdf_chunks):
    """Use LLM + PDF context to infer what each column means."""
    col_list = []
    for p in profiles.values():
        for c in p["columns"]:
            sample = ""
            if c.get("top_values"):
                sample = str([v for v, _ in c["top_values"][:5]])
            elif c["category"] == "numeric":
                sample = f"min={c.get('min')} max={c.get('max')} mean={c.get('mean')}"
            elif c["category"] == "temporal":
                sample = f"{c.get('min')} .. {c.get('max')}"
            col_list.append(f"  {p['label']}.{c['name']}  type={c['type']}  "
                            f"distinct={c['distinct']}  sample={sample}")

    pdf_context = ""
    if pdf_chunks:
        kw = [c["name"] for p in profiles.values() for c in p["columns"]]
        pdf_context = relevant_chunks(pdf_chunks, kw[:20], top_k=5)

    prompt = f"""Given these table columns with sample values, infer what each one
represents. Use the PDF context to match domain terminology.

COLUMNS:
{chr(10).join(col_list)}

{"PDF CONTEXT:" + chr(10) + pdf_context[:5000] if pdf_context else "(no PDFs provided)"}

For EACH column output:
- table: which table
- col: column name
- meaning: what it represents (be specific — e.g. "Total Prescriptions (TRx)")
- display_label: clean label for chart axes (e.g. "Total Rx", "Provider NPI")
- role: one of entity_id | time | metric | grouping | attribute | unknown

Output ONLY a JSON array of objects."""

    resp = call_llm("You are a pharmaceutical data analyst. Output ONLY valid JSON.",
                    prompt)
    col_map = parse_json(resp, [])

    # index by table.col
    result = {}
    for entry in col_map:
        key = f"{entry.get('table','')}.{entry.get('col','')}"
        result[key] = entry
    return result


# COMMAND ----------
# CELL 7: Prompt templates — code generation
# COMMAND ----------

_SYS_CODE = """You are a senior data analyst generating Python code for Databricks
notebooks. You produce DETERMINISTIC, REPRODUCIBLE code for slide-ready figures.

RULES:
- Use spark.sql("...").toPandas() for data — ALWAYS include ORDER BY
- Use matplotlib for figures (available in all Databricks runtimes)
- White background, professional styling, clear labels
- Figure size (12, 6) default, or (10, 8) for heatmaps
- Format numbers: $, K, M, B, % as appropriate
- Include a descriptive title and axis labels
- Use the display_label from the column map when available
- Add reference lines from benchmarks where relevant
- Each code block must be fully self-contained (own imports)
- Add a commented-out savefig line at the end
- NO print() of DataFrames — only figures
- Output ONLY Python code — no markdown fences, no explanation text"""


def make_plan_prompt(user_prompt, profiles_text, col_map_text,
                     completeness_text, benchmarks_text, pdf_context):
    return f"""The user wants analyses of their data. Plan the specific
code cells to generate.

USER REQUEST:
{user_prompt}

DATA PROFILES:
{profiles_text}

COLUMN MEANINGS:
{col_map_text}

PANEL COMPLETENESS:
{completeness_text}

REFERENCE BENCHMARKS:
{benchmarks_text}

{"PDF CONTEXT:" + chr(10) + pdf_context[:2000] if pdf_context else ""}

1. Break the user request into specific, actionable visualizations.
2. Add 2-3 SUGGESTIONS the user didn't ask for but are valuable given
   the data structure (e.g. completeness heatmap, distribution checks,
   time-series decomposition, entity-level variation).
3. For each, specify exactly what SQL to run and what chart to make.

Output JSON:
{{"cells": [
  {{"title": "...",
    "source": "user_request" or "suggestion",
    "purpose": "one line",
    "chart_type": "line|bar|heatmap|histogram|box|scatter|table|stacked_bar",
    "sql": "SELECT ... ORDER BY ...",
    "x": "col", "y": "col", "color": "col or null",
    "reference_lines": [{{"value": N, "label": "...", "color": "red"}}],
    "figure_size": [12, 6],
    "notes": "any caveats"}}
]}}"""


def make_codegen_prompt(cell_spec, col_map_text, table_labels):
    refs = ""
    if cell_spec.get("reference_lines"):
        refs = f"\nREFERENCE LINES: {json.dumps(cell_spec['reference_lines'])}"
    return f"""Generate a COMPLETE, SELF-CONTAINED Python code cell for Databricks.

TITLE: {cell_spec['title']}
PURPOSE: {cell_spec['purpose']}
CHART TYPE: {cell_spec['chart_type']}
SQL: {cell_spec['sql']}
X axis: {cell_spec.get('x','')}   Y axis: {cell_spec.get('y','')}
Color/Group: {cell_spec.get('color','none')}
Figure size: {cell_spec.get('figure_size', [12,6])}
{refs}

COLUMN MEANINGS (use display_label for axis labels):
{col_map_text[:2000]}

TABLES AVAILABLE: {', '.join(table_labels)}

REQUIREMENTS:
- Start with: # ── {cell_spec['title']} ──
- import matplotlib.pyplot as plt (and ticker, dates, numpy as needed)
- df = spark.sql(\"\"\"...SQL...\"\"\").toPandas()
- Professional slide-ready figure with white background
- plt.style.use('seaborn-v0_8-whitegrid') or manual rcParams
- Proper number formatting on axes
- Legend outside plot if >4 groups
- plt.tight_layout() then plt.show()
- Last line commented: # fig.savefig('/dbfs/FileStore/figures/{{filename}}.png', dpi=150, bbox_inches='tight')

Output ONLY the Python code."""


def make_benchmark_prompt(pdf_sum):
    return f"""Extract ALL quantitative benchmarks and chart descriptions from these PDFs.

PDF CONTENT:
{pdf_sum[:8000]}

Output JSON:
{{"benchmarks": [
    {{"metric_name":"...", "value":N, "unit":"...", "period":"...",
      "context":"...", "source_doc":"..."}}],
  "chart_references": [
    {{"chart_type":"...", "what_it_shows":"...", "axes":"..."}}]
}}"""


def make_sanity_prompt(profiles_text, benchmarks, custom):
    return f"""Compare actual data against reference benchmarks.

DATA PROFILES:
{profiles_text}

BENCHMARKS:
{json.dumps(benchmarks, indent=2, default=str) if benchmarks else "(none)"}

USER CHECKS:
{json.dumps(custom, indent=2, default=str) if custom else "(none)"}

Output JSON:
{{"checks": [
    {{"metric":"...", "actual_value":N, "actual_source":"...",
      "reference_value":N, "reference_source":"...",
      "deviation_pct":N, "severity":"PASS|WARNING|ALERT",
      "explanation":"..."}}],
  "summary":"..."}}"""


# COMMAND ----------
# CELL 8: Code generator + sanity checker
# COMMAND ----------

def generate_code_cells(spark_session, user_prompt, profiles, col_map,
                        completeness_info, benchmarks, chart_refs,
                        pdf_chunks, table_labels):
    """Turn user prompt + data intelligence into executable code cells."""

    profiles_text = "\n\n".join(format_profile_for_llm(p) for p in profiles.values())
    col_map_text = json.dumps(list(col_map.values()), indent=1, default=str)[:3000]

    comp_text = ""
    for label, info in completeness_info.items():
        if info:
            comp_text += (f"{label}: {info['n_entities']} entities × {info['n_periods']} periods, "
                          f"fill={info['fill_rate_pct']}%\n")

    bench_text = json.dumps(benchmarks[:15], indent=1, default=str) if benchmarks else "(none)"

    pdf_ctx = ""
    if pdf_chunks:
        kw = user_prompt.lower().split()[:15]
        pdf_ctx = relevant_chunks(pdf_chunks, kw, top_k=3)

    # Step 1: plan the cells
    print("    Planning cells...", end=" ", flush=True)
    plan_resp = call_llm(
        "You are a senior data analyst. Output ONLY valid JSON.",
        make_plan_prompt(user_prompt, profiles_text, col_map_text,
                         comp_text, bench_text, pdf_ctx))
    plan = parse_json(plan_resp, {"cells": []})
    cells = plan.get("cells", [])
    print(f"{len(cells)} planned")

    # Step 2: generate code for each cell
    code_cells = []
    for i, spec in enumerate(cells):
        print(f"    🔄 [{i+1}/{len(cells)}] {spec.get('title','')}...", end=" ", flush=True)
        code = call_llm(_SYS_CODE,
                        make_codegen_prompt(spec, col_map_text, table_labels))
        code = strip_fences(code)
        code_cells.append({
            "title": spec.get("title", f"Cell {i+1}"),
            "source": spec.get("source", "suggestion"),
            "purpose": spec.get("purpose", ""),
            "chart_type": spec.get("chart_type", ""),
            "code": code,
            "sql": spec.get("sql", ""),
        })
        print("✅")
        time.sleep(1)

    return code_cells


def run_custom_checks(checks, spark_session):
    results = []
    for chk in checks:
        sql = chk.get("sql") or f"SELECT {chk['agg']} FROM {chk['table']}"
        try:
            actual = spark_session.sql(sql).collect()[0][0]
            expected = chk.get("expected_range") or chk.get("expected")
            sev, dev = "INFO", None
            if isinstance(expected, list) and len(expected) == 2:
                lo, hi = expected
                if lo <= actual <= hi: sev, dev = "PASS", 0
                elif actual < lo:
                    dev = (lo - actual) / lo * 100
                    sev = "WARNING" if dev < 20 else "ALERT"
                else:
                    dev = (actual - hi) / hi * 100
                    sev = "WARNING" if dev < 20 else "ALERT"
            elif expected is not None:
                dev = abs(actual - expected) / expected * 100 if expected else 0
                sev = "PASS" if dev < 10 else ("WARNING" if dev < 30 else "ALERT")
            results.append({"metric": chk["label"], "actual_value": actual,
                            "reference_value": expected, "deviation_pct": _rnd(dev,1),
                            "severity": sev, "source": "user-specified"})
        except Exception as e:
            results.append({"metric": chk["label"], "severity": "ERROR",
                            "explanation": str(e), "source": "user-specified"})
    return results


def run_benchmark_checks(profiles_text, benchmarks):
    if not benchmarks: return [], ""
    resp = call_llm("You are a data validation analyst. Output ONLY valid JSON.",
                    make_sanity_prompt(profiles_text, benchmarks, []))
    result = parse_json(resp, {"checks": [], "summary": ""})
    return result.get("checks", []), result.get("summary", "")


# COMMAND ----------
# CELL 9: Output formatting
# ============================================================
#  Formats generated code cells for display + saves as a
#  runnable .py notebook file the user can import.
# ============================================================

def print_code_cells(code_cells):
    """Print each generated code cell with a header for copy-paste."""
    sep = "=" * 70
    for i, cell in enumerate(code_cells):
        tag = "📌 REQUESTED" if cell["source"] == "user_request" else "💡 SUGGESTED"
        print(f"\n{sep}")
        print(f"  {tag}  [{i+1}/{len(code_cells)}]  {cell['title']}")
        print(f"  {cell['purpose']}")
        print(sep)
        print(cell["code"])
        print()


def save_as_notebook(code_cells, project_name, profiles):
    """Save all generated cells as a Databricks .py notebook."""
    lines = ["# Databricks notebook source"]
    lines.append(f"# Generated by Ad Hoc Analyzer — {project_name}")
    lines.append(f"# {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append(f"# Tables: {', '.join(profiles.keys())}")
    lines.append("")

    # setup cell
    lines.append("# COMMAND ----------")
    lines.append("# SETUP: common imports and style")
    lines.append("# COMMAND ----------")
    lines.append("")
    lines.append("import matplotlib.pyplot as plt")
    lines.append("import matplotlib.ticker as mticker")
    lines.append("import matplotlib.dates as mdates")
    lines.append("import numpy as np")
    lines.append("import pandas as pd")
    lines.append("")
    lines.append("plt.rcParams.update({")
    lines.append("    'figure.facecolor': 'white', 'axes.facecolor': 'white',")
    lines.append("    'font.family': 'sans-serif', 'font.size': 11,")
    lines.append("    'axes.titlesize': 14, 'axes.titleweight': 'bold',")
    lines.append("    'axes.grid': True, 'grid.alpha': 0.3,")
    lines.append("    'figure.figsize': (12, 6),")
    lines.append("})")
    lines.append("")

    for i, cell in enumerate(code_cells):
        lines.append("# COMMAND ----------")
        lines.append(f"# [{i+1}] {cell['title']}")
        if cell.get("purpose"):
            lines.append(f"# {cell['purpose']}")
        lines.append("# COMMAND ----------")
        lines.append("")
        lines.append(cell["code"])
        lines.append("")

    path = f"/dbfs/FileStore/generated_notebooks/{project_name.replace(' ','_')}_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.py"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write("\n".join(lines))
    return path


def print_sanity_results(checks, summary):
    icons = {"PASS": "✅", "WARNING": "⚠️", "ALERT": "🔴", "ERROR": "❌", "INFO": "ℹ️"}
    for c in checks:
        ic = icons.get(c.get("severity"), "❓")
        dev = f"  dev={c['deviation_pct']}%" if c.get("deviation_pct") is not None else ""
        print(f"  {ic} {c.get('severity','?'):<8s} {c.get('metric',''):<35s}"
              f"  actual={c.get('actual_value','?')}"
              f"  ref={c.get('reference_value','?')}{dev}")
        if c.get("explanation"):
            print(f"     └─ {c['explanation']}")
    if summary:
        print(f"\n  Summary: {summary}")


# COMMAND ----------
# CELL 10: Pipeline orchestrator
# COMMAND ----------

def analyze(spark_session, tables, pdfs=None, user_prompt="",
            sanity_checks=None, cache_dir=None, force_rebuild=None):
    """
    Main entry point.

    Returns dict with: profiles, col_map, completeness, benchmarks,
    code_cells, sanity_checks, notebook_path.

    cache_dir / force_rebuild default to the module-level
    CONTEXT_CACHE_DIR / FORCE_REBUILD_CONTEXT settings.  Profiles
    and col-map entries are cached per (table_address, schema)
    so repeat runs against the same table skip profiling and
    LLM column inference entirely.
    """
    if cache_dir is None:
        cache_dir = CONTEXT_CACHE_DIR
    if force_rebuild is None:
        force_rebuild = FORCE_REBUILD_CONTEXT

    results = {
        "project_name": PROJECT_NAME,
        "profiles": {}, "col_map": {},
        "completeness": {}, "benchmarks": [], "chart_references": [],
        "code_cells": [], "sanity_checks": [], "sanity_summary": "",
        "notebook_path": None,
    }

    # ── Phase 1: Profile (with context cache) ──
    print("📊 Phase 1 — Profiling tables...")
    new_labels = []      # labels that need col-map inference
    table_schemas = {}   # label → (source, schema) for cache writes later
    for label, source in tables.items():
        # cache lookup (only for string sources — DataFrames have no stable address)
        cached = None
        if isinstance(source, str) and not force_rebuild:
            df_probe = spark_session.table(source)
            df_probe.createOrReplaceTempView(label)  # restore view for downstream SQL
            table_schemas[label] = (source, df_probe.schema)
            cached = load_context(cache_dir, source, df_probe.schema)

        if cached:
            p = cached["profile"]
            results["profiles"][label] = p
            results["col_map"].update(cached.get("col_map_entries") or {})
            results["completeness"][label] = p.get("completeness")
            print(f"  ⚡ {label}  (cached: {p['row_count']:,} rows × {p['col_count']} cols)")
            continue

        print(f"  🔄 {label}...", end=" ", flush=True)
        p = profile_table(spark_session, source, label)
        results["profiles"][label] = p
        new_labels.append(label)
        if isinstance(source, str) and label not in table_schemas:
            table_schemas[label] = (source, spark_session.table(source).schema)
        print(f"✅ {p['row_count']:,} rows × {p['col_count']} cols")
        if p.get("entities"):
            print(f"    entity key: {p['entities'][0]['col']}")
        if p.get("time_cols"):
            print(f"    time col:   {p['time_cols'][0]['col']}  grain={p.get('grain','?')}")
        if p.get("completeness"):
            c = p["completeness"]
            print(f"    panel:      {c['n_entities']:,} entities × {c['n_periods']} periods  "
                  f"fill={c['fill_rate_pct']}%")
        if p.get("groupings"):
            print(f"    groupings:  {', '.join(g['col'] for g in p['groupings'][:4])}")
        results["completeness"][label] = p.get("completeness")

    profiles_text = "\n\n".join(format_profile_for_llm(p) for p in results["profiles"].values())
    table_labels = list(results["profiles"].keys())

    # ── Phase 2: PDF ingest ──
    all_chunks = []
    if pdfs:
        print("\n📄 Phase 2 — Processing PDFs...")
        for label, reader in pdfs.items():
            text = extract_pdf_text(reader, label)
            chunks = chunk_text(text)
            all_chunks.extend(chunks)
            print(f"  ✅ {label}: {len(reader.pages)} pages, {len(chunks)} chunks")

        print("\n📏 Phase 2b — Extracting benchmarks...")
        resp = call_llm("You are a data analyst. Output ONLY valid JSON.",
                        make_benchmark_prompt(pdf_summary(all_chunks)))
        bench = parse_json(resp, {"benchmarks": [], "chart_references": []})
        results["benchmarks"] = bench.get("benchmarks", [])
        results["chart_references"] = bench.get("chart_references", [])
        print(f"  ✅ {len(results['benchmarks'])} benchmarks, "
              f"{len(results['chart_references'])} chart refs")

    # ── Phase 3: Column inference (new tables only) ──
    if new_labels:
        print(f"\n🏷️  Phase 3 — Inferring column meanings for {len(new_labels)} new table(s)...")
        new_profiles = {l: results["profiles"][l] for l in new_labels}
        new_col_map  = infer_columns(new_profiles, all_chunks)
        results["col_map"].update(new_col_map)
        print(f"  ✅ {len(new_col_map)} new columns mapped "
              f"({len(results['col_map'])} total)")

        # ── persist cache for newly profiled tables ──
        for label in new_labels:
            if label not in table_schemas:
                continue
            src, schema = table_schemas[label]
            entries = {k: v for k, v in new_col_map.items()
                       if k.startswith(f"{label}.")}
            save_context(cache_dir, src, schema,
                         results["profiles"][label], entries)
    else:
        print(f"\n🏷️  Phase 3 — All tables cached, skipping column inference "
              f"({len(results['col_map'])} columns mapped from cache)")

    for key, info in list(results["col_map"].items())[:8]:
        print(f"    {key:<35s} → {info.get('meaning','?')[:50]}")

    # cache for interactive use
    global _profiles_text, _all_chunks, _table_labels, _col_map, _completeness
    _profiles_text = profiles_text
    _all_chunks = all_chunks
    _table_labels = table_labels
    _col_map = results["col_map"]
    _completeness = results["completeness"]

    # ── Phase 4: Generate code cells ──
    if user_prompt.strip():
        print(f"\n🔧 Phase 4 — Generating code from prompt...")
        results["code_cells"] = generate_code_cells(
            spark_session, user_prompt, results["profiles"], results["col_map"],
            results["completeness"], results["benchmarks"],
            results["chart_references"], all_chunks, table_labels)

    # ── Phase 5: Sanity checks ──
    all_checks = []
    if sanity_checks:
        print("\n🔍 Phase 5a — User-specified checks...")
        all_checks += run_custom_checks(sanity_checks, spark_session)
    if results["benchmarks"]:
        print("🔍 Phase 5b — Benchmark checks...")
        bc, summary = run_benchmark_checks(profiles_text, results["benchmarks"])
        all_checks += bc
        results["sanity_summary"] = summary
    results["sanity_checks"] = all_checks

    # ── Phase 6: Save notebook ──
    if results["code_cells"]:
        print("\n💾 Phase 6 — Saving generated notebook...")
        results["notebook_path"] = save_as_notebook(
            results["code_cells"], PROJECT_NAME, results["profiles"])
        print(f"  ✅ {results['notebook_path']}")

    return results


# COMMAND ----------
# CELL 11: === RUN ===
# COMMAND ----------

results = analyze(
    spark_session=spark,
    tables=TABLE_INPUTS,
    pdfs=PDF_INPUTS or None,
    user_prompt=USER_PROMPT,
    sanity_checks=SANITY_CHECKS or None,
)

# ── Print structural findings ──
print("\n" + "=" * 70)
print("  STRUCTURAL FINDINGS")
print("=" * 70)
for label, p in results["profiles"].items():
    print(f"\n  📊 {label}:")
    if p.get("entities"):
        for e in p["entities"][:2]:
            print(f"     Entity key: {e['col']} (confidence={e['score']})")
    if p.get("time_cols"):
        print(f"     Time:       {p['time_cols'][0]['col']}  grain={p.get('grain')}")
    if p.get("groupings"):
        for g in p["groupings"][:3]:
            print(f"     Grouping:   {g['col']} ({g['distinct']} values)")
    if p.get("completeness"):
        c = p["completeness"]
        print(f"     Panel:      {c['n_entities']:,} × {c['n_periods']} periods  "
              f"fill={c['fill_rate_pct']}%  "
              f"({c['entities_fully_complete_pct']}% entities fully complete)")

# ── Print sanity checks ──
if results["sanity_checks"]:
    print("\n" + "=" * 70)
    print("  SANITY CHECKS")
    print("=" * 70)
    print_sanity_results(results["sanity_checks"], results["sanity_summary"])

# ── Print generated code cells ──
if results["code_cells"]:
    print("\n" + "=" * 70)
    print(f"  GENERATED CODE — {len(results['code_cells'])} cells")
    print(f"  Copy each block into a new cell, or import the notebook:")
    print(f"  {results.get('notebook_path','')}")
    print("=" * 70)
    print_code_cells(results["code_cells"])


# COMMAND ----------
# CELL 12: Interactive — change prompt and re-run
# ============================================================
# The profiles and column map are cached.  Change the prompt
# below and re-run this cell to generate new code cells.
# ============================================================

FOLLOW_UP_PROMPT = """
Show me a histogram of TRx per prescriber, and a scatter plot
of TRx vs NRx colored by specialty.
"""

follow_up_cells = generate_code_cells(
    spark, FOLLOW_UP_PROMPT,
    results["profiles"], _col_map, _completeness,
    results.get("benchmarks", []), results.get("chart_references", []),
    _all_chunks, _table_labels)

print_code_cells(follow_up_cells)

# To save these too:
# path = save_as_notebook(follow_up_cells, PROJECT_NAME + "_followup", results["profiles"])
# print(f"Saved: {path}")
