"""
AI-assisted data QC + FAIR scoring for AI/ML readiness.

Two-pronged review:

  1. Deterministic rule-based checks (always run, no LLM needed):
       - schema: missing/null/duplicate primary keys
       - quality: high null rates, single-value columns
       - distributional: heavy tails, negative-where-positive-expected
       - temporal: sparse panels, shrinking panels, high churn
       - cross-table: orphan foreign keys

  2. AI-assisted checks (when an LLM client is provided):
       - LLM scans the profile for issues a rule list can't see
         (typos in categoricals, business-logic violations, leakage…)
       - LLM writes a 2-3 sentence summary + 2-4 prioritized actions

Each finding carries:
  severity   : BLOCKER | WARNING | INFO
  category   : schema | quality | distributional | temporal | cross-table | ai_assisted
  fix_type   : ai_auto | human_review | data_owner | pause
  ai_fix     : executable Python snippet (when fix_type=ai_auto)
  human_action : what a human needs to decide / do (otherwise)

The module also computes FAIR-style scores adapted for AI/ML readiness:
  Findable / Accessible / Interoperable / Reusable, each 0-100,
  combined into an overall AI Readiness Score.

Public API:
  run_qc(tables, results, llm_call=None)  -> dict
  print_qc_report(qc)
  has_blockers(qc) -> bool
"""
from __future__ import annotations
import json
import re
from typing import Callable, Optional


# ─────────────────────────────────────────────────────────────────
# Deterministic checks
# ─────────────────────────────────────────────────────────────────

def _check_schema(tables: dict, results: dict) -> list:
    """Schema-level checks: primary keys, uniqueness, nullability."""
    findings = []
    profiles = results.get("profiles", {})

    for label, p in profiles.items():
        if not p.get("entities"):
            if p["row_count"] > 1 and p["col_count"] > 1:
                findings.append({
                    "id":       f"schema_no_entity_{label}",
                    "severity": "WARNING",
                    "category": "schema",
                    "title":    "No entity key detected",
                    "detail":   f"Table `{label}` has no recognizable entity / primary-key column. "
                                f"Joins and grouping operations will be ad-hoc.",
                    "table":    label,
                    "columns":  [],
                    "fix_type": "human_review",
                    "human_action": "Add a primary-key column or rename an existing one to match a recognizable pattern (id, _id, _key, account_id, store_id, npi…).",
                })
            continue

        ec = p["entities"][0]["col"]
        ec_info = next((c for c in p["columns"] if c["name"] == ec), None)
        if not ec_info:
            continue

        # nulls in primary key — ANY null in a PK breaks downstream joins/grouping
        if ec_info["null_count"] > 0:
            findings.append({
                "id":       f"schema_pk_nulls_{label}",
                "severity": "BLOCKER",
                "category": "schema",
                "title":    "Primary key has null values",
                "detail":   f"{ec_info['null_count']:,} rows ({ec_info['null_pct']}%) have null `{ec}` in `{label}`. "
                            f"These rows can't be joined or grouped reliably — any null in a primary "
                            f"key is a blocker for downstream analysis.",
                "table":    label,
                "columns":  [ec],
                "fix_type": "ai_auto",
                "ai_fix":   f"{label} = {label}.dropna(subset=['{ec}'])  # drops {ec_info['null_count']:,} rows",
            })

        # duplicates on master tables (no panel)
        if not p.get("completeness") and ec_info["distinct"] < p["row_count"]:
            dups = p["row_count"] - ec_info["distinct"]
            findings.append({
                "id":       f"schema_pk_dups_{label}",
                "severity": "WARNING",
                "category": "schema",
                "title":    "Master table has duplicate keys",
                "detail":   f"{dups:,} duplicate `{ec}` values in `{label}`. "
                            f"Master tables should have one row per entity.",
                "table":    label,
                "columns":  [ec],
                "fix_type": "ai_auto",
                "ai_fix":   f"{label} = {label}.drop_duplicates(subset=['{ec}'], keep='first')  # drops {dups:,} rows",
            })

    return findings


def _check_quality(tables: dict, results: dict) -> list:
    """Per-column data quality."""
    findings = []
    profiles = results.get("profiles", {})

    for label, p in profiles.items():
        for c in p.get("columns", []):
            cn = c["name"]

            # 100% null
            if c["null_pct"] >= 99.99:
                findings.append({
                    "id":       f"quality_all_null_{label}_{cn}",
                    "severity": "WARNING",
                    "category": "quality",
                    "title":    "Column is empty",
                    "detail":   f"`{label}.{cn}` is 100% null — provides no signal.",
                    "table":    label,
                    "columns":  [cn],
                    "fix_type": "ai_auto",
                    "ai_fix":   f"{label} = {label}.drop(columns=['{cn}'])",
                })
                continue

            # high null rate
            if c["null_pct"] > 30:
                severity = "BLOCKER" if c["null_pct"] > 70 else "WARNING"
                findings.append({
                    "id":       f"quality_high_null_{label}_{cn}",
                    "severity": severity,
                    "category": "quality",
                    "title":    f"Column has high null rate ({c['null_pct']}%)",
                    "detail":   f"`{label}.{cn}` is {c['null_pct']}% null. Decide whether to drop, "
                                f"impute, or accept the loss.",
                    "table":    label,
                    "columns":  [cn],
                    "fix_type": "human_review",
                    "human_action": "Drop the column, impute (mean/median for numeric, mode for categorical), or leave as-is and document the limitation.",
                })

            # single value
            if c["distinct"] == 1 and c["null_pct"] < 99.99:
                findings.append({
                    "id":       f"quality_single_value_{label}_{cn}",
                    "severity": "INFO",
                    "category": "quality",
                    "title":    "Column has only one distinct value",
                    "detail":   f"`{label}.{cn}` has a single value across all rows — no analytical signal.",
                    "table":    label,
                    "columns":  [cn],
                    "fix_type": "ai_auto",
                    "ai_fix":   f"{label} = {label}.drop(columns=['{cn}'])",
                })

    return findings


def _check_distributional(tables: dict, results: dict) -> list:
    """Distributional sanity: outliers, sign flips, suspicious ranges."""
    findings = []
    profiles = results.get("profiles", {})

    for label, p in profiles.items():
        for c in p.get("columns", []):
            if c.get("dtype_kind") not in ("numeric", "integer"):
                continue
            cn = c["name"]
            p99 = c.get("p99")
            p75 = c.get("p75")

            # heavy tail (top 1% is much larger than upper quartile)
            if p99 is not None and p75 is not None and p75 > 0:
                ratio = p99 / p75
                if ratio > 5:
                    findings.append({
                        "id":       f"dist_heavy_tail_{label}_{cn}",
                        "severity": "INFO",
                        "category": "distributional",
                        "title":    "Heavy tail / outliers",
                        "detail":   f"`{label}.{cn}` has p99/p75 = {ratio:.1f}x — top 1% of values "
                                    f"are far above the upper quartile.",
                        "table":    label,
                        "columns":  [cn],
                        "fix_type": "human_review",
                        "human_action": "Investigate top values; consider winsorizing or log-transforming for visualizations.",
                    })

            # negative values where the column name implies positive
            if (c.get("min") is not None and c["min"] < 0
                and re.search(r'(?i)(count|qty|quantity|units|sales|revenue|amount|score|rate|pct|n_)', cn)):
                findings.append({
                    "id":       f"dist_negative_{label}_{cn}",
                    "severity": "WARNING",
                    "category": "distributional",
                    "title":    "Negative values in a positive-expected column",
                    "detail":   f"`{label}.{cn}` has min={c['min']} but the column name suggests "
                                f"counts/amounts/rates. Possible refunds, errors, or sign flip.",
                    "table":    label,
                    "columns":  [cn],
                    "fix_type": "human_review",
                    "human_action": "Investigate negative rows. May be returns/refunds (legitimate) or data errors.",
                })

            # rate/percentage out of bounds
            if re.search(r'(?i)(rate|pct|percent)', cn):
                if c.get("max") is not None and c["max"] > 100.5:
                    findings.append({
                        "id":       f"dist_pct_over_100_{label}_{cn}",
                        "severity": "WARNING",
                        "category": "distributional",
                        "title":    "Percentage column exceeds 100",
                        "detail":   f"`{label}.{cn}` has max={c['max']} but the column name implies a percentage.",
                        "table":    label,
                        "columns":  [cn],
                        "fix_type": "human_review",
                        "human_action": "Confirm the unit (is it a fraction × 100, or basis points?). Cap or rescale as needed.",
                    })

    return findings


def _check_temporal(tables: dict, results: dict) -> list:
    """Time-series and panel-completeness checks."""
    findings = []
    profiles = results.get("profiles", {})

    for label, p in profiles.items():
        if not p.get("completeness"):
            continue
        c = p["completeness"]

        if c["fill_rate_pct"] < 50:
            findings.append({
                "id":       f"temporal_sparse_panel_{label}",
                "severity": "INFO",
                "category": "temporal",
                "title":    "Sparse panel",
                "detail":   f"`{label}` panel is {c['fill_rate_pct']}% filled. "
                            f"Models that assume balanced panels will need imputation.",
                "table":    label,
                "columns":  [c["entity_col"], c["time_col"]],
                "fix_type": "human_review",
                "human_action": "Either reindex+fillna to balance the panel, or use a sparse-aware model.",
            })

        if c["entities_fully_complete_pct"] < 30 and c["n_periods"] > 3:
            findings.append({
                "id":       f"temporal_high_churn_{label}",
                "severity": "INFO",
                "category": "temporal",
                "title":    "High entity churn",
                "detail":   f"Only {c['entities_fully_complete_pct']}% of entities in `{label}` are present "
                            f"in all {c['n_periods']} periods.  Cohort effects likely.",
                "table":    label,
                "columns":  [c["entity_col"], c["time_col"]],
                "fix_type": "human_review",
                "human_action": "Add cohort indicators (first_period, n_periods_observed) before modeling.",
            })

        per = c.get("per_period_entity_counts", [])
        if len(per) >= 4:
            counts = [r.get("entity_count", 0) for r in per]
            recent_avg = sum(counts[-3:]) / 3
            early_avg  = sum(counts[:3])  / 3
            if early_avg > 0 and recent_avg / early_avg < 0.5:
                findings.append({
                    "id":       f"temporal_shrinking_panel_{label}",
                    "severity": "WARNING",
                    "category": "temporal",
                    "title":    "Panel coverage is shrinking",
                    "detail":   f"Active entities in `{label}` dropped from ~{int(early_avg):,} (early periods) "
                                f"to ~{int(recent_avg):,} (recent) — a {(1 - recent_avg/early_avg)*100:.0f}% decline.",
                    "table":    label,
                    "columns":  [c["entity_col"], c["time_col"]],
                    "fix_type": "data_owner",
                    "human_action": "Talk to the data owner — is the source dropping records? Pipeline issue upstream?",
                })

    return findings


def _check_cross_table(tables: dict, results: dict) -> list:
    """Foreign-key integrity across tables that share an entity column."""
    findings = []
    profiles = results.get("profiles", {})

    entity_to_labels = {}
    for label, p in profiles.items():
        if p.get("entities"):
            ec = p["entities"][0]["col"]
            entity_to_labels.setdefault(ec, []).append(label)

    for ec, labels in entity_to_labels.items():
        if len(labels) < 2:
            continue
        for i, t1 in enumerate(labels):
            for t2 in labels[i + 1:]:
                df1 = tables.get(t1)
                df2 = tables.get(t2)
                if df1 is None or df2 is None:
                    continue
                if ec not in df1.columns or ec not in df2.columns:
                    continue
                vals1 = set(df1[ec].dropna().unique())
                vals2 = set(df2[ec].dropna().unique())
                if not vals1 or not vals2:
                    continue
                orphan1 = vals1 - vals2
                if orphan1:
                    pct = len(orphan1) / len(vals1) * 100
                    severity = "WARNING" if pct > 5 else "INFO"
                    findings.append({
                        "id":       f"crosstable_orphan_{t1}_to_{t2}_{ec}",
                        "severity": severity,
                        "category": "cross-table",
                        "title":    f"Orphan keys in `{t1}` not in `{t2}`",
                        "detail":   f"{len(orphan1):,} of {len(vals1):,} `{ec}` values in `{t1}` "
                                    f"({pct:.1f}%) have no match in `{t2}`. Inner joins will lose these rows.",
                        "table":    t1,
                        "columns":  [ec],
                        "fix_type": "data_owner",
                        "human_action": f"Verify upstream: should every `{ec}` in `{t1}` exist in `{t2}`? If yes, fix the source.",
                    })

    return findings


# ─────────────────────────────────────────────────────────────────
# LLM-assisted checks
# ─────────────────────────────────────────────────────────────────

def _llm_find_issues(tables: dict, results: dict, llm_call: Callable) -> list:
    """Ask the LLM to find quality issues a rule list would miss.

    Examples it should catch:
      - typos in categorical values ('Bronze' vs 'bronz')
      - numeric columns that should be categorical (small distinct count)
      - implausible value ranges given the column meaning
      - obvious leakage (target-like columns named 'is_churned' next to features)
      - inconsistent naming conventions across tables
    """
    profiles = results.get("profiles", {})
    profiles_text = []
    for label, p in profiles.items():
        profiles_text.append(f"TABLE {label}: {p['row_count']:,} rows × {p['col_count']} cols")
        for c in p["columns"][:25]:
            tv = c.get("top_values", [])[:5]
            tv_str = ", ".join(f"{v}({n})" for v, n in tv) if tv else ""
            stat = ""
            if c.get("min") is not None:
                stat = f"range=[{c['min']}, {c['max']}]"
            elif tv_str:
                stat = f"top=[{tv_str}]"
            profiles_text.append(f"  {c['name']:<28s} {c['dtype']:<12s} "
                                 f"distinct={c['distinct']:<6} null%={c['null_pct']:<5}  {stat}")
        profiles_text.append("")

    prompt = f"""You are a senior data engineer doing a pre-flight quality review.
Look at these table profiles and identify QUALITY ISSUES that a rule-based
checker would MISS.  Focus on things that need a human eye:

  - Categorical typos / inconsistent casing  ('Bronze' vs 'bronze' vs 'BRONZE')
  - Numeric columns that look like they should be categorical (very few distinct values)
  - Implausible value ranges given the column meaning
  - Possible target leakage (a feature that looks suspiciously like the answer)
  - Inconsistent naming or units across tables
  - Suspicious value distributions (everything zero, all from one bucket, etc.)
  - Mixed-type or coerced columns

Skip things that are obvious from null counts or duplicate counts — those
are already handled by deterministic rules.

DATA PROFILES:
{chr(10).join(profiles_text)}

Output JSON ONLY:
{{"issues": [
    {{
      "title": "short phrase, <60 chars",
      "detail": "what's wrong, 1-2 sentences",
      "table": "table_label",
      "column": "col_name (or empty string if cross-column)",
      "severity": "BLOCKER|WARNING|INFO",
      "fix_suggestion": "what a human or AI fix would look like, 1 sentence"
    }}
]}}

Return empty issues list if you don't see anything.  Output ONLY the JSON, no prose."""

    try:
        resp = llm_call("You are a senior data engineer. Output ONLY valid JSON.", prompt)
        parsed = _parse_json(resp, {"issues": []})
    except Exception as e:
        print(f"    !  LLM issue scan failed: {e}")
        return []

    issues = parsed.get("issues", []) if isinstance(parsed, dict) else []
    findings = []
    for it in issues:
        if not isinstance(it, dict):
            continue
        title = (it.get("title") or "").strip()
        if not title:
            continue
        col = (it.get("column") or "").strip()
        findings.append({
            "id":       f"ai_{re.sub(r'[^a-z0-9]', '_', title.lower())[:40]}",
            "severity": (it.get("severity") or "INFO").upper(),
            "category": "ai_assisted",
            "title":    title,
            "detail":   it.get("detail", ""),
            "table":    it.get("table", ""),
            "columns":  [col] if col else [],
            "fix_type": "human_review",
            "human_action": it.get("fix_suggestion", ""),
            "source":   "llm",
        })
    return findings


def _llm_summarize(findings: list, fair_scores: dict, llm_call: Callable) -> tuple:
    """Ask the LLM for a 2-3 sentence summary + 2-4 prioritized recommendations."""
    findings_text = "\n".join(
        f"[{f['severity']}] {f['title']} ({f['table']}.{','.join(f['columns'])})"
        for f in findings[:30]
    )
    fair_text = ", ".join(f"{k}={v}/100" for k, v in fair_scores.items())

    prompt = f"""You are a senior data engineer summarizing a pre-flight QC report
for a non-technical audience.

FAIR SCORES: {fair_text}

FINDINGS ({len(findings)} total):
{findings_text}

Output JSON ONLY:
{{
  "summary": "2-3 sentences. Be direct. Name the most important finding by title. If there are BLOCKERs, say the pipeline should pause until they are fixed. If only WARNING/INFO, say it can proceed with caveats.",
  "recommendations": [
    {{"title": "short action", "rationale": "one-sentence why", "effort": "minutes|hours|days"}}
  ]
}}

Provide 2-4 recommendations prioritized highest-impact-first."""

    try:
        resp = llm_call("You are a senior data engineer. Output ONLY valid JSON.", prompt)
        parsed = _parse_json(resp, {"summary": "", "recommendations": []})
    except Exception as e:
        print(f"    !  LLM summary failed: {e}")
        return "", []

    summary = parsed.get("summary", "") if isinstance(parsed, dict) else ""
    recs    = parsed.get("recommendations", []) if isinstance(parsed, dict) else []
    return summary, [r for r in recs if isinstance(r, dict)]


# ─────────────────────────────────────────────────────────────────
# FAIR scoring
# ─────────────────────────────────────────────────────────────────

def _score_fair(tables: dict, results: dict, findings: list) -> dict:
    """Compute FAIR-style scores (0-100) adapted for AI/ML readiness.

    Findable     — meaningful column names, primary keys present, schema documented
    Accessible   — file readable, no encoding issues
    Interoperable — standard types, time grain detected, cross-table joins valid
    Reusable     — documented, no severe quality issues, reproducible
    """
    profiles = results.get("profiles", {})
    col_map  = results.get("col_map", {})

    # ── Findable ──
    findable = 100
    if not col_map:
        findable -= 25  # no LLM column inference => no documentation
    n_with_entity = sum(1 for p in profiles.values() if p.get("entities"))
    if profiles and n_with_entity < len(profiles):
        findable -= int(25 * (len(profiles) - n_with_entity) / max(1, len(profiles)))
    vague = sum(
        1 for p in profiles.values() for c in p.get("columns", [])
        if len(c["name"]) < 3 or re.match(r'^(col|c|x|y|var|f)\d*$', c["name"], re.IGNORECASE)
    )
    findable -= min(20, vague * 5)
    findable = max(0, findable)

    # ── Accessible ──
    accessible = 100
    if not profiles:
        accessible = 0
    accessible = max(0, accessible)

    # ── Interoperable ──
    interoperable = 100
    crosstable_issues = sum(1 for f in findings if f["category"] == "cross-table")
    interoperable -= crosstable_issues * 15
    has_time_col = any(p.get("time_cols") for p in profiles.values())
    has_grain    = any(p.get("grain") and p["grain"] not in (None, "unknown") for p in profiles.values())
    if has_time_col and not has_grain:
        interoperable -= 15
    interoperable = max(0, interoperable)

    # ── Reusable ──
    reusable = 100
    if not col_map:
        reusable -= 25
    blockers = sum(1 for f in findings if f["severity"] == "BLOCKER")
    warnings = sum(1 for f in findings if f["severity"] == "WARNING")
    reusable -= blockers * 15
    reusable -= warnings * 5
    reusable = max(0, reusable)

    return {
        "findable":      int(findable),
        "accessible":    int(accessible),
        "interoperable": int(interoperable),
        "reusable":      int(reusable),
    }


def _aggregate_score(fair_scores: dict, findings: list) -> int:
    """AI Readiness Score: average FAIR minus a penalty per blocker."""
    base = sum(fair_scores.values()) / 4
    blocker_penalty = sum(15 for f in findings if f["severity"] == "BLOCKER")
    return max(0, min(100, int(base - blocker_penalty)))


# ─────────────────────────────────────────────────────────────────
# Main entry
# ─────────────────────────────────────────────────────────────────

def run_qc(tables: dict, results: dict,
           llm_call: Optional[Callable] = None,
           ai_assisted: bool = True) -> dict:
    """Run the full QC pipeline.

    tables      : {label: pandas DataFrame}  — needed for cross-table checks
    results     : output from local_analyzer.analyze()
    llm_call    : optional (system, user) -> str function
    ai_assisted : if True and llm_call is provided, also run the LLM-assisted
                  issue scan (one extra LLM call)

    Returns dict with:
      fair_scores, ai_readiness_score, findings, blockers, warnings, info,
      summary, recommendations
    """
    print("[Phase 5] Data QC + FAIR scoring...")

    findings = []
    findings += _check_schema(tables, results)
    findings += _check_quality(tables, results)
    findings += _check_distributional(tables, results)
    findings += _check_temporal(tables, results)
    findings += _check_cross_table(tables, results)
    print(f"  -> {len(findings)} deterministic finding(s)")

    if llm_call and ai_assisted:
        ai_findings = _llm_find_issues(tables, results, llm_call)
        if ai_findings:
            findings += ai_findings
            print(f"  -> {len(ai_findings)} AI-assisted finding(s)")

    fair_scores  = _score_fair(tables, results, findings)
    ai_readiness = _aggregate_score(fair_scores, findings)

    # de-duplicate by id (if both deterministic and LLM flagged the same thing)
    seen = set()
    unique = []
    for f in findings:
        if f["id"] not in seen:
            seen.add(f["id"])
            unique.append(f)
    findings = unique

    blockers = [f for f in findings if f["severity"] == "BLOCKER"]
    warnings = [f for f in findings if f["severity"] == "WARNING"]
    info     = [f for f in findings if f["severity"] == "INFO"]

    summary = ""
    recommendations = []
    if llm_call:
        summary, recommendations = _llm_summarize(findings, fair_scores, llm_call)

    return {
        "fair_scores":         fair_scores,
        "ai_readiness_score":  ai_readiness,
        "findings":            findings,
        "blockers":            blockers,
        "warnings":            warnings,
        "info":                info,
        "summary":             summary,
        "recommendations":     recommendations,
    }


# ─────────────────────────────────────────────────────────────────
# Pretty printing + helpers
# ─────────────────────────────────────────────────────────────────

def has_blockers(qc: dict) -> bool:
    return bool(qc.get("blockers"))


def print_qc_report(qc: dict):
    print("\n" + "=" * 64)
    print("DATA QC + AI READINESS REPORT")
    print("=" * 64)

    fair = qc["fair_scores"]
    print("\nFAIR scores (AI/ML readiness):")
    print(f"  Findable      {fair['findable']:>3}/100  {_bar(fair['findable'])}")
    print(f"  Accessible    {fair['accessible']:>3}/100  {_bar(fair['accessible'])}")
    print(f"  Interoperable {fair['interoperable']:>3}/100  {_bar(fair['interoperable'])}")
    print(f"  Reusable      {fair['reusable']:>3}/100  {_bar(fair['reusable'])}")
    print(f"\n  AI READINESS  {qc['ai_readiness_score']:>3}/100  {_bar(qc['ai_readiness_score'])}")

    print(f"\nFindings: {len(qc['blockers'])} blocker(s), "
          f"{len(qc['warnings'])} warning(s), {len(qc['info'])} info")

    icons = {"BLOCKER": "[X]", "WARNING": "[!]", "INFO": "[i]"}
    for sev in ("BLOCKER", "WARNING", "INFO"):
        items = [f for f in qc["findings"] if f["severity"] == sev]
        if not items:
            continue
        print(f"\n{sev}:")
        for f in items[:12]:
            tail = f"  ({f.get('source', f['category'])})"
            print(f"  {icons[sev]} {f['title']}{tail}")
            print(f"        {f['detail']}")
            if f.get("ai_fix"):
                print(f"        AI fix: {f['ai_fix']}")
            elif f.get("human_action"):
                print(f"        Action: {f['human_action']}")

    if qc.get("summary"):
        print(f"\nSummary:")
        for line in _wrap(qc["summary"], 60):
            print(f"  {line}")

    if qc.get("recommendations"):
        print(f"\nPrioritized recommendations:")
        for i, r in enumerate(qc["recommendations"][:5], 1):
            print(f"  {i}. {r.get('title','')} [{r.get('effort','?')}]")
            if r.get("rationale"):
                print(f"     {r['rationale']}")

    if qc["blockers"]:
        line = "!" * 64
        print(f"\n{line}")
        print(f"  PIPELINE WOULD PAUSE — {len(qc['blockers'])} BLOCKER(s) need a fix.")
        print(f"  Set FORCE_CONTINUE_PAST_BLOCKERS = True in the demo script to override.")
        print(f"{line}")


def _bar(score: int, width: int = 20) -> str:
    filled = int(round(score / 100 * width))
    filled = max(0, min(width, filled))
    return "#" * filled + "." * (width - filled)


def _wrap(text: str, width: int) -> list:
    import textwrap
    return textwrap.wrap(text, width=width) or [text]


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
