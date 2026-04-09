"""
Bridge: local_analyzer results -> deck-builder Deck.

Builds a slide deck primarily from the structured analyzer output
(profiles, completeness, time-series stats, insights) using deck-builder's
direct Python API -- no second LLM call required.  Narrative slides
come from the analyzer's already-distilled `insights` list.

This is intentionally deterministic: same `results` in -> same Deck out.
That makes the video demo predictable: no chart-code execution, no
LLM re-roll, no surprise failures mid-take.
"""
from __future__ import annotations
from datetime import datetime

# deck-builder imports — installed via the wheel from
# https://github.com/jamesyoung93/deck-builder
from slide_engine.schema import (
    Deck, Slide, SlideType, StylePreset,
    BulletPoint, DataCallout, BarItem, ColumnContent,
)


def build_deck_from_results(results: dict,
                            project_name: str,
                            qc: dict | None = None,
                            style: str = "executive_dark",
                            subtitle: str | None = None) -> Deck:
    """Construct a Deck from local_analyzer.analyze() output.

    results : dict from local_analyzer.analyze()
    project_name : title for the deck
    qc : optional dict from data_qc.run_qc() — adds AI-readiness slides
    style : executive_dark | corporate_clean | accent_green | neutral
    subtitle : optional subtitle override (defaults to today's date)
    """
    profiles  = results.get("profiles", {})
    insights  = results.get("insights", [])

    deck = Deck(
        title    = project_name,
        subtitle = subtitle or f"Generated {datetime.now():%Y-%m-%d}",
        date     = datetime.now().strftime("%B %Y"),
        style    = StylePreset(style),
    )

    # ── 1. Cover ──
    deck.add_slide(Slide(
        type     = SlideType.COVER,
        title    = project_name,
        subtitle = f"{len(profiles)} dataset(s) • automatic profile + LLM analysis",
    ))

    # ── 2. Agenda ──
    deck.add_slide(Slide(
        type  = SlideType.AGENDA,
        title = "In this deck",
        agenda_items = [
            "What the data looks like",
            "Panel structure and completeness",
            "Trends over time",
            "Where to investigate next",
        ],
    ))

    # ── 3. Executive summary: what the profiler detected ──
    summary_bullets = []
    for label, p in profiles.items():
        parts = [f"{p['row_count']:,} rows × {p['col_count']} cols"]
        if p.get("entities"):
            parts.append(f"entity: {p['entities'][0]['col']}")
        if p.get("time_cols"):
            parts.append(f"grain: {p.get('grain','unknown')}")
        if p.get("completeness"):
            parts.append(f"{p['completeness']['fill_rate_pct']}% panel fill")
        summary_bullets.append(BulletPoint(
            lead=label, detail=" • ".join(parts),
        ))
    deck.add_slide(Slide(
        type   = SlideType.EXECUTIVE_SUMMARY,
        title  = "What the profiler detected without being told",
        bullets = summary_bullets[:6],
        source = "AdHocOnTap automatic profiler",
    ))

    # ── 3b. AI Readiness (FAIR scores) ──
    if qc and qc.get("fair_scores"):
        f = qc["fair_scores"]
        ai = qc.get("ai_readiness_score", 0)
        readiness_label = (
            "Ready for analysis"   if ai >= 80 else
            "Usable with caveats"  if ai >= 60 else
            "Needs cleanup first"  if ai >= 40 else
            "Not AI-ready"
        )
        deck.add_slide(Slide(
            type  = SlideType.DATA_CALLOUT,
            title = f"Data AI Readiness: {ai}/100 — {readiness_label}",
            callouts = [
                DataCallout(value=f"{f['findable']}",      label="Findable",      context="meaningful names + keys + docs"),
                DataCallout(value=f"{f['accessible']}",    label="Accessible",    context="readable, no encoding issues"),
                DataCallout(value=f"{f['interoperable']}", label="Interoperable", context="standard types + valid joins"),
                DataCallout(value=f"{f['reusable']}",      label="Reusable",      context="documented, no severe issues"),
            ],
            source = "FAIR scoring (auto + LLM-assisted)",
        ))

    # ── 3c. QC findings: blockers vs warnings ──
    if qc and (qc.get("blockers") or qc.get("warnings")):
        blockers = qc.get("blockers", [])
        warnings = qc.get("warnings", [])
        cols = []
        if blockers:
            cols.append(ColumnContent(
                heading = f"Blockers ({len(blockers)})",
                bullets = [f"{b['title']}: {b['detail']}" for b in blockers[:5]],
            ))
        if warnings:
            cols.append(ColumnContent(
                heading = f"Warnings ({len(warnings)})",
                bullets = [f"{w['title']}: {w['detail']}" for w in warnings[:5]],
            ))
        if cols:
            title = ("Pipeline would pause — blockers must be fixed first"
                     if blockers else "Quality flags to review before sharing")
            deck.add_slide(Slide(
                type    = SlideType.TWO_COLUMN,
                title   = title,
                columns = cols,
                source  = "Automated QC + AI-assisted scan",
            ))

    # ── 3d. LLM-prioritized recommendations from QC ──
    if qc and qc.get("recommendations"):
        recs = qc["recommendations"][:5]
        deck.add_slide(Slide(
            type     = SlideType.ACTION_BULLETS,
            title    = "Prioritized data fixes",
            subtitle = "Highest-impact first",
            bullets  = [
                BulletPoint(
                    lead   = f"{r.get('title','')}  [{r.get('effort','?')}]",
                    detail = r.get("rationale", ""),
                )
                for r in recs
            ],
            source = "LLM-distilled QC recommendations",
        ))

    # ── 4. Data callout: panel structure for the primary table ──
    primary = next((p for p in profiles.values() if p.get("completeness")), None)
    if primary:
        c = primary["completeness"]
        deck.add_slide(Slide(
            type  = SlideType.DATA_CALLOUT,
            title = f"Panel structure for {primary['label']}",
            callouts = [
                DataCallout(
                    value   = f"{c['n_entities']:,}",
                    label   = "Unique entities",
                    context = f"key column: {c['entity_col']}",
                ),
                DataCallout(
                    value   = f"{c['n_periods']}",
                    label   = "Time periods",
                    context = f"{primary.get('grain','?')} grain",
                ),
                DataCallout(
                    value   = f"{c['fill_rate_pct']}%",
                    label   = "Panel fill",
                    context = f"{c['entities_fully_complete_pct']}% entities have full history",
                ),
            ],
            source = primary.get("source", ""),
        ))

    # ── 5. Bar chart: active entities by period (panel evolution) ──
    if primary and primary.get("completeness", {}).get("per_period_entity_counts"):
        per = primary["completeness"]["per_period_entity_counts"]
        per_display = per[-12:] if len(per) > 12 else per
        time_col = primary["completeness"]["time_col"]
        if per_display:
            max_v = max(r.get("entity_count", 0) for r in per_display)
            bars = [
                BarItem(
                    label     = str(r.get(time_col, "?")),
                    value     = float(r.get("entity_count", 0)),
                    highlight = (r.get("entity_count", 0) == max_v),
                )
                for r in per_display
            ]
            deck.add_slide(Slide(
                type   = SlideType.BAR_CHART,
                title  = f"Active entities per period — {primary['label']}",
                bars   = bars,
                source = "Panel completeness analysis",
            ))

    # ── 6. Bar chart: time series of a key metric ──
    if primary and primary.get("time_stats"):
        ts = primary["time_stats"]
        time_col = primary["time_cols"][0]["col"] if primary.get("time_cols") else None
        if ts and time_col:
            sum_keys = [k for k in ts[0].keys() if k.endswith("_sum")]
            if sum_keys:
                metric_key  = sum_keys[0]
                metric_name = metric_key.replace("_sum", "").replace("_", " ").title()
                ts_display  = ts[-12:] if len(ts) > 12 else ts
                vals = [float(r.get(metric_key, 0) or 0) for r in ts_display]
                if any(v > 0 for v in vals):
                    max_v = max(vals)
                    bars = [
                        BarItem(
                            label     = str(r.get(time_col, "?")),
                            value     = float(r.get(metric_key, 0) or 0),
                            highlight = (float(r.get(metric_key, 0) or 0) == max_v),
                        )
                        for r in ts_display
                    ]
                    deck.add_slide(Slide(
                        type   = SlideType.BAR_CHART,
                        title  = f"{metric_name} by period",
                        bars   = bars,
                        source = "Time-aware summary stats",
                    ))

    # ── 7. Two-column: data strengths vs caveats ──
    strengths, caveats = [], []
    for label, p in profiles.items():
        if p.get("completeness"):
            c = p["completeness"]
            if c["fill_rate_pct"] >= 80:
                strengths.append(f"{label}: {c['fill_rate_pct']}% fill rate")
            else:
                caveats.append(f"{label}: only {c['fill_rate_pct']}% fill rate")
            if c["entities_fully_complete_pct"] >= 70:
                strengths.append(f"{label}: {c['entities_fully_complete_pct']}% entities full history")
            elif c["entities_fully_complete_pct"] < 30:
                caveats.append(f"{label}: only {c['entities_fully_complete_pct']}% entities have full history")
        if p.get("entities"):
            strengths.append(f"{label}: entity key auto-detected ({p['entities'][0]['col']})")
        if not p.get("time_cols") and p["col_count"] > 2:
            caveats.append(f"{label}: no time column detected")

    if strengths or caveats:
        cols = []
        if strengths:
            cols.append(ColumnContent(heading="Ready for analysis", bullets=strengths[:6]))
        if caveats:
            cols.append(ColumnContent(heading="Caveats to flag", bullets=caveats[:6]))
        deck.add_slide(Slide(
            type    = SlideType.TWO_COLUMN,
            title   = "What the data is — and isn't — ready for",
            columns = cols,
            source  = "Profiler structural analysis",
        ))

    # ── 8. Action bullets: LLM-distilled investigations ──
    if insights:
        deck.add_slide(Slide(
            type     = SlideType.ACTION_BULLETS,
            title    = "What to investigate next",
            subtitle = "From your prompt + the profile",
            bullets  = [
                BulletPoint(lead=ins.get("lead", ""), detail=ins.get("detail", ""))
                for ins in insights[:5]
            ],
            source = "LLM analysis of profile",
        ))

    # ── 9. Closing ──
    deck.add_slide(Slide(
        type  = SlideType.CLOSING,
        title = "Next steps",
        bullets = [
            BulletPoint(
                lead   = "Run the generated code",
                detail = "Code cells are in the archive bundle, ready to copy into Jupyter or VS Code",
            ),
            BulletPoint(
                lead   = "Edit any slide",
                detail = "All shapes are native PowerPoint — drag, resize, restyle freely",
            ),
            BulletPoint(
                lead   = "Re-run with a new prompt",
                detail = "Profiles are deterministic — only the prompt-specific work re-runs",
            ),
        ],
    ))

    return deck
