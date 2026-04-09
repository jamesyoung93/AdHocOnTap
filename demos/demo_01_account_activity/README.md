# Demo 1 — Account Activity Quarterly Review

End-to-end local demo showing how the AdHocOnTap analyzer + deck-builder turn three synthetic CSVs into a boardroom-ready PowerPoint, with all the intermediate artifacts (profiles, code, insights) bundled into a downloadable `.zip`.

## What's in the dataset

Fully synthetic, generated from a fixed seed in `01_generate_data.py`. **No PHI, no PII, no real identifiers**. Three tables:

| Table | Rows | Cols | Description |
|-------|-----:|-----:|-------------|
| `account_master.csv` | 5,000 | 6 | One row per `ACCOUNT_xxxxx`. Columns: `account_id`, `region_code` (R1–R5), `category`, `tier` (Bronze/Silver/Gold/Platinum), `onboarded_year`, `active_flag`. |
| `account_activity.csv` | ~50,000 | 6 | The panel: `account_id × quarter`. Columns: `engagements`, `outreach_completed`, `response_rate_pct`, `satisfaction_score`. **Intentionally incomplete** — most accounts only appear in some quarters, so the profiler's panel-completeness analysis has something to find. |
| `region_targets.csv` | 15 | 4 | Tiny reference table: `region_code × category` → quarterly targets and minimum response rate. |

## Run it

```bash
# from the repo root
pip install -r demos/requirements.txt
pip install --no-deps https://github.com/jamesyoung93/deck-builder/raw/master/deck_builder-0.2.0-py3-none-any.whl

export ANTHROPIC_API_KEY=sk-ant-...    # or GEMINI_API_KEY / GROQ_API_KEY / OPENAI_API_KEY

cd demos/demo_01_account_activity
python 01_generate_data.py        # writes the CSVs to ./data/
python 02_run_pipeline.py         # profile → LLM → deck → archive
```

## What the analyzer should detect (without being told)

This is the moment that makes the video — call it out as it prints:

- **Entity key** = `account_id` (~5,000 distinct, ~99% non-null, matches the `account.?id` pattern)
- **Time column** = `quarter` (string in `YYYYQN` format, detected as `quarterly(YYYYQN)` grain)
- **Groupings** = `region_code` (5), `category` (3), `tier` (4), `active_flag` (2)
- **Panel** = ~5,000 entities × 10 periods, with a fill rate <100% (because lower-tier accounts churn) — this is the structural finding the deck calls out

## What you get in `output/`

```
output/
├── deck.pptx                                           ← the slide deck
├── Account_Activity_Quarterly_Review_<timestamp>/      ← the unzipped archive
│   ├── manifest.json
│   ├── profiles/
│   │   ├── activity.json
│   │   ├── accounts.json
│   │   └── targets.json
│   ├── col_map.json
│   ├── insights.json
│   ├── generated_code/   ← one .py per analysis cell the LLM wrote
│   ├── all_cells.py
│   └── deck.pptx
└── Account_Activity_Quarterly_Review_<timestamp>.zip
```

## Editing the prompt

The "what should the analyst look at" prompt lives at the top of `02_run_pipeline.py` as `USER_PROMPT`. Edit it and re-run — only the LLM-driven phases will change, the profile is deterministic.

## Tweaking the deck style

Change `DECK_STYLE` in `02_run_pipeline.py`. Options: `executive_dark`, `corporate_clean`, `accent_green`, `neutral`.
