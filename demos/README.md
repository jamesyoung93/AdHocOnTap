# AdHocOnTap × deck-builder — local demos

End-to-end local demos joining the **AdHocOnTap** profiling/analysis capability with the **deck-builder** PowerPoint generator. Designed for recording a video on a laptop, no Databricks cluster required.

The story each demo tells, in one line:

> raw CSVs → automatic profile → LLM column inference → reproducible analysis code → AI-assisted QC + FAIR scoring → boardroom-ready PowerPoint deck → archive bundle (`.zip`)

| Demo | Dataset | What it showcases |
|------|---------|-------------------|
| **`demo_01_account_activity/`** | Synthetic generic account activity (5k accounts × 10 quarters) | Entity detection, time-grain detection, panel completeness on a sparse panel, low-cardinality groupings |
| **`demo_02_retail_sales/`** | Synthetic retail store-week sales (1500 stores × 50 weeks × 8 categories ≈ 75k rows) | Multi-table profiling, weekly time-series, regional/format groupings, seasonal patterns |

Both datasets are 100% synthetic — generated from a fixed seed in the demo's `01_generate_data.py`. No PHI/PII, no real identifiers.

---

## Quick start

> Running in a Jupyter notebook (local Jupyter, JupyterLab, VS Code, or Colab)? **Skip this section** and jump to [Running in a notebook](#running-in-a-notebook) — the commands below are for a terminal. In a notebook you need `%cd` magic (not plain `cd`) and `!` prefixes on shell commands.

### Terminal (bash / zsh / PowerShell)

```bash
# 1. Clone the repo and enter it
git clone https://github.com/jamesyoung93/AdHocOnTap.git
cd AdHocOnTap

# 2. Install Python deps (run from the AdHocOnTap repo root)
pip install -r demos/requirements.txt

# 3. Install deck-builder (the wheel — dependencies already installed in step 2)
pip install --no-deps https://github.com/jamesyoung93/deck-builder/raw/master/deck_builder-0.2.0-py3-none-any.whl

# 4. Pick an LLM provider (pick ONE — see "LLM providers" below for free options)
export ANTHROPIC_API_KEY=sk-ant-...
# OR
export GEMINI_API_KEY=...      # free tier
# OR
export GROQ_API_KEY=gsk_...    # free tier
# Windows PowerShell equivalent:  $env:GEMINI_API_KEY = "..."

# 5. Generate the synthetic data + run the pipeline
cd demos/demo_01_account_activity
python 01_generate_data.py
python 02_run_pipeline.py
```

The output `.zip` lands in `demos/demo_01_account_activity/output/`.

> **No `pip install .` needed.** The demos import the shared modules via `sys.path` (see the `sys.path.insert(...)` at the top of each `02_run_pipeline.py`), so there's no packaging step for AdHocOnTap itself — just clone and run.

---

## LLM providers

The local demos use a unified `LLMClient` (in `shared/llm_client.py`) that auto-detects which provider to use from environment variables. Pick whichever you have:

| Provider | Cost | Get a key |
|----------|------|-----------|
| **Google Gemini** | **FREE** (~1500 req/day on Flash) | https://aistudio.google.com/app/apikey → set `GEMINI_API_KEY` |
| **Groq** | **FREE** tier, fastest inference | https://console.groq.com/keys → set `GROQ_API_KEY` |
| **Anthropic Claude** | Pay-as-you-go (~$0.01 per demo run on Haiku 4.5) | https://console.anthropic.com/ → set `ANTHROPIC_API_KEY` |
| **OpenAI** | Pay-as-you-go | https://platform.openai.com/api-keys → set `OPENAI_API_KEY` |
| **Ollama** | **FREE** (local) | https://ollama.com/ — run `ollama serve` then `ollama pull llama3.1` |

Detection priority is `ANTHROPIC → GEMINI → GROQ → OPENAI → ollama (localhost)`. To force a different one set `DEFAULT_LLM_PROVIDER=gemini` (etc.).

---

## AI-assisted QC + FAIR scoring

Between profiling and deck-building the pipeline runs a **two-pronged data quality review**:

**Deterministic rule checks** (always run, no LLM):
- Schema: missing/null/duplicate primary keys
- Quality: high null rates, single-value columns, all-null cols
- Distributional: heavy-tail outliers, negative-where-positive-expected, % > 100
- Temporal: sparse panels, shrinking panel coverage, high entity churn
- Cross-table: orphan foreign keys

**AI-assisted scan** (runs when an LLM client is available):
- The LLM looks at the profiles and surfaces issues a rule list would miss — typo'd categoricals, numeric columns that should be categorical, leakage candidates, business-logic violations, inconsistent units across tables.
- The LLM also writes a 2-3 sentence executive summary and 2-4 prioritized recommendations (`title`, `rationale`, `effort`).

**FAIR scoring** (adapted for AI/ML readiness):
- **Findable** — meaningful column names, primary keys present, schema documented (col_map exists)
- **Accessible** — file readable, no encoding issues
- **Interoperable** — standard types, time grain detected, cross-table joins valid
- **Reusable** — documented, no severe quality issues

Each is scored 0–100 and combined into a single **AI Readiness Score** (average minus a 15-point penalty per blocker).

**Pause behavior**: each finding has a severity (`BLOCKER`/`WARNING`/`INFO`) and a fix type. BLOCKERs halt the pipeline by default — set `FORCE_CONTINUE_PAST_BLOCKERS = True` in the demo script to override (the demos do this so the video can show both the pause message and the resulting deck). Each finding includes either an `ai_fix` field with executable Python (e.g., `stores = stores.dropna(subset=['store_id'])`) or a `human_action` field with a one-sentence judgment call.

**Demo 2 ships with deliberately dirty data** so the QC step has things to find on video — null primary keys, duplicate keys, mixed-case categoricals, an orphan foreign key. Demo 1's data is clean so you can see the contrast.

The QC report appears in three places: printed to stdout during the run, added to the deck as three slides (FAIR callouts → blockers/warnings → prioritized fixes), and saved to the archive as `qc_report.json`.

---

## What the pipeline produces

After `02_run_pipeline.py` finishes, look in `<demo>/output/`:

```
output/
├── deck.pptx                                ← the generated PowerPoint
├── <Project>_<timestamp>/                   ← unzipped archive bundle
│   ├── manifest.json                        ← prompt, tables, timestamp, git SHA, QC summary
│   ├── profiles/
│   │   ├── activity.json                    ← full per-table profile (cols, stats, completeness)
│   │   ├── accounts.json
│   │   └── targets.json
│   ├── col_map.json                         ← LLM column-meaning inferences
│   ├── insights.json                        ← LLM "what to investigate next" bullets
│   ├── qc_report.json                       ← FAIR scores + findings + AI recommendations
│   ├── generated_code/                      ← one .py per analysis cell
│   │   ├── 01_top_regions_by_engagement.py
│   │   ├── 02_quarterly_trend_by_tier.py
│   │   └── ...
│   ├── all_cells.py                         ← all generated cells in a single runnable file
│   └── deck.pptx
└── <Project>_<timestamp>.zip                ← the same bundle, zipped for sharing
```

Open `deck.pptx` to see the slide deck. Open `all_cells.py` in any editor to see (and run) the generated analysis code — the cells reference the demo dataframes by their label name (`activity`, `sales`, etc.), so they work as soon as you load the CSVs.

---

## Repo layout

```
demos/
├── README.md                              ← this file
├── requirements.txt
├── shared/
│   ├── llm_client.py                      ← multi-provider LLM client
│   ├── local_analyzer.py                  ← pandas-based profiler/analyzer (no Spark)
│   ├── data_qc.py                         ← AI-assisted QC + FAIR / AI Readiness scoring
│   ├── analyzer_to_deck.py                ← analyzer results → Deck object bridge
│   └── archive.py                         ← results bundle → timestamped folder + .zip
├── demo_01_account_activity/
│   ├── 01_generate_data.py                ← synthesizes the CSVs
│   ├── 02_run_pipeline.py                 ← profile → deck → archive
│   ├── data/                              ← generated CSVs
│   └── output/                            ← deck + archive land here
└── demo_02_retail_sales/
    ├── 01_generate_data.py
    ├── 02_run_pipeline.py
    ├── data/
    └── output/
```

The local demo stack is intentionally separate from the Databricks notebook (`ad_hoc_analyzer.py` in the repo root). The notebook is for production at scale on Delta tables; the local demos are for laptops, Colab, and recording videos.

---

## Running in a notebook

Works unchanged in **any Jupyter environment** — local Jupyter, JupyterLab, VS Code notebook, or Google Colab. Two things to know:

- Use `%cd` (the IPython magic) — **not** plain `cd`. `!cd foo` only changes directory inside that one subshell and the next `!python` call lands back at the original cwd, so you'll get "file not found".
- Use `!python script.py` (or `%run script.py`) to execute the demo scripts. **Don't** `exec(open(...).read())` — the scripts use `__file__` to locate their data/output folders, and `__file__` isn't defined inside an `exec()` call.

```python
# Cell 1 — clone and install (one time per environment)
!git clone https://github.com/jamesyoung93/AdHocOnTap.git
%cd AdHocOnTap
!pip install -q -r demos/requirements.txt
!pip install -q --no-deps https://github.com/jamesyoung93/deck-builder/raw/master/deck_builder-0.2.0-py3-none-any.whl

# Cell 2 — set your API key.  Gemini's free tier is the easiest path on Colab.
import os
os.environ["GEMINI_API_KEY"] = "..."  # paste from https://aistudio.google.com/app/apikey

# Cell 3 — run the demo (note %cd, not cd)
%cd demos/demo_01_account_activity
!python 01_generate_data.py
!python 02_run_pipeline.py

# Cell 4 — open the resulting deck
# Colab:
from google.colab import files
files.download("output/deck.pptx")
# Local Jupyter / VS Code — just open the file:
import os; os.startfile("output/deck.pptx")   # Windows
# (on macOS:  !open output/deck.pptx   ; on Linux:  !xdg-open output/deck.pptx )
```

If you **just restarted the kernel** partway through, your working directory resets — re-run the `%cd` cells before running the pipeline again.

---

## Recording the video — suggested flow

A clean ~5 minute take, demo 1:

1. **Show the synthetic data** (~30 sec) — open one CSV, highlight that there are no IDs, names, or anything sensitive. Just `ACCOUNT_xxxxx`, quarters, and engagement counts.
2. **Run `01_generate_data.py`** (~10 sec) — point out the seed, the row counts.
3. **Run `02_run_pipeline.py`** and narrate as it prints (~120 sec):
   - Phase 1 prints which entity / time / grouping columns the profiler detected automatically — this is the "magic" moment.
   - Phase 2 shows the LLM mapping `engagements → "Activity Volume"` etc.
   - Phase 3 lists the code cells the LLM is writing for your prompt.
   - Phase 4 shows the deck-narrative bullets.
   - Phase 5 runs the AI-assisted QC + FAIR scoring. On demo 2 (dirty data) it surfaces a BLOCKER ("primary key has null values") with an executable `ai_fix`, plus warnings about duplicate keys and an LLM-found mixed-case categorical, and prints "PIPELINE WOULD PAUSE" — but FORCE_CONTINUE_PAST_BLOCKERS is on so the deck still builds.
4. **Open `output/deck.pptx`** in PowerPoint (~60 sec) — flip through, pause on the data callout slide, point at the panel-completeness numbers and explain they came from the profile, not from any human input.
5. **Open `output/all_cells.py`** (~30 sec) — show one or two of the generated analysis cells, copy one into a notebook, run it to show a real chart.
6. **Open the `.zip`** (~30 sec) — show the archive structure: manifest, profiles, col_map, code cells, deck, all in one file.
7. **Repeat with demo 2** if doing the full ~10 minute video — same flow, different dataset, same code.

The whole pipeline (excluding the LLM call latency) takes a couple of seconds. The LLM phase is the slowest part — Groq is the fastest provider for live recordings (~3-5 sec total).
