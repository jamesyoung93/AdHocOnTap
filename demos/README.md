# AdHocOnTap × deck-builder — local demos

End-to-end local demos joining the **AdHocOnTap** profiling/analysis capability with the **deck-builder** PowerPoint generator. Designed for recording a video on a laptop, no Databricks cluster required.

The story each demo tells, in one line:

> raw CSVs → automatic profile → LLM column inference → reproducible analysis code → boardroom-ready PowerPoint deck → archive bundle (`.zip`)

| Demo | Dataset | What it showcases |
|------|---------|-------------------|
| **`demo_01_account_activity/`** | Synthetic generic account activity (5k accounts × 10 quarters) | Entity detection, time-grain detection, panel completeness on a sparse panel, low-cardinality groupings |
| **`demo_02_retail_sales/`** | Synthetic retail store-week sales (1500 stores × 50 weeks × 8 categories ≈ 75k rows) | Multi-table profiling, weekly time-series, regional/format groupings, seasonal patterns |

Both datasets are 100% synthetic — generated from a fixed seed in the demo's `01_generate_data.py`. No PHI/PII, no real identifiers.

---

## Quick start (5 commands)

```bash
# 1. Install Python deps
pip install -r demos/requirements.txt

# 2. Install deck-builder (the wheel)
pip install --no-deps https://github.com/jamesyoung93/deck-builder/raw/master/deck_builder-0.2.0-py3-none-any.whl

# 3. Pick an LLM provider (pick ONE — see "LLM providers" below for free options)
export ANTHROPIC_API_KEY=sk-ant-...
# OR
export GEMINI_API_KEY=...     # free tier
# OR
export GROQ_API_KEY=gsk_...    # free tier

# 4. Generate the synthetic data + run the pipeline
cd demos/demo_01_account_activity
python 01_generate_data.py
python 02_run_pipeline.py
```

The output `.zip` lands in `demos/demo_01_account_activity/output/`.

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

## What the pipeline produces

After `02_run_pipeline.py` finishes, look in `<demo>/output/`:

```
output/
├── deck.pptx                                ← the generated PowerPoint
├── <Project>_<timestamp>/                   ← unzipped archive bundle
│   ├── manifest.json                        ← prompt, tables, timestamp, git SHA
│   ├── profiles/
│   │   ├── activity.json                    ← full per-table profile (cols, stats, completeness)
│   │   ├── accounts.json
│   │   └── targets.json
│   ├── col_map.json                         ← LLM column-meaning inferences
│   ├── insights.json                        ← LLM "what to investigate next" bullets
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

## Running on Google Colab

Each demo also runs unchanged in Colab. In a fresh notebook:

```python
# Cell 1 — clone the repo + install
!git clone https://github.com/jamesyoung93/AdHocOnTap.git
%cd AdHocOnTap
!pip install -q -r demos/requirements.txt
!pip install -q --no-deps https://github.com/jamesyoung93/deck-builder/raw/master/deck_builder-0.2.0-py3-none-any.whl

# Cell 2 — set your API key (Gemini's free tier is the easiest in Colab)
import os
os.environ["GEMINI_API_KEY"] = "..."  # paste from https://aistudio.google.com/app/apikey

# Cell 3 — run the demo
%cd demos/demo_01_account_activity
!python 01_generate_data.py
!python 02_run_pipeline.py

# Cell 4 — download the resulting deck
from google.colab import files
files.download("output/deck.pptx")
```

---

## Recording the video — suggested flow

A clean ~5 minute take, demo 1:

1. **Show the synthetic data** (~30 sec) — open one CSV, highlight that there are no IDs, names, or anything sensitive. Just `ACCOUNT_xxxxx`, quarters, and engagement counts.
2. **Run `01_generate_data.py`** (~10 sec) — point out the seed, the row counts.
3. **Run `02_run_pipeline.py`** and narrate as it prints (~90 sec):
   - Phase 1 prints which entity / time / grouping columns the profiler detected automatically — this is the "magic" moment.
   - Phase 2 shows the LLM mapping `engagements → "Activity Volume"` etc.
   - Phase 3 lists the code cells the LLM is writing for your prompt.
   - Phase 4 shows the deck-narrative bullets.
4. **Open `output/deck.pptx`** in PowerPoint (~60 sec) — flip through, pause on the data callout slide, point at the panel-completeness numbers and explain they came from the profile, not from any human input.
5. **Open `output/all_cells.py`** (~30 sec) — show one or two of the generated analysis cells, copy one into a notebook, run it to show a real chart.
6. **Open the `.zip`** (~30 sec) — show the archive structure: manifest, profiles, col_map, code cells, deck, all in one file.
7. **Repeat with demo 2** if doing the full ~10 minute video — same flow, different dataset, same code.

The whole pipeline (excluding the LLM call latency) takes a couple of seconds. The LLM phase is the slowest part — Groq is the fastest provider for live recordings (~3-5 sec total).
