# Demo 2 — Retail Store Sales Annual Review

End-to-end local demo on a classic store × week × category retail panel. Showcases the analyzer's strength on time-series data with strong groupings and a clear seasonal pattern.

## What's in the dataset

Fully synthetic, generated from a fixed seed in `01_generate_data.py`. Three tables:

| Table | Rows | Cols | Description |
|-------|-----:|-----:|-------------|
| `store_master.csv` | 1,500 | 7 | One row per `STORE_xxxx`. Columns: `store_id`, `region` (Northeast/Southeast/Midwest/Southwest/West), `store_format` (Express/Standard/Supercenter), `square_feet`, `opened_year`, `remodel_year`, `active_flag`. |
| `category_master.csv` | 8 | 4 | Reference: `category_id`, `category_name`, `basket_attach_rate`, `margin_class`. |
| `store_week_sales.csv` | ~75,000 | 9 | The panel: `store_id × category_id × week_start`. Columns: `revenue`, `units_sold`, `customer_count`, `avg_basket`, `promo_flag`, `stockouts`. **Mixed completeness** — recently-opened stores only appear partway through, churned stores drop out early. |

The data has a built-in **20% sinusoidal seasonal pattern** across the year so the time-series slides actually show something interesting.

## Run it — notebook (Google Colab, Jupyter, JupyterLab, VS Code)

Paste each block into its own cell and run them in order.  Works identically in Colab, local Jupyter, JupyterLab, and VS Code notebooks.  **Use `%cd`, not `cd`** — `!cd` runs in a throwaway subshell and the next `!python` call lands in the wrong directory.

```python
# Cell 1 — clone the repo and install everything (run once per kernel)
!git clone https://github.com/jamesyoung93/AdHocOnTap.git
%cd AdHocOnTap
!pip install -q -r demos/requirements.txt
!pip install -q --no-deps https://github.com/jamesyoung93/deck-builder/raw/master/deck_builder-0.2.0-py3-none-any.whl
```

```python
# Cell 2 — set an LLM API key (pick ONE provider)
# Free options: Gemini (easiest in Colab), Groq (fastest)
import os
os.environ["GEMINI_API_KEY"] = "..."   # https://aistudio.google.com/app/apikey
# os.environ["GROQ_API_KEY"]     = "gsk_..."      # https://console.groq.com/keys
# os.environ["ANTHROPIC_API_KEY"] = "sk-ant-..."  # https://console.anthropic.com/
```

```python
# Cell 3 — cd into this demo's folder (note: %cd, NOT cd)
%cd demos/demo_02_retail_sales
```

```python
# Cell 4 — generate the synthetic data (with deliberately injected quality issues)
!python 01_generate_data.py
```

```python
# Cell 5 — run the full pipeline: profile → QC → deck → archive
# Expect: AI Readiness ~61/100, 1 BLOCKER (null primary keys in `stores`),
# 3 WARNINGs, 1 INFO.  The script sets FORCE_CONTINUE_PAST_BLOCKERS = True
# so the deck still builds despite the blocker.
!python 02_run_pipeline.py
```

```python
# Cell 6 — open / download the resulting deck
# On Google Colab:
from google.colab import files
files.download("output/deck.pptx")
# On local Jupyter (Windows):  import os; os.startfile("output/deck.pptx")
# On local Jupyter (macOS):    !open output/deck.pptx
# On local Jupyter (Linux):    !xdg-open output/deck.pptx
```

> **If you restart the kernel partway through**, your working directory resets to the repo root.  Re-run Cell 3 (`%cd demos/demo_02_retail_sales`) before re-running the pipeline.

### Terminal alternative (bash / zsh / PowerShell)

```bash
# One-time setup — clone the repo and install deps
git clone https://github.com/jamesyoung93/AdHocOnTap.git
cd AdHocOnTap
pip install -r demos/requirements.txt
pip install --no-deps https://github.com/jamesyoung93/deck-builder/raw/master/deck_builder-0.2.0-py3-none-any.whl

# Set a provider API key (free options: GEMINI_API_KEY, GROQ_API_KEY)
export GEMINI_API_KEY=...
# Windows PowerShell: $env:GEMINI_API_KEY = "..."

# Run this demo
cd demos/demo_02_retail_sales
python 01_generate_data.py        # writes the CSVs to ./data/
python 02_run_pipeline.py         # profile → QC → deck → archive
```

## What the analyzer should detect (without being told)

- **Entity key** = `store_id` (and on the panel table, the de facto grain is `store_id × category_id × week_start`)
- **Time column** = `week_start` (`YYYY-MM-DD`, detected as `weekly` from a 7-day median gap)
- **Groupings** = `region` (5), `store_format` (3), `category_id` (8), `promo_flag` (2)
- **Panel** = ~94.8% fill, mixed history (late-opened and churned stores)

## QC step — this demo is deliberately dirty

`01_generate_data.py` injects four quality issues so the AI-assisted QC step has things to find on video. They are flagged in the data generator with `INJECT_QC_ISSUES = True` (set False for a clean run):

| Issue | What's in the data | What the QC will say |
|-------|-------------------|----------------------|
| Null primary keys | 3 rows in `store_master` have `store_id = NULL` | **BLOCKER** — pipeline would pause. AI fix: `stores = stores.dropna(subset=['store_id'])` |
| Duplicate primary keys | 2 rows are exact duplicates of existing stores | **WARNING** — AI fix: `stores = stores.drop_duplicates(subset=['store_id'], keep='first')` |
| Mixed-case categoricals | 20 rows have `store_format = "express"` instead of `"Express"` | **WARNING** (LLM-assisted finding) — human action: normalize case before grouping |
| Orphan foreign key | 5 rows in `store_week_sales` reference `STORE_GHOST` (not in master) | **INFO** — orphan FK between sales and stores |

You'll also see a high-null-rate WARNING on `remodel_year` (~37% null by design — most stores haven't been remodeled).

The expected output: **AI Readiness Score around 60/100**, 1 blocker, 3 warnings, 1 info. The pipeline prints "PIPELINE WOULD PAUSE" but `FORCE_CONTINUE_PAST_BLOCKERS = True` in the script overrides it so the deck still builds — this is the moment in the video to explain "in production you'd set this to False, fix the data, and re-run".

## What you get in `output/`

```
output/
├── deck.pptx                                                ← the slide deck
├── Retail_Store_Sales___Annual_Review_<timestamp>/          ← unzipped archive
│   ├── manifest.json
│   ├── profiles/
│   │   ├── sales.json
│   │   ├── stores.json
│   │   └── categories.json
│   ├── col_map.json
│   ├── insights.json
│   ├── generated_code/
│   ├── all_cells.py
│   └── deck.pptx
└── Retail_Store_Sales___Annual_Review_<timestamp>.zip
```

## Editing the prompt

`USER_PROMPT` at the top of `02_run_pipeline.py`. The default asks for revenue trends across regions/formats, category YoY growth, opened/churned stores, seasonality, and promo vs non-promo comparisons.

## Tweaking the deck style

`DECK_STYLE` in `02_run_pipeline.py`. This demo defaults to `corporate_clean`; the account activity demo defaults to `executive_dark` so the video can show both.
