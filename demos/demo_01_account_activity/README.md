# Demo 1 — Account Activity Quarterly Review

End-to-end local demo showing how the AdHocOnTap analyzer + deck-builder turn three synthetic CSVs into a boardroom-ready PowerPoint, with all the intermediate artifacts (profiles, code, insights) bundled into a downloadable `.zip`.

## What's in the dataset

Fully synthetic, generated from a fixed seed in `01_generate_data.py`. **No PHI, no PII, no real identifiers**. Three tables:

| Table | Rows | Cols | Description |
|-------|-----:|-----:|-------------|
| `account_master.csv` | 5,000 | 6 | One row per `ACCOUNT_xxxxx`. Columns: `account_id`, `region_code` (R1–R5), `category`, `tier` (Bronze/Silver/Gold/Platinum), `onboarded_year`, `active_flag`. |
| `account_activity.csv` | ~50,000 | 6 | The panel: `account_id × quarter`. Columns: `engagements`, `outreach_completed`, `response_rate_pct`, `satisfaction_score`. **Intentionally incomplete** — most accounts only appear in some quarters, so the profiler's panel-completeness analysis has something to find. |
| `region_targets.csv` | 15 | 4 | Tiny reference table: `region_code × category` → quarterly targets and minimum response rate. |

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
%cd demos/demo_01_account_activity
```

```python
# Cell 4 — generate the synthetic data
!python 01_generate_data.py
```

```python
# Cell 5 — run the full pipeline: profile → QC → deck → archive
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

> **If you restart the kernel partway through**, your working directory resets to the repo root.  Re-run Cell 3 (`%cd demos/demo_01_account_activity`) before re-running the pipeline.

### Terminal alternative (bash / zsh / PowerShell)

```bash
# One-time setup — clone the repo and install deps
git clone https://github.com/jamesyoung93/AdHocOnTap.git
cd AdHocOnTap
pip install -r demos/requirements.txt
pip install --no-deps https://github.com/jamesyoung93/deck-builder/raw/master/deck_builder-0.2.0-py3-none-any.whl

# Set a provider API key (free options: GEMINI_API_KEY, GROQ_API_KEY)
export ANTHROPIC_API_KEY=sk-ant-...
# Windows PowerShell: $env:ANTHROPIC_API_KEY = "sk-ant-..."

# Run this demo
cd demos/demo_01_account_activity
python 01_generate_data.py        # writes the CSVs to ./data/
python 02_run_pipeline.py         # profile → QC → deck → archive
```

## What the analyzer should detect (without being told)

This is the moment that makes the video — call it out as it prints:

- **Entity key** = `account_id` (~5,000 distinct, ~99% non-null, matches the `account.?id` pattern)
- **Time column** = `quarter` (string in `YYYYQN` format, detected as `quarterly(YYYYQN)` grain)
- **Groupings** = `region_code` (5), `category` (3), `tier` (4), `active_flag` (2)
- **Panel** = ~5,000 entities × 10 periods, with a fill rate <100% (because lower-tier accounts churn) — this is the structural finding the deck calls out

## QC step

This demo's data is **clean** by design — the QC step should report 0 blockers and a high AI Readiness score (around 95–100). Use it as the contrast to demo 2, which deliberately has issues. The QC slides will still appear in the deck (FAIR callouts + a sparse "all green" findings slide).

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
