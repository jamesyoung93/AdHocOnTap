# AdHocOnTap

Two Databricks notebooks for AI-powered data analysis and report generation using LLM endpoints (Databricks Model Serving).

| Notebook | What it does |
|----------|-------------|
| **`ad_hoc_analyzer.py`** | Point at tables + PDFs. Detects entity keys (NPI), time grain, panel completeness. You describe what you want in plain English — it generates deterministic, reproducible Python code cells you run to produce slide-ready figures. |
| **`iterative_html_builder_v4.py`** | Builds rich, interactive HTML reports from PDF inputs using a 6-phase LLM pipeline (blueprint → scaffold → build → assemble → critique → repair). |

---

## Prerequisites

- **Databricks workspace** (Azure, AWS, or GCP) with a running cluster
- **Databricks Model Serving endpoint** — one of:
  - `databricks-qwen3-next-80b-a3b-instruct` (default)
  - `databricks-meta-llama-3-1-70b-instruct`
  - Or any OpenAI-compatible endpoint on your workspace
- **Unity Catalog tables** (or DataFrames) you want to analyze
- **PDFs** uploaded to `/dbfs/FileStore/pdf_sources/` (optional but recommended)

---

## Setup — Step by Step

### Option A: Databricks Repos (recommended)

This links the GitHub repo directly into your workspace. Changes you pull are reflected immediately.

1. **In Databricks**, click **Workspace** in the left sidebar
2. Navigate to your user folder (or any folder you want)
3. Click **⋮** → **Add** → **Git folder**
4. Paste the repo URL:
   ```
   https://github.com/jamesyoung93/AdHocOnTap.git
   ```
5. Branch: `main` → Click **Create Git folder**
6. Expand the **AdHocOnTap** folder that appears in your workspace
7. **Click on `ad_hoc_analyzer.py`** (or `iterative_html_builder_v4.py`) — **this opens it as a notebook**. The `# COMMAND ----------` markers automatically split it into cells. You do NOT need to create a separate notebook — the `.py` file IS the notebook.
8. Attach a running cluster, edit Cell 1, and click **Run All**

> **Common mistake:** Don't create a fresh empty notebook and try to import the code — just open the `.py` file directly from the repo folder. It's already a notebook.

### Option B: Import as notebooks

1. Download the `.py` files from this repo
2. In Databricks, click **Workspace** → navigate to your target folder
3. Click **⋮** → **Import**
4. Select the `.py` file → Databricks auto-detects the `# COMMAND ----------` markers and converts it to a multi-cell notebook
5. Open the imported notebook, edit Cell 1, Run All

### Option C: Copy-paste

1. Create a new **Python notebook** in Databricks
2. Copy the entire contents of the `.py` file
3. Paste into the first cell
4. **Close and re-open the notebook** — Databricks splits it into cells at each `# COMMAND ----------` marker on re-open

---

## Running the Ad Hoc Analyzer

### 1. Install dependencies (first run only)

Uncomment the first two lines in Cell 1 and **run Cell 1 alone** (not Run All yet):

```python
%pip install PyPDF2
dbutils.library.restartPython()
```

This installs the library and restarts the Python process. After the restart:
- Comment those two lines back out (or leave them — they just re-install each time)
- **You are still in the same notebook** — the `.py` file you opened. Don't switch to a different notebook.
- Now edit the rest of Cell 1 (tables, PDFs, prompt) and **Run All** from Cell 1 down.

### 2. Configure Cell 1

Edit the four config sections in Cell 1:

#### Tables
```python
TABLE_INPUTS = {
    "rx_claims":  "my_catalog.my_schema.rx_claims_2024",
    "targets":    "my_catalog.my_schema.hcp_targets",
    # "custom":   my_dataframe,  # DataFrames work too
}
```

Keys become SQL view names — use short, meaningful labels.

#### PDFs (optional)
```python
PDF_INPUTS = {
    "iqvia": PyPDF2.PdfReader("/dbfs/FileStore/pdf_sources/IQVIA_Report.pdf"),
}
```

Upload PDFs first:
- In Databricks, click **Catalog** → **Browse DBFS** (or use the CLI)
- Upload to `/FileStore/pdf_sources/`

#### User Prompt
```python
USER_PROMPT = """
Show me monthly TRx trends for the top 20 prescribers by volume,
broken down by product. Also compare my total TRx to the industry
benchmarks from the PDF.
"""
```

Describe what you want. Be specific about columns, groupings, and comparisons. The system delivers what you ask for **plus** suggestions based on the data structure.

#### Sanity Checks (optional)
```python
SANITY_CHECKS = [
    {"label": "Total TRx 2024",
     "sql": "SELECT SUM(trx) FROM rx_claims",
     "expected_range": [800_000, 1_200_000]},
]
```

### 3. Run All

Click **Run All** or run cells 1 through 11 in order. The pipeline:

| Phase | What happens |
|-------|-------------|
| **1. Profile** | PySpark-based profiling: schema, stats, entity key detection (NPI etc.), time grain detection, panel completeness, grouping columns, time-aware summary stats. Results are cached per `(table_address, schema)` — repeat runs against the same table skip this phase entirely. |
| **2. PDF Ingest** | Extracts text, chunks it, pulls out benchmark numbers and chart references |
| **3. Column Inference** | LLM matches your column names to domain concepts using PDF context (e.g. `trx` → "Total Prescriptions"). Also cached — only runs for tables that weren't a cache hit in Phase 1. |
| **4. Code Generation** | Breaks your prompt into specific visualizations, generates self-contained Python cells for each |
| **5. Sanity Checks** | Compares your data against PDF benchmarks and user-specified checks (PASS / WARNING / ALERT) |
| **6. Save** | Writes all generated code to a `.py` notebook file you can import |

### 4. Use the generated code

The output is printed as code blocks. For each one:

1. **Create a new cell** below the output (or in a new notebook)
2. **Paste the code block**
3. **Run it** — it produces a matplotlib figure

Or import the saved notebook directly:
- The path is printed at the end (e.g. `/dbfs/FileStore/generated_notebooks/...`)
- Download it or import it via the Databricks UI

### 5. Interactive follow-up (Cell 12)

After the pipeline runs, change `FOLLOW_UP_PROMPT` in Cell 12 and re-run to generate more code cells without re-profiling:

```python
FOLLOW_UP_PROMPT = """
Show me a histogram of TRx per prescriber,
and a box plot of NRx by specialty.
"""
```

---

## Running the HTML Builder

### 1. Install dependencies (same as above)

### 2. Configure Cell 1

```python
PROJECT_NAME = "My Report Title"

DESIGN_BRIEF = """
Describe the report content, structure, audience, and tone.
"""

pdf1 = PyPDF2.PdfReader("/dbfs/FileStore/pdf_sources/my_doc.pdf")
PDF_INPUTS = {"my_doc": pdf1}
```

### 3. Run All (Cells 1–15)

The 6-phase pipeline runs automatically:
- **Blueprint** → LLM plans sections from PDFs
- **Scaffold** → LLM builds HTML/CSS/JS framework
- **Build** → LLM generates each section
- **Assemble** → combines into single HTML page
- **Critique** → LLM reviews for quality issues
- **Repair** → LLM fixes flagged sections

### 4. Preview

Uncomment the `displayHTML` line in Cell 15:
```python
displayHTML(state["assembled_html"])
```

The HTML file is also saved to `/dbfs/FileStore/html_reports/`.

---

## What the Analyzer Detects Automatically

The smart profiler goes beyond basic stats. Here's what it finds:

**Entity Keys** — Matches column names against known patterns (`npi`, `*_id`, `*_key`, `prescriber`, `physician`) and scores by cardinality and null rate. NPI (National Provider Identifier) gets the highest weight.

**Time Columns & Grain** — Catches native `DateType`/`TimestampType`, but also disguised formats: `YYYYMM` strings, `YYYY` integers, `*_dt` naming conventions. Samples distinct values and measures gaps to determine daily/weekly/monthly/quarterly/yearly granularity.

**Panel Completeness** — Cross-joins entity × time to compute fill rate. Reports: total cells expected vs actual, % of entities with complete history, per-period entity counts. Flags panels with churn or late-arriving entities.

**Grouping Columns** — Low-cardinality categoricals (2–50 distinct values, <30% null) suitable for `GROUP BY`, color, or facet in charts.

**Time-Aware Summary Stats** — Per-period aggregates for all numeric columns: sum, mean, min, max, stddev. Shows how metrics evolve, not just overall averages.

**Column Meaning Inference** — Sends column names + sample values + PDF excerpts to LLM. Returns domain-specific meanings (e.g. `trx` → "Total Prescriptions"), display labels for chart axes, and role classification (entity_id / time / metric / grouping).

---

## Context Cache

The analyzer caches each table's profile **and** its LLM-inferred column meanings to disk so repeat runs don't redo expensive work. The cache lives at `/dbfs/FileStore/ad_hoc_analyzer_cache/` and is keyed by `sha256(table_address + schema.json())` — if the table's schema changes, the cache auto-invalidates.

On a cached run you'll see `⚡ table_name (cached: …)` instead of the usual profiling output, and Phase 3 (column inference) is skipped entirely for those tables.

**Settings (Cell 1):**
```python
CONTEXT_CACHE_DIR     = "/dbfs/FileStore/ad_hoc_analyzer_cache"
FORCE_REBUILD_CONTEXT = False   # set True to bypass the cache for one run
```

**Manual cache management:**
```python
list_context_cache()    # show what's cached
clear_context_cache()   # wipe everything (call after rewriting a table in place)
```

If a table's data changes but its schema doesn't (e.g. an in-place overwrite), call `clear_context_cache()` or set `FORCE_REBUILD_CONTEXT=True` for that run.

---

## Profiling Speed on Wide Tables

A few knobs in Cell 1 control how the profiler handles large/wide tables:

```python
MAX_PROFILE_COLS    = 60         # cap columns per table (first N are profiled)
SAMPLE_FOR_STATS    = 500_000    # tables larger than this use a cached sample
                                 # for percentiles + top-value queries
PROFILE_PARALLELISM = 8          # driver threads for per-column top-value queries
```

Under the hood the profiler:
- Runs **all** null/distinct/min/max/mean/stddev aggregates in **one** Spark pass over the full table (using `approx_count_distinct` instead of exact, which is dramatically faster on wide tables).
- Batches every numeric column's percentiles into a **single** `approxQuantile` call.
- Fans out per-column string top-value queries across `PROFILE_PARALLELISM` driver threads.
- Caches a downsampled subset for the per-column heavy work when the table exceeds `SAMPLE_FOR_STATS` rows.
- Projects the panel down to `distinct(entity, time)` once and caches it before computing completeness — reduces 5–6 passes over the raw table to a few passes over a tiny one.

If profiling is still slow, the levers in order of impact are: lower `MAX_PROFILE_COLS`, lower `SAMPLE_FOR_STATS`, raise `PROFILE_PARALLELISM` (bounded by your driver cores).

---

## LLM Endpoint Configuration

Both notebooks default to `databricks-qwen3-next-80b-a3b-instruct`. To change:

```python
LLM_ENDPOINT_NAME = "databricks-meta-llama-3-1-70b-instruct"
```

The endpoint must be a Databricks Model Serving endpoint accessible via:
```python
WorkspaceClient().serving_endpoints.get_open_ai_client()
```

To verify your endpoint works, run in any Databricks notebook:
```python
from databricks.sdk import WorkspaceClient
client = WorkspaceClient().serving_endpoints.get_open_ai_client()
resp = client.chat.completions.create(
    model="databricks-qwen3-next-80b-a3b-instruct",
    messages=[{"role": "user", "content": "Say hello"}],
    max_tokens=50,
)
print(resp.choices[0].message.content)
```

---

## File Structure

```
AdHocOnTap/
├── README.md
├── .gitignore
├── ad_hoc_analyzer.py           # Smart profiler + code generator (12 cells)
└── iterative_html_builder_v4.py # HTML report builder (16 cells)
```

Both files use the `# Databricks Notebook Source` / `# COMMAND ----------` format that Databricks recognizes as multi-cell notebooks.

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `ModuleNotFoundError: PyPDF2` | Run `%pip install PyPDF2` then `dbutils.library.restartPython()` |
| `LLM endpoint not found` | Check `LLM_ENDPOINT_NAME` matches an active Model Serving endpoint in your workspace |
| `Table not found` | Verify the Unity Catalog path: `spark.table("catalog.schema.table").limit(1).show()` |
| `Permission denied on DBFS` | Ensure your cluster has access to `/dbfs/FileStore/`. Try: `dbutils.fs.ls("/FileStore/")` |
| JSON parse error from LLM | The LLM sometimes returns markdown fences around JSON — the code strips these automatically. If it persists, try a different `LLM_ENDPOINT_NAME` |
| Slow profiling | See [Profiling Speed on Wide Tables](#profiling-speed-on-wide-tables). Lower `MAX_PROFILE_COLS` and `SAMPLE_FOR_STATS`, raise `PROFILE_PARALLELISM`. Repeat runs against the same table are nearly instant thanks to the [Context Cache](#context-cache). |
| Stale cached profile | The cache invalidates automatically on schema changes, but if a table is rewritten in place with the same schema, run `clear_context_cache()` or set `FORCE_REBUILD_CONTEXT = True` |
| Generated SQL fails | The interactive cell (Cell 12) lets you retry with different phrasing. Check that your table labels in `TABLE_INPUTS` match what you reference in the prompt |
