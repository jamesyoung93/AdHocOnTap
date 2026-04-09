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

## Run it

```bash
# from the repo root
pip install -r demos/requirements.txt
pip install --no-deps https://github.com/jamesyoung93/deck-builder/raw/master/deck_builder-0.2.0-py3-none-any.whl

export GEMINI_API_KEY=...   # or any other supported provider

cd demos/demo_02_retail_sales
python 01_generate_data.py        # writes the CSVs to ./data/
python 02_run_pipeline.py         # profile → LLM → deck → archive
```

## What the analyzer should detect (without being told)

- **Entity key** = `store_id` (and on the panel table, the de facto grain is `store_id × category_id × week_start`)
- **Time column** = `week_start` (auto-detected as `YYYY-MM-DD` daily-like → `irregular(median_gap=7d)` which the analyzer reads as weekly)
- **Groupings** = `region` (5), `store_format` (3), `category_id` (8), `promo_flag` (2)
- **Panel** = a high-fill but non-100% panel because of late-opened and churned stores

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
