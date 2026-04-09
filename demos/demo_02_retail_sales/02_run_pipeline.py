"""
Demo 2 — end-to-end local pipeline for the retail store sales dataset.

  data CSVs  →  profile + LLM analysis  →  PowerPoint deck  →  zip archive

Run:
  pip install -r ../requirements.txt
  export ANTHROPIC_API_KEY=...   # or GEMINI_API_KEY / GROQ_API_KEY / OPENAI_API_KEY
  python 01_generate_data.py
  python 02_run_pipeline.py
"""
from __future__ import annotations
import sys
from pathlib import Path

import pandas as pd

# allow `from shared.* import ...`
sys.path.insert(0, str(Path(__file__).parents[1]))

from shared.local_analyzer    import analyze
from shared.llm_client        import LLMClient, get_call_llm_fn
from shared.analyzer_to_deck  import build_deck_from_results
from shared.archive           import build_archive


PROJECT_NAME = "Retail Store Sales — Annual Review"
DATA_DIR     = Path(__file__).parent / "data"
OUTPUT_DIR   = Path(__file__).parent / "output"

USER_PROMPT = """
Show me top-line revenue trends across regions and store formats over the
last 50 weeks.  Highlight which categories drive the biggest YoY growth and
which are slipping.  Surface any stores that have been closing or only
recently opened.  Are there seasonal patterns?  Compare promo vs non-promo
weeks for the highest-revenue categories.
""".strip()

DECK_STYLE = "corporate_clean"  # executive_dark | corporate_clean | accent_green | neutral


def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    # ── 1. Load data ──
    print(f"[load] reading CSVs from {DATA_DIR}/")
    if not (DATA_DIR / "store_week_sales.csv").exists():
        print("[err] no data found.  Run `python 01_generate_data.py` first.")
        sys.exit(1)

    tables = {
        "sales":      pd.read_csv(DATA_DIR / "store_week_sales.csv"),
        "stores":     pd.read_csv(DATA_DIR / "store_master.csv"),
        "categories": pd.read_csv(DATA_DIR / "category_master.csv"),
    }
    for label, df in tables.items():
        print(f"       {label:<11s} {len(df):>7,} rows × {len(df.columns)} cols")

    # ── 2. LLM client ──
    print("\n[llm]  initializing client (auto-detected from env vars)...")
    client = LLMClient()
    print(f"       provider={client.provider}  model={client.model}")
    call_llm = get_call_llm_fn(client)

    # ── 3. Run the pipeline ──
    print("\n" + "=" * 64)
    results = analyze(tables, user_prompt=USER_PROMPT, llm_call=call_llm)
    print("=" * 64)

    # ── 4. Build the deck ──
    print("\n[deck] building PowerPoint via deck-builder...")
    deck_path = OUTPUT_DIR / "deck.pptx"
    try:
        from slide_engine.pptx_builder import PptxBuilder
        deck = build_deck_from_results(results, project_name=PROJECT_NAME, style=DECK_STYLE)
        PptxBuilder().build(deck, str(deck_path))
        print(f"       wrote {deck_path}")
    except ImportError:
        print("       deck-builder not installed.")
        print("       pip install python-pptx pyyaml matplotlib pillow")
        print("       pip install --no-deps https://github.com/jamesyoung93/deck-builder/raw/master/deck_builder-0.2.0-py3-none-any.whl")
        deck_path = None
    except Exception as e:
        print(f"       deck build failed: {e}")
        deck_path = None

    # ── 5. Archive ──
    print("\n[bundle] packing archive...")
    zip_path = build_archive(
        output_dir   = str(OUTPUT_DIR),
        project_name = PROJECT_NAME,
        results      = results,
        deck_path    = str(deck_path) if deck_path else None,
    )

    print(f"\n[done] {zip_path}")
    print(f"       open the .zip to see profiles, generated code, and the deck.")


if __name__ == "__main__":
    main()
