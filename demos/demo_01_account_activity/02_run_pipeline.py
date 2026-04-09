"""
Demo 1 — end-to-end local pipeline.

  data CSVs  →  profile + LLM analysis  →  PowerPoint deck  →  zip archive

Steps:
  1. Loads the synthetic account-activity CSVs from ./data/
  2. Profiles all three tables with the local pandas analyzer
  3. Calls the LLM (auto-detected from env vars) for column inference,
     code-cell generation, and the deck-narrative insights
  4. Builds a deck via deck-builder's direct Python API
  5. Packs everything into a downloadable .zip in ./output/

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
from shared.data_qc           import run_qc, print_qc_report, has_blockers
from shared.analyzer_to_deck  import build_deck_from_results
from shared.archive           import build_archive


PROJECT_NAME = "Account Activity Quarterly Review"
DATA_DIR     = Path(__file__).parent / "data"
OUTPUT_DIR   = Path(__file__).parent / "output"

# What you'd ask a senior analyst to look into:
USER_PROMPT = """
Show me which regions and tiers have the strongest engagement trends over the
last few quarters.  Highlight any regions where panel coverage is dropping,
and any tiers where the satisfaction score is consistently below 7.  Compare
actual quarterly engagements against the per-region targets where possible.
""".strip()

DECK_STYLE = "executive_dark"   # executive_dark | corporate_clean | accent_green | neutral

# QC behavior — set False to halt the pipeline when BLOCKERs are found.
# (Demo 1's data is clean, so this never trips here. Demo 2 has injected issues.)
FORCE_CONTINUE_PAST_BLOCKERS = True


def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    # ── 1. Load data ──
    print(f"[load] reading CSVs from {DATA_DIR}/")
    if not (DATA_DIR / "account_activity.csv").exists():
        print("[err] no data found.  Run `python 01_generate_data.py` first.")
        sys.exit(1)

    tables = {
        "activity": pd.read_csv(DATA_DIR / "account_activity.csv"),
        "accounts": pd.read_csv(DATA_DIR / "account_master.csv"),
        "targets":  pd.read_csv(DATA_DIR / "region_targets.csv"),
    }
    for label, df in tables.items():
        print(f"       {label:<10s} {len(df):>7,} rows × {len(df.columns)} cols")

    # ── 2. LLM client ──
    print("\n[llm]  initializing client (auto-detected from env vars)...")
    client = LLMClient()
    print(f"       provider={client.provider}  model={client.model}")
    call_llm = get_call_llm_fn(client)

    # ── 3. Profile + LLM analysis ──
    print("\n" + "=" * 64)
    results = analyze(tables, user_prompt=USER_PROMPT, llm_call=call_llm)
    print("=" * 64)

    # ── 4. AI-assisted QC + FAIR scoring ──
    qc = run_qc(tables, results, llm_call=call_llm)
    print_qc_report(qc)

    if has_blockers(qc) and not FORCE_CONTINUE_PAST_BLOCKERS:
        print("\n[halt] FORCE_CONTINUE_PAST_BLOCKERS=False — pausing pipeline.")
        print("       Fix the data and re-run, or set FORCE_CONTINUE_PAST_BLOCKERS=True to override.")
        sys.exit(2)

    # ── 5. Build the deck ──
    print("\n[deck] building PowerPoint via deck-builder...")
    deck_path = OUTPUT_DIR / "deck.pptx"
    try:
        from slide_engine.pptx_builder import PptxBuilder
        deck = build_deck_from_results(results, qc=qc, project_name=PROJECT_NAME, style=DECK_STYLE)
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

    # ── 6. Archive ──
    print("\n[bundle] packing archive...")
    zip_path = build_archive(
        output_dir   = str(OUTPUT_DIR),
        project_name = PROJECT_NAME,
        results      = results,
        qc           = qc,
        deck_path    = str(deck_path) if deck_path else None,
    )

    print(f"\n[done] {zip_path}")
    print(f"       open the .zip to see profiles, generated code, and the deck.")


if __name__ == "__main__":
    main()
