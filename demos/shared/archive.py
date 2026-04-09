"""
Pack analyzer results + deck into a single .zip download bundle.

Contents:
  manifest.json         — provenance: prompt, tables, timestamp, repo SHA
  profiles/<table>.json — full profiles per table
  col_map.json          — LLM column-meaning inferences
  generated_code/       — one .py per code cell
  all_cells.py          — all generated cells in a single runnable file
  deck.pptx             — the slide deck
  figures/              — any extra files passed via extra_files
"""
from __future__ import annotations
import json
import os
import re
import shutil
import subprocess
import zipfile
from datetime import datetime
from pathlib import Path


def build_archive(output_dir: str,
                  project_name: str,
                  results: dict,
                  deck_path: str | None = None,
                  extra_files: list | None = None) -> str:
    """Build a timestamped folder + .zip bundle. Returns the .zip path."""
    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    bundle_name = f"{re.sub(r'[^A-Za-z0-9_-]', '_', project_name)}_{timestamp}"
    output_dir  = Path(output_dir)
    bundle_root = output_dir / bundle_name
    bundle_root.mkdir(parents=True, exist_ok=True)

    # ── 1. Manifest with provenance ──
    manifest = {
        "project_name":      project_name,
        "generated_at_utc":  datetime.utcnow().isoformat() + "Z",
        "user_prompt":       results.get("user_prompt", ""),
        "tables":            list(results.get("profiles", {}).keys()),
        "code_cell_count":   len(results.get("code_cells", [])),
        "insight_count":     len(results.get("insights", [])),
        "git_sha":           _git_sha(),
    }
    (bundle_root / "manifest.json").write_text(
        json.dumps(manifest, indent=2, default=str), encoding="utf-8"
    )

    # ── 2. Per-table profiles ──
    profiles_dir = bundle_root / "profiles"
    profiles_dir.mkdir(exist_ok=True)
    for label, p in results.get("profiles", {}).items():
        safe = re.sub(r'[^A-Za-z0-9_-]', '_', label)
        (profiles_dir / f"{safe}.json").write_text(
            json.dumps(p, indent=2, default=str), encoding="utf-8"
        )

    # ── 3. Column map ──
    if results.get("col_map"):
        (bundle_root / "col_map.json").write_text(
            json.dumps(results["col_map"], indent=2, default=str), encoding="utf-8"
        )

    # ── 4. Generated code cells ──
    cells = results.get("code_cells", [])
    if cells:
        code_dir = bundle_root / "generated_code"
        code_dir.mkdir(exist_ok=True)
        combined = [
            "# AdHocOnTap generated code",
            f"# Project: {project_name}",
            f"# Generated: {datetime.utcnow().isoformat()}Z",
            f"# User prompt: {results.get('user_prompt','').strip()}",
            "",
            "import pandas as pd",
            "import matplotlib.pyplot as plt",
            "import numpy as np",
            "",
            "# NOTE: load your dataframes here before running the cells, e.g.:",
            "# activity = pd.read_csv('data/activity.csv')",
            "",
        ]
        for i, cell in enumerate(cells):
            title_safe = re.sub(r'[^A-Za-z0-9_-]', '_',
                                cell.get('title', f'cell_{i+1}'))[:60]
            fn = f"{i+1:02d}_{title_safe}.py"
            content = (
                f"# {cell.get('title','')}\n"
                f"# {cell.get('purpose','')}\n\n"
                f"{cell.get('code','')}\n"
            )
            (code_dir / fn).write_text(content, encoding="utf-8")
            combined += [
                f"# {'─'*60}",
                f"# [{i+1}] {cell.get('title','')}",
                f"# {cell.get('purpose','')}",
                f"# {'─'*60}",
                "",
                cell.get('code', ''),
                "",
            ]
        (bundle_root / "all_cells.py").write_text(
            "\n".join(combined), encoding="utf-8"
        )

    # ── 5. Insights (deck-narrative bullets) ──
    if results.get("insights"):
        (bundle_root / "insights.json").write_text(
            json.dumps(results["insights"], indent=2, default=str), encoding="utf-8"
        )

    # ── 6. The deck itself ──
    if deck_path and Path(deck_path).exists():
        shutil.copy2(deck_path, bundle_root / "deck.pptx")

    # ── 7. Any extra files (matplotlib figures, etc.) ──
    if extra_files:
        extras_dir = bundle_root / "figures"
        extras_dir.mkdir(exist_ok=True)
        for f in extra_files:
            fp = Path(f)
            if fp.exists():
                shutil.copy2(fp, extras_dir / fp.name)

    # ── 8. Zip everything ──
    zip_path = output_dir / f"{bundle_name}.zip"
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for fp in bundle_root.rglob("*"):
            if fp.is_file():
                zf.write(fp, fp.relative_to(output_dir))

    print(f"\n[archive] {zip_path}")
    print(f"  contents:")
    print(f"    manifest.json")
    print(f"    profiles/                 ({len(results.get('profiles', {}))} files)")
    if results.get("col_map"):
        print(f"    col_map.json")
    if cells:
        print(f"    generated_code/           ({len(cells)} cells)")
        print(f"    all_cells.py")
    if results.get("insights"):
        print(f"    insights.json")
    if deck_path:
        print(f"    deck.pptx")
    if extra_files:
        print(f"    figures/                  ({len(extra_files)} files)")

    return str(zip_path)


def _git_sha() -> str | None:
    """Best-effort git SHA of the repo this script lives in. Returns None if unavailable."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).parent,
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            return result.stdout.strip()[:12]
    except Exception:
        pass
    return None
