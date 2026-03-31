# Databricks notebook source
# ============================================================
#  Iterative HTML Builder v4 — Databricks Agent Framework
# ============================================================
#  Builds complex, multi-section HTML reports from PDF inputs
#  using Databricks Model Serving endpoints.
#
#  QUICK START:
#    1. Edit USER CONFIG in Cell 1 (project name, design brief, PDFs)
#    2. Run All (Cells 1-15)
#    3. Preview with displayHTML(state["assembled_html"])
#
#  The design system (colors, typography, component classes) is
#  hardcoded from your established report style. The LLM builds
#  content within this system — it doesn't invent a new look.
# ============================================================


# COMMAND ----------
# CELL 1: USER CONFIG + Read PDFs
# ============================================================
# ✏️  THIS IS THE ONLY CELL YOU EDIT FOR EACH NEW REPORT.
# ============================================================

# %pip install PyPDF2
# dbutils.library.restartPython()

import PyPDF2

# ============================================================
# ✏️  PROJECT NAME — what this build is called
# ============================================================
PROJECT_NAME = "Pharma AI Innovation Report"

# ============================================================
# ✏️  DESIGN BRIEF — the one-off prompt for THIS specific report.
#     Describe what the report should cover, how it should be
#     structured, what data to emphasize, who the audience is.
#     The visual style is already locked in (see SYSTEM_PROMPT).
#     This brief controls CONTENT and STRUCTURE only.
# ============================================================
DESIGN_BRIEF = """
Create a comprehensive, interactive HTML report covering AI innovation
in pharmaceutical commercial operations. Structure around 4 pillars:
  1. Customer Strategy & Segmentation (AI-driven targeting, propensity)
  2. Integrated Customer Experience (omnichannel orchestration)
  3. Customer-Facing Team Impact (field analytics, next-best-action)
  4. Foundational Capabilities (data infrastructure, MLOps, governance)

Audience: Senior commercial leadership (VP+, C-suite).
Tone: Authoritative, data-rich, McKinsey-style with clear recommendations.
Include stat cards with REAL metrics from the PDFs (not placeholder numbers).
Include maturity assessment grids, architecture diagrams (CSS-drawn),
collapsible deep-dive sections, and tabbed panels for comparing approaches.
Target: ~1200-1500 lines total, 8-12 sections.
"""

# ============================================================
# ✏️  PDFs — read your source documents here.
#     Add/remove as needed. Labels are just for logging.
# ============================================================
pdf1 = PyPDF2.PdfReader("/dbfs/FileStore/pdf_sources/Precision_AQ_Global_Trends_Report_2024.pdf")
pdf2 = PyPDF2.PdfReader("/dbfs/FileStore/pdf_sources/2025-zs-biopharma-commercialization-report.pdf")
pdf3 = PyPDF2.PdfReader("/dbfs/FileStore/pdf_sources/IQVIA_AI_in_LS_Commercialization_2025.pdf")
pdf4 = PyPDF2.PdfReader("/dbfs/FileStore/pdf_sources/Top_Software_Tools_Pharma_Commercial_Analytics_2025.pdf")

PDF_INPUTS = {
    "precision_aq": pdf1,
    "zs_biopharma": pdf2,
    "iqvia": pdf3,
    "software_tools": pdf4,
}

# ============================================================
# ✏️  LLM ENDPOINT — change to swap models
# ============================================================
LLM_ENDPOINT_NAME = "databricks-qwen3-next-80b-a3b-instruct"
# LLM_ENDPOINT_NAME = "databricks-meta-llama-3-1-70b-instruct"

MAX_OUTPUT_TOKENS = 4096
TEMPERATURE = 0.2

# ============================================================
# Extract text from PDFs (no edits needed below this line)
# ============================================================
def extract_text_from_reader(reader, label=""):
    pages = []
    for i, page in enumerate(reader.pages):
        text = page.extract_text() or ""
        pages.append(f"--- PAGE {i + 1} ---\n{text}")
    return "\n\n".join(pages)

pdf_texts = {}
for name, reader in PDF_INPUTS.items():
    pdf_texts[name] = extract_text_from_reader(reader, name)
    print(f"  ✅ {name}: {len(reader.pages)} pages, {len(pdf_texts[name]):,} chars")

print(f"\n  Total: {len(pdf_texts)} PDFs, {sum(len(t) for t in pdf_texts.values()):,} chars")


# COMMAND ----------
# CELL 2: Imports
# COMMAND ----------

import json
import os
import re
import time
import hashlib
from datetime import datetime
from typing import List, Dict, Any, Optional
import warnings
warnings.filterwarnings("ignore")


# COMMAND ----------
# CELL 3: LLM Client
# COMMAND ----------

from openai import OpenAI
from databricks.sdk import WorkspaceClient

LLM_CLIENT = WorkspaceClient().serving_endpoints.get_open_ai_client()

def call_llm(system_prompt: str, user_prompt: str) -> str:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore",
            message="PydanticSerializationUnexpectedValue")
        response = LLM_CLIENT.chat.completions.create(
            model=LLM_ENDPOINT_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=MAX_OUTPUT_TOKENS,
            temperature=TEMPERATURE,
        )
    return response.choices[0].message.content


# COMMAND ----------
# CELL 4: Text chunking and context utilities
# COMMAND ----------

def chunk_text(text, chunk_size=3000):
    paragraphs = text.split("\n\n")
    chunks, current = [], ""
    for para in paragraphs:
        if len(current) + len(para) > chunk_size and current:
            chunks.append(current.strip())
            current = para
        else:
            current += "\n\n" + para
    if current.strip():
        chunks.append(current.strip())
    return chunks

def chunk_all_pdfs(pdf_texts):
    all_chunks = []
    for name, text in pdf_texts.items():
        c = chunk_text(text)
        all_chunks.extend(c)
        print(f"  {name}: {len(c)} chunks")
    print(f"  Total: {len(all_chunks)} chunks")
    return all_chunks

def get_relevant_chunks(chunks, keywords, top_k=3):
    kw = [k.lower() for k in keywords]
    scored = [(sum(1 for k in kw if k in c.lower()), c) for c in chunks]
    scored = [(s, c) for s, c in scored if s > 0]
    scored.sort(key=lambda x: -x[0])
    sel = [c for _, c in scored[:top_k]]
    return "\n---\n".join(sel) if sel else chunks[0] if chunks else ""

def build_status_map(sections):
    icons = {"pending": "⬜", "in_progress": "🔄", "built": "✅",
             "critique_flagged": "⚠️", "repaired": "🔧", "approved": "✅"}
    return "\n".join(
        f"{icons.get(s['status'],'❓')} [{s['order']:2d}] {s['title']:<40s} "
        f"{(s.get('html_fragment','').count(chr(10))+1 if s.get('html_fragment') else 0):>4d}L  id={s['id']}"
        for s in sorted(sections, key=lambda x: x["order"])
    )

def summarize_built_sections(sections):
    out = []
    for s in sections:
        if s["status"] in ("built","repaired","approved") and s.get("html_fragment"):
            h = s["html_fragment"]
            out.append(f"  {s['title']} ({s['id']}): {h.count(chr(10))+1}L, "
                       f"{h.count('<div')} divs, {len(re.findall(r'<h[1-6]',h))} headings")
    return "\n".join(out) if out else "  (none yet)"

def make_pdf_summary(chunks, n=400):
    return "\n".join(
        f"[Chunk {i+1}] {c[:n].replace(chr(10),' ').strip()}..."
        for i, c in enumerate(chunks[:40])
    )

def extract_scaffold_classes(scaffold):
    return ", ".join(sorted(set(re.findall(r'\.([a-zA-Z][\w-]*)\s*\{', scaffold)))[:50])

def strip_markdown_fences(text):
    text = text.strip()
    text = re.sub(r"^```\w*\n?", "", text)
    text = re.sub(r"\n?```$", "", text)
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL)
    return text.strip()

# ---- Chunk the PDFs ----
print("📄 Chunking PDF text...")
pdf_chunks = chunk_all_pdfs(pdf_texts)
pdf_summary = make_pdf_summary(pdf_chunks)


# COMMAND ----------
# CELL 5: State management
# COMMAND ----------

STATE_DIR = "/dbfs/FileStore/html_builder_state"

def new_project(name):
    pid = hashlib.md5(f"{name}-{datetime.utcnow().isoformat()}".encode()).hexdigest()[:12]
    os.makedirs(STATE_DIR, exist_ok=True)
    state = {
        "project_id": pid, "project_name": name, "phase": "init",
        "created_at": datetime.utcnow().isoformat(), "updated_at": "",
        "llm_endpoint": LLM_ENDPOINT_NAME,
        "pdf_chunks": [], "pdf_summary": "", "design_brief": "",
        "sections": [], "css_block": "", "js_block": "", "html_head": "",
        "total_rounds": 0, "critique_round": 0,
        "assembled_html": "", "is_complete": False,
    }
    save_state(state)
    print(f"  Project created: {pid}")
    return state

def save_state(state):
    state["updated_at"] = datetime.utcnow().isoformat()
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(os.path.join(STATE_DIR, f"{state['project_id']}.json"), "w") as f:
        json.dump(state, f, indent=2, default=str)

def load_state(pid):
    with open(os.path.join(STATE_DIR, f"{pid}.json")) as f:
        return json.load(f)

def list_projects():
    os.makedirs(STATE_DIR, exist_ok=True)
    out = []
    for f in os.listdir(STATE_DIR):
        if f.endswith(".json"):
            with open(os.path.join(STATE_DIR, f)) as fh:
                s = json.load(fh)
                out.append({"id": s["project_id"], "name": s["project_name"],
                            "phase": s["phase"], "sections": len(s.get("sections",[])),
                            "updated": s.get("updated_at","")})
    return out


# COMMAND ----------
# CELL 6: Design system and prompt templates
# ============================================================
# TWO LAYERS:
#   STYLE_SYSTEM — the visual DNA (colors, fonts, theming, transitions).
#                  Same across ALL reports. Edit only to rebrand.
#   COMPONENT_LIBRARY — the full set of available components.
#                  The LLM picks which ones to use based on DESIGN_BRIEF.
#                  You never need to edit this — the brief steers selection.
#
# For per-report customization, edit DESIGN_BRIEF in Cell 1.
# ============================================================

STYLE_SYSTEM = """
COLOR PALETTE (CSS custom properties — use these exactly):
  --navy: #1a1a2e          (base dark background)
  --navy-light: #232342    (sidebar, header, secondary bg)
  --navy-mid: #2d2d50      (tertiary bg, table headers)
  --gold: #d4a843          (primary accent — headings, borders, highlights)
  --gold-light: #e8c97a    (gradient endpoints)
  --blue: #4a90d9          (secondary accent — subheadings, links)
  --blue-light: #6ba8f0    (hover states)
  --green: #2ecc71         (success, positive, action items)
  --red: #e74c3c           (warnings, negative, blockers)
  --orange: #f39c12        (caution, mid-priority)
  --purple: #9b59b6        (innovation, future-state)
  --off-white: #f0f0f5     (dark mode text-primary)
  --gray-300: #d0d0dd      (text-secondary)
  --gray-400: #9999aa      (text-muted, labels)

THEMING:
  [data-theme="dark"] — navy backgrounds, off-white text, rgba card borders
  [data-theme="light"] — #f5f5fa bg, #1a1a2e text, solid card borders
  All transitions: 0.3s cubic-bezier(0.4, 0, 0.2, 1)

TYPOGRAPHY:
  Font: Inter (300-900 weights) via Google Fonts
  h2: 1.7rem, weight 800, gold gradient (background-clip: text), gold bottom border
  h3: 1.25rem, weight 700, color var(--blue-light)
  h4: 1.02rem, weight 600
  Body: 0.9rem, line-height 1.7, color var(--text-secondary)
  Labels/tags: 0.68-0.78rem, uppercase, letter-spacing 0.04-0.08em

LAYOUT:
  .sidebar — fixed left, 290px, grouped nav with .sidebar-label headings
  .header — fixed top, 60px, backdrop-filter blur, theme toggle button
  .main — margin-left sidebar, max-width 1100px, 3rem padding
  .section — 3.5rem margin-bottom, scroll-margin-top offset
  #progress-bar — fixed top 3px, scroll-linked width, gradient accent

RESPONSIVE:
  @media (max-width: 768px): hide sidebar, full-width main
  @media print: hide chrome, white bg, expand collapsed/tabbed content

INTERACTIVITY (vanilla JS):
  Theme toggle via data-theme + localStorage
  Progress bar via scroll listener
  Sidebar active state via IntersectionObserver
  Collapsible sections via click toggle
  Tabbed panels via click swap .active
  Smooth scroll on anchor clicks"""

COMPONENT_LIBRARY = """
AVAILABLE COMPONENTS — use whichever fit the content. Not all are needed.

CORE (almost always used):
  .card — bg-card, 12px radius, 1.5rem padding, hover lift+shadow
  .card-grid — CSS grid, auto-fit minmax(280px,1fr), 1.5rem gap
  .stat-card — centered metric card: .stat-number (2.4rem/900/gold) + .stat-label (uppercase/muted)
  .callout — left-border accent box. Variants: .callout-gold, -blue, -green, -red, -purple
             Inner: .callout-title (bold) + <p> content
  table — full width, th=gold uppercase on tertiary bg, tr:hover highlight

NAVIGATION & STRUCTURE:
  .tab-container > .tab-buttons > .tab-btn + .tab-panel — tabbed views, gold active state
  .collapsible (h3/h4) + .collapsible-content.hidden — expandable deep-dives

CATEGORICAL:
  .tag — inline pill. Variants: .tag-quick(green), .tag-strategic(blue),
         .tag-horizon(purple), .tag-novel(orange). Or create domain-appropriate variants.

ASSESSMENT:
  .maturity-bar — flex row of .maturity-level blocks (colored 1-5 scale:
         .filled-1=red, .filled-2=orange, .filled-3=gold, .filled-4=blue, .filled-5=green, .empty=tertiary)

DIAGRAMS:
  .arch-diagram — flex-wrap container for .arch-box nodes + .arch-arrow connectors
  .timeline — vertical left-border gradient with .timeline-item + .timeline-date markers

EVIDENCE & ACTIONS:
  .evidence — tertiary bg, .evidence-title (gold uppercase) + bullet list
  .action-box — green-tinted bg, .action-title (green uppercase) + ordered list
  .limitations — red-tinted bg, red border, bullet list of caveats

REFERENCES:
  .ref — superscript [N] link, blue, hover gold
  .ref-block — citation card with source details
  .refs-inline — toggle-able inline reference display"""


def build_system_prompt(design_brief=""):
    """
    Assemble the system prompt from style + components + brief context.
    The brief tells the LLM what KIND of report, so it picks
    appropriate components without being forced into a specific workflow.
    """
    brief_context = ""
    if design_brief.strip():
        brief_context = f"""

=== REPORT CONTEXT ===
{design_brief.strip()}

Use this context to choose which components from the library are appropriate.
For example: a data-heavy report needs .stat-card and tables; a strategic
recommendations report needs .callout and .action-box; a technical architecture
doc needs .arch-diagram and .timeline. Don't force components that don't fit."""

    return f"""You are an expert front-end developer building rich,
interactive single-file HTML reports in an established design system.

=== OUTPUT RULES ===
- Output ONLY raw HTML/CSS/JS — no markdown fences, no explanations
- Stay UNDER 280 lines per response (hard output limit)
- No frameworks — vanilla JS only, DOMContentLoaded pattern

=== VISUAL STYLE (mandatory — do not deviate) ===
{STYLE_SYSTEM}

=== COMPONENT LIBRARY (use what fits the content) ===
{COMPONENT_LIBRARY}
{brief_context}"""


# Build the prompt for this project
SYSTEM_PROMPT = build_system_prompt(DESIGN_BRIEF)


# ---- BLUEPRINT PROMPT ----
def make_blueprint_prompt(pdf_summary, design_brief):
    return f"""Analyze these PDF source materials and plan an interactive HTML report.

PDF CONTENT OVERVIEW:
{pdf_summary[:6000]}

DESIGN BRIEF:
{design_brief}

Plan sections that each fit in ~200-280 lines of HTML. Split large topics.
Group sidebar nav into labeled categories that make sense for this content.

Output ONLY valid JSON (no markdown, no explanation):
{{
  "report_title": "...",
  "total_estimated_sections": N,
  "sidebar_groups": [
    {{"label": "Group Name", "section_ids": ["section-xxx", ...]}}
  ],
  "sections": [
    {{
      "id": "section-xxx",
      "title": "Human Readable Title",
      "order": 0,
      "description": "Specific content: what metrics, data points, comparisons to include",
      "estimated_lines": 200,
      "html_tag_hint": "section.section",
      "depends_on": [],
      "data_keywords": ["keyword1", "keyword2"],
      "suggested_components": ["stat-card", "callout", "table"]
    }}
  ]
}}"""


# ---- SCAFFOLD PROMPT ----
def make_scaffold_prompt(blueprint):
    section_list = "\n".join(f"  - {s['id']}: {s['title']}" for s in blueprint["sections"])

    # Detect which components sections plan to use
    all_components = set()
    for s in blueprint.get("sections", []):
        all_components.update(s.get("suggested_components", []))

    # Build sidebar groups if provided
    sidebar_groups = blueprint.get("sidebar_groups", [])
    if sidebar_groups:
        sidebar_nav = "\n".join(
            f'    <div class="sidebar-section">\n'
            f'      <div class="sidebar-label">{g["label"]}</div>\n'
            + "\n".join(f'      <a href="#{sid}">{next((s["title"] for s in blueprint["sections"] if s["id"]==sid), sid)}</a>'
                        for sid in g["section_ids"])
            + '\n    </div>'
            for g in sidebar_groups
        )
    else:
        sidebar_nav = "\n".join(
            f'    <a href="#{s["id"]}">{s["title"]}</a>'
            for s in blueprint["sections"]
        )

    return f"""Build the complete HTML scaffold for this report.
Use the EXACT visual style from the system prompt — same CSS variables,
same class patterns. Do NOT invent new color schemes or fonts.

REPORT TITLE: {blueprint['report_title']}
SECTIONS:
{section_list}

COMPONENTS THAT SECTIONS WILL USE: {', '.join(sorted(all_components)) if all_components else '(standard set)'}

The scaffold must include:
1. <head> with Inter font import and complete CSS for all components above
2. #progress-bar div
3. .header with title and theme toggle button
4. .sidebar with this nav structure:
{sidebar_nav}
5. <main class="main"> with <!-- SECTION_PLACEHOLDER --> comment
6. <script> with: theme toggle, progress bar, IntersectionObserver,
   collapsibles, tab switching, smooth scroll

CRITICAL: CSS must define ALL component classes that sections reference.
Include core classes (.card, .card-grid, .stat-card, .callout variants,
table styles) PLUS any specialized ones sections need ({', '.join(sorted(all_components))}).

STAY UNDER 280 LINES. Output ONLY the HTML code."""


# ---- SECTION BUILD PROMPT ----
def make_section_prompt(section, relevant_content, status_map,
                        built_summary, scaffold_classes):
    deps = ""
    if section.get("depends_on"):
        deps = f"\nDEPENDS ON: {', '.join(section['depends_on'])}"

    components = section.get("suggested_components", [])
    component_hint = ""
    if components:
        component_hint = f"\nSUGGESTED COMPONENTS: {', '.join(components)}"

    return f"""Build ONE section of the HTML report.

SECTION: "{section['title']}" (id: {section['id']}, order: {section['order']})
DESCRIPTION: {section['description']}
{deps}{component_hint}

RELEVANT SOURCE DATA (use real numbers/facts from this, not placeholders):
{relevant_content[:4000]}

BUILD PROGRESS:
{status_map}

ALREADY BUILT (structural summary):
{built_summary}

AVAILABLE CSS CLASSES (from scaffold):
{scaffold_classes}

REQUIREMENTS:
- Start with <section id="{section['id']}" class="section">
- Open with .section-header: h2 (gold gradient via background-clip) + .section-subtitle
- Use components from the scaffold that fit this content
- Data/metrics must be REAL from the source content above — not generic placeholders
- Match the visual style: gold accents, navy backgrounds, Inter typography
- End with </section>
- STAY UNDER 280 LINES
- Output ONLY HTML code"""


# ---- CRITIQUE PROMPT ----
def make_critique_prompt(html_sample, status_map):
    return f"""Review this HTML report for quality issues.

SECTION LIST:
{status_map}

HTML SAMPLE:
{html_sample[:8000]}

CHECK:
1. STRUCTURAL — closing tags, nesting, orphaned elements
2. STYLE — do classes reference CSS that exists in the scaffold?
   Are CSS variables used correctly? (--gold, --bg-card, --text-primary, etc.)
3. CONTENT — real data or placeholder? Abrupt transitions? Empty sections?
4. NAV — sidebar links match section IDs?
5. INTERACTIVE — tabs have matching panels? Collapsibles have triggers?
6. CONSISTENCY — same heading pattern across sections?

Output ONLY a JSON array:
[{{"section_id":"...","severity":"high|medium|low","issue":"...","fix_hint":"..."}}]
If clean: []"""


# ---- REPAIR PROMPT ----
def make_repair_prompt(section):
    return f"""Repair this section to match the design system.

SECTION: "{section['title']}" (id: {section['id']})

CURRENT HTML:
{section['html_fragment']}

ISSUES:
{section['critique_notes']}

Fix all issues. Use CSS variables (--gold, --navy, --bg-card, etc.) and
component classes from the scaffold. Output the COMPLETE repaired section.
UNDER 280 lines. ONLY HTML code."""


# COMMAND ----------
# CELL 7: Assembly and helpers
# COMMAND ----------

def assemble_html(state):
    scaffold = state.get("css_block", "")
    fragments = []
    for s in sorted(state["sections"], key=lambda x: x["order"]):
        if s.get("html_fragment"):
            fragments.append(f"\n    <!-- === {s['title']} === -->")
            fragments.append(s["html_fragment"])
    section_html = "\n".join(fragments)
    if "<!-- SECTION_PLACEHOLDER" in scaffold:
        return re.sub(r"<!-- SECTION_PLACEHOLDER.*?-->", section_html, scaffold, flags=re.DOTALL)
    elif "</main>" in scaffold:
        return scaffold.replace("</main>", f"\n{section_html}\n  </main>")
    return scaffold.replace("</body>", f"\n{section_html}\n</body>")

def make_critique_sample(html):
    lines = html.split("\n")
    if len(lines) <= 200:
        return html
    head, tail = lines[:60], lines[-40:]
    middle = lines[60:-40]
    sampled = []
    for i, line in enumerate(middle):
        if "<section" in line or "</section>" in line or re.match(r"\s*<h[1-6]", line):
            sampled.extend(middle[max(0,i-2):min(len(middle),i+5)])
            sampled.append("  ...")
            if len("\n".join(sampled)) > 3000:
                break
    return "\n".join(head + ["\n...(sampled)...\n"] + sampled + ["\n...\n"] + tail)


# COMMAND ----------
# CELL 8: Dashboard
# COMMAND ----------

def show_dashboard(state):
    sections = state.get("sections", [])
    total = len(sections)
    built = sum(1 for s in sections if s["status"] in ("built","repaired","approved"))
    flagged = sum(1 for s in sections if s["status"] == "critique_flagged")
    pending = sum(1 for s in sections if s["status"] == "pending")
    pct = (built/total*100) if total else 0
    bar = "█"*int(pct/5) + "░"*(20-int(pct/5))
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║  {state['project_name']:<60s}║
║  ID: {state['project_id']}  Phase: {state['phase']:<20s}  LLM: {state.get('llm_endpoint','?')[:20]:<20s}║
╠══════════════════════════════════════════════════════════════╣
║  [{bar}] {pct:5.1f}%   Built:{built}/{total}  Flagged:{flagged}  Pending:{pending}        ║
╠══════════════════════════════════════════════════════════════╣""")
    icons = {"pending":"⬜","in_progress":"🔄","built":"✅","critique_flagged":"⚠️","repaired":"🔧","approved":"✅"}
    for s in sorted(sections, key=lambda x: x["order"]):
        ic = icons.get(s["status"],"❓")
        lc = (s.get("html_fragment","").count("\n")+1) if s.get("html_fragment") else 0
        print(f"║  {ic} [{s['order']:2d}] {s['title']:<40s} {lc:>4d}L  {s['status']:<14s}║")
    print(f"╚══════════════════════════════════════════════════════════════╝")


# COMMAND ----------
# CELL 9: === PHASE 1 — BLUEPRINT ===
# COMMAND ----------

state = new_project(PROJECT_NAME)
state["pdf_chunks"] = pdf_chunks
state["pdf_summary"] = pdf_summary
state["design_brief"] = DESIGN_BRIEF
save_state(state)

print("\n📋 Blueprint phase...")
bp_response = call_llm(SYSTEM_PROMPT, make_blueprint_prompt(pdf_summary, DESIGN_BRIEF))
bp_json = strip_markdown_fences(bp_response)

try:
    blueprint = json.loads(bp_json)
except json.JSONDecodeError:
    match = re.search(r'\{.*\}', bp_json, re.DOTALL)
    blueprint = json.loads(match.group()) if match else None
    if not blueprint:
        print(f"⚠️ Parse failed. Raw:\n{bp_json[:500]}")
        raise

state["sections"] = [
    {**s, "status":"pending", "html_fragment":"", "critique_notes":"", "build_attempts":0}
    for s in blueprint["sections"]
]
state["phase"] = "blueprint"
state["total_rounds"] = 1
save_state(state)

print(f"  {len(state['sections'])} sections planned:")
for s in state["sections"]:
    print(f"    [{s['order']:2d}] {s['title']} (~{s['estimated_lines']}L)")
print(f"  Project ID: {state['project_id']}")


# COMMAND ----------
# CELL 10: === PHASE 2 — SCAFFOLD ===
# COMMAND ----------

print("🏗️  Scaffold...")
scaffold_html = strip_markdown_fences(
    call_llm(SYSTEM_PROMPT, make_scaffold_prompt(blueprint))
)
state["css_block"] = scaffold_html
state["phase"] = "scaffold"
state["total_rounds"] += 1
save_state(state)
print(f"  {scaffold_html.count(chr(10))+1} lines, classes: {extract_scaffold_classes(scaffold_html)[:150]}...")


# COMMAND ----------
# CELL 11: === PHASE 3 — BUILD SECTIONS ===
# COMMAND ----------

print("🔨 Building sections...\n")
state["phase"] = "building"

for i, section in enumerate(state["sections"]):
    if section["status"] in ("built","repaired","approved"):
        print(f"  ⏭️  [{section['order']:2d}] {section['title']} — done")
        continue
    print(f"  🔄 [{section['order']:2d}] {section['title']}...", end=" ", flush=True)
    section["status"] = "in_progress"

    kw = section.get("data_keywords",[]) + section["title"].lower().split()
    prompt = make_section_prompt(
        section,
        get_relevant_chunks(pdf_chunks, kw, top_k=3),
        build_status_map(state["sections"]),
        summarize_built_sections(state["sections"]),
        extract_scaffold_classes(state.get("css_block",""))
    )
    html = strip_markdown_fences(call_llm(SYSTEM_PROMPT, prompt))
    if "<section" not in html.lower():
        html = f'<section id="{section["id"]}" class="section">\n{html}\n</section>'

    section["html_fragment"] = html
    section["status"] = "built"
    section["build_attempts"] += 1
    state["sections"][i] = section
    state["total_rounds"] += 1
    save_state(state)
    print(f"✅ {html.count(chr(10))+1} lines")
    time.sleep(2)

print(f"\n  Done. Rounds: {state['total_rounds']}")


# COMMAND ----------
# CELL 12: === PHASE 4 — ASSEMBLE ===
# COMMAND ----------

print("📦 Assembling...")
assembled = assemble_html(state)
state["assembled_html"] = assembled
state["phase"] = "assembled"
save_state(state)
print(f"  {assembled.count(chr(10))+1} lines, {len(assembled):,} chars")
# displayHTML(assembled)


# COMMAND ----------
# CELL 13: === PHASE 5 — CRITIQUE ===
# COMMAND ----------

print("🔍 Critique...")
crit_response = call_llm(
    "You are a senior front-end code reviewer. Output ONLY valid JSON.",
    make_critique_prompt(make_critique_sample(assembled), build_status_map(state["sections"]))
)
crit_json = strip_markdown_fences(crit_response)
try:
    issues = json.loads(crit_json)
except json.JSONDecodeError:
    m = re.search(r'\[.*\]', crit_json, re.DOTALL)
    issues = json.loads(m.group()) if m else []

print(f"  {len(issues)} issues:")
for iss in issues:
    print(f"    [{iss.get('severity','?').upper():>6}] {iss.get('section_id','?')}: {iss.get('issue','?')}")

for iss in issues:
    if iss.get("severity") in ("high","medium"):
        for s in state["sections"]:
            if s["id"] == iss.get("section_id"):
                s["status"] = "critique_flagged"
                s["critique_notes"] += f"\n[{iss['severity'].upper()}] {iss['issue']} — Hint: {iss.get('fix_hint','')}"

state["critique_round"] = state.get("critique_round",0) + 1
state["phase"] = "critiqued"
save_state(state)


# COMMAND ----------
# CELL 14: === PHASE 6 — REPAIR ===
# COMMAND ----------

flagged = [s for s in state["sections"] if s["status"]=="critique_flagged"]
print(f"🔧 {len(flagged)} repairs\n")

for i, section in enumerate(state["sections"]):
    if section["status"] != "critique_flagged":
        continue
    print(f"  {section['title']}...", end=" ", flush=True)
    html = strip_markdown_fences(call_llm(SYSTEM_PROMPT, make_repair_prompt(section)))
    section["html_fragment"] = html
    section["status"] = "repaired"
    section["critique_notes"] = ""
    state["sections"][i] = section
    state["total_rounds"] += 1
    save_state(state)
    print(f"✅ {html.count(chr(10))+1}L")
    time.sleep(2)

assembled = assemble_html(state)
state["assembled_html"] = assembled
state["phase"] = "complete"
state["is_complete"] = True
save_state(state)
print(f"\n  Final: {assembled.count(chr(10))+1} lines, {len(assembled):,} chars")


# COMMAND ----------
# CELL 15: Save and preview
# COMMAND ----------

output_path = f"/dbfs/FileStore/html_reports/{state['project_id']}.html"
os.makedirs(os.path.dirname(output_path), exist_ok=True)
with open(output_path, "w") as f:
    f.write(state["assembled_html"])

print(f"💾 {output_path}")
print(f"  https://<workspace>.azuredatabricks.net/files/html_reports/{state['project_id']}.html")
# displayHTML(state["assembled_html"])
show_dashboard(state)


# COMMAND ----------
# CELL 16: Resume
# COMMAND ----------

# After restart: re-run Cells 1-8, then:
# state = load_state("your_project_id")
# show_dashboard(state)
# Then re-run Cell 11 (skips built), 12, 13, 14, 15
#
# List projects:
# for p in list_projects(): print(f"  {p['id']} {p['name']} [{p['phase']}]")
