#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import re
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


@dataclass
class Check:
    name: str
    ok: bool
    detail: str
    blocker: bool = False


HEADING_RE = re.compile(r"^##\s+(.+?)\s*$")


def parse_sections(text: str) -> dict[str, str]:
    lines = text.splitlines()
    starts: list[tuple[str, int]] = []
    for i, line in enumerate(lines):
        m = HEADING_RE.match(line)
        if m:
            starts.append((m.group(1).strip(), i))
    sections: dict[str, str] = {}
    for idx, (name, start) in enumerate(starts):
        end = starts[idx + 1][1] if idx + 1 < len(starts) else len(lines)
        body = "\n".join(lines[start + 1 : end]).strip()
        sections[name] = body
    return sections


def count_words(text: str) -> int:
    return len(re.findall(r"\b\w+[\w\-]*\b", text))


def parse_reference_numbers(ref_text: str) -> list[int]:
    nums = []
    for line in ref_text.splitlines():
        m = re.match(r"^\s*(\d+)\.\s+", line)
        if m:
            nums.append(int(m.group(1)))
    return nums


def _read_tsv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def _compute_repro_counts() -> tuple[int, int]:
    # Source of truth: results/audit/reproducibility_checks.tsv
    rows = _read_tsv(ROOT / "results" / "audit" / "reproducibility_checks.tsv")
    n_total = len(rows)
    n_pass = sum(1 for r in rows if (r.get("pass") or "").strip() in {"1", "true", "True"})
    return n_pass, n_total


def _compute_ops_counts_terminal_rows() -> tuple[int, int]:
    # Source of truth: results/benchmarks/runtime_cost_failure.tsv
    # Manuscript uses terminal rows (not unique run_id) by design.
    rows = _read_tsv(ROOT / "results" / "benchmarks" / "runtime_cost_failure.tsv")
    term = [r for r in rows if (r.get("status") or "").strip() in {"ok", "fail"}]
    n_total = len(term)
    n_ok = sum(1 for r in term if (r.get("status") or "").strip() == "ok")
    return n_ok, n_total


def _compute_robust_failures() -> tuple[int, int]:
    # Source of truth: results/benchmarks/robustness_matrix.tsv
    rows = _read_tsv(ROOT / "results" / "benchmarks" / "robustness_matrix.tsv")
    n_total = len(rows)
    n_fail = sum(1 for r in rows if (r.get("pass") or "").strip() == "0")
    return n_fail, n_total


def main() -> int:
    ap = argparse.ArgumentParser(description="PLOS Computational Biology preflight checks.")
    ap.add_argument("--manuscript", default="docs/submissions/PLOS_Computational_Biology/manuscript_submission.md")
    ap.add_argument("--cover", default="docs/submissions/PLOS_Computational_Biology/cover_letter.md")
    ap.add_argument("--reviewers", default="docs/submissions/PLOS_Computational_Biology/reviewer_suggestions_template.tsv")
    ap.add_argument("--fig-png", default="docs/submissions/PLOS_Computational_Biology/attachments/figures_png")
    ap.add_argument("--fig-pdf", default="docs/submissions/PLOS_Computational_Biology/attachments/figures_pdf")
    ap.add_argument("--fig-tiff", default="docs/submissions/PLOS_Computational_Biology/attachments/figures_tiff")
    ap.add_argument("--output", default="docs/submissions/PLOS_Computational_Biology/desk_reject_guard_20260302.md")
    args = ap.parse_args()

    manuscript_path = (ROOT / args.manuscript).resolve()
    cover_path = (ROOT / args.cover).resolve()
    reviewers_path = (ROOT / args.reviewers).resolve()
    fig_png_dir = (ROOT / args.fig_png).resolve()
    fig_pdf_dir = (ROOT / args.fig_pdf).resolve()
    fig_tiff_dir = (ROOT / args.fig_tiff).resolve()
    out_path = (ROOT / args.output).resolve()

    text = manuscript_path.read_text(encoding="utf-8")
    lines = text.splitlines()
    sections = parse_sections(text)

    checks: list[Check] = []

    # Title / short title
    title = lines[0].lstrip("# ").strip() if lines else ""
    checks.append(Check("Title <= 200 chars", len(title) <= 200, f"{len(title)}"))

    short_title = ""
    for line in lines[:40]:
        m = re.match(r"^\*\*Short title:\*\*\s*(.+)$", line.strip())
        if m:
            short_title = m.group(1).strip()
            break
    checks.append(Check("Short title <= 70 chars", bool(short_title) and len(short_title) <= 70, f"{len(short_title)}" if short_title else "missing", blocker=True))

    # Required sections
    required_sections = [
        "Abstract",
        "Author Summary",
        "Introduction",
        "Results",
        "Discussion",
        "Data Availability Statement",
        "Author Contributions (CRediT)",
        "Funding",
        "Competing Interests",
        "References",
    ]
    for sec in required_sections:
        checks.append(Check(f"Section present: {sec}", sec in sections and bool(sections.get(sec, "").strip()), "ok" if sec in sections else "missing", blocker=True))

    # Abstract word count
    abstract_words = count_words(sections.get("Abstract", ""))
    checks.append(Check("Abstract <= 300 words", abstract_words <= 300, str(abstract_words), blocker=True))

    # Keyed citation tokens
    keyed_tokens = re.findall(r"\{[A-Za-z0-9_\-]+\}", text)
    checks.append(Check("No keyed citation tokens remain", len(keyed_tokens) == 0, f"{len(keyed_tokens)}"))

    # Vancouver references numbering
    ref_nums = parse_reference_numbers(sections.get("References", ""))
    contiguous = ref_nums == list(range(1, len(ref_nums) + 1)) and len(ref_nums) > 0
    checks.append(Check("Vancouver refs contiguous from 1", contiguous, f"count={len(ref_nums)}", blocker=True))

    # Data Availability checks
    das = sections.get("Data Availability Statement", "")
    has_url = bool(re.search(r"https?://", das))
    has_doi = bool(re.search(r"doi", das, flags=re.IGNORECASE))
    has_code_word = bool(re.search(r"\bcode\b", das, flags=re.IGNORECASE))
    checks.append(Check("Data Availability includes URL/DOI", has_url or has_doi, "ok" if (has_url or has_doi) else "none", blocker=True))
    checks.append(Check("Data Availability mentions code access", has_code_word, "ok" if has_code_word else "missing", blocker=True))

    # Ethics statement for no-new-subjects declaration
    ethics = sections.get("Ethics Statement", "")
    checks.append(Check("Ethics statement present (or explicitly N/A)", bool(ethics), "ok" if ethics else "missing", blocker=False))

    # CRediT specificity
    credit = sections.get("Author Contributions (CRediT)", "")
    credit_placeholder = bool(re.search(r"all authors|\[[^\]]+\]", credit, flags=re.IGNORECASE))
    checks.append(Check("CRediT is author-specific", not credit_placeholder, "placeholder/generic" if credit_placeholder else "ok", blocker=True))

    # Cover letter placeholder check
    cover = cover_path.read_text(encoding="utf-8") if cover_path.exists() else ""
    cover_placeholders = re.findall(r"\[[^\]]+\]", cover)
    checks.append(Check("Cover letter has no placeholders", len(cover_placeholders) == 0, f"{len(cover_placeholders)}", blocker=True))

    # Reviewer suggestions
    reviewer_rows = []
    if reviewers_path.exists():
        with reviewers_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            reviewer_rows = list(reader)
    filled_reviewers = [r for r in reviewer_rows if (r.get("name", "").strip() and r.get("email", "").strip())]
    checks.append(Check("At least 4 reviewer suggestions with name+email", len(filled_reviewers) >= 4, f"{len(filled_reviewers)}", blocker=True))

    # Figure asset checks
    pngs = sorted(fig_png_dir.glob("*.png")) if fig_png_dir.exists() else []
    pdfs = sorted(fig_pdf_dir.glob("*.pdf")) if fig_pdf_dir.exists() else []
    tiffs = sorted(fig_tiff_dir.glob("*.tif")) if fig_tiff_dir.exists() else []
    checks.append(Check("Figures PNG count == 6", len(pngs) == 6, str(len(pngs))))
    checks.append(Check("Figures PDF count == 6", len(pdfs) == 6, str(len(pdfs))))
    checks.append(Check("Figures TIFF count == 6", len(tiffs) == 6, str(len(tiffs)), blocker=False))

    oversize_tiff = [p.name for p in tiffs if p.stat().st_size > 10 * 1024 * 1024]
    checks.append(Check("TIFF files <= 10 MB", len(oversize_tiff) == 0, "ok" if not oversize_tiff else ",".join(oversize_tiff), blocker=False))

    # Metrics anchors: derive expected headline counts from results tables and verify manuscript mentions them.
    repro_pass, repro_total = _compute_repro_counts()
    repro_token = f"{repro_pass}/{repro_total}" if repro_total else "0/0"
    checks.append(Check("Repro checks token matches results table", repro_token in text, repro_token if repro_token in text else f"missing ({repro_token})"))

    ok_runs, term_runs = _compute_ops_counts_terminal_rows()
    ops_token = f"{ok_runs}/{term_runs}" if term_runs else "0/0"
    checks.append(Check("Operational success token matches results table", ops_token in text, ops_token if ops_token in text else f"missing ({ops_token})"))

    robust_fail, robust_total = _compute_robust_failures()
    robust_token = f"{robust_fail}/{robust_total}" if robust_total else "0/0"
    checks.append(Check("Robustness failures token matches results table", robust_token in text, robust_token if robust_token in text else f"missing ({robust_token})"))

    blockers = [c for c in checks if c.blocker and not c.ok]
    tech_ok = all(c.ok for c in checks if not c.blocker)
    ready = len(blockers) == 0

    ts = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    out_lines = []
    out_lines.append("# Desk-Reject Guard Checklist (PLOS Comp Bio)")
    out_lines.append("")
    out_lines.append(f"- Check time (UTC): `{ts}`")
    out_lines.append("")
    out_lines.append("## Automated Checks")
    out_lines.append("")
    for c in checks:
        status = "PASS" if c.ok else "FAIL"
        out_lines.append(f"- {c.name}: `{status}` ({c.detail})")

    out_lines.append("")
    out_lines.append("## Blocking Items Before Submission")
    out_lines.append("")
    if blockers:
        for c in blockers:
            out_lines.append(f"- {c.name}")
    else:
        out_lines.append("- None")

    out_lines.append("")
    out_lines.append(f"- Technical status: `{'PASS' if tech_ok else 'FAIL'}`")
    out_lines.append(f"- Submission-ready status: `{'READY' if ready else 'BLOCKED'}`")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(out_lines) + "\n", encoding="utf-8")
    print(f"OK: wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
