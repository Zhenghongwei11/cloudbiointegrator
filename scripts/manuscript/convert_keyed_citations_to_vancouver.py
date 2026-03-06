#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MANUSCRIPT_IN = ROOT / "docs" / "MANUSCRIPT_DRAFT_KEYED.md"
MANUSCRIPT_OUT = ROOT / "docs" / "MANUSCRIPT_DRAFT.md"
CITATIONS_TSV = ROOT / "docs" / "CITATION_VERIFICATION.tsv"


TOKEN_RE = re.compile(r"\{([A-Za-z0-9_\\-]+)\}")
ADJ_NUM_CITES_RE = re.compile(r"(?:\[\d+\]){2,}")


@dataclass(frozen=True)
class Citation:
    ref_id: str
    citation_text: str
    status: str
    full_text_checked: str


def load_citations(path: Path) -> dict[str, Citation]:
    out: dict[str, Citation] = {}
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            ref_id = (row.get("ref_id") or "").strip()
            if not ref_id:
                continue
            out[ref_id] = Citation(
                ref_id=ref_id,
                citation_text=(row.get("citation_text") or "").strip(),
                status=(row.get("status") or "").strip(),
                full_text_checked=(row.get("full_text_checked") or "").strip().lower(),
            )
    return out


def _compress_numeric_run(nums: list[int]) -> str:
    parts: list[str] = []
    i = 0
    while i < len(nums):
        j = i
        while j + 1 < len(nums) and nums[j + 1] == nums[j] + 1:
            j += 1
        run_len = j - i + 1
        if run_len >= 3:
            parts.append(f"{nums[i]}-{nums[j]}")
        elif run_len == 2:
            parts.append(f"{nums[i]}")
            parts.append(f"{nums[j]}")
        else:
            parts.append(f"{nums[i]}")
        i = j + 1
    return "[" + ",".join(parts) + "]"


def compact_adjacent_citations(text: str) -> str:
    def repl(m: re.Match[str]) -> str:
        raw = m.group(0)
        nums = [int(x) for x in re.findall(r"\[(\d+)\]", raw)]
        if len(nums) < 2:
            return raw
        return _compress_numeric_run(nums)

    return ADJ_NUM_CITES_RE.sub(repl, text)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Convert a keyed-citation markdown manuscript to Vancouver-numbered markdown."
    )
    ap.add_argument(
        "--input",
        default=str(MANUSCRIPT_IN),
        help=f"Input keyed markdown (default: {MANUSCRIPT_IN}).",
    )
    ap.add_argument(
        "--output",
        default=str(MANUSCRIPT_OUT),
        help=f"Output Vancouver markdown (default: {MANUSCRIPT_OUT}).",
    )
    ap.add_argument(
        "--citations",
        default=str(CITATIONS_TSV),
        help=f"Citation verification TSV (default: {CITATIONS_TSV}).",
    )
    ap.add_argument(
        "--min-refs",
        type=int,
        default=50,
        help="Minimum required unique references before conversion (default: 50).",
    )
    args = ap.parse_args()

    manuscript_in = Path(args.input).resolve()
    manuscript_out = Path(args.output).resolve()
    citations_tsv = Path(args.citations).resolve()

    if not manuscript_in.exists():
        raise SystemExit(f"missing keyed manuscript: {manuscript_in}")
    if not citations_tsv.exists():
        raise SystemExit(f"missing citation verification table: {citations_tsv}")

    citations = load_citations(citations_tsv)
    text = manuscript_in.read_text(encoding="utf-8")

    # Find citations by first appearance.
    ordered: list[str] = []
    seen: set[str] = set()
    for m in TOKEN_RE.finditer(text):
        ref_id = m.group(1)
        if ref_id not in seen:
            ordered.append(ref_id)
            seen.add(ref_id)

    if len(ordered) < args.min_refs:
        raise SystemExit(
            f"manuscript cites {len(ordered)} unique refs; need >= {args.min_refs} before Vancouver conversion."
        )

    missing = [r for r in ordered if r not in citations]
    if missing:
        raise SystemExit(f"missing ref_id(s) in {CITATIONS_TSV}: {missing[:10]}{'...' if len(missing)>10 else ''}")

    not_ok = [r for r in ordered if citations[r].status != "verified_ok" or citations[r].full_text_checked != "yes"]
    if not_ok:
        raise SystemExit(f"unverified reference(s) (status/full_text): {not_ok[:10]}{'...' if len(not_ok)>10 else ''}")

    # Map to Vancouver numbers.
    ref_to_num = {ref_id: i + 1 for i, ref_id in enumerate(ordered)}

    def repl(m: re.Match[str]) -> str:
        rid = m.group(1)
        return f"[{ref_to_num[rid]}]"

    new_text = TOKEN_RE.sub(repl, text)
    new_text = compact_adjacent_citations(new_text)

    # Replace References section with numbered list.
    marker = "## References"
    if marker not in new_text:
        raise SystemExit("manuscript missing '## References' heading")

    before, after = new_text.split(marker, 1)
    # Keep heading, drop everything after, write generated list.
    refs_lines = []
    refs_lines.append(marker)
    refs_lines.append("")
    for ref_id in ordered:
        c = citations[ref_id].citation_text
        refs_lines.append(f"{ref_to_num[ref_id]}. {c}")
    refs_lines.append("")

    manuscript_out.parent.mkdir(parents=True, exist_ok=True)
    manuscript_out.write_text(before + "\n".join(refs_lines), encoding="utf-8")
    print(f"OK: wrote {manuscript_out} with Vancouver numbering ({len(ordered)} refs)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
