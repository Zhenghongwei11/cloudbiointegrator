#!/usr/bin/env python3
from __future__ import annotations

import csv
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MANUSCRIPT_NUMERIC = ROOT / "docs" / "MANUSCRIPT_DRAFT.md"
MANUSCRIPT_KEYED = ROOT / "docs" / "MANUSCRIPT_DRAFT_KEYED.md"
CITATIONS_TSV = ROOT / "docs" / "CITATION_VERIFICATION.tsv"


REFS_HEADING = "## References"


def load_citation_text_to_id(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            rid = (row.get("ref_id") or "").strip()
            txt = (row.get("citation_text") or "").strip()
            if rid and txt:
                out[txt] = rid
    return out


def main() -> int:
    if not MANUSCRIPT_NUMERIC.exists():
        raise SystemExit(f"missing {MANUSCRIPT_NUMERIC}")
    if not CITATIONS_TSV.exists():
        raise SystemExit(f"missing {CITATIONS_TSV}")

    citation_text_to_id = load_citation_text_to_id(CITATIONS_TSV)
    text = MANUSCRIPT_NUMERIC.read_text(encoding="utf-8")
    if REFS_HEADING not in text:
        raise SystemExit("numeric manuscript missing References section")

    before, refs_block = text.split(REFS_HEADING, 1)
    refs_block = refs_block.strip("\n")

    # Parse numbered references from the numeric manuscript.
    ref_lines = []
    for line in refs_block.splitlines():
        if not line.strip():
            continue
        m = re.match(r"^(\d+)\.\s+(.*)$", line.strip())
        if not m:
            # stop when leaving the numbered list
            break
        ref_lines.append((int(m.group(1)), m.group(2).strip()))

    if not ref_lines:
        raise SystemExit("could not parse any numbered references")

    num_to_ref_id: dict[int, str] = {}
    for n, cite_text in ref_lines:
        rid = citation_text_to_id.get(cite_text)
        if not rid:
            raise SystemExit(f"reference line {n} not found in CITATION_VERIFICATION.tsv citation_text:\n{cite_text}")
        num_to_ref_id[n] = rid

    # Replace in-text numeric citations with keyed citations.
    def repl(m: re.Match[str]) -> str:
        n = int(m.group(1))
        rid = num_to_ref_id.get(n)
        if not rid:
            raise SystemExit(f"found in-text citation [{n}] not present in reference list")
        return "{" + rid + "}"

    keyed = re.sub(r"\[(\d+)\]", repl, before)
    keyed = keyed.rstrip() + "\n\n" + REFS_HEADING + " (placeholder)\n\n" + "Add Vancouver numeric references after citation verification.\n"

    MANUSCRIPT_KEYED.write_text(keyed, encoding="utf-8")
    print(f"OK: wrote {MANUSCRIPT_KEYED}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

