#!/usr/bin/env python3
from __future__ import annotations

import csv
import datetime as dt
import hashlib
import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CITATIONS_TSV = ROOT / "docs" / "CITATION_VERIFICATION.tsv"
OUT_TSV = ROOT / "docs" / "CROSSREF_EVIDENCE.tsv"
OUT_DIR = ROOT / "docs" / "crossref_responses"


@dataclass(frozen=True)
class DoiRef:
    ref_id: str
    doi: str
    doi_url: str


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _safe_name(ref_id: str) -> str:
    # Keep file names stable and readable.
    return "".join([c if c.isalnum() or c in ("-", "_") else "_" for c in ref_id])


def _extract_doi(doi_or_pmid: str) -> str:
    # Input is a semicolon-delimited string like 'DOI:10.xxxx/yyy' or 'URL'.
    for part in (doi_or_pmid or "").split(";"):
        part = part.strip()
        if part.startswith("DOI:"):
            return part[len("DOI:") :].strip()
    return ""


def _get_year(msg: dict) -> str:
    issued = msg.get("issued", {}) or {}
    parts = issued.get("date-parts", []) or []
    if parts and isinstance(parts[0], list) and parts[0]:
        y = parts[0][0]
        return str(y)
    return ""


def _first_str(v) -> str:
    if isinstance(v, list) and v:
        return str(v[0])
    if isinstance(v, str):
        return v
    return ""


def main() -> int:
    if not CITATIONS_TSV.exists():
        raise SystemExit(f"missing: {CITATIONS_TSV}")

    rows: list[dict[str, str]] = []
    doi_refs: list[DoiRef] = []

    with CITATIONS_TSV.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        for row in r:
            doi = _extract_doi(row.get("doi_or_pmid", ""))
            if not doi:
                continue
            doi_refs.append(DoiRef(ref_id=row["ref_id"], doi=doi, doi_url=f"https://doi.org/{doi}"))

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    headers = {
        # Crossref asks for a UA that identifies your tool. We include a generic label.
        "User-Agent": "CloudBioAgent/0.1 (Crossref evidence fetch; no email provided)",
        "Accept": "application/json",
    }

    retrieved_at = dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    for i, ref in enumerate(doi_refs, start=1):
        # Crossref API expects the DOI in path form; encode conservatively.
        doi_path = urllib.parse.quote(ref.doi, safe="/")
        url = f"https://api.crossref.org/works/{doi_path}"
        req = urllib.request.Request(url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                raw = r.read()
                status = getattr(r, "status", 200)
        except Exception as e:
            raw = (str(e) + "\n").encode("utf-8", errors="replace")
            status = 0

        sha = _sha256_bytes(raw)

        out_json = OUT_DIR / f"{_safe_name(ref.ref_id)}.json"
        out_json.write_bytes(raw)

        title = ""
        container = ""
        typ = ""
        publisher = ""
        year = ""
        crossref_url = ""
        if status == 200:
            try:
                payload = json.loads(raw.decode("utf-8"))
                msg = payload.get("message", {}) if isinstance(payload, dict) else {}
                title = _first_str(msg.get("title"))
                container = _first_str(msg.get("container-title"))
                typ = str(msg.get("type") or "")
                publisher = str(msg.get("publisher") or "")
                year = _get_year(msg)
                crossref_url = str(msg.get("URL") or "")
            except Exception:
                # Keep row; parsing issues will show in empty fields.
                pass

        rows.append(
            {
                "ref_id": ref.ref_id,
                "doi": ref.doi,
                "doi_url": ref.doi_url,
                "crossref_api_url": url,
                "crossref_work_url": crossref_url,
                "retrieved_at_utc": retrieved_at,
                "http_status": str(status),
                "sha256": sha,
                "title": title,
                "container_title": container,
                "issued_year": year,
                "type": typ,
                "publisher": publisher,
                "response_file": str(out_json.relative_to(ROOT)),
            }
        )

        # Be polite to Crossref.
        if i != len(doi_refs):
            time.sleep(0.15)

    with OUT_TSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "ref_id",
                "doi",
                "doi_url",
                "crossref_api_url",
                "crossref_work_url",
                "retrieved_at_utc",
                "http_status",
                "sha256",
                "title",
                "container_title",
                "issued_year",
                "type",
                "publisher",
                "response_file",
            ],
            delimiter="\t",
        )
        w.writeheader()
        w.writerows(rows)

    ok = sum(1 for r in rows if r["http_status"] == "200")
    print(f"OK: wrote {OUT_TSV} ({ok}/{len(rows)} DOI refs fetched); responses in {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
