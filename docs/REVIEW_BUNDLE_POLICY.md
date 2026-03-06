# Review Bundle Policy (Product Reproducibility)

This project maintains `docs/review_bundle/review_bundle.zip` as a **product reproducibility bundle** for development and external validation of the containerized method library.

## Included
- Product/runtime documentation needed to understand scope, methods, datasets, and reproduction steps (e.g., `docs/CLAIMS.tsv`, `docs/METHOD_LIBRARY_*.md`, runbooks, checklists).
- Contract schema (`schemas/action_schema_v1.json`).
- Data manifest (`data/manifest.tsv`) describing authoritative sources and integrity metadata.
- Publication figures produced by the pipeline (`plots/publication/`).
- Evidence tables produced by the pipeline (`results/`).
- Required run bundles listed in `docs/SUBMISSION_AUDIT_SET.tsv` (the minimum set of run zips needed to validate end-to-end reproduction on fresh infrastructure).

## Excluded (by design)
- Manuscript drafts (Markdown/DOCX) and journal submission artifacts (cover letters, journal-specific packaging).
- Citation verification and literature-benchmarking materials.
- Any scripts whose sole purpose is manuscript formatting/conversion.

Rationale: the review bundle exists to validate the **software product and its outputs**, independent of any specific paper.

