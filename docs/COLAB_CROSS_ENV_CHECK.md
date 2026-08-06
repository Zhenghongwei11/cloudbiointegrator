# Cross-Environment Check on Google Colab (free, no second computer needed)

Google Colab gives you a free Linux VM. Because the minimal reproducible path
(skeleton + smoke) produces environment-independent summaries, a Colab run can be used as
the "second environment" for the cross-environment check.

## Steps (about 5 minutes)

1. Download the pack: `output/cbi_repro_pack.zip` (already generated; ~56 KB).
2. Open https://colab.research.google.com and create a new notebook.
3. Upload the zip: click the folder icon in the left sidebar -> Upload -> select
   `cbi_repro_pack.zip`.
4. Paste and run this cell (installs deps, unpacks):

   ```python
   !apt-get -y install make >/dev/null 2>&1 || true
   !pip install -q anndata scipy numpy pandas scikit-learn matplotlib
   !unzip -q -o cbi_repro_pack.zip -d /tmp/cbi_repo
   %cd /tmp/cbi_repo
   ```

5. Paste and run this cell (runs the check and records the manifest):

   ```python
   !make skeleton
   !make smoke
   !python3 scripts/audit/record_run_manifest.py --out results/runs/colab_manifest.tsv
   !cat results/runs/colab_manifest.tsv
   ```

   The output should end with two rows: `skeleton / NA` and `smoke_ingest / SMOKE_TOY_10X_MTX`.

6. Download the manifest back to your Mac:

   ```python
   from google.colab import files
   files.download('/tmp/cbi_repo/results/runs/colab_manifest.tsv')
   ```

7. On your Mac, compare with the local baseline:

   ```bash
   python3 scripts/audit/compare_run_manifests.py \
     results/runs/baseline_manifest.tsv \
     ~/Downloads/colab_manifest.tsv
   ```

## Expected result

```
PASS  skeleton / NA ...
PASS  smoke_ingest / SMOKE_TOY_10X_MTX ...
RESULT: PASS — run-scoped artifacts are byte-identical across environments
```

That PASS is the cross-environment evidence: your local Docker (macOS) and a fresh Google
Colab Linux VM produced byte-identical run-scoped outcomes for the declared minimal path.

## Troubleshooting

- `make: command not found`: the first cell already tries to install it; re-run cell 1.
- `Rscript` warnings during `make skeleton`/`make smoke`: expected and harmless (R figure
  scripts are skipped; the compared summaries do not depend on figures).
- A `FAIL` row in the comparison means real drift: copy the full output and share it.
