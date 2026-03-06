#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from PIL import Image

ROOT = Path(__file__).resolve().parents[2]

FIG_MAP = {
    "F1_system_contract.png": "Fig1.tif",
    "F2_reproducibility.png": "Fig2.tif",
    "F3_scrna_benchmark.png": "Fig3.tif",
    "F4_spatial_benchmark.png": "Fig4.tif",
    "F5_ops_benchmark.png": "Fig5.tif",
    "F6_robustness_matrix.png": "Fig6.tif",
}


def _flatten_to_rgb(im: Image.Image) -> Image.Image:
    if im.mode in ("RGBA", "LA"):
        alpha = im.split()[-1]
        bg = Image.new("RGB", im.size, (255, 255, 255))
        bg.paste(im, mask=alpha)
        return bg
    if im.mode != "RGB":
        return im.convert("RGB")
    return im


def main() -> int:
    ap = argparse.ArgumentParser(description="Convert publication PNGs to PLOS-style TIFF files.")
    ap.add_argument("--input-dir", default="docs/submissions/PLOS_Computational_Biology/attachments/figures_png")
    ap.add_argument("--output-dir", default="docs/submissions/PLOS_Computational_Biology/attachments/figures_tiff")
    ap.add_argument("--dpi", type=int, default=600)
    args = ap.parse_args()

    inp = (ROOT / args.input_dir).resolve()
    out = (ROOT / args.output_dir).resolve()
    out.mkdir(parents=True, exist_ok=True)

    missing: list[str] = []
    for src_name, dst_name in FIG_MAP.items():
        src = inp / src_name
        dst = out / dst_name
        if not src.exists():
            missing.append(src_name)
            continue
        with Image.open(src) as im:
            rgb = _flatten_to_rgb(im)
            rgb.save(
                dst,
                format="TIFF",
                compression="tiff_lzw",
                dpi=(args.dpi, args.dpi),
            )

    if missing:
        print("WARN: missing source PNGs:")
        for m in missing:
            print(f"  - {m}")

    print(f"OK: wrote TIFF figures to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
