#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from PIL import Image


ROOT = Path(__file__).resolve().parents[2]


def _render_a4_canvas(src: Image.Image, *, target_w: int, target_h: int) -> Image.Image:
    # Fit without cropping; pad with white to match A4 landscape aspect.
    if src.mode not in ("RGB", "RGBA"):
        src = src.convert("RGB")
    w, h = src.size
    scale = min(target_w / w, target_h / h)
    new_w = max(1, int(round(w * scale)))
    new_h = max(1, int(round(h * scale)))
    resized = src.resize((new_w, new_h), resample=Image.Resampling.LANCZOS)

    canvas = Image.new("RGB", (target_w, target_h), (255, 255, 255))
    x0 = (target_w - new_w) // 2
    y0 = (target_h - new_h) // 2
    canvas.paste(resized, (x0, y0))
    return canvas


def _write_pdf_from_png(img: Image.Image, out_pdf: Path) -> None:
    # Embed the raster at A4 landscape size; this is acceptable for a conceptual schematic.
    import matplotlib.pyplot as plt

    # A4 landscape inches.
    fig = plt.figure(figsize=(11.69, 8.27), dpi=300)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.imshow(img)
    ax.axis("off")
    fig.savefig(out_pdf, format="pdf", dpi=300)
    plt.close(fig)


def main() -> int:
    ap = argparse.ArgumentParser(description="Export AI-generated F1 roadmap into plots/publication (PNG+PDF).")
    ap.add_argument(
        "--source",
        default="docs/figures/F1_ai_roadmap_source_20260305.png",
        help="Source PNG (default: docs/figures/F1_ai_roadmap_source_20260305.png).",
    )
    ap.add_argument("--outdir", default="plots/publication", help="Output dir root (default: plots/publication).")
    ap.add_argument("--freeze", action="store_true", help="Also write copies under plots/publication/frozen/.")
    ap.add_argument("--target-w", type=int, default=3507, help="Target width in px for A4-like output.")
    ap.add_argument("--target-h", type=int, default=2481, help="Target height in px for A4-like output.")
    ap.add_argument("--dpi", type=int, default=300, help="PNG DPI metadata (default: 300).")
    args = ap.parse_args()

    src_path = (ROOT / args.source).resolve()
    if not src_path.exists():
        raise SystemExit(f"missing --source: {src_path}")

    out_root = (ROOT / args.outdir).resolve()
    out_png_dir = out_root / "png"
    out_pdf_dir = out_root / "pdf"
    out_png_dir.mkdir(parents=True, exist_ok=True)
    out_pdf_dir.mkdir(parents=True, exist_ok=True)

    out_png = out_png_dir / "F1_system_contract.png"
    out_pdf = out_pdf_dir / "F1_system_contract.pdf"

    with Image.open(src_path) as im:
        rendered = _render_a4_canvas(im, target_w=args.target_w, target_h=args.target_h)
        rendered.save(out_png, format="PNG", dpi=(args.dpi, args.dpi))
        _write_pdf_from_png(rendered, out_pdf)

    if args.freeze:
        frozen = out_root / "frozen"
        (frozen / "png").mkdir(parents=True, exist_ok=True)
        (frozen / "pdf").mkdir(parents=True, exist_ok=True)
        (frozen / "png" / "F1_system_contract.png").write_bytes(out_png.read_bytes())
        (frozen / "pdf" / "F1_system_contract.pdf").write_bytes(out_pdf.read_bytes())

    print(f"OK: wrote {out_png.relative_to(ROOT)}")
    print(f"OK: wrote {out_pdf.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

