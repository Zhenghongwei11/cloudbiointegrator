#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import Circle, FancyArrowPatch, FancyBboxPatch, Rectangle


ROOT = Path(__file__).resolve().parents[2]


PALETTE = {
    "navy": "#2F5D7E",
    "navy_fill": "#EAF1F6",
    "orange": "#C56E3D",
    "orange_fill": "#F8E9DF",
    "slate": "#5E6B73",
    "slate_fill": "#EEF1F3",
    "green": "#4E7A66",
    "green_fill": "#E8F1EC",
    "line": "#C9D1D8",
    "text": "#1F2A33",
    "muted": "#6F7C84",
    "bg": "#FFFFFF",
}


def add_round_box(ax, x, y, w, h, *, fc, ec, lw=1.6, radius=0.02, z=1):
    patch = FancyBboxPatch(
        (x, y),
        w,
        h,
        boxstyle=f"round,pad=0.008,rounding_size={radius}",
        linewidth=lw,
        edgecolor=ec,
        facecolor=fc,
        zorder=z,
    )
    ax.add_patch(patch)
    return patch


def add_text(ax, x, y, text, *, size=12, weight="regular", color=None, ha="center", va="center", z=5):
    ax.text(
        x,
        y,
        text,
        fontsize=size,
        fontweight=weight,
        color=color or PALETTE["text"],
        ha=ha,
        va=va,
        family="DejaVu Sans",
        zorder=z,
    )


def add_stage_header(ax, x_center, label):
    add_text(ax, x_center, 0.77, label, size=16, weight="bold")


def add_chip(ax, x, y, w, h, text, fc, ec, tc):
    add_round_box(ax, x, y, w, h, fc=fc, ec=ec, lw=1.0, radius=0.015, z=3)
    add_text(ax, x + w / 2, y + h / 2, text, size=9.8, weight="medium", color=tc)


def draw_matrix_icon(ax, x, y, size, color):
    cell = size / 4.5
    for r in range(3):
        for c in range(3):
            alpha = 0.18 + 0.11 * (r + c)
            ax.add_patch(
                Rectangle(
                    (x + c * cell * 1.1, y + r * cell * 1.1),
                    cell,
                    cell,
                    facecolor=color,
                    edgecolor=color,
                    lw=0.6,
                    alpha=min(alpha, 0.75),
                    zorder=4,
                )
            )


def draw_spot_icon(ax, x, y, color):
    coords = [(0.0, 0.0), (0.028, 0.0), (0.056, 0.0), (0.014, 0.024), (0.042, 0.024), (0.028, 0.048)]
    for dx, dy in coords:
        ax.add_patch(Circle((x + dx, y + dy), 0.008, facecolor=color, edgecolor="white", lw=0.8, zorder=4))


def draw_heatmap_icon(ax, x, y, w, h):
    colors = ["#F9E1D3", "#EFB18E", "#D97F53", "#B75934"]
    cell_w = w / 4.2
    cell_h = h / 4.2
    for r in range(4):
        for c in range(4):
            idx = min(3, (r + c) // 2)
            ax.add_patch(
                Rectangle(
                    (x + c * cell_w, y + r * cell_h),
                    cell_w * 0.9,
                    cell_h * 0.9,
                    facecolor=colors[idx],
                    edgecolor="white",
                    lw=0.8,
                    zorder=4,
                )
            )


def draw_spatial_icon(ax, x, y, w, h):
    draw_spot_icon(ax, x + 0.008, y + 0.012, PALETTE["orange"])
    ax.add_patch(Circle((x + 0.062, y + 0.055), 0.032, facecolor="#F6D7C8", edgecolor=PALETTE["orange"], lw=1.2, zorder=4))
    ax.add_patch(Circle((x + 0.062, y + 0.055), 0.016, facecolor=PALETTE["orange"], edgecolor=PALETTE["orange"], lw=1.0, zorder=5, alpha=0.85))


def draw_table_icon(ax, x, y, w, h):
    ax.add_patch(Rectangle((x, y), w, h, facecolor="white", edgecolor=PALETTE["slate"], lw=1.2, zorder=4))
    for k in range(1, 4):
        ax.plot([x, x + w], [y + k * h / 4, y + k * h / 4], color=PALETTE["line"], lw=0.9, zorder=5)
    ax.plot([x + w * 0.36, x + w * 0.36], [y, y + h], color=PALETTE["line"], lw=0.9, zorder=5)
    bars = [(0.50, 0.15), (0.62, 0.28), (0.74, 0.42)]
    for bx, bh in bars:
        ax.add_patch(
            Rectangle((x + w * bx, y + h * 0.12), w * 0.07, h * bh, facecolor=PALETTE["navy"], edgecolor="none", zorder=5)
        )


def arrow(ax, start, end):
    patch = FancyArrowPatch(
        start,
        end,
        arrowstyle="-|>",
        mutation_scale=14,
        lw=1.6,
        linestyle=(0, (2.5, 2.5)),
        color=PALETTE["line"],
        zorder=2,
    )
    ax.add_patch(patch)


def build_figure():
    fig = plt.figure(figsize=(14, 8.4), facecolor=PALETTE["bg"])
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    add_text(ax, 0.5, 0.93, "CloudBioIntegrator", size=24, weight="bold")
    add_text(
        ax,
        0.5,
        0.895,
        "Exposing method-dependent discordance and perturbation sensitivity in scRNA-seq and Visium analyses",
        size=12.5,
        color=PALETTE["muted"],
    )

    stage_centers = [0.09, 0.28, 0.50, 0.72, 0.90]
    stage_labels = [
        "Modality\nInputs",
        "Standardization",
        "Method Library",
        "Systematic\nEvaluation",
        "Scientific\nSynthesis",
    ]
    for x, label in zip(stage_centers, stage_labels):
        add_stage_header(ax, x, label)
    for x0, x1 in zip(stage_centers[:-1], stage_centers[1:]):
        ax.plot([x0 + 0.06, x1 - 0.06], [0.77, 0.77], color=PALETTE["line"], lw=1.4, linestyle=(0, (2, 2)), zorder=1)

    add_text(ax, 0.09, 0.69, "Multi-modal inputs", size=11.5, color=PALETTE["muted"])
    add_round_box(ax, 0.03, 0.43, 0.085, 0.13, fc="white", ec=PALETTE["line"], radius=0.012)
    draw_matrix_icon(ax, 0.045, 0.495, 0.06, PALETTE["navy"])
    add_text(ax, 0.0725, 0.465, "scRNA-seq", size=10.5, weight="bold")
    add_text(ax, 0.0725, 0.441, "Cell Ranger matrix", size=8.8, color=PALETTE["muted"])
    add_round_box(ax, 0.03, 0.27, 0.085, 0.13, fc="white", ec=PALETTE["line"], radius=0.012)
    draw_spot_icon(ax, 0.048, 0.325, PALETTE["orange"])
    add_text(ax, 0.0725, 0.305, "Visium", size=10.5, weight="bold")
    add_text(ax, 0.0725, 0.281, "Space Ranger bundle", size=8.5, color=PALETTE["muted"])

    add_text(ax, 0.28, 0.69, "Shared preprocessing core", size=11.5, color=PALETTE["muted"])
    add_round_box(ax, 0.18, 0.37, 0.20, 0.14, fc=PALETTE["slate_fill"], ec=PALETTE["slate"], lw=1.8, radius=0.02)
    box_y = 0.395
    box_h = 0.09
    widths = [0.045, 0.05, 0.07]
    xs = [0.19, 0.242, 0.299]
    labels = ["Ingest", "QC &\nFilter", "Normalize\n& Cluster"]
    for x, w, label in zip(xs, widths, labels):
        add_round_box(ax, x, box_y, w, box_h, fc="white", ec=PALETTE["line"], lw=1.1, radius=0.012)
        add_text(ax, x + w / 2, box_y + box_h / 2, label, size=9.5, weight="medium")
    add_text(ax, 0.28, 0.335, "Predeclared parameters", size=10.2, weight="medium", color=PALETTE["slate"])

    add_text(ax, 0.50, 0.69, "Standardized analytical modules", size=11.5, color=PALETTE["muted"])
    add_round_box(ax, 0.40, 0.45, 0.20, 0.15, fc="white", ec=PALETTE["navy"], lw=1.6, radius=0.014)
    ax.add_patch(Rectangle((0.40, 0.548), 0.20, 0.052, facecolor=PALETTE["navy"], edgecolor=PALETTE["navy"], zorder=2))
    add_text(ax, 0.50, 0.573, "scRNA Modules", size=11.5, weight="bold", color="white")
    chip_y = 0.430
    chip_w = 0.048
    chip_h = 0.028
    chip_xs = [0.424, 0.480, 0.424, 0.480]
    chip_ys = [chip_y + 0.040, chip_y + 0.040, chip_y, chip_y]
    chip_labels = ["Scanpy", "Seurat", "Harmony", "scVI"]
    for x, y, label in zip(chip_xs, chip_ys, chip_labels):
        add_chip(ax, x, y, chip_w, chip_h, label, PALETTE["navy_fill"], PALETTE["navy"], PALETTE["navy"])

    add_round_box(ax, 0.40, 0.25, 0.20, 0.16, fc="white", ec=PALETTE["orange"], lw=1.6, radius=0.014)
    ax.add_patch(Rectangle((0.40, 0.358), 0.20, 0.052, facecolor=PALETTE["orange"], edgecolor=PALETTE["orange"], zorder=2))
    add_text(ax, 0.50, 0.383, "Visium Modules", size=11.5, weight="bold", color="white")
    chip_xs = [0.426, 0.484, 0.452]
    chip_ys = [0.285, 0.285, 0.245]
    chip_labels = ["RCTD", "Tangram", "cell2location"]
    chip_ws = [0.045, 0.054, 0.084]
    for x, y, label, w in zip(chip_xs, chip_ys, chip_labels, chip_ws):
        add_chip(ax, x, y, w, chip_h, label, PALETTE["orange_fill"], PALETTE["orange"], PALETTE["orange"])

    add_text(ax, 0.72, 0.69, "Unified evaluation matrix", size=11.5, color=PALETTE["muted"])
    centers = [(0.67, 0.55), (0.76, 0.55), (0.67, 0.41), (0.76, 0.41)]
    labels = ["Stability", "Comparability", "Runtime /\nfailure", "Robustness"]
    edge_colors = [PALETTE["navy"], PALETTE["navy"], PALETTE["slate"], PALETTE["orange"]]
    fill_colors = [PALETTE["navy_fill"], PALETTE["navy_fill"], PALETTE["slate_fill"], PALETTE["orange_fill"]]
    for (cx, cy), label, ec, fc in zip(centers, labels, edge_colors, fill_colors):
        ax.add_patch(Circle((cx, cy), 0.048, facecolor=fc, edgecolor=ec, lw=1.8, zorder=2))
        add_text(ax, cx, cy, label, size=10.5, weight="medium")

    add_text(ax, 0.90, 0.69, "Reported results", size=11.5, color=PALETTE["muted"])
    card_x = 0.842
    card_w = 0.118
    card_h = 0.118
    ys = [0.50, 0.352, 0.204]
    for y in ys:
        add_round_box(ax, card_x, y, card_w, card_h, fc="white", ec=PALETTE["line"], lw=1.1, radius=0.012)
    draw_heatmap_icon(ax, card_x + 0.026, ys[0] + 0.056, 0.055, 0.048)
    add_text(ax, card_x + card_w / 2, ys[0] + 0.026, "Benchmark\nsummaries", size=10.0, weight="medium")
    draw_spatial_icon(ax, card_x + 0.022, ys[1] + 0.038, 0.06, 0.055)
    add_text(ax, card_x + card_w / 2, ys[1] + 0.025, "Spatial maps\n+ uncertainty", size=10.0, weight="medium")
    draw_table_icon(ax, card_x + 0.032, ys[2] + 0.044, 0.048, 0.042)
    add_text(ax, card_x + card_w / 2, ys[2] + 0.015, "Key quantitative\nfindings", size=9.4, weight="medium")

    arrow(ax, (0.116, 0.495), (0.18, 0.44))
    arrow(ax, (0.116, 0.335), (0.18, 0.44))
    arrow(ax, (0.38, 0.44), (0.40, 0.52))
    arrow(ax, (0.38, 0.44), (0.40, 0.33))
    arrow(ax, (0.60, 0.52), (0.62, 0.52))
    arrow(ax, (0.60, 0.33), (0.62, 0.43))
    arrow(ax, (0.81, 0.48), (0.845, 0.555))
    arrow(ax, (0.81, 0.48), (0.845, 0.41))
    arrow(ax, (0.81, 0.48), (0.845, 0.265))

    add_round_box(ax, 0.08, 0.08, 0.84, 0.09, fc="#FBFCFD", ec=PALETTE["line"], lw=1.1, radius=0.016)
    add_text(ax, 0.14, 0.124, "Structured outputs", size=11.2, weight="bold", color=PALETTE["text"])
    ribbon_items = [
        ("Parameters", PALETTE["slate_fill"], PALETTE["slate"], PALETTE["slate"]),
        ("Tables", PALETTE["navy_fill"], PALETTE["navy"], PALETTE["navy"]),
        ("Runtime logs", PALETTE["slate_fill"], PALETTE["slate"], PALETTE["slate"]),
        ("Robustness", PALETTE["orange_fill"], PALETTE["orange"], PALETTE["orange"]),
        ("Provenance", PALETTE["green_fill"], PALETTE["green"], PALETTE["green"]),
    ]
    x = 0.30
    for label, fc, ec, tc in ribbon_items:
        w = 0.09 if label != "Runtime logs" else 0.11
        if label == "Provenance":
            w = 0.10
        add_chip(ax, x, 0.107, w, 0.032, label, fc, ec, tc)
        x += w + 0.015

    return fig


def export(outdir: Path, dpi: int):
    fig = build_figure()
    png_dir = outdir / "png"
    pdf_dir = outdir / "pdf"
    svg_dir = outdir / "svg"
    png_dir.mkdir(parents=True, exist_ok=True)
    pdf_dir.mkdir(parents=True, exist_ok=True)
    svg_dir.mkdir(parents=True, exist_ok=True)

    png_path = png_dir / "F1_system_architecture.png"
    pdf_path = pdf_dir / "F1_system_architecture.pdf"
    svg_path = svg_dir / "F1_system_architecture.svg"
    src_svg_path = ROOT / "docs/figures/F1_roadmap_manual.svg"
    src_svg_path.parent.mkdir(parents=True, exist_ok=True)

    fig.savefig(png_path, dpi=dpi, facecolor=PALETTE["bg"], bbox_inches="tight", pad_inches=0.02)
    fig.savefig(pdf_path, format="pdf", facecolor=PALETTE["bg"], bbox_inches="tight", pad_inches=0.02)
    fig.savefig(svg_path, format="svg", facecolor=PALETTE["bg"], bbox_inches="tight", pad_inches=0.02)
    fig.savefig(src_svg_path, format="svg", facecolor=PALETTE["bg"], bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    return png_path, pdf_path, svg_path, src_svg_path


def main() -> int:
    ap = argparse.ArgumentParser(description="Render Figure 1 as a scripted publication-grade roadmap.")
    ap.add_argument("--outdir", default="plots/publication", help="Output dir root (default: plots/publication).")
    ap.add_argument("--dpi", type=int, default=300, help="PNG DPI metadata (default: 300).")
    ap.add_argument("--freeze", action="store_true", help="Also write copies under plots/publication/frozen/.")
    args = ap.parse_args()

    out_root = (ROOT / args.outdir).resolve()
    png_path, pdf_path, svg_path, src_svg_path = export(out_root, args.dpi)

    if args.freeze:
        frozen = out_root / "frozen"
        (frozen / "png").mkdir(parents=True, exist_ok=True)
        (frozen / "pdf").mkdir(parents=True, exist_ok=True)
        (frozen / "svg").mkdir(parents=True, exist_ok=True)
        (frozen / "png" / png_path.name).write_bytes(png_path.read_bytes())
        (frozen / "pdf" / pdf_path.name).write_bytes(pdf_path.read_bytes())
        (frozen / "svg" / svg_path.name).write_bytes(svg_path.read_bytes())

    print(f"OK: wrote {png_path.relative_to(ROOT)}")
    print(f"OK: wrote {pdf_path.relative_to(ROOT)}")
    print(f"OK: wrote {svg_path.relative_to(ROOT)}")
    print(f"OK: wrote {src_svg_path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
