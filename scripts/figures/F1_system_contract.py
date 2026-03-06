#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, Circle, RegularPolygon, Rectangle, FancyArrowPatch
import matplotlib.patheffects as PathEffects

ROOT = Path(__file__).resolve().parents[2]

def setup_style():
    plt.rcParams.update({
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
        "font.family": "sans-serif",
        "font.sans-serif": ["Arial", "Helvetica", "DejaVu Sans"],
        "text.color": "#334155"
    })

def draw_fancy_arrow(ax, x1, y1, x2, y2, color="#94A3B8", lw=1.5, connectionstyle="arc3,rad=0"):
    arrow = FancyArrowPatch(
        (x1, y1), (x2, y2),
        arrowstyle='-|>',
        mutation_scale=15,
        linewidth=lw,
        color=color,
        connectionstyle=connectionstyle,
        zorder=5
    )
    ax.add_patch(arrow)

def draw_cell_icon(ax, x, y, size=2, color="#3B82F6"):
    ax.add_patch(Circle((x, y), size, facecolor=color, alpha=0.3, zorder=4))
    ax.add_patch(Circle((x, y), size*0.4, facecolor=color, alpha=0.8, zorder=5))

def draw_visium_grid(ax, x, y, rows=3, cols=3, size=1.5):
    for r in range(rows):
        for c in range(cols):
            ox = x + c * size * 1.5
            oy = y + r * size * 1.7 + (c % 2) * size * 0.8
            # RegularPolygon(xy, numVertices, radius=5, ...)
            ax.add_patch(RegularPolygon((ox, oy), 6, radius=size, facecolor="#F59E0B", alpha=0.4, zorder=4))

def draw_umap_mini(ax, x, y, w=10, h=10):
    np.random.seed(42)
    for c, color in enumerate(["#EF4444", "#10B981", "#6366F1"]):
        pts = np.random.normal(0, 1, (15, 2))
        ax.scatter(x + pts[:,0]*2 + c*3, y + pts[:,1]*2 + c*2, s=3, c=color, alpha=0.6, zorder=6)

def render_f1_q1_style(out_pdf: Path, out_png: Path):
    fig = plt.figure(figsize=(12, 8))
    ax = fig.add_subplot(111)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis("off")

    # --- 渐变背景层 (模拟高阶层级感) ---
    ax.add_patch(Rectangle((0, 0), 100, 100, facecolor="#FFFFFF", zorder=0))
    ax.axvspan(0, 100, ymin=0.45, ymax=0.95, facecolor="#F8FAFC", alpha=0.5, zorder=1)
    ax.axvspan(0, 100, ymin=0.05, ymax=0.40, facecolor="#F1F5F9", alpha=0.8, zorder=1)

    # --- 左侧：具象化输入 (Biological Raw Data) ---
    ax.text(5, 85, "INPUTS", fontsize=14, fontweight='bold', color="#1E293B")
    draw_cell_icon(ax, 10, 75, size=2.5, color="#3B82F6")
    ax.text(15, 74, "scRNA-seq\n(Count Matrices)", fontsize=9, va='center')
    
    draw_visium_grid(ax, 8, 60, rows=2, cols=3, size=1.2)
    ax.text(15, 61, "10x Visium\n(Spatial Bundle)", fontsize=9, va='center')

    # --- 中间：核心方法库 (The Harmonizing Engine) ---
    center_x = 50
    ax.add_patch(FancyBboxPatch((32, 48), 36, 42, boxstyle="round,pad=2", 
                                facecolor="#FFFFFF", edgecolor="#8B5CF6", linewidth=2, zorder=2))
    ax.text(center_x, 86, "CloudBioIntegrator Core", ha='center', fontsize=13, fontweight='bold', color="#5B21B6")
    
    module_y = [75, 67, 59]
    module_names = ["Standardized Preprocessing", "Integration (Harmony/scVI)", "Spatial Deconvolution"]
    for y, name in zip(module_y, module_names):
        ax.add_patch(Rectangle((35, y-3), 30, 6, facecolor="#F5F3FF", edgecolor="#DDD6FE", lw=1, zorder=3))
        ax.text(center_x, y, name, ha='center', va='center', fontsize=9, fontweight='medium')
    
    ax.plot([35, 35], [55, 80], color="#C4B5FD", lw=1, ls=':', zorder=3)
    ax.plot([65, 65], [55, 80], color="#C4B5FD", lw=1, ls=':', zorder=3)

    # --- 右侧：可验证的发现 (Biological Insights) ---
    ax.text(75, 85, "VERIFIABLE RESULTS", fontsize=14, fontweight='bold', color="#1E293B")
    draw_umap_mini(ax, 80, 72)
    ax.text(85, 68, "Conserved\nClustering", ha='center', fontsize=9)
    
    ax.add_patch(Rectangle((78, 52), 14, 10, facecolor="#FEF3C7", edgecolor="#F59E0B", alpha=0.3, zorder=4))
    np.random.seed(42)
    ax.scatter(80 + np.random.rand(20)*10, 53 + np.random.rand(20)*8, s=2, c="#D97706", zorder=5)
    ax.text(85, 50, "Spatial Maps", ha='center', fontsize=9)

    # --- 跨层桥梁 (SHA-256 Stamp) ---
    stamp_x, stamp_y = 50, 42
    ax.add_patch(Circle((stamp_x, stamp_y), 6, facecolor="#FFFFFF", edgecolor="#10B981", lw=2, zorder=10))
    ax.text(stamp_x, stamp_y, "SHA-256\nLOCKED", ha='center', va='center', fontsize=8, fontweight='bold', color="#059669")
    for angle in range(0, 360, 45):
        rad = np.deg2rad(angle)
        ax.plot([stamp_x + np.cos(rad)*6, stamp_x + np.cos(rad)*8], 
                [stamp_y + np.sin(rad)*6, stamp_y + np.sin(rad)*8], color="#10B981", lw=1, zorder=9)

    # --- 底部：可重复性基石 (Infrastructure) ---
    ax.text(5, 32, "PROVENANCE BEDROCK", fontsize=12, fontweight='bold', color="#475569")
    pillars = [
        ("Pinned Environments", "Docker / Apptainer\nOS Fingerprints", 10),
        ("Traceable Parameters", "JSON Manifests\nRun Identifiers", 40),
        ("Evaluation Ethics", "Predeclared Metrics\nRobustness Checks", 70)
    ]
    for name, detail, x in pillars:
        ax.add_patch(FancyBboxPatch((x, 10), 20, 18, boxstyle="round,pad=1", 
                                    facecolor="#FFFFFF", edgecolor="#94A3B8", linewidth=1, zorder=2))
        ax.text(x+10, 24, name, ha='center', fontsize=10, fontweight='bold')
        ax.text(x+10, 16, detail, ha='center', fontsize=8, color="#64748B", linespacing=1.5)

    # --- 连线与流动 ---
    draw_fancy_arrow(ax, 22, 70, 32, 70, color="#3B82F6", lw=2)
    draw_fancy_arrow(ax, 68, 70, 75, 70, color="#10B981", lw=2)
    for x in [20, 50, 80]:
        draw_fancy_arrow(ax, x, 30, x, 48 if x!=50 else 36, color="#CBD5E1", lw=1.5, connectionstyle="arc3,rad=0.1")

    title_obj = ax.text(50, 96, "CloudBioIntegrator: A Verifiable Framework for Stable Transcriptomic Discovery", 
                        ha='center', va='center', fontsize=16, fontweight='bold', color="#0F172A")
    title_obj.set_path_effects([PathEffects.withSimplePatchShadow(offset=(1, -1), shadow_rgbFace='#E2E8F0', alpha=0.3)])

    plt.tight_layout()
    fig.savefig(out_pdf, format="pdf", bbox_inches="tight")
    fig.savefig(out_png, format="png", dpi=300, bbox_inches="tight")
    plt.close(fig)

if __name__ == "__main__":
    setup_style()
    outdir = ROOT / "plots" / "publication"
    outdir.mkdir(parents=True, exist_ok=True)
    render_f1_q1_style(outdir / "pdf" / "F1_system_contract.pdf", outdir / "png" / "F1_system_contract.png")
    print("F1 Overhauled: Q1 Tier standards applied.")
