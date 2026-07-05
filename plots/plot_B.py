"""Figures for the paper (local, no AWS). Figure labels are Ukrainian (paper language).

Fig. 1 -- architecture diagram (ingress gate + post-ingress loop).
Fig. 2 -- relative feature importance of model B (COMPUTED from real RF B, same protocol as train_local).
Fig. 3 -- containment recovery: absolute blast radius (numbers from the closed-loop results).

Run:  ./.venv/bin/python plots/plot_B.py
Out:  plots/fig1_architecture.{png,pdf}, fig2_feature_importance.{png,pdf}, fig3_blast_radius.{png,pdf}
"""
import os
import sys

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Patch
from sklearn.ensemble import RandomForestClassifier

plt.rcParams.update({
    "font.family": "DejaVu Sans",   # supports Cyrillic
    "font.size": 11,
    "axes.titlesize": 12,
    "savefig.dpi": 300,
})

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "..")
sys.path.insert(0, os.path.join(ROOT, "src", "lambdas", "trainer"))
from train_local import split3, B_FEATURES, SEEDS   # reuse the protocol

CSV = os.path.join(ROOT, "data", "maskB_dataset.csv")


def save(fig, name):
    for ext in ("png", "pdf"):
        fig.savefig(os.path.join(HERE, f"{name}.{ext}"), bbox_inches="tight")
    plt.close(fig)
    print("  ->", name + ".png / .pdf")


# ------------------------- Fig. 3: containment recovery -------------------------

def fig_blast():
    fams = ["Маскування", "Зловживання\nобсягом"]
    blind = [884.26, 622.61]        # blind gate (= no defense)
    loop = [29.63, 6.11]            # gate + model loop
    red = [96.6, 99.0]
    x = np.arange(len(fams)); w = 0.36
    fig, ax = plt.subplots(figsize=(6.2, 3.8))
    b1 = ax.bar(x - w / 2, blind, w, label="сліпий шлюз", color="#c0504d")
    b2 = ax.bar(x + w / 2, loop, w, label="шлюз + контур моделі", color="#4f81bd")
    ax.set_ylabel("Дочірні події на одне\nзловмисне джерело")
    ax.set_title("Відновлення стримування радіуса ураження")
    ax.set_xticks(x); ax.set_xticklabels(fams)
    ax.legend(frameon=False, loc="upper right")
    for rects in (b1, b2):
        for r in rects:
            ax.annotate(f"{r.get_height():.0f}", (r.get_x() + r.get_width() / 2, r.get_height()),
                        ha="center", va="bottom", fontsize=9)
    for i in range(len(fams)):
        ax.annotate(f"−{red[i]:.1f} %", (x[i], max(blind) * 0.55), ha="center",
                    color="#222", fontsize=11, fontweight="bold")
    ax.set_ylim(0, max(blind) * 1.12)
    ax.spines[["top", "right"]].set_visible(False)
    save(fig, "fig3_blast_radius")


# ------------------------- Fig. 2: feature importance -------------------------

FEAT_UA = {
    "declaredType": "заявлений тип",
    "declaredFanout": "заявлений ступінь",
    "payloadSize": "розмір навантаження",
    "hopCount": "кількість стрибків",
    "emittedChildren": "фактичне розгалуження",
    "gap": "розрив заявлене↔фактичне",
    "rep_count": "репутація: к-сть подій",
    "rep_meanEmitted": "репутація: середнє розгал.",
    "rep_maxEmitted": "репутація: макс. розгал.",
    "rep_cumChildren": "репутація: сумарні дочірні",
    "rep_rate": "репутація: частота",
}


def fig_importance():
    df = pd.read_csv(CSV)
    imps = []
    for seed in SEEDS:
        tr, _va, _te = split3(df, seed)
        m = RandomForestClassifier(n_estimators=300, n_jobs=-1, random_state=seed)
        m.fit(tr[B_FEATURES], tr.label)
        imps.append(m.feature_importances_)
    imp = np.mean(imps, axis=0); sd = np.std(imps, axis=0)
    order = np.argsort(imp)
    labels = [FEAT_UA[B_FEATURES[i]] for i in order]
    colors = ["#4f81bd" if B_FEATURES[i].startswith("rep_") else "#9bbb59" for i in order]
    fig, ax = plt.subplots(figsize=(6.8, 4.4))
    ax.barh(range(len(order)), imp[order], xerr=sd[order], color=colors,
            error_kw=dict(ecolor="#888", lw=1))
    ax.set_yticks(range(len(order))); ax.set_yticklabels(labels)
    ax.set_xlabel("Середня важливість ознаки (за Джині), RF B, 5 насінь")
    ax.set_title("Відносна важливість ознак моделі B")
    ax.legend(handles=[Patch(color="#4f81bd", label="репутація"),
                       Patch(color="#9bbb59", label="подія / заявлене")],
              frameon=False, loc="lower right")
    ax.spines[["top", "right"]].set_visible(False)
    save(fig, "fig2_feature_importance")
    top = B_FEATURES[order[-1]]
    print(f"  (top feature: {FEAT_UA[top]} = {imp[order[-1]]:.3f}; "
          f"reputation share = {sum(imp[i] for i in range(len(B_FEATURES)) if B_FEATURES[i].startswith('rep_')):.2f})")


# ------------------------- Fig. 1: architecture -------------------------

def fig_arch():
    from matplotlib.patches import Rectangle
    fig, ax = plt.subplots(figsize=(11.0, 4.5)); ax.axis("off")
    ax.set_xlim(0, 11.0); ax.set_ylim(0, 5.0)
    boxes = [
        (0.20, "Джерело\nподій", "#dbe5f1"),
        (2.35, "Вхідний шлюз\nризику R(e)", "#e5e0ec"),
        (4.75, "Обробник:\nрозгалуження,\nдочірні події", "#eaf1dd"),
        (6.75, "Збір ознак і\nрепутації", "#fde9d9"),
        (8.75, "Модель B\n(скоринг)", "#f2dcdb"),
    ]
    w, h, y = 1.80, 1.10, 1.75
    cx = [x + w / 2 for x, _, _ in boxes]
    right = [x + w for x, _, _ in boxes]
    left = [x for x, _, _ in boxes]
    top = y + h
    xb = (right[1] + left[2]) / 2               # admission boundary

    # background band for the "after admission" phase
    ax.add_patch(Rectangle((xb, 0.80), right[4] + 0.25 - xb, 2.75, fc="#f3f3f6", ec="none", zorder=0))
    # vertical admission boundary
    ax.plot([xb, xb], [0.80, top + 0.55], ls=(0, (4, 3)), color="#999", lw=1.1, zorder=1)
    ax.text(xb, y + h / 2, "момент\nпропускання", ha="center", va="center", rotation=90,
            fontsize=8, color="#666", zorder=5,
            bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none"))

    # boxes
    for x, text, fc in boxes:
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.10",
                                    fc=fc, ec="#555", lw=1.2, zorder=3))
        ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=9, zorder=4)

    # forward flow left to right
    for i in range(len(boxes) - 1):
        ax.add_patch(FancyArrowPatch((right[i], y + h / 2), (left[i + 1], y + h / 2),
                     arrowstyle="-|>", mutation_scale=13, color="#444", lw=1.5, zorder=2))
    # quarantine out of the gate (down); label to the left with a gap
    ax.add_patch(FancyArrowPatch((cx[1], y), (cx[1], y - 0.75), arrowstyle="-|>",
                 mutation_scale=12, color="#c0504d", lw=1.4, zorder=2))
    ax.text(cx[1] - 0.22, y - 0.38, "карантин", ha="right", va="center", fontsize=8.5, color="#c0504d")
    # enforcement loop: orthogonal feedback ABOVE the boxes
    ax.add_patch(FancyArrowPatch((cx[4], top), (cx[1], top), arrowstyle="-|>",
                 mutation_scale=13, color="#c0504d", lw=1.6, ls=(0, (5, 3)), zorder=2,
                 connectionstyle="arc,angleA=90,angleB=90,armA=40,armB=40,rad=10"))
    ax.text((cx[1] + cx[4]) / 2, top + 1.05,
            "позначення джерела → карантин його наступних подій",
            ha="center", va="center", fontsize=8.5, color="#c0504d")
    # phase labels at the bottom (separate row, clear of the arrows)
    ax.text((left[0] + right[1]) / 2, 0.42, "до пропускання", ha="center", va="center",
            fontsize=8.5, color="#777", style="italic")
    ax.text((xb + right[4]) / 2, 0.42, "після пропускання (контур спостереження й реагування)",
            ha="center", va="center", fontsize=8.5, color="#777", style="italic")
    save(fig, "fig1_architecture")


if __name__ == "__main__":
    print("Generating paper figures:")
    fig_blast()
    fig_arch()
    fig_importance()
    print("Done. Files in plots/.")
