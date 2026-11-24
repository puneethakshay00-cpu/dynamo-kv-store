"""Charts for the README, drawn from results/*.json (needs matplotlib).

    python -m dynamokv.plots
"""

from __future__ import annotations

import json
from pathlib import Path

THEMES = {
    "light": {"surface": "#fcfcfb", "text": "#0b0b0b", "secondary": "#52514e", "muted": "#898781",
              "grid": "#e1e0d9", "axis": "#c3c2b7", "blue": "#2a78d6", "orange": "#eb6834"},
    "dark": {"surface": "#1a1a19", "text": "#f0efec", "secondary": "#c3c2b7", "muted": "#898781",
             "grid": "#2c2c2a", "axis": "#383835", "blue": "#3987e5", "orange": "#d95926"},
}
CRITICAL, GOOD = "#d03b3b", "#0ca30c"


def _style(ax, t) -> None:
    from matplotlib.ticker import FuncFormatter

    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax.set_facecolor(t["surface"])
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(t["axis"])
    ax.tick_params(colors=t["muted"], labelsize=9, length=0)
    ax.grid(axis="y", color=t["grid"], linewidth=0.8)
    ax.set_axisbelow(True)


def node_failure(bench: dict, out: Path, theme: str) -> None:
    import matplotlib.pyplot as plt

    t = THEMES[theme]
    failure = bench["node_failure"]
    seconds = [s["t"] + 0.5 for s in failure["per_second"]]
    ops = [s["ops"] for s in failure["per_second"]]
    hint_t = [h[0] for h in failure["hints"] if h[0] <= failure["duration"]]
    hints = [h[1] for h in failure["hints"] if h[0] <= failure["duration"]]

    fig, (top, bottom) = plt.subplots(2, 1, figsize=(8, 5.2), sharex=True, facecolor=t["surface"],
                                      gridspec_kw={"height_ratios": [1, 1], "hspace": 0.6})
    rate = failure["success_rate_while_down"]
    rate_text = "100%" if rate == 1 else f"{rate:.2%}"
    panels = ((top, "Successful requests per second",
               f"{rate_text} of requests succeeded while {failure['victim']} was down"),
              (bottom, f"Hinted replicas waiting for {failure['victim']}",
               f"peak {failure['peak_hints']:,}; all handed back "
               f"{failure['hints_drained_after_restart_s']:.1f} s after the restart"))
    for ax, title, subtitle in panels:
        _style(ax, t)
        ax.set_title(title, loc="left", color=t["text"], fontsize=10.5, pad=20)
        ax.text(0, 1.05, subtitle, transform=ax.transAxes, color=t["secondary"], fontsize=9)
        for at, label, color in ((failure["kill_at"], f"{failure['victim']} killed", CRITICAL),
                                 (failure["restart_at"], f"{failure['victim']} restarted", GOOD)):
            ax.axvline(at, color=color, linewidth=1.2, linestyle=(0, (3, 2)))
            if ax is top:
                ax.text(at + 0.25, 0.06, label, transform=ax.get_xaxis_transform(), color=t["secondary"],
                        fontsize=9, va="bottom")
    top.plot(seconds, ops, color=t["blue"], linewidth=2)
    top.set_ylim(0, max(ops) * 1.15)
    bottom.plot(hint_t, hints, color=t["orange"], linewidth=2)
    bottom.set_ylim(0, max(hints) * 1.15)
    bottom.set_xlabel("seconds", color=t["muted"], fontsize=9)
    fig.savefig(out / f"node_failure_{theme}.png", dpi=160, bbox_inches="tight", facecolor=t["surface"])
    plt.close(fig)


def lost_writes(sim: dict, out: Path, theme: str) -> None:
    import matplotlib.pyplot as plt

    t = THEMES[theme]
    groups = [("No faults", "dvv_no_faults", "vclock_no_faults"),
              ("Crashes, partitions, packet loss", "dvv_faults", "vclock_faults")]

    def per_100k(name: str) -> float:
        e = sim[name]
        return 100_000 * e["lost_writes"] / max(1, e["writes_acknowledged"])

    fig, ax = plt.subplots(figsize=(8, 3.2), facecolor=t["surface"])
    _style(ax, t)
    width = 0.34
    for i, (_label, dvv, vclock) in enumerate(groups):
        for offset, name, color in ((-width / 2 - 0.01, dvv, t["blue"]), (width / 2 + 0.01, vclock, t["orange"])):
            value = per_100k(name)
            ax.bar(i + offset, value, width, color=color)
            ax.text(i + offset, value + 30, f"{value:,.0f}", ha="center", va="bottom", color=t["text"], fontsize=9)
    ax.set_xticks(range(len(groups)), [g[0] for g in groups], color=t["secondary"], fontsize=9)
    top = max(per_100k(g[2]) for g in groups)
    ax.set_ylim(0, top * 1.25)
    ax.set_title("Acknowledged writes lost, per 100,000", loc="left", color=t["text"], fontsize=10.5, pad=24)
    ax.legend(handles=[plt.Rectangle((0, 0), 1, 1, color=t["blue"]), plt.Rectangle((0, 0), 1, 1, color=t["orange"])],
              labels=["Dotted version vectors (this store)", "Vector clocks as in the Dynamo paper"],
              loc="upper left", bbox_to_anchor=(0, 1.13), ncol=2, frameon=False, fontsize=9, labelcolor=t["secondary"])
    fig.savefig(out / f"lost_writes_{theme}.png", dpi=160, bbox_inches="tight", facecolor=t["surface"])
    plt.close(fig)


def main() -> None:
    results = Path("results")
    out = results / "figures"
    out.mkdir(parents=True, exist_ok=True)
    bench = json.loads((results / "benchmark.json").read_text())
    sim = json.loads((results / "simulation.json").read_text())
    for theme in THEMES:
        node_failure(bench, out, theme)
        lost_writes(sim, out, theme)
    print(f"wrote charts to {out}")


if __name__ == "__main__":
    main()
