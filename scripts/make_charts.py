"""Builds the report charts in results/charts from summary.csv, payload_sizes.csv and codec_bench.txt."""
import csv
import os
import re
import statistics

import matplotlib

matplotlib.use("Agg")  # render to files; must be set before pyplot is imported
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_DIR = os.path.join(ROOT, "results")
CHARTS_DIR = os.path.join(RESULTS_DIR, "charts")

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_MUTED = "#52514e"
GRID = "#e4e3df"
COLOR = {"grpc": "#2a78d6", "rest": "#eb6834", "h2c": "#1baf7a", "h2c4": "#eda100"}
LABEL = {"grpc": "gRPC", "rest": "REST/HTTP1.1", "h2c": "REST/h2c (1 conn)", "h2c4": "REST/h2c (4 conns)"}
MARKER = {"grpc": "o", "rest": "s", "h2c": "^", "h2c4": "D"}
MESSAGE_SIZE_COLOR = {"small": "#9ec5f4", "large": "#4f94e6", "batch": "#1b5fb0"}

PROTOCOLS = ["grpc", "rest", "h2c"]
CONCURRENCIES = [1, 64, 256]
BAR_GAP = 0.02

STYLE = {
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": INK_MUTED, "xtick.color": INK_MUTED, "ytick.color": INK_MUTED,
    "text.color": INK, "font.size": 10, "axes.titlesize": 12, "axes.titleweight": "bold",
    "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True,
    "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
}

CODEC_LINE = re.compile(r"Benchmark(Proto|JSON)(Marshal|Unmarshal)(Request|Response)/(\w+)-\d+\s+\d+\s+([\d.]+) ns/op")


def read_csv(name):
    with open(os.path.join(RESULTS_DIR, name)) as f:
        return list(csv.DictReader(f))


def find_group(groups, variant, op, payload, store, mode, concurrency, reliability="false"):
    """Returns the summary.csv row for one configuration, or None."""
    wanted = (variant, op, payload, store, mode, str(concurrency), reliability)
    for row in groups:
        if (row["variant"], row["op"], row["payload"], row["store"], row["mode"],
                row["concurrency"], row["reliability"]) == wanted:
            return row
    return None


def thousands(value, _=None):
    return f"{value/1000:.0f}k" if value >= 1000 else f"{value:.0f}"


def kilobytes(value, _=None):
    return f"{value/1000:g}k" if value >= 1000 else f"{value:.0f}"


def bar_positions(count, slot, width):
    """x positions for bar series number `slot` (0, 1, 2) in a group of three side-by-side bars."""
    return [i + (slot - 1) * (width + BAR_GAP) for i in range(count)]


def label_bars(ax, xs, values, offset, text, fontsize=9):
    for x, value in zip(xs, values):
        ax.text(x, value + offset, text(value), ha="center", va="bottom", fontsize=fontsize, color=INK_MUTED)


def save(fig, name):
    fig.tight_layout()
    fig.savefig(os.path.join(CHARTS_DIR, name), dpi=160)
    plt.close(fig)


def chart_throughput_and_p99(groups):
    fig, (throughput_ax, p99_ax) = plt.subplots(1, 2, figsize=(11, 4.2))
    for variant in ["grpc", "rest", "h2c", "h2c4"]:
        positions, throughput, p99_ms = [], [], []
        for i, concurrency in enumerate(CONCURRENCIES):
            row = find_group(groups, variant, "publish", "small", "memory", "closed", concurrency)
            if row:
                positions.append(i)
                throughput.append(float(row["rps_med"]))
                p99_ms.append(float(row["p99_us_med"]) / 1000)
        for ax, values in ((throughput_ax, throughput), (p99_ax, p99_ms)):
            ax.plot(positions, values, color=COLOR[variant], lw=2, marker=MARKER[variant], ms=8, mec=SURFACE, mew=2,
                    label=LABEL[variant])
        throughput_ax.annotate(thousands(throughput[-1]), (positions[-1], throughput[-1]), xytext=(8, 0),
                               textcoords="offset points", va="center", color=INK_MUTED, fontsize=9)
        p99_ax.annotate(f"{p99_ms[-1]:.1f} ms", (positions[-1], p99_ms[-1]), xytext=(8, 0),
                        textcoords="offset points", va="center", color=INK_MUTED, fontsize=9)
    for ax in (throughput_ax, p99_ax):
        ax.set_xticks(range(3), [f"c={c}" for c in CONCURRENCIES])
        ax.set_xlim(-0.2, 2.6)
        ax.set_xlabel("concurrent in-flight requests")
    throughput_ax.set_title("Throughput (higher is better)", loc="left")
    throughput_ax.set_ylabel("successful requests / s")
    throughput_ax.yaxis.set_major_formatter(FuncFormatter(thousands))
    throughput_ax.set_ylim(0, None)
    p99_ax.set_title("p99 latency (lower is better)", loc="left")
    p99_ax.set_ylabel("ms (log scale)")
    p99_ax.set_yscale("log")
    p99_ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:g}"))
    p99_ax.legend(frameon=False, loc="lower right")
    fig.suptitle("Publish (small event), in-memory store — median of 3 runs", x=0.01, ha="left", color=INK_MUTED,
                 fontsize=10)
    save(fig, "throughput_p99_publish_small.png")


def chart_server_cpu(groups):
    cases = [("publish", "small", "Publish small"), ("publish", "large", "Publish large"), ("status", "small", "GetStatus")]
    width = 0.26
    fig, ax = plt.subplots(figsize=(8, 4))
    for slot, variant in enumerate(PROTOCOLS):
        cpu_us = [float(find_group(groups, variant, op, payload, "memory", "closed", 64)["server_cpu_us_per_req_med"])
                  for op, payload, _ in cases]
        xs = bar_positions(len(cases), slot, width)
        ax.bar(xs, cpu_us, width, color=COLOR[variant], label=LABEL[variant])
        label_bars(ax, xs, cpu_us, 1.5, lambda value: f"{value:.0f}")
    ax.set_xticks(range(len(cases)), [title for _, _, title in cases])
    ax.set_ylabel("server CPU µs per request")
    ax.set_title("Server CPU cost per request at c=64 (lower is better)", loc="left")
    ax.legend(frameon=False, loc="upper left")
    ax.grid(axis="x", visible=False)
    save(fig, "server_cpu_per_request.png")


def chart_memory_vs_postgres(groups):
    stores = [("memory", "In-memory store"), ("postgres", "PostgreSQL store")]
    width = 0.26
    fig, ax = plt.subplots(figsize=(7, 4))
    for slot, variant in enumerate(PROTOCOLS):
        relative = []
        for store, _ in stores:
            rest_rps = float(find_group(groups, "rest", "publish", "small", store, "closed", 64)["rps_med"])
            relative.append(float(find_group(groups, variant, "publish", "small", store, "closed", 64)["rps_med"]) / rest_rps)
        xs = bar_positions(len(stores), slot, width)
        ax.bar(xs, relative, width, color=COLOR[variant], label=LABEL[variant])
        label_bars(ax, xs, relative, 0.03, lambda value: f"{value:.2f}×")
    ax.axhline(1, color=INK_MUTED, lw=1, ls="--")
    ax.set_xticks(range(len(stores)), [title for _, title in stores])
    ax.set_ylabel("throughput relative to REST/HTTP1.1")
    ax.set_title("The database hides the protocol difference (publish small, c=64)", loc="left", fontsize=11)
    ax.legend(frameon=False, loc="upper right")
    ax.grid(axis="x", visible=False)
    save(fig, "memory_vs_postgres.png")


def wire_bytes_per_request():
    """Returns {(scenario, protocol): bytes in both directions} from payload_sizes.csv."""
    total = {}
    for row in read_csv("payload_sizes.csv"):
        if row["kind"] == "wire":
            key = (row["scenario"], row["protocol"])
            total[key] = total.get(key, 0) + float(row["bytes"])
    return total


def chart_wire_bytes():
    wire = wire_bytes_per_request()
    protocols = [("REST/HTTP1.1", "rest"), ("REST/h2c", "h2c"), ("gRPC", "grpc")]
    scenarios = ["small", "medium", "large", "batch"]
    fig, axes = plt.subplots(1, 4, figsize=(12, 3.8))
    for ax, scenario in zip(axes, scenarios):
        totals = [wire[(scenario, name)] for name, _ in protocols]
        ax.bar(range(3), totals, 0.6, color=[COLOR[key] for _, key in protocols])
        for i, total in enumerate(totals):
            ax.text(i, total * 1.02, f"{total:,.0f} B", ha="center", va="bottom", fontsize=8.5, color=INK_MUTED)
        ax.set_xticks(range(3), ["HTTP/1.1", "h2c", "gRPC"])
        ax.set_title(scenario + (" (100 events)" if scenario == "batch" else ""), loc="left", fontsize=11)
        ax.set_ylim(0, max(totals) * 1.18)
        ax.yaxis.set_major_formatter(FuncFormatter(kilobytes))
        ax.grid(axis="x", visible=False)
    axes[0].set_ylabel("bytes on the wire per request\n(request + response, headers incl.)")
    fig.suptitle("Wire bytes per call — REST/HTTP1.1 vs REST/h2c vs gRPC", x=0.01, ha="left", fontsize=12,
                 fontweight="bold")
    save(fig, "wire_bytes.png")


def codec_medians():
    """Returns {(codec, operation, message, size): median ns/op} from codec_bench.txt."""
    samples = {}
    with open(os.path.join(RESULTS_DIR, "codec_bench.txt")) as f:
        for line in f:
            match = CODEC_LINE.match(line)
            if match:
                samples.setdefault(match.groups()[:4], []).append(float(match.group(5)))
    return {key: statistics.median(values) for key, values in samples.items()}


def chart_codec_ratio(medians):
    operations = [("Marshal", "Request"), ("Unmarshal", "Request"), ("Marshal", "Response"), ("Unmarshal", "Response")]
    width = 0.25
    fig, ax = plt.subplots(figsize=(9, 4))
    for slot, size in enumerate(["small", "large", "batch"]):
        ratios = [medians[("JSON", op, message, size)] / medians[("Proto", op, message, size)]
                  for op, message in operations]
        xs = bar_positions(len(operations), slot, width)
        ax.bar(xs, ratios, width, color=MESSAGE_SIZE_COLOR[size], label=size)
        label_bars(ax, xs, ratios, 0.1, lambda value: f"{value:.1f}×", fontsize=8.5)
    ax.axhline(1, color=INK_MUTED, lw=1, ls="--")
    ax.set_xticks(range(len(operations)), [f"{op} {message.lower()}" for op, message in operations])
    ax.set_ylabel("JSON time ÷ protobuf time")
    ax.set_title("Encoding cost: how many times slower JSON is than protobuf", loc="left")
    ax.legend(title="message", frameon=False, loc="upper left")
    ax.grid(axis="x", visible=False)
    save(fig, "codec_ratio.png")


def main():
    os.makedirs(CHARTS_DIR, exist_ok=True)
    plt.rcParams.update(STYLE)
    groups = read_csv("summary.csv")
    chart_throughput_and_p99(groups)
    chart_server_cpu(groups)
    chart_memory_vs_postgres(groups)
    chart_wire_bytes()
    medians = codec_medians()
    chart_codec_ratio(medians)
    for key in sorted(medians):
        print(key, round(medians[key], 1))


if __name__ == "__main__":
    main()
