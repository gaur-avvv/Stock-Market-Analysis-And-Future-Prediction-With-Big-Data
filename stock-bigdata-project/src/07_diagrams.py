#!/usr/bin/env python3
"""
====================================================================================
 STAGE 7c — TECHNICAL DIAGRAMS (drawn in matplotlib from MEASURED project facts)
   arch_lake.png          end-to-end data-lake architecture
   hdfs_arch.png          how HDFS is used (NameNode/DataNode/blocks + real stats)
   mapreduce_diagram.png  MapReduce concept explained with our actual job's numbers
   five_vs.png            the 5 Vs infographic with measured values
====================================================================================
"""
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Rectangle

PROJ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CH = os.path.join(PROJ, "output", "charts")
FOOT = "Big Data Stock Market Analysis — single-node Hadoop 3.3.6 + Spark 3.5.3 cluster (1 GiB cgroup, 2 vCPU)"
plt.rcParams.update({"figure.dpi":150,"savefig.dpi":150,"savefig.bbox":"tight","font.size":8.5})

def box(ax, x, y, w, h, title, body, fc, ec=None, ts=8.6, bs=7.2, tc="white", bold=True):
    ec = ec or fc
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.35,rounding_size=0.8",
                                fc=fc, ec=ec, lw=1.1, alpha=.94, zorder=2))
    ntl = title.count("\n") + 1
    ax.text(x+w/2, y+h-0.9, title, ha="center", va="top", fontsize=ts,
            fontweight="bold" if bold else "normal", color=tc, zorder=3, linespacing=1.2)
    ax.text(x+w/2, y+h-0.9-1.55*ntl, body, ha="center", va="top", fontsize=bs, color=tc, zorder=3,
            linespacing=1.35)

def arrow(ax, x1, y1, x2, y2, label="", col="#334155", ls="-", lw=1.3, lo=(0,0), fs=7):
    ax.add_patch(FancyArrowPatch((x1,y1),(x2,y2), arrowstyle="-|>", mutation_scale=13,
                                 color=col, lw=lw, ls=ls, zorder=4,
                                 connectionstyle="arc3,rad=0.06"))
    if label:
        ax.text((x1+x2)/2+lo[0], (y1+y2)/2+lo[1], label, fontsize=fs, color=col,
                ha="center", va="center", zorder=5,
                bbox=dict(fc="white", ec="none", alpha=.85, pad=1.0))

def canvas(w, h, xlim, ylim, title):
    fig = plt.figure(figsize=(w, h))
    ax = fig.add_axes([0, 0, 1, 1]); ax.set_xlim(*xlim); ax.set_ylim(*ylim)
    ax.axis("off")
    ax.text(xlim[0]+1, ylim[1]-1.5, title, fontsize=13.5, fontweight="bold", color="#0f172a")
    ax.text(xlim[1]-1, ylim[0]+0.6, FOOT, fontsize=5.8, color="#94a3b8", ha="right")
    return fig, ax

# ---------------------------------------------------------------- 1. ARCHITECTURE ----
def arch():
    fig, ax = canvas(13.2, 7.6, (0, 132), (0, 76),
                     "End-to-end Big Data architecture — stock market data lake")
    box(ax, 1, 60, 24, 12, "SOURCES (external)", "Yahoo Finance chart API\n503 S&P 500 stocks · 22 extras\nindices · VIX · 10y yield · BTC · gold · oil", "#0f172a")
    box(ax, 1, 44, 24, 12, "INGEST  00_ingest_data.py", "pure Python · 6 threads · retry/backoff\n4,354,506 rows in 116 s, 0 failures\nlands AS-IS: OHLCV + dividends + splits", "#1e3a5f")
    box(ax, 31, 60, 26, 12, "HDFS RAW ZONE (immutable)", "/datalake/raw — 525 CSV files · 479 MB\nschema-on-read · replication=1\nhdfs dfs -put / -ls / -cat / -du", "#155e75")
    box(ax, 31, 44, 26, 12, "HADOOP MAPREDUCE", "streaming job on raw CSVs in HDFS\npython mapper + reducer (+combiner)\n4.35M rows → 130-row year stats · 70 s", "#7c2d12")
    box(ax, 63, 52, 26, 20, "SPARK ETL PIPELINE (PySpark 3.5)",
        "T2 veracity audit  — 9-check DQ report\nT1 staging parquet (map-only, 248 s)\nT4 chunked windowed transform (3 JVMs)\nT5 load curated parquet (sorted writers)",
        "#1e40af")
    box(ax, 63, 34, 26, 14, "CURATED ZONE (parquet+snappy)", "partitioned source_type × year\n251 MB on HDFS + local mirror\n4,564,956 rows · 24 typed columns", "#166534")
    box(ax, 95, 60, 34, 12, "ANALYTICS CONSUMERS", "Spark SQL: 16 insight queries (129 s)\nSpark MLlib: LR + RF direction model\npandas/matplotlib/seaborn/mplfinance EDA", "#6d28d9")
    box(ax, 95, 44, 34, 12, "VELOCITY DEMO", "Python tick producer → TCP socket\nSpark Structured Streaming, 10 s windows\n7,308 events · 0% loss · 9 micro-batches", "#9d174d")
    box(ax, 95, 26, 34, 14, "REPORT (this PDF)", "reportlab-generated, ~40 pp\nall charts, tables, diagrams embedded\n5 Vs mapped to measured numbers", "#0f172a")
    box(ax, 31, 26, 26, 14, "ORCHESTRATION", "run_pipeline.py (pure Python DAG)\nstage isolation: 1 fresh JVM per phase\ncgroup 1 GiB budget enforced per stage", "#374151")
    box(ax, 63, 12, 26, 10, "EVIDENCE & METRICS", "dq_report_raw.json · etl_metrics.json\nstreaming/ml/benchmark json · hdfs_evidence.txt\n16 sql_*.csv result tables", "#475569")
    arrow(ax, 25, 66, 31, 66, "REST→CSV")
    arrow(ax, 13, 60, 13, 56)
    arrow(ax, 25, 50, 31, 62, "hdfs dfs -put")
    arrow(ax, 44, 60, 44, 56, "read hdfs://")
    arrow(ax, 57, 66, 63, 64, "hdfs:// + file://")
    arrow(ax, 76, 52, 76, 48, "load")
    arrow(ax, 89, 41, 95, 62, "spark.sql")
    arrow(ax, 89, 38, 95, 48, "stream demo", ls="--")
    arrow(ax, 112, 44, 112, 40)
    arrow(ax, 112, 60, 112, 56)
    arrow(ax, 89, 34, 89, 20, "metrics")
    arrow(ax, 89, 17, 95, 26, "json/csv")
    arrow(ax, 57, 33, 63, 38, "curated publish", ls="--")
    arrow(ax, 44, 44, 44, 40, "drives", ls=":")
    fig.savefig(os.path.join(CH, "architecture_lake.png")); plt.close(fig)
    print("  ✔ architecture_lake.png")

# --------------------------------------------------------------------- 2. HDFS -------
def hdfs():
    fig, ax = canvas(12.6, 7.0, (0, 126), (0, 70),
                     "How HDFS is used in this project (Hadoop 3.3.6, pseudo-distributed)")
    box(ax, 2, 46, 30, 18, "CLIENT — Python / CLI (this project)",
        "hdfs dfs -mkdir -p /datalake/raw/stocks …\nhdfs dfs -put data/raw/stocks/. /datalake/raw/stocks/\nhdfs dfs -ls · -cat AAPL.csv · -du -s -h · -count\nhdfs dfs -rm -r · -mv (layout fixes)\nhdfs fsck /datalake → HEALTHY\nhdfs dfsadmin -report · -safemode wait", "#1e3a5f", bs=7.0)
    box(ax, 44, 46, 34, 18, "NAMENODE (master, RPC :19000)",
        "namespace tree of /datalake (raw · curated ·\nmr_output · reports · staging)\nfsimage + edits in /opt/hdfsdata/name\nblock map: file → [block ids, datanode]\nWeb UI :19070 · single point of metadata", "#155e75", bs=7.0)
    box(ax, 90, 46, 34, 18, "DATANODE (worker, :19010)",
        "stores blocks on /opt/hdfsdata/data\n128 MB default block size\nheartbeats + block reports to NN\nserves reads/writes directly to client\n(data path bypasses the NameNode)", "#166534", bs=7.0)
    box(ax, 44, 20, 34, 20, "BLOCK LAYOUT — measured",
        "raw zone:   525 CSV files · 479.2 MB → ~530 blocks\ncurated:    642 parquet files · 251.2 MB\nmr_output:  part-00000 · 2.4 KB · 1 block\nreplication = 1 (single node)\nunder-replicated / corrupt / missing = 0", "#0f172a", bs=7.0)
    box(ax, 90, 20, 34, 20, "CLUSTER STATS — dfsadmin",
        "Configured capacity   9.84 GB\nDFS used              ~730 MB (7.4%)\nLive datanodes        1\nBlocks w/ bad replica 0\nfsck /datalake        HEALTHY\nJVM heaps: NN 192m · DN 224m (1 GiB node)", "#7c2d12", bs=7.0)
    box(ax, 2, 20, 30, 20, "WHY HDFS HERE",
        "· single namespace for the whole lake\n· write-once-read-many fits raw zone\n· streaming MR job reads hdfs:// natively\n· curated parquet published for serving\n· fsck/dfsadmin = cluster health evidence", "#374151", bs=7.0)
    arrow(ax, 32, 55, 44, 55, "1. metadata ops\n(open/put/ls)", lo=(0,1.4))
    arrow(ax, 78, 55, 90, 55, "2. block map /\nlease grant", lo=(0,1.4))
    arrow(ax, 96, 46, 60, 40, "3. block reports", ls="--", lo=(-6,-1))
    arrow(ax, 61, 46, 61, 40, "")
    arrow(ax, 107, 46, 107, 40, "")
    fig.savefig(os.path.join(CH, "hdfs_architecture.png")); plt.close(fig)
    print("  ✔ hdfs_architecture.png")

# ----------------------------------------------------------------- 3. MAPREDUCE ------
def mapred():
    fig, ax = canvas(13.6, 7.4, (0, 136), (0, 74),
                     "MapReduce concept — explained with our real job (Hadoop Streaming, Python)")
    box(ax, 2, 44, 22, 20, "INPUT — HDFS", "/datalake/raw/stocks/*.csv\n525 files · 4.35M rows\n479 MB · line = CSV row\nDate,Open,High,…,Volume", "#155e75", bs=7.2)
    box(ax, 27, 44, 20, 20, "SPLIT & MAP", "TextInputFormat: one split\nper file (small files)\nmapper.py per line:\n  parse year, volume\n  agg[year_rows] += 1\n  agg[year_vol]  += V", "#1e40af", bs=7.2)
    box(ax, 50, 44, 20, 20, "COMBINE\n(map-side)", "in-process dict of ~240 keys\nemitted ONCE at end of map\n8,764,916 candidate records\n→ 240 combined records\n(99.99% shuffle reduction)", "#6d28d9", bs=7.2)
    box(ax, 73, 44, 20, 20, "SHUFFLE & SORT", "framework groups by key:\n1962_rows … 2026_vol\nsorts keys; streams all\nvalues of a key contiguously\nto the reducer", "#9d174d", bs=7.2)
    box(ax, 96, 44, 18, 20, "REDUCE", "reducer.py:\nrunning sum per key\nO(1) memory (streaming)\nemit year, metric, value", "#b45309", bs=7.2)
    box(ax, 117, 44, 17, 20, "OUTPUT — HDFS", "/datalake/mr_output/\nyearly_activity/\npart-00000\n130 rows (65 years\n× {rows, vol})", "#166534", bs=7.2)
    for x1, x2 in [(24,27),(47,50),(70,73),(93,96),(114,117)]:
        arrow(ax, x1, 54, x2, 54)
    ax.text(68, 67.5, "the MapReduce programming model: embarrassingly-parallel map  →  key-grouping shuffle  →  associative reduce",
            fontsize=8.4, ha="center", style="italic", color="#334155")
    box(ax, 2, 18, 42, 20, "v1 — naive mapper (what we ran first)",
        "emitted one record per input row:\n8.8M shuffle records ≈ 200 MB through the sorter\n→ JVM heap OOM after 172 s  ✗\nlesson: shuffle volume is the cost you optimize", "#7f1d1d", bs=7.2)
    box(ax, 50, 18, 42, 20, "v2 — with map-side combining (final)",
        "partial sums kept in mapper memory:\n240 records ≈ 10 KB shuffled\n→ map 100% / reduce 100% in 70 s  ✓\nthis is exactly what Hadoop's Combiner does", "#14532d", bs=7.2)
    box(ax, 96, 18, 38, 20, "WHY IT MATTERS FOR BIG DATA",
        "same code scales from 1 node to 1000:\nadd DataNodes + TaskTrackers/YARN\nand each split maps in parallel;\nthe shuffle is the only data movement.\nSpark later re-used the same idea (and\nbeat it on this dataset — see benchmark).", "#374151", bs=7.2)
    arrow(ax, 23, 44, 23, 38, "failed path", ls="--", col="#b91c1c")
    arrow(ax, 60, 44, 60, 38, "winning path", col="#15803d")
    fig.savefig(os.path.join(CH, "mapreduce_concept.png")); plt.close(fig)
    print("  ✔ mapreduce_concept.png")

# ------------------------------------------------------------------- 4. FIVE V's -----
def fivev():
    cols = [("#1e40af","VOLUME"), ("#b91c1c","VELOCITY"), ("#15803d","VARIETY"),
            ("#b45309","VERACITY"), ("#6d28d9","VALUE")]
    texts = [
        "· 4,564,958 daily-bar rows\n· 479 MB CSV raw zone on HDFS\n· +251 MB curated parquet (snappy)\n· 730 MB total in the lake\n· 18,502 (symbol,year) groups\n· 99-year span 1927→2026\n· 525 files, 1-file-per-symbol\n  → embarrassingly parallel",
        "· ingest 4.35M rows in 116 s\n  (6 parallel producers)\n· ETL: CSV→parquet 248 s,\n  chunked transform ~5 min\n· live stream: 7,308 ticks/60 s\n  ≈ 137 events/s, 0% loss\n· micro-batch latency 1.6 s avg\n· 16 SQL insights in 129 s",
        "· 525 symbols, 8 source types:\n  stock · etf · sector_etf · index ·\n  volatility · treasury · crypto ·\n  commodity\n· 11 GICS sectors joined\n· formats: CSV → parquet →\n  JSON/CSV reports\n· dividends & splits as events",
        "· 9-check DQ audit on raw:\n  2 null rows, 450 OHLC breaks,\n  1,254 weekend rows (crypto ok),\n  25,048 zero-volume flags,\n  88 true >50% anomalies\n· policy: FLAG, never delete\n· LVS adj-close artifact found\n  & documented as evidence",
        "· MO +2,981,661% since 1962;\n  NVDA 37% CAGR since 1999\n· crashes: GFC −56.8% (1,480 d\n  recovery) vs COVID −33.9% (148 d)\n· VIX↔SPX corr −0.79 (2010s)\n· Sep/Oct weakest months\n· ML: AUC≈0.50 → honest EMH;\n  gated backtest Sharpe 1.65"]
    fig, ax = canvas(13.4, 7.2, (0, 134), (0, 72), "The 5 Vs of Big Data — measured in this project")
    w = 24.6
    for i, ((col, name), txt) in enumerate(zip(cols, texts)):
        x = 2 + i*(w+1.6)
        ax.add_patch(FancyBboxPatch((x, 6), w, 52, boxstyle="round,pad=0.4,rounding_size=1.2",
                                    fc="white", ec=col, lw=1.6, zorder=2))
        ax.add_patch(FancyBboxPatch((x, 48), w, 10, boxstyle="round,pad=0.4,rounding_size=1.2",
                                    fc=col, ec=col, lw=1.6, zorder=2))
        ax.text(x+w/2, 53, name, ha="center", va="center", color="white",
                fontsize=12.5, fontweight="bold", zorder=3)
        ax.text(x+1.6, 45.5, txt, ha="left", va="top", fontsize=7.5, color="#111827",
                linespacing=1.5, zorder=3, family="monospace")
    tools = ["HDFS · Parquet · snappy", "Kafka-shaped socket stream ·\nSpark Structured Streaming",
             "schema-on-read · GICS join ·\nmulti-asset lake", "Spark DQ audit · anomaly flags ·\nquality reports on HDFS",
             "Spark SQL · MLlib · backtest ·\nPDF report"]
    for i, ((col, _nm), t) in enumerate(zip(cols, tools)):
        x = 2 + i*(w+1.6)
        ax.add_patch(FancyBboxPatch((x, 1.2), w, 4.2, boxstyle="round,pad=0.3,rounding_size=0.8",
                                    fc="#f1f5f9", ec=col, lw=1.0, zorder=2))
        ax.text(x+w/2, 3.3, t, ha="center", va="center", fontsize=6.6, color=col, zorder=3)
    fig.savefig(os.path.join(CH, "five_vs_infographic.png")); plt.close(fig)
    print("  ✔ five_vs_infographic.png")

if __name__ == "__main__":
    print("DIAGRAMS"); arch(); hdfs(); mapred(); fivev(); print("done")
