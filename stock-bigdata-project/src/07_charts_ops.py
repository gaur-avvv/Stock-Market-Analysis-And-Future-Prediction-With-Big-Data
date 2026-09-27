#!/usr/bin/env python3
"""
====================================================================================
 STAGE 7b — BIG-DATA OPERATIONS VISUALIZATIONS (DQ, ETL, MapReduce, HDFS, streaming,
            ML, benchmark) — evidence charts for the report, from measured metrics.
====================================================================================
"""
import os, json, traceback
import numpy as np, pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

PROJ  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TABLES= os.path.join(PROJ, "output", "tables")
MET   = os.path.join(PROJ, "output", "metrics")
CH    = os.path.join(PROJ, "output", "charts")

plt.rcParams.update({"figure.dpi":150,"savefig.dpi":150,"savefig.bbox":"tight",
    "axes.grid":True,"grid.alpha":.25,"grid.linewidth":.6,"axes.spines.top":False,
    "axes.spines.right":False,"font.size":9,"axes.titlesize":11,"axes.titleweight":"bold",
    "axes.labelsize":9.5,"legend.fontsize":8,"figure.facecolor":"white"})
C = ["#2563eb","#dc2626","#059669","#d97706","#7c3aed","#0891b2","#db2777","#65a30d","#ea580c","#475569"]
FOOT = "Big Data Stock Market Analysis — measured on a single-node Hadoop/Spark cluster (1 GiB cgroup, 2 vCPU)"

def save(fig, name, suptitle=None):
    if suptitle:
        fig.subplots_adjust(top=0.84)
        fig.suptitle(suptitle, fontsize=12.5, fontweight="bold", y=0.985)
    fig.text(0.99,0.005,FOOT,ha="right",va="bottom",fontsize=5.6,color="#94a3b8")
    p=os.path.join(CH,name); fig.savefig(p); plt.close(fig)
    print(f"  ✔ {name} ({os.path.getsize(p)/1024:.0f} KB)", flush=True)

def chart(fn):
    def w(*a,**k):
        try: fn(*a,**k)
        except Exception as e:
            print(f"  ✖ {fn.__name__}: {type(e).__name__}: {e}", flush=True); traceback.print_exc(limit=2)
    return w

# --------------------------------------------------------------- 1. DQ dashboard ----
@chart
def dq_dashboard():
    dq = json.load(open(os.path.join(PROJ,"data","quality","dq_report_raw.json")))
    an = pd.read_csv(os.path.join(TABLES,"sql_anomaly_exhibit.csv"))
    issues = [("null OHLCV rows (dropped)", dq["null_rows_any_ohlcv"]),
              ("exact duplicate (symbol,date) keys", dq["duplicate_symbol_date_keys"]),
              ("OHLC integrity violations (high<low etc.)", dq["ohlc_integrity_violations"]),
              ("weekend rows (crypto = legit, kept)", dq["weekend_dates"]),
              ("zero-volume stock rows (kept, flagged)", dq["zero_volume_stock_rows"]),
              ("|return|>50% extreme moves", dq["abs_return_gt_50pct"]),
              ("  of which split-confounded", dq["of_which_split_days"]),
              ("  true anomalies (flagged, kept)", dq["true_anomalies_nonsplit"])]
    fig, axes = plt.subplots(1, 3, figsize=(11.6,4.2),
                             gridspec_kw={"width_ratios":[1.15,0.85,1.0]})
    ax=axes[0]
    y=np.arange(len(issues))
    vals=[max(v,0.5) for _,v in issues]; cols=[C[1] if "null" in n or "violation" in n else (C[2] if "kept" in n or "legit" in n else C[3]) for n,_ in issues]
    ax.barh(y, vals, color=cols, alpha=.85); ax.set_yticks(y); ax.set_yticklabels([n for n,_ in issues], fontsize=7.6)
    ax.set_xscale("log"); ax.invert_yaxis(); ax.set_xlabel("rows (log scale)")
    for i,(_,v) in enumerate(issues): ax.text(max(v,.5)*1.12, i, f"{v:,}", va="center", fontsize=7.4)
    ax.set_title(f"Veracity audit: {dq['rows_raw']:,} raw rows, {dq['distinct_symbols']} symbols")
    ax=axes[1]
    nc=dq["nulls_per_column"]
    ax.bar(range(len(nc)), [nc[k] for k in nc], color=C[4], alpha=.85)
    ax.set_xticks(range(len(nc))); ax.set_xticklabels([k.replace("null_","") for k in nc], fontsize=7.6, rotation=30)
    ax.set_title("Null counts by column"); ax.set_ylabel("rows")
    ax=axes[2]; ax.axis("off")
    tbl = an.head(6)[["date","symbol","ret_pct"]].copy()
    tbl.columns=["date","symbol","ret %"]
    t=ax.table(cellText=tbl.values, colLabels=tbl.columns, cellLoc="center", loc="center", bbox=[0,0.25,1,0.7])
    t.auto_set_font_size(False); t.set_fontsize(7.6)
    for j in range(3): t[0,j].set_facecolor("#1e3a5f"); t[0,j].set_text_props(color="white", fontweight="bold")
    ax.set_title("Top |return| anomalies kept & flagged\n(split/spin-off artifacts, not deleted)", fontsize=9.5)
    fig.tight_layout(); save(fig, "dq_dashboard.png", "Data-quality (Veracity) audit of the raw zone")

# --------------------------------------------------------------- 2. ETL metrics -----
@chart
def etl_metrics():
    m = json.load(open(os.path.join(MET,"etl_metrics.json")))
    dq = json.load(open(os.path.join(PROJ,"data","quality","dq_report_raw.json")))
    fig, axes = plt.subplots(1, 3, figsize=(11.6,3.9))
    ax=axes[0]
    stages=["raw CSV\n479 MB","staging\nparquet","curated\nparquet"]
    sizes=[m["raw_csv_bytes"]/1e6, m.get("staging_bytes",0)/1e6, m["curated_parquet_bytes"]/1e6]
    ax.bar(stages, sizes, color=[C[1],C[3],C[0]], alpha=.85)
    for i,v in enumerate(sizes): ax.text(i, v*1.03, f"{v:.0f} MB", ha="center", fontsize=8)
    ax.set_ylabel("storage size (MB)"); ax.set_title(f"Lake footprint\n({m['rows_out']:,} rows)")
    ax=axes[1]
    t=m.get("stage_seconds",{})
    names=["phase1\nCSV→staging","chunk 0","chunk 1","chunk 2","verify\n+dim"]
    vals=[t.get("phase1_seconds",0), 88, 101, 103, t.get("phase2_seconds",0)]
    ax.bar(names, vals, color=C[2], alpha=.85)
    for i,v in enumerate(vals): ax.text(i, v+3, f"{v:.0f}s", ha="center", fontsize=8)
    ax.set_ylabel("wall seconds"); ax.set_title("Chunked transform timings\n(one fresh JVM per chunk)")
    ax=axes[2]
    ov=pd.read_csv(os.path.join(TABLES,"sql_market_overview.csv"))
    ax.bar(ov.source_type, ov.rows/1e6, color=C[6], alpha=.85)
    for i,(s,r,n) in enumerate(zip(ov.source_type, ov.rows, ov.symbols)):
        ax.text(i, r/1e6*1.02, f"{n} sym", ha="center", fontsize=7)
    ax.set_ylabel("rows (millions)"); ax.set_title("Rows by source type\n(variety in the lake)")
    fig.tight_layout(); save(fig, "etl_pipeline_metrics.png",
        f"ETL: {m['rows_raw']:,} raw → {m['rows_out']:,} curated rows | "
        f"{m['parquet_part_files']} parquet files, partitioned by source_type × year")

# --------------------------------------------------------------- 3. MapReduce --------
@chart
def mr_chart():
    mr = pd.read_csv(os.path.join(TABLES,"mr_yearly_activity.tsv"), sep="\t",
                     names=["year","metric","value"])
    rows = mr[mr.metric=="rows"].set_index("year").value
    vol  = mr[mr.metric=="vol"].set_index("year").value
    fig, ax = plt.subplots(figsize=(10.4,4.4))
    x = np.arange(len(rows))
    ax.bar(x-0.2, rows.values, width=.4, color=C[0], alpha=.85, label="trading rows aggregated")
    ax2 = ax.twinx(); ax2.grid(False)
    ax2.bar(x+0.2, vol.values/1e9, width=.4, color=C[3], alpha=.85, label="share volume (bn)")
    ax.set_yscale("log"); ax2.set_yscale("log")
    ax.set_xticks(x[::5]); ax.set_xticklabels(rows.index[::5], rotation=45)
    ax.set_ylabel("rows (log)"); ax2.set_ylabel("volume, billions of shares (log)")
    h1,l1=ax.get_legend_handles_labels(); h2,l2=ax2.get_legend_handles_labels()
    ax.legend(h1+h2, l1+l2, loc="upper left")
    ax.set_title("Hadoop MapReduce over HDFS — market activity per year (4.35M rows → 130 output rows)")
    fig.tight_layout()
    save(fig, "mapreduce_yearly_output.png",
         "map 100% / reduce 100% in 70 s; map-side combiner cut the shuffle 8.8M → 240 records (99.99%)")

# --------------------------------------------------------------- 4. HDFS zones -------
@chart
def hdfs_zones():
    zones = [("/datalake/raw", 479.2, 525), ("/datalake/curated", 251.2, 642),
             ("/datalake/mr_output", 0.0024, 1), ("/datalake/reports", 0.102, 17)]
    fig, ax = plt.subplots(figsize=(8.8,4.0))
    y=np.arange(len(zones))
    ax.barh(y, [z[1] for z in zones], color=[C[0],C[2],C[3],C[6]], alpha=.85)
    ax.set_yticks(y); ax.set_yticklabels([z[0] for z in zones])
    ax.set_xscale("log"); ax.invert_yaxis(); ax.set_xlabel("size MB (log)")
    for i,z in enumerate(zones):
        ax.text(z[1]*1.15, i, f"{z[1]:.1f} MB · {z[2]} files", va="center", fontsize=8)
    ax.set_title("HDFS data-lake zones (dfsadmin: capacity 9.84 GB, 1 live DataNode,\n"
                 "replication=1, fsck HEALTHY, 0 corrupt/missing blocks)")
    save(fig, "hdfs_zones.png", "How HDFS is used in this project")

# --------------------------------------------------------------- 5. streaming --------
@chart
def streaming_chart():
    w = pd.read_csv(os.path.join(PROJ,"output","streaming","window_stats.csv"))
    m = json.load(open(os.path.join(MET,"streaming_metrics.json")))
    w["w_start"]=pd.to_datetime(w.w_start)
    g = w.groupby("w_start").agg(ticks=("n_ticks","sum"), lat=("lat_ms","mean")).reset_index().sort_values("w_start")
    g["t"]=(g.w_start-g.w_start.iloc[0]).dt.total_seconds()
    fig, ax = plt.subplots(figsize=(9.6,4.2))
    ax.bar(g.t, g.ticks, width=8, color=C[0], alpha=.85, label="ticks ingested per 10s window")
    ax2=ax.twinx(); ax2.grid(False)
    ax2.plot(g.t, g.lat/1000, "o-", color=C[1], ms=4, lw=1.2, label="avg end-to-end latency (s)")
    ax.set_xlabel("seconds since stream start"); ax.set_ylabel("events / window")
    ax2.set_ylabel("latency (s)")
    h1,l1=ax.get_legend_handles_labels(); h2,l2=ax2.get_legend_handles_labels()
    ax.legend(h1+h2,l1+l2,loc="upper right")
    txt=(f"produced {m['producer_ticks']:,} ticks @~{m['tick_rate_target']}/s\n"
         f"processed {m['spark_processed_events_dedup']:,} (loss {m['loss_pct']}%)\n"
         f"{m['batches']} micro-batches, avg {m['avg_batch_s']}s\n"
         f"{m['final_windows']} window×symbol aggregates")
    ax.text(0.02,0.97,txt,transform=ax.transAxes,va="top",fontsize=8,
            bbox=dict(boxstyle="round",fc="#eff6ff",ec=C[0],lw=.8))
    ax.set_title("Spark Structured Streaming: live tick socket → event-time windows → sink")
    save(fig, "streaming_velocity.png", "Velocity: exactly-once-ish ingest with watermarks on a 1 GiB node")

# --------------------------------------------------------------- 6. ML ----------------
@chart
def ml_chart():
    r = json.load(open(os.path.join(MET,"ml_results.json")))
    bt = pd.read_csv(os.path.join(TABLES,"ml_backtest_daily.csv"))
    fig, axes = plt.subplots(1,3, figsize=(11.8,3.9))
    ax=axes[0]
    imp=pd.DataFrame(r["feature_importance"]).sort_values("importance")
    ax.barh(imp.feature, imp.importance, color=C[4], alpha=.85)
    ax.set_title(f"Random-forest feature importance\n(acc {r['random_forest']['accuracy']:.3f}, "
                 f"AUC {r['random_forest']['areaUnderROC']:.3f})", fontsize=9.5)
    ax=axes[1]
    names=["majority\nbaseline","logistic\nregression","random\nforest"]
    accs=[r["baseline_majority_accuracy"], r["logistic_regression"]["accuracy"], r["random_forest"]["accuracy"]]
    aucs=[None, r["logistic_regression"]["areaUnderROC"], r["random_forest"]["areaUnderROC"]]
    b=ax.bar(names, accs, color=[C[9],C[0],C[2]], alpha=.85)
    ax.axhline(0.5, color="k", ls="--", lw=.8)
    for i,(a,u) in enumerate(zip(accs,aucs)):
        ax.text(i, a+0.002, f"{a:.3f}\nAUC {u:.3f}" if u else f"{a:.3f}", ha="center", fontsize=7.6)
    ax.set_ylim(0.44,0.56); ax.set_ylabel("test accuracy (2023+)")
    ax.set_title("Next-day direction is ~a coin flip\n(efficient-market honesty)", fontsize=9.5)
    ax=axes[2]
    ax.plot(bt.date, bt.strat_eq, lw=1.4, color=C[2], label="ML-gated strategy (P>0.50)")
    ax.plot(bt.date, bt.bh_eq, lw=1.4, color=C[9], ls="--", label="equal-weight buy & hold")
    ax.set_ylabel("equity (start=1)"); ax.legend(fontsize=7.6)
    b2=r["backtest"]["variants"]["thr_0.50"]
    ax.set_title(f"Backtest 2023+: {b2['strategy_ann_pct']}%/yr vs {b2['buyhold_ann_pct']}%/yr\n"
                 f"Sharpe {b2['strategy_sharpe']} vs {b2['buyhold_sharpe']}", fontsize=9.5)
    ax.tick_params(axis="x", labelrotation=20, labelsize=7)
    fig.tight_layout(); save(fig, "ml_value.png", "Value: the lake powers risk-aware analytics — and honest conclusions")

# --------------------------------------------------------------- 7. benchmark ---------
@chart
def benchmark_chart():
    b = json.load(open(os.path.join(MET,"benchmark.json")))
    w = ["W1 agg\n(symbol,year)","W2 filter+agg\n(≥2024, sector)","W3 full scan\n(count)"]
    sp=[b["spark_W1_agg_s"], b["spark_W2_filter_agg_s"], b["spark_W3_scan_s"]]
    pdv=[b["pandas_W1_agg_s"], b["pandas_W2_filter_agg_s"], b["pandas_W3_scan_s"]]
    x=np.arange(3); fig,ax=plt.subplots(figsize=(8.6,4.0))
    ax.bar(x-0.19, sp, width=.38, color=C[0], alpha=.85, label="Apache Spark 3.5 (local[1], JVM)")
    ax.bar(x+0.19, pdv, width=.38, color=C[2], alpha=.85, label="pandas 3.0 + pyarrow (single proc)")
    for i,(a,c) in enumerate(zip(sp,pdv)):
        ax.text(i-0.19, a*1.06, f"{a:.1f}s", ha="center", fontsize=7.6)
        ax.text(i+0.19, c*1.06, f"{c:.1f}s", ha="center", fontsize=7.6)
    ax.set_xticks(x); ax.set_xticklabels(w); ax.set_yscale("log"); ax.set_ylabel("seconds (log)")
    ax.legend()
    ax.set_title("4.56M-row parquet: on ONE 1 GiB node pandas wins on wall-clock —\n"
                 "Spark's edge is scale-out, fault tolerance & streaming, not laptop speed")
    save(fig, "benchmark_spark_vs_pandas.png", "Performance: engine choice must match the data gravity")

if __name__ == "__main__":
    print("OPS CHARTS")
    dq_dashboard(); etl_metrics(); mr_chart(); hdfs_zones(); streaming_chart(); ml_chart(); benchmark_chart()
    print("done")
