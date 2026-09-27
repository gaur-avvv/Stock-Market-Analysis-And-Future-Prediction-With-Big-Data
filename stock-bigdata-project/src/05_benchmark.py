#!/usr/bin/env python3
"""
====================================================================================
 STAGE 5 — PERFORMANCE: Apache Spark vs pandas on identical lake workloads
------------------------------------------------------------------------------------
 Fair-ish wall-clock comparison on this 1 GiB / 2-core node:
   W1 agg    : per (symbol, year) avg close + row count     — 4.56M-row curated parquet
   W2 filt+ag: date >= 2024-01-01 → per-sector avg vol21 + $volume
   W3 scan   : full count of the 4.56M-row table
 Spark runs first (fresh JVM per workload via subprocess is overkill here; single
 session, one query per workload). pandas runs AFTER spark.stop() so the JVM does
 not compete for the cgroup's 1 GiB. Numbers feed the benchmark chart in report.
====================================================================================
"""
import os, json, time, subprocess, sys
import pandas as pd

PROJ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CURATED = os.path.join(PROJ, "data", "curated", "prices")
METRICS = os.path.join(PROJ, "output", "metrics")
OUT = {}

def spark_side():
    from pyspark.sql import SparkSession, functions as F
    spark = (SparkSession.builder.appName("StockLake_Benchmark")
        .master("local[1]").config("spark.driver.memory", "512m")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.local.dir", "/opt/sparktmp")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraJavaOptions",
                "-XX:+UseG1GC -XX:MaxMetaspaceSize=160m -XX:ReservedCodeCacheSize=36m "
                "-Xss384k -XX:MaxDirectMemorySize=32m -XX:+ExitOnOutOfMemoryError")
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    df = spark.read.parquet("file://" + CURATED)
    t0=time.time(); n = df.count(); OUT["spark_W3_scan_s"]=round(time.time()-t0,2); OUT["rows"]=n
    t0=time.time()
    a = (df.groupBy("symbol","year").agg(F.avg("close").alias("ac"), F.count("*").alias("n"))
           .agg(F.count("*").alias("g"), F.avg("ac").alias("avg_ac")).collect()[0])
    OUT["spark_W1_agg_s"]=round(time.time()-t0,2); OUT["W1_groups"]=int(a["g"])
    t0=time.time()
    b = (df.filter(F.col("date") >= F.lit("2024-01-01").cast("date"))
           .groupBy("sector").agg(F.avg("volatility_21d").alias("v"),
                                  F.sum("dollar_volume").alias("dv")).collect())
    OUT["spark_W2_filter_agg_s"]=round(time.time()-t0,2); OUT["W2_groups"]=len(b)
    spark.stop()

def pandas_side():
    t0=time.time()
    d1 = pd.read_parquet(CURATED, columns=["symbol","year","close"])
    g = d1.groupby(["symbol","year"], observed=True).agg(ac=("close","mean"), n=("close","size"))
    OUT["pandas_W1_agg_s"]=round(time.time()-t0,2); OUT["pandas_W1_rows"]=len(d1)
    del d1, g
    t0=time.time()
    d2 = pd.read_parquet(CURATED, columns=["date","sector","volatility_21d","dollar_volume"])
    d2["date"] = pd.to_datetime(d2["date"])
    d2 = d2[d2.date >= pd.Timestamp("2024-01-01")]
    g2 = d2.groupby("sector", observed=True).agg(v=("volatility_21d","mean"), dv=("dollar_volume","sum"))
    OUT["pandas_W2_filter_agg_s"]=round(time.time()-t0,2); OUT["pandas_W2_rows"]=len(d2)
    del d2, g2
    t0=time.time()
    d3 = pd.read_parquet(CURATED, columns=["symbol"])
    OUT["pandas_W3_scan_s"]=round(time.time()-t0,2); OUT["pandas_W3_rows"]=len(d3)

if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv)>1 else "all"
    if which in ("spark","all"): spark_side()
    if which in ("pandas","all"): pandas_side()
    mpath = os.path.join(METRICS, "benchmark.json")
    old = json.load(open(mpath)) if os.path.exists(mpath) else {}
    old.update(OUT); json.dump(old, open(mpath,"w"), indent=2)
    print(json.dumps(old, indent=2))
    print("BENCHMARK_SUCCESS")
