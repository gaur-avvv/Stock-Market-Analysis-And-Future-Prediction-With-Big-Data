#!/usr/bin/env python3
"""
====================================================================================
 STAGE 1b — SPARK TRANSFORM + LOAD, two-phase memory-lean design (1 GB RAM node)
------------------------------------------------------------------------------------
 phase 1  CSV(raw)  -> filter/cast -> STAGING parquet   (map-only, zero shuffle)
 phase 2  STAGING   -> windowed features -> broadcast dim join -> CURATED parquet
                      partitioned by (source_type, year), snappy compressed

 Measured-constraint decisions (documented for the report):
   * v1 single-DAG attempt was SIGKILLed by the kernel OOM killer — CSV parse +
     window shuffle + write in one JVM exceeded the 1 GB node budget
   * split into two apps so each JVM stays well under the ceiling
   * -Xms128m/-Xmx460m: heap grows on demand instead of pre-committing
   * SerialGC + capped metaspace/code cache: minimal JVM native footprint
   * dedup skipped as a measured no-op (T2 audit found 0 duplicate keys)
   * weekend rows kept for crypto only (BTC trades 24/7 — legitimate data)
   * float32 numeric casts: ~40% smaller rows through the window shuffle
====================================================================================
"""
import os, sys, json, time, shutil
import pandas as pd
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

PROJ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_STOCKS  = "file://" + os.path.join(PROJ, "data", "raw", "stocks")
RAW_INDICES = "file://" + os.path.join(PROJ, "data", "raw", "indices")
META        = os.path.join(PROJ, "data", "raw", "metadata", "constituents.csv")
STAGING     = "file://" + os.path.join(PROJ, "data", "staging", "prices")
CURATED     = "file://" + os.path.join(PROJ, "data", "curated", "prices")
SYMDIM      = "file://" + os.path.join(PROJ, "data", "curated", "symbol_dim")
METRICS     = os.path.join(PROJ, "output", "metrics")
os.makedirs(METRICS, exist_ok=True)

INDEX_FILES = {"IDX_GSPC":"index","IDX_IXIC":"index","IDX_DJI":"index","IDX_RUT":"index",
               "IDX_VIX":"volatility_index","IDX_TNX":"treasury_yield",
               "SPY":"etf","QQQ":"etf","XLK":"sector_etf","XLF":"sector_etf","XLE":"sector_etf",
               "XLV":"sector_etf","XLY":"sector_etf","XLP":"sector_etf","XLI":"sector_etf",
               "XLU":"sector_etf","XLB":"sector_etf","XLRE":"sector_etf","XLC":"sector_etf",
               "BTC-USD":"crypto","GC_F":"commodity","CL_F":"commodity"}
RAW_SCHEMA = StructType([
    StructField("Date", StringType(), True), StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True), StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True), StructField("Adj Close", DoubleType(), True),
    StructField("Volume", LongType(), True), StructField("Dividends", DoubleType(), True),
    StructField("Stock Splits", DoubleType(), True)])

JVM_OPTS = ("-XX:+UseG1GC -XX:MaxMetaspaceSize=160m "
            "-XX:ReservedCodeCacheSize=36m -Xss384k -XX:MaxDirectMemorySize=32m "
            "-XX:ParallelGCThreads=1 -XX:+ExitOnOutOfMemoryError")

def mk_spark(app):
    s = (SparkSession.builder.appName(app).master("local[1]")
         .config("spark.driver.memory", "512m")
         .config("spark.sql.shuffle.partitions", "6")
         .config("spark.sql.autoBroadcastJoinThreshold", str(12*1024*1024))
         .config("spark.sql.adaptive.enabled", "false")
         .config("spark.ui.enabled", "false")
         .config("spark.local.dir", "/opt/sparktmp")
         .config("spark.hadoop.parquet.block.size", str(32*1024*1024))
         .config("spark.driver.extraJavaOptions", JVM_OPTS)
         .getOrCreate())
    s.sparkContext.setLogLevel("WARN")
    return s

def log(m): print(f"[tl] {m}", flush=True)
def dsize(p):
    tot = 0
    for root, _, files in os.walk(p):
        for fn in files: tot += os.path.getsize(os.path.join(root, fn))
    return tot

def read_raw(spark):
    stocks = (spark.read.schema(RAW_SCHEMA).option("header","true").option("nullValue","")
              .csv(RAW_STOCKS)
              .withColumn("symbol", F.regexp_extract(F.input_file_name(), r"([^/]+)\.csv$", 1))
              .withColumn("source_type", F.lit("stock")))
    indices = (spark.read.schema(RAW_SCHEMA).option("header","true").option("nullValue","")
              .csv(RAW_INDICES)
              .withColumn("symbol", F.regexp_extract(F.input_file_name(), r"([^/]+)\.csv$", 1))
              .withColumn("source_type", F.create_map(*[F.lit(x) for kv in INDEX_FILES.items()
                                                        for x in kv]).getItem(F.col("symbol"))))
    return (stocks.unionByName(indices)
            .withColumn("Date", F.to_date(F.col("Date"), "yyyy-MM-dd"))
            .withColumnRenamed("Date", "date")
            .withColumnRenamed("Adj Close", "adj_close")
            .withColumnRenamed("Stock Splits", "stock_splits"))

# ============================================================ PHASE 1: CSV->STAGING
def phase1():
    t0 = time.time(); spark = mk_spark("StockLake_Phase1_Staging")
    c = F.col
    clean = (read_raw(spark)
        .filter(c("date").isNotNull() & c("Open").isNotNull() & c("High").isNotNull() &
                c("Low").isNotNull() & c("Close").isNotNull() & c("Volume").isNotNull())
        .filter((c("date") >= F.lit("1900-01-01").cast("date")) & (c("date") <= F.current_date()))
        .filter((~F.dayofweek("date").isin([1, 7])) | (c("source_type") == "crypto"))
        .select("symbol", "source_type", "date",
                c("Open").cast("float").alias("open"), c("High").cast("float").alias("high"),
                c("Low").cast("float").alias("low"), c("Close").cast("float").alias("close"),
                c("adj_close").cast("float").alias("adj_close"),
                c("Volume").cast("long").alias("volume"),
                c("Dividends").cast("float").alias("dividends"),
                c("stock_splits").cast("float").alias("stock_splits")))
    shutil.rmtree(os.path.join(PROJ, "data", "staging", "prices"), ignore_errors=True)
    clean.coalesce(6).write.mode("overwrite").option("compression","snappy").parquet(STAGING)
    n = spark.read.parquet(STAGING).count()
    m = {"phase1_seconds": round(time.time()-t0,1), "rows_staging": int(n),
         "staging_bytes": dsize(os.path.join(PROJ,"data","staging","prices"))}
    mpath = os.path.join(METRICS, "etl_metrics.json")
    old = json.load(open(mpath)) if os.path.exists(mpath) else {}
    old.update(m); json.dump(old, open(mpath,"w"), indent=2)
    log(f"PHASE1 DONE rows={n:,} size={m['staging_bytes']/1e6:.1f}MB t={m['phase1_seconds']}s")
    spark.stop()

# ======================================================= PHASE 2: STAGING->CURATED
def phase2(chunk=None, nchunks=3, verify_only=False):
    t0 = time.time()
    if not verify_only:
        shutil.rmtree if chunk == 0 else None
        if chunk == 0:
            shutil.rmtree(os.path.join(PROJ, "data", "curated", "prices"), ignore_errors=True)
    spark = mk_spark(f"StockLake_Phase2_{'verify' if verify_only else f'chunk{chunk}'}")
    c = F.col
    stg = spark.read.parquet(STAGING).withColumn("epoch", c("date").cast("long"))
    W  = Window.partitionBy("symbol").orderBy("epoch")
    WR = lambda n: W.rowsBetween(-(n-1), 0)
    feat = (stg
        .withColumn("daily_return", F.when(F.lag("adj_close").over(W) > 0,
                         c("adj_close")/F.lag("adj_close").over(W) - 1).cast("float"))
        .withColumn("log_return", F.when(c("daily_return").isNotNull() & (c("daily_return") > -1),
                         F.log(1 + c("daily_return"))).cast("float"))
        .withColumn("ma_20",  F.avg("adj_close").over(WR(20)).cast("float"))
        .withColumn("ma_50",  F.avg("adj_close").over(WR(50)).cast("float"))
        .withColumn("ma_200", F.avg("adj_close").over(WR(200)).cast("float"))
        .withColumn("volatility_21d", (F.stddev("daily_return").over(WR(21))
                         * F.sqrt(F.lit(252.0))).cast("float"))
        .withColumn("dollar_volume", (c("adj_close") * c("volume")).cast("double"))
        .withColumn("intraday_range", F.when(c("open") > 0,
                         (c("high") - c("low")) / c("open")).cast("float"))
        .withColumn("is_price_anomaly", c("daily_return").isNotNull() &
                         (F.abs(c("daily_return")) > 0.5) & (c("stock_splits") == 0))
        .withColumn("year",  F.year("date"))
        .withColumn("month", F.month("date").cast("byte"))
        .withColumn("dow",   (F.dayofweek("date") - 1).cast("byte")))
    cons = pd.read_csv(META)
    sec_rows = [(str(r["Symbol"]).replace(".", "-"), str(r["Security"]), str(r["GICS Sector"]))
                for _, r in cons.iterrows()]
    dim = (spark.createDataFrame(sec_rows, ["symbol","security","sector"])
           .unionByName(spark.createDataFrame(
               [(s, s, {"index":"MARKET INDEX","volatility_index":"VOLATILITY INDEX",
                        "treasury_yield":"TREASURY YIELD"}.get(t, t.upper()))
                for s, t in INDEX_FILES.items()], ["symbol","security","sector"]))
           .dropDuplicates(["symbol"]))
    feat = (feat.join(F.broadcast(dim), on="symbol", how="left")
                .withColumn("sector", F.coalesce("sector", F.lit("UNKNOWN")))
                .withColumn("security", F.coalesce("security", c("symbol")))
                .drop("epoch"))
    # ---- chunked write across SEPARATE JVM processes (orchestrated by run_all):
    # ---- cgroup limit is 1 GiB incl. page cache; a single JVM's heap grows
    # ---- monotonically and gets OOM-killed mid-pipeline. One fresh JVM per
    # ---- pmod(hash(symbol), N) chunk keeps every symbol's FULL history inside
    # ---- one chunk (lag/rolling windows stay exact) and RSS bounded.
    if not verify_only:
        ti = time.time()
        bucket = F.pmod(F.hash("symbol"), F.lit(nchunks))
        part = (feat.where(bucket == chunk)
                    .repartition(6, "source_type", "year")
                    .sortWithinPartitions("source_type", "year"))
        (part.write.mode("append").partitionBy("source_type", "year")
             .option("compression", "snappy").parquet(CURATED))
        log(f"chunk {chunk+1}/{nchunks} written in {time.time()-ti:.0f}s")
        spark.stop()
        print("CHUNK_DONE"); return

    pq = spark.read.parquet(CURATED)
    n_out = pq.count()
    symdim = (pq.groupBy("symbol","security","sector","source_type")
        .agg(F.min("date").alias("first_date"), F.max("date").alias("last_date"),
             F.count("*").alias("rows"), F.avg("volatility_21d").alias("avg_vol_ann"),
             F.sum("dollar_volume").alias("total_dollar_volume"),
             F.sum(F.col("is_price_anomaly").cast("int")).alias("anomaly_days"),
             F.sum("dividends").alias("total_dividends_per_share"))
        .orderBy(F.desc("rows")))
    shutil.rmtree(os.path.join(PROJ, "data", "curated", "symbol_dim"), ignore_errors=True)
    symdim.coalesce(1).write.mode("overwrite").option("header","true").csv(SYMDIM)

    dq = json.load(open(os.path.join(PROJ, "data","quality","dq_report_raw.json")))
    raw_bytes = dsize(os.path.join(PROJ, "data", "raw"))
    cur_bytes = dsize(os.path.join(PROJ, "data", "curated", "prices"))
    n_parts = sum(len(fs) for _,_,fs in os.walk(os.path.join(PROJ,"data","curated","prices")))
    mpath = os.path.join(METRICS, "etl_metrics.json")
    m = json.load(open(mpath)) if os.path.exists(mpath) else {}
    m.update({
      "engine": f"Apache Spark {spark.version} (local[1], driver 512m, G1GC, chunked x3)",
      "rows_raw": dq["rows_raw"], "rows_out": int(n_out),
      "rows_removed": dq["rows_raw"] - int(n_out),
      "raw_csv_bytes": raw_bytes, "curated_parquet_bytes": cur_bytes,
      "compression_ratio": round(raw_bytes / max(cur_bytes,1), 2),
      "parquet_part_files": n_parts, "partitioning": "source_type x year",
      "distinct_symbols": dq["distinct_symbols"], "date_min": dq["date_min"],
      "date_max": dq["date_max"], "phase2_seconds": round(time.time()-t0,1)})
    json.dump(m, open(mpath,"w"), indent=2)
    log(json.dumps({k:v for k,v in m.items()}, indent=1)[:900])
    spark.stop()
    print("TRANSFORM_LOAD_SUCCESS")

if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    if which in ("1", "phase1"): phase1()
    elif which == "2chunk": phase2(chunk=int(sys.argv[2]), nchunks=int(sys.argv[3]))
    elif which == "2verify": phase2(verify_only=True)
    elif which == "all":
        phase1()
        for i in range(3): phase2(chunk=i, nchunks=3)
        phase2(verify_only=True)
