#!/usr/bin/env python3
"""
====================================================================================
 STAGE 1 — SPARK ETL PIPELINE  (Transform + Load phases)
------------------------------------------------------------------------------------
 Source : data lake RAW zone (526 CSV files, 4.35M rows, 479 MB)
 Sink   : CURATED zone — partitioned, Snappy-compressed Parquet (columnar)
 Engine : Apache Spark 3.5.3 (local[2] cluster, JVM tuned for a 1 GB-RAM node)

 Steps
   T1  Extract   — read raw CSVs with EXPLICIT schema (no inference cost),
                   derive `symbol` from input_file_name() (schema-on-read)
   T2  Veracity  — data-quality audit in 3 aggregation passes: nulls, duplicates,
                   OHLC integrity, split-confounded outliers, weekend/future dates
   T3  Cleanse   — drop nulls/dupes/invalid dates, cast types
   T4  Transform — per-symbol windowed features: daily & log returns, MA20/50/200,
                   21d annualized volatility, dollar volume, intraday range,
                   anomaly flag; join GICS sector dimension
   T5  Load      — curated parquet partitioned by (source_type, year) + symbol dim
====================================================================================
"""
import os, sys, json, time, shutil
import pandas as pd
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark import StorageLevel

PROJ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_STOCKS  = "file://" + os.path.join(PROJ, "data", "raw", "stocks")
RAW_INDICES = "file://" + os.path.join(PROJ, "data", "raw", "indices")
META        = os.path.join(PROJ, "data", "raw", "metadata", "constituents.csv")
CURATED     = "file://" + os.path.join(PROJ, "data", "curated", "prices")
SYMDIM      = "file://" + os.path.join(PROJ, "data", "curated", "symbol_dim")
QUALITY_DIR = os.path.join(PROJ, "data", "quality")
METRICS     = os.path.join(PROJ, "output", "metrics")
os.makedirs(QUALITY_DIR, exist_ok=True); os.makedirs(METRICS, exist_ok=True)

INDEX_FILES = {"IDX_GSPC":"index","IDX_IXIC":"index","IDX_DJI":"index","IDX_RUT":"index",
               "IDX_VIX":"volatility_index","IDX_TNX":"treasury_yield",
               "SPY":"etf","QQQ":"etf","XLK":"sector_etf","XLF":"sector_etf","XLE":"sector_etf",
               "XLV":"sector_etf","XLY":"sector_etf","XLP":"sector_etf","XLI":"sector_etf",
               "XLU":"sector_etf","XLB":"sector_etf","XLRE":"sector_etf","XLC":"sector_etf",
               "BTC-USD":"crypto","GC_F":"commodity","CL_F":"commodity"}

RAW_SCHEMA = StructType([
    StructField("Date", StringType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Adj Close", DoubleType(), True),
    StructField("Volume", LongType(), True),
    StructField("Dividends", DoubleType(), True),
    StructField("Stock Splits", DoubleType(), True),
])

def log(msg): print(f"[etl] {msg}", flush=True)

t_start = time.time(); T = {}
spark = (SparkSession.builder
    .appName("StockLake_ETL_v1")
    .master("local[2]")
    .config("spark.driver.memory", "480m")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:MaxDirectMemorySize=64m")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
log(f"Spark {spark.version} session up in {time.time()-t_start:.1f}s")

# ------------------------------------------------------------------ T1 EXTRACT ---
t0 = time.time()
stocks = (spark.read.schema(RAW_SCHEMA).option("header", "true").option("nullValue", "")
          .csv(RAW_STOCKS)
          .withColumn("symbol", F.regexp_extract(F.input_file_name(), r"([^/]+)\.csv$", 1))
          .withColumn("source_type", F.lit("stock")))
indices = (spark.read.schema(RAW_SCHEMA).option("header", "true").option("nullValue", "")
          .csv(RAW_INDICES)
          .withColumn("symbol", F.regexp_extract(F.input_file_name(), r"([^/]+)\.csv$", 1))
          .withColumn("source_type", F.create_map(*[F.lit(x) for kv in INDEX_FILES.items()
                                                    for x in kv]).getItem(F.col("symbol"))))
raw = stocks.unionByName(indices)
# NOTE: convert Date->date in place then rename (Spark's drop() is case-insensitive,
# so drop("Date") would also kill a new "date" column!)
raw = (raw.withColumn("Date", F.to_date(F.col("Date"), "yyyy-MM-dd"))
          .withColumnRenamed("Date", "date")
          .withColumnRenamed("Adj Close", "adj_close")
          .withColumnRenamed("Stock Splits", "stock_splits"))
raw = raw.persist(StorageLevel.MEMORY_AND_DISK)
n_raw = raw.count()
T["extract_s"] = round(time.time() - t0, 1)
log(f"T1 extract: {n_raw:,} raw rows, {raw.select('symbol').distinct().count()} symbols "
    f"in {T['extract_s']}s")

# ------------------------------------------------------- T2 VERACITY (3 passes) ---
t0 = time.time()
c = F.col
int_ = lambda e: e.cast("int")
null_any = (c("date").isNull() | c("Open").isNull() | c("High").isNull() |
            c("Low").isNull() | c("Close").isNull() | c("Volume").isNull())
ohlc_bad = ((c("Close") <= 0) | (c("High") < c("Low")) | (c("High") < c("Open")) |
            (c("High") < c("Close")) | (c("Low") > c("Open")) | (c("Low") > c("Close")))
agg1 = raw.agg(
    F.sum(int_(c("date").isNull())).alias("null_date"),
    F.sum(int_(c("Open").isNull())).alias("null_open"),
    F.sum(int_(c("High").isNull())).alias("null_high"),
    F.sum(int_(c("Low").isNull())).alias("null_low"),
    F.sum(int_(c("Close").isNull())).alias("null_close"),
    F.sum(int_(c("adj_close").isNull())).alias("null_adj_close"),
    F.sum(int_(c("Volume").isNull())).alias("null_volume"),
    F.sum(int_(null_any)).alias("null_rows_any_ohlcv"),
    F.sum(int_(ohlc_bad)).alias("ohlc_integrity_violations"),
    F.sum(int_(c("date") > F.current_date())).alias("future_dates"),
    F.sum(int_(c("date") < F.lit("1900-01-01").cast("date"))).alias("pre_1900_dates"),
    F.sum(int_(F.dayofweek("date").isin([1, 7]))).alias("weekend_dates"),
    F.sum(int_((c("source_type") == "stock") & (c("Volume") == 0))).alias("zero_volume_stock_rows"),
    F.min("date").alias("date_min"), F.max("date").alias("date_max"),
    F.countDistinct("symbol").alias("distinct_symbols"),
).collect()[0]

w0 = Window.partitionBy("symbol").orderBy("date")
raw_ret = raw.withColumn("prev_adj", F.lag("adj_close").over(w0))
big_move = (c("prev_adj") > 0) & (F.abs(c("adj_close") / c("prev_adj") - 1) > 0.5)
agg2 = raw_ret.agg(
    F.sum(int_(big_move)).alias("abs_return_gt_50pct"),
    F.sum(int_(big_move & (c("stock_splits") > 0))).alias("of_which_split_days"),
    F.sum(int_(big_move & (c("stock_splits") == 0))).alias("true_anomalies_nonsplit"),
).collect()[0]

# spill-friendly duplicate detection: groupBy key -> count (sort-based agg spills
# to disk under memory pressure; countDistinct(struct(*)) blew the JVM instead)
dupg = raw.groupBy("symbol", "date").agg(F.count("*").alias("n"))
agg3 = dupg.agg(
    F.sum(int_(c("n") > 1)).alias("dup_keys"),
    F.sum(F.when(c("n") > 1, c("n") - 1).otherwise(F.lit(0))).alias("dup_extra_rows"),
).collect()[0]

dq = {"rows_raw": int(n_raw),
      "files_raw": len(os.listdir(os.path.join(PROJ,"data","raw","stocks"))) +
                   len(os.listdir(os.path.join(PROJ,"data","raw","indices"))),
      "nulls_per_column": {k: int(agg1[k] or 0) for k in
        ["null_date","null_open","null_high","null_low","null_close","null_adj_close","null_volume"]},
      "null_rows_any_ohlcv": int(agg1["null_rows_any_ohlcv"] or 0),
      "ohlc_integrity_violations": int(agg1["ohlc_integrity_violations"] or 0),
      "future_dates": int(agg1["future_dates"] or 0),
      "pre_1900_dates": int(agg1["pre_1900_dates"] or 0),
      "weekend_dates": int(agg1["weekend_dates"] or 0),
      "zero_volume_stock_rows": int(agg1["zero_volume_stock_rows"] or 0),
      "abs_return_gt_50pct": int(agg2["abs_return_gt_50pct"] or 0),
      "of_which_split_days": int(agg2["of_which_split_days"] or 0),
      "true_anomalies_nonsplit": int(agg2["true_anomalies_nonsplit"] or 0),
      "duplicate_symbol_date_keys": int(agg3["dup_keys"] or 0),
      "duplicate_extra_rows": int(agg3["dup_extra_rows"] or 0),
      "date_min": str(agg1["date_min"]), "date_max": str(agg1["date_max"]),
      "distinct_symbols": int(agg1["distinct_symbols"])}
bad_ohlc_rows = raw.filter(ohlc_bad).limit(5).collect()
dq["ohlc_violation_examples"] = [{k: str(v) for k, v in r.asDict().items()
                                  if k in ("symbol","date","Open","High","Low","Close")}
                                 for r in bad_ohlc_rows]
T["quality_s"] = round(time.time() - t0, 1)
with open(os.path.join(QUALITY_DIR, "dq_report_raw.json"), "w") as f:
    json.dump(dq, f, indent=2, default=str)
log(f"T2 veracity audit done in {T['quality_s']}s")
for k in ["null_rows_any_ohlcv","duplicate_symbol_date_keys","duplicate_extra_rows",
          "ohlc_integrity_violations","weekend_dates","zero_volume_stock_rows",
          "abs_return_gt_50pct","of_which_split_days","true_anomalies_nonsplit"]:
    log(f"   {k}: {dq[k]:,}")

# ------------------------------------------------------------------- T3 CLEANSE ----
t0 = time.time()
clean = (raw
    .filter(~null_any)
    .filter((c("date") >= F.lit("1900-01-01").cast("date")) &
            (c("date") <= F.current_date()) &
            (~F.dayofweek("date").isin([1, 7])))
    .withColumn("row_n", F.row_number().over(
        Window.partitionBy("symbol", "date").orderBy(c("Volume").desc())))
    .filter(c("row_n") == 1).drop("row_n")
    .withColumn("Close", c("Close").cast("double"))
    .withColumn("Volume", c("Volume").cast("long")))
clean = clean.persist(StorageLevel.MEMORY_AND_DISK)
n_clean = clean.count()
T["cleanse_s"] = round(time.time() - t0, 1)
log(f"T3 cleanse: {n_raw:,} → {n_clean:,} rows ({n_raw-n_clean:,} removed) in {T['cleanse_s']}s")

# ------------------------------------------------------------------ T4 TRANSFORM ---
t0 = time.time()
w  = Window.partitionBy("symbol").orderBy("date")
wr = lambda n: Window.partitionBy("symbol").orderBy(F.col("date").cast("long")).rowsBetween(-(n-1), 0)
feat = (clean
    .withColumn("daily_return", F.when(F.lag("adj_close").over(w) > 0,
                     c("adj_close")/F.lag("adj_close").over(w) - 1))
    .withColumn("log_return", F.when(c("daily_return").isNotNull() & (c("daily_return") > -1),
                     F.log(1 + c("daily_return"))))
    .withColumn("ma_20",  F.avg("adj_close").over(wr(20)))
    .withColumn("ma_50",  F.avg("adj_close").over(wr(50)))
    .withColumn("ma_200", F.avg("adj_close").over(wr(200)))
    .withColumn("volatility_21d", F.stddev("daily_return").over(wr(21)) * F.sqrt(F.lit(252.0)))
    .withColumn("dollar_volume", c("adj_close") * c("Volume"))
    .withColumn("intraday_range", F.when(c("Open") > 0, (c("High") - c("Low")) / c("Open")))
    .withColumn("is_price_anomaly", c("daily_return").isNotNull() &
                     (F.abs(c("daily_return")) > 0.5) & (c("stock_splits") == 0))
    .withColumn("year",  F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("dow",   F.dayofweek("date") - 1))
T["features_s"] = round(time.time() - t0, 1)
log(f"T4 transform: 14 engineered columns in {T['features_s']}s")

cons = pd.read_csv(META)
sec_rows = [(str(r["Symbol"]).replace(".", "-"), str(r["Security"]), str(r["GICS Sector"]))
            for _, r in cons.iterrows()]
sec_df = spark.createDataFrame(sec_rows, ["symbol", "security", "sector"])
extra_df = spark.createDataFrame(
    [(s, s, {"index":"MARKET INDEX","volatility_index":"VOLATILITY INDEX",
             "treasury_yield":"TREASURY YIELD"}.get(t, t.upper()))
     for s, t in INDEX_FILES.items()], ["symbol", "security", "sector"])
dim = sec_df.unionByName(extra_df).dropDuplicates(["symbol"])
feat = (feat.join(dim, on="symbol", how="left")
           .withColumn("sector", F.coalesce("sector", F.lit("UNKNOWN")))
           .withColumn("security", F.coalesce("security", F.col("symbol")))
           .persist(StorageLevel.MEMORY_AND_DISK))

# ---------------------------------------------------------------------- T5 LOAD ----
t0 = time.time()
shutil.rmtree(os.path.join(PROJ, "data", "curated", "prices"), ignore_errors=True)
(feat.write.mode("overwrite").partitionBy("source_type", "year")
     .option("compression", "snappy").parquet(CURATED))
symdim = (feat.groupBy("symbol", "security", "sector", "source_type")
    .agg(F.min("date").alias("first_date"), F.max("date").alias("last_date"),
         F.count("*").alias("rows"),
         F.avg("volatility_21d").alias("avg_vol_ann"),
         F.sum("dollar_volume").alias("total_dollar_volume"))
    .orderBy(F.desc("rows")))
shutil.rmtree(os.path.join(PROJ, "data", "curated", "symbol_dim"), ignore_errors=True)
symdim.coalesce(1).write.mode("overwrite").option("header", "true").csv(SYMDIM)
n_out = feat.count()
anomalies = int(feat.filter(c("is_price_anomaly")).count())
T["load_s"] = round(time.time() - t0, 1)

def dsize(p):
    tot = 0
    for root, _, files in os.walk(p):
        for fn in files: tot += os.path.getsize(os.path.join(root, fn))
    return tot
raw_bytes = dsize(os.path.join(PROJ, "data", "raw"))
curated_bytes = dsize(os.path.join(PROJ, "data", "curated", "prices"))
n_parts = sum(len(fs) for _, _, fs in os.walk(os.path.join(PROJ,"data","curated","prices")))

metrics = {
  "engine": f"Apache Spark {spark.version} (local[2], driver 512m)",
  "rows_raw": int(n_raw), "rows_clean": int(n_clean), "rows_out": int(n_out),
  "rows_removed": int(n_raw - n_clean),
  "anomalies_flagged_kept": anomalies,
  "raw_csv_bytes": raw_bytes, "curated_parquet_bytes": curated_bytes,
  "compression_ratio": round(raw_bytes / max(curated_bytes, 1), 2),
  "parquet_part_files": n_parts, "partitioning": "source_type x year",
  "distinct_symbols": dq["distinct_symbols"], "date_min": dq["date_min"], "date_max": dq["date_max"],
  "stage_seconds": T, "total_seconds": round(time.time() - t_start, 1),
}
with open(os.path.join(METRICS, "etl_metrics.json"), "w") as f:
    json.dump(metrics, f, indent=2, default=str)
log(f"T5 load: {n_out:,} rows → parquet {curated_bytes/1e6:.1f} MB "
    f"({metrics['compression_ratio']}x smaller than CSV) in {T['load_s']}s")
log(f"ETL TOTAL: {metrics['total_seconds']}s | stages: {T}")
raw.unpersist(); clean.unpersist(); feat.unpersist()
spark.stop()
print("ETL_PIPELINE_SUCCESS")
