#!/usr/bin/env python3
"""
====================================================================================
 STAGE 3 — VELOCITY DEMO: Spark Structured Streaming over a live tick socket
------------------------------------------------------------------------------------
 Simulates a market-data feed (what Apache Kafka + Flume would deliver in
 production): a pure-Python producer replays a realistic random-walk tick stream
 for 40 liquid S&P 500 names anchored at their REAL last closing prices over TCP:

     {"ts":"...","symbol":"AAPL","price":254.31,"volume":1200}

 Spark side (production-shaped streaming pipeline):
   socket source → from_json → event-time tumbling windows (10 s, 30 s watermark)
   → per-symbol count/avg/min/max price + volume → foreachBatch telemetry sink

 Hardened for a 1 GiB cgroup node: single collect per batch, incremental metrics
 flush, watchdog thread that force-finalizes (os._exit) if the JVM ever stalls.
====================================================================================
"""
import os, json, socket, threading, time, datetime as dt
import numpy as np, pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType, LongType)

PROJ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_STREAM  = os.path.join(PROJ, "output", "streaming"); os.makedirs(OUT_STREAM, exist_ok=True)
METRICS     = os.path.join(PROJ, "output", "metrics");    os.makedirs(METRICS, exist_ok=True)
HOST, PORT  = "127.0.0.1", 9999
DURATION_S  = int(os.environ.get("STREAM_SECONDS", "60"))
TICK_RATE   = int(os.environ.get("TICK_RATE", "150"))
TRIGGER_S   = "10 seconds"
SEED        = 42

# ------------------------------------------------- real anchors: tail of raw CSVs --
symdim_path = os.path.join(PROJ, "data", "curated", "symbol_dim")
part = [f for f in os.listdir(symdim_path) if f.endswith(".csv")][0]
dim = pd.read_csv(os.path.join(symdim_path, part))
liq = (dim[dim.source_type == "stock"]
       .sort_values("total_dollar_volume", ascending=False).head(40))
LAST_PX = {}
for sym in liq.symbol:
    fp = os.path.join(PROJ, "data", "raw", "stocks", f"{sym}.csv")
    if not os.path.exists(fp): continue
    with open(fp, "rb") as f:
        f.seek(max(os.path.getsize(fp) - 4096, 0))
        last = f.read().decode().strip().splitlines()[-1].split(",")
    try: LAST_PX[sym] = float(last[4])
    except (ValueError, IndexError): continue
SYM = list(LAST_PX)
print(f"[setup] anchors loaded for {len(SYM)} symbols (e.g. {SYM[0]}=${LAST_PX[SYM[0]]:.2f})", flush=True)

# ------------------------------------------------------------------ tick producer --
stats = {"produced": 0}
def producer():
    rng = np.random.default_rng(SEED)
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT)); srv.listen(1)
    print("[producer] listening", flush=True)
    conn, _ = srv.accept()
    print("[producer] spark connected — streaming ticks", flush=True)
    px = dict(LAST_PX)
    t_end = time.time() + DURATION_S
    interval = 1.0 / TICK_RATE
    buf = []
    while time.time() < t_end:
        t0 = time.time()
        s = SYM[rng.integers(len(SYM))]
        px[s] *= float(np.exp(rng.normal(0, 0.0006)))
        buf.append(json.dumps({"ts": dt.datetime.now(dt.timezone.utc).isoformat(),
                               "symbol": s, "price": round(px[s], 4),
                               "volume": int(rng.integers(100, 20000))}) + "\n")
        stats["produced"] += 1
        if len(buf) >= 50:
            conn.sendall("".join(buf).encode()); buf.clear()
        nap = interval - (time.time() - t0)
        if nap > 0: time.sleep(nap)
    if buf: conn.sendall("".join(buf).encode())
    time.sleep(1)
    try: conn.shutdown(socket.SHUT_WR); conn.close()
    except OSError: pass
    srv.close()
    print(f"[producer] done: {stats['produced']:,} ticks in {DURATION_S}s", flush=True)

# ------------------------------------------------------------- spark streaming job --
SCHEMA = StructType([StructField("ts", StringType()), StructField("symbol", StringType()),
                     StructField("price", DoubleType()), StructField("volume", LongType())])
spark = (SparkSession.builder.appName("StockLake_VelocityDemo")
    .master("local[2]")
    .config("spark.driver.memory", "512m")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.local.dir", "/opt/sparktmp")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.extraJavaOptions",
            "-XX:+UseG1GC -XX:MaxMetaspaceSize=160m -XX:ReservedCodeCacheSize=36m "
            "-Xss384k -XX:MaxDirectMemorySize=32m -XX:+ExitOnOutOfMemoryError")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

ticks = (spark.readStream.format("socket")
         .option("host", HOST).option("port", PORT).option("maxLineLength", "512")
         .load()
         .select(F.from_json("value", SCHEMA).alias("d")).select("d.*")
         .withColumn("event_time", F.to_timestamp("ts"))
         .withWatermark("event_time", "30 seconds")
         .withColumn("latency_ms", (F.current_timestamp().cast("double")
                        - F.col("event_time").cast("double")) * 1000))

win = (ticks.groupBy(F.window("event_time", "10 seconds"), "symbol")
       .agg(F.count("*").alias("n_ticks"), F.avg("price").alias("avg_px"),
            F.min("price").alias("min_px"), F.max("price").alias("max_px"),
            F.sum("volume").alias("vol"),
            F.avg("latency_ms").alias("lat_ms"),
            F.max("latency_ms").alias("lat_ms_max")))

sink = {"events": 0, "batches": 0, "lat_w": 0.0, "p95": [], "rows": [],
        "t_first": None, "t_last": None, "batch_times": []}
MFILE = os.path.join(METRICS, "streaming_metrics.json")
def dump_metrics(final=False):
    el = max((sink["t_last"] or time.time()) - (sink["t_first"] or time.time()), 1e-9)
    m = {"producer_ticks": stats["produced"], "spark_processed_events": sink["events"],
         "loss_pct": round(100*(1 - sink["events"]/max(stats["produced"],1)), 2),
         "batches": sink["batches"], "window_rows": len(sink["rows"]),
         "stream_wall_seconds": round(el,1),
         "throughput_events_per_s": round(sink["events"]/el, 1),
         "tick_rate_target": TICK_RATE, "duration_target_s": DURATION_S,
         "trigger": TRIGGER_S,
         "avg_latency_ms": round(sink["lat_w"]/max(sink["events"],1), 1),
         "p95_latency_ms": round(float(np.percentile(sink["p95"], 95)),1) if sink["p95"] else -1,
         "avg_batch_s": round(float(np.mean(sink["batch_times"])),2) if sink["batch_times"] else -1,
         "max_batch_s": round(float(np.max(sink["batch_times"])),2) if sink["batch_times"] else -1,
         "symbols": len(SYM), "window": "10s tumble / 30s watermark",
         "final": final}
    json.dump(m, open(MFILE, "w"), indent=2)
    return m

def foreach_batch(bdf, epoch_id):
    tb = time.time()
    if sink["t_first"] is None: sink["t_first"] = tb
    rows = (bdf.withColumn("w_start", F.col("window.start"))
               .select("w_start","symbol","n_ticks","avg_px","min_px","max_px","vol","lat_ms","lat_ms_max")
               .collect())                       # SINGLE collect per batch
    now = time.time()
    sink["t_last"] = now; sink["batches"] += 1; sink["batch_times"].append(round(now-tb,2))
    for r in rows:
        n = int(r.n_ticks)
        sink["events"] += n; sink["lat_w"] += float(r.lat_ms or 0)*n
        sink["p95"].append(float(r.lat_ms_max or 0))
        sink["rows"].append([str(r.w_start), r.symbol, n, float(r.avg_px), float(r.min_px),
                             float(r.max_px), int(r.vol), float(r.lat_ms or 0)])
    dump_metrics()

q = (win.writeStream.foreachBatch(foreach_batch).outputMode("update")
     .option("checkpointLocation", "file:///opt/sparktmp/stream_ckpt2")
     .trigger(processingTime=TRIGGER_S).start())

# watchdog: guarantee termination even if the JVM stalls (happened once: GC thrash)
def watchdog():
    time.sleep(DURATION_S + 100)
    print("[watchdog] forcing finalization", flush=True)
    pd.DataFrame(sink["rows"], columns=["w_start","symbol","n_ticks","avg_px","min_px",
                 "max_px","vol","lat_ms"]).to_csv(os.path.join(OUT_STREAM,"window_stats.csv"),
                 index=False)
    dump_metrics(final=True)
    os._exit(3)
threading.Thread(target=watchdog, daemon=True).start()

th = threading.Thread(target=producer, daemon=True); th.start()
time.sleep(1)
q.awaitTermination((DURATION_S + 60) * 1000)
try: q.stop()
except Exception: pass
pd.DataFrame(sink["rows"], columns=["w_start","symbol","n_ticks","avg_px","min_px",
             "max_px","vol","lat_ms"]).to_csv(os.path.join(OUT_STREAM,"window_stats.csv"),
             index=False)
m = dump_metrics(final=True)
try: spark.stop()
except Exception: pass
print("\n==== STREAMING METRICS ====\n" + json.dumps(m, indent=2))
print("STREAMING_SUCCESS")
os._exit(0)   # hard exit: no lingering non-daemon JVM waiter threads
