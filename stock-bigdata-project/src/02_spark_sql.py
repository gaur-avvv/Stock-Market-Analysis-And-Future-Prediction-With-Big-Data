#!/usr/bin/env python3
"""
====================================================================================
 STAGE 2 — SPARK SQL ANALYTICS  (insight extraction from the curated zone)
------------------------------------------------------------------------------------
 Hive-style SQL on lakehouse parquet (curated zone), results land as CSV tables +
 an insights JSON that feeds the report. All queries are collect-safe (top-N /
 aggregates only) and window-heavy queries are restricted to symbol subsets.
====================================================================================
"""
import os, json, time
import pandas as pd
from pyspark.sql import SparkSession, functions as F, Window

PROJ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CURATED = "file://" + os.path.join(PROJ, "data", "curated", "prices")
SYMDIM  = "file://" + os.path.join(PROJ, "data", "curated", "symbol_dim")
TABLES  = os.path.join(PROJ, "output", "tables")
os.makedirs(TABLES, exist_ok=True)

spark = (SparkSession.builder.appName("StockLake_SQL_Analytics")
    .master("local[1]")
    .config("spark.driver.memory", "512m")
    .config("spark.sql.shuffle.partitions", "6")
    .config("spark.sql.autoBroadcastJoinThreshold", str(12*1024*1024))
    .config("spark.local.dir", "/opt/sparktmp")
    .config("spark.hadoop.parquet.block.size", str(32*1024*1024))
    .config("spark.ui.enabled", "false")
    .config("spark.driver.extraJavaOptions",
            "-XX:+UseG1GC -XX:MaxMetaspaceSize=160m -XX:ReservedCodeCacheSize=36m "
            "-Xss384k -XX:MaxDirectMemorySize=32m -XX:ParallelGCThreads=1 "
            "-XX:+ExitOnOutOfMemoryError")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
INSIGHTS, TIMES = {}, {}

df = spark.read.parquet(CURATED)
df.createOrReplaceTempView("prices")
dim = spark.read.option("header", "true").csv(SYMDIM)
dim.createOrReplaceTempView("symbol_dim")

def q(name, sql_or_df, note=""):
    t0 = time.time()
    res = sql_or_df if isinstance(sql_or_df, str) and sql_or_df.strip().upper().startswith(("WITH","SELECT")) \
          else sql_or_df
    pdf = spark.sql(res).toPandas() if isinstance(res, str) else res.toPandas()
    TIMES[name] = round(time.time()-t0, 1)
    path = os.path.join(TABLES, f"sql_{name}.csv")
    pdf.to_csv(path, index=False)
    print(f"\n### {name}  ({len(pdf)} rows, {TIMES[name]}s) {note}\n{pdf.head(12).to_string(index=False)}", flush=True)
    return pdf

stocks = "p.source_type='stock'"

# 1 — market overview -----------------------------------------------------------
ov = q("market_overview", f"""
SELECT source_type, COUNT(*) rows, COUNT(DISTINCT symbol) symbols,
       MIN(date) first_date, MAX(date) last_date,
       ROUND(AVG(volatility_21d),3) avg_ann_vol
FROM prices GROUP BY source_type ORDER BY rows DESC""")
INSIGHTS["overview"] = ov.to_dict("records")

# 2 — all-time champions ---------------------------------------------------------
ch = q("alltime_champions", f"""
WITH fl AS (
  SELECT symbol, security, sector,
         MIN(STRUCT(date, adj_close)).adj_close  AS first_px,
         MIN(STRUCT(date, adj_close)).date       AS first_dt,
         MAX(STRUCT(date, adj_close)).adj_close  AS last_px,
         MAX(STRUCT(date, adj_close)).date       AS last_dt
  FROM prices p WHERE {stocks} GROUP BY symbol, security, sector)
SELECT symbol, security, sector, first_dt, last_dt,
       ROUND(first_px,2) first_adj_px, ROUND(last_px,2) last_adj_px,
       ROUND((last_px/first_px - 1)*100, 1) AS total_return_pct,
       ROUND(POW(last_px/first_px, 365.25/GREATEST(DATEDIFF(last_dt, first_dt),1)) - 1, 4)*100 AS cagr_pct
FROM fl ORDER BY total_return_pct DESC LIMIT 15""", note="total return since IPO (adjusted)")
INSIGHTS["champions"] = ch.head(10).to_dict("records")

# 3/4 — most extreme single days --------------------------------------------------
wd = q("worst_single_days", f"""
SELECT p.date, p.symbol, p.security, p.sector, ROUND(p.daily_return*100,2) ret_pct,
       ROUND(p.adj_close,2) adj_close, p.volume
FROM prices p
WHERE {stocks} AND p.daily_return IS NOT NULL AND NOT p.is_price_anomaly
ORDER BY p.daily_return ASC LIMIT 15""")
bd = q("best_single_days", f"""
SELECT p.date, p.symbol, security, ROUND(daily_return*100,2) ret_pct
FROM prices p WHERE {stocks} AND p.daily_return IS NOT NULL AND NOT p.is_price_anomaly
ORDER BY p.daily_return DESC LIMIT 15""")
INSIGHTS["worst_day"] = wd.iloc[0].to_dict(); INSIGHTS["best_day"] = bd.iloc[0].to_dict()

# 5 — sector scoreboard ------------------------------------------------------------
sec = q("sector_scoreboard", f"""
SELECT p.sector, COUNT(DISTINCT p.symbol) n_stocks,
       ROUND(AVG(p.daily_return)*10000, 2) avg_daily_ret_bps,
       ROUND(AVG(p.volatility_21d)*100, 1) avg_ann_vol_pct,
       ROUND(SUM(p.dollar_volume)/1e12, 2) dollar_vol_trn
FROM prices p
WHERE {stocks} AND p.date >= '2015-01-01'
GROUP BY p.sector ORDER BY avg_daily_ret_bps DESC""")
INSIGHTS["sectors"] = sec.to_dict("records")

# 6 — S&P 500 index by year (window on single symbol: cheap) ------------------------
spx = q("spx_yearly", """
WITH s AS (SELECT date, year, adj_close, daily_return,
                  MAX(adj_close) OVER (PARTITION BY symbol ORDER BY date
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) peak
           FROM prices WHERE symbol='IDX_GSPC'),
     d AS (SELECT *, adj_close/peak - 1 dd FROM s)
SELECT year, ROUND(AVG(adj_close),1) avg_close,
       ROUND((MAX(CASE WHEN rn_desc=1 THEN adj_close END)/
              MAX(CASE WHEN rn_asc=1  THEN adj_close END)-1)*100,1) annual_ret_pct,
       ROUND(MIN(dd)*100,1) max_drawdown_pct,
       ROUND(MIN(daily_return)*100,2) worst_day_pct
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY year ORDER BY date) rn_asc,
             ROW_NUMBER() OVER (PARTITION BY year ORDER BY date DESC) rn_desc FROM d)
GROUP BY year ORDER BY year""")
INSIGHTS["spx_best_year"] = spx.loc[spx.annual_ret_pct.idxmax()].to_dict()
INSIGHTS["spx_worst_year"] = spx.loc[spx.annual_ret_pct.idxmin()].to_dict()

# 7 — VIX vs S&P correlation --------------------------------------------------------
vx = q("vix_spx_correlation", """
WITH r AS (
  SELECT date,
         MAX(CASE WHEN symbol='IDX_GSPC' THEN daily_return END) spx_ret,
         MAX(CASE WHEN symbol='IDX_VIX'  THEN daily_return END) vix_ret
  FROM prices WHERE symbol IN ('IDX_GSPC','IDX_VIX') GROUP BY date)
SELECT CASE WHEN date < '2000-01-01' THEN '1990s'
            WHEN date < '2010-01-01' THEN '2000s'
            WHEN date < '2020-01-01' THEN '2010s' ELSE '2020s' END era,
       COUNT(*) n_days, ROUND(CORR(spx_ret, vix_ret), 3) corr_spx_vix
FROM r WHERE spx_ret IS NOT NULL AND vix_ret IS NOT NULL
GROUP BY 1 ORDER BY 1""")
INSIGHTS["vix_corr"] = vx.to_dict("records")

# 8/9 — seasonality ------------------------------------------------------------------
mo = q("monthly_seasonality", f"""
SELECT month, ROUND(AVG(daily_return)*10000,2) avg_ret_bps, COUNT(*) n
FROM prices p WHERE {stocks} AND date>='1990-01-01' AND daily_return IS NOT NULL
GROUP BY month ORDER BY month""")
dow = q("dayofweek_effect", f"""
SELECT dow, ROUND(AVG(daily_return)*10000,2) avg_ret_bps, COUNT(*) n
FROM prices p WHERE {stocks} AND date>='1990-01-01' AND daily_return IS NOT NULL
GROUP BY dow ORDER BY dow""")
INSIGHTS["best_month"] = mo.loc[mo.avg_ret_bps.idxmax()].to_dict()
INSIGHTS["worst_month"] = mo.loc[mo.avg_ret_bps.idxmin()].to_dict()
INSIGHTS["dow"] = dow.to_dict("records")

# 10 — liquidity leaders --------------------------------------------------------------
vk = q("liquidity_leaders_2025", f"""
SELECT p.symbol, p.security, p.sector,
       ROUND(AVG(p.dollar_volume)/1e9, 2) avg_daily_dollar_vol_bn
FROM prices p
WHERE {stocks} AND p.year=2025
GROUP BY p.symbol, p.security, p.sector ORDER BY avg_daily_dollar_vol_bn DESC LIMIT 12""")
INSIGHTS["liquidity_top"] = vk.head(5).to_dict("records")

# 11 — dividend leaders -----------------------------------------------------------------
dv = q("dividend_leaders", """
SELECT symbol, security, sector, ROUND(CAST(total_dividends_per_share AS DOUBLE),2) div_per_share,
       CAST(rows AS INT) days_listed
FROM symbol_dim WHERE source_type='stock'
ORDER BY CAST(total_dividends_per_share AS DOUBLE) DESC LIMIT 12""")

# 12 — anomaly exhibit (veracity) ---------------------------------------------------------
an = q("anomaly_exhibit", f"""
SELECT p.date, p.symbol, p.security, ROUND(p.daily_return*100,1) ret_pct, ROUND(p.adj_close,2) px
FROM prices p
WHERE p.is_price_anomaly ORDER BY ABS(p.daily_return) DESC LIMIT 12""")
INSIGHTS["n_anomaly_days"] = int(df.filter("is_price_anomaly").count())

# 13 — golden crosses for the 30 most-liquid names ------------------------------------------
top30 = [r.symbol for r in dim.filter("source_type='stock'")
         .orderBy(F.desc(F.col("total_dollar_volume").cast("double"))).select("symbol").limit(30).collect()]
gc = q("golden_cross_count", f"""
WITH s AS (SELECT symbol, date, SIGN(ma_50 - ma_200) sg,
                  LAG(SIGN(ma_50 - ma_200)) OVER (PARTITION BY symbol ORDER BY date) psg
           FROM prices WHERE symbol IN ({",".join(repr(x) for x in top30)})
             AND date >= '2000-01-01' AND ma_200 IS NOT NULL)
SELECT symbol, SUM(CASE WHEN sg>0 AND (psg<=0 OR psg IS NULL) THEN 1 ELSE 0 END) golden_crosses,
                SUM(CASE WHEN sg<0 AND psg>=0 THEN 1 ELSE 0 END) death_crosses
FROM s GROUP BY symbol ORDER BY golden_crosses DESC LIMIT 12""")

# 14 — famous crash windows (SPX) -------------------------------------------------------------
cr = q("crash_windows", """
WITH s AS (SELECT date, adj_close FROM prices WHERE symbol='IDX_GSPC')
SELECT label, ROUND((trough/peak-1)*100,1) peak_to_trough_pct,
       DATEDIFF(trough_d, peak_d) days_down, DATEDIFF(rec_d, trough_d) days_to_recover
FROM (
 SELECT 'Dot-com 2000-02' label,
   (SELECT adj_close FROM s WHERE date='2000-03-24') peak,
   (SELECT MIN(adj_close) FROM s WHERE date BETWEEN '2000-03-25' AND '2002-12-31') trough,
   (SELECT date FROM s WHERE date='2000-03-24') peak_d,
   (SELECT date FROM s WHERE adj_close=(SELECT MIN(adj_close) FROM s WHERE date BETWEEN '2000-03-25' AND '2002-12-31') AND date<'2003-01-01' LIMIT 1) trough_d,
   (SELECT MIN(date) FROM s WHERE date>'2002-10-01' AND adj_close>=(SELECT adj_close FROM s WHERE date='2000-03-24')) rec_d
 UNION ALL
 SELECT 'GFC 2007-09',
   (SELECT adj_close FROM s WHERE date='2007-10-09'),
   (SELECT MIN(adj_close) FROM s WHERE date BETWEEN '2007-10-10' AND '2009-12-31'),
   (SELECT date FROM s WHERE date='2007-10-09'),
   (SELECT date FROM s WHERE date='2009-03-09'),
   (SELECT MIN(date) FROM s WHERE date>'2009-03-09' AND adj_close>=(SELECT adj_close FROM s WHERE date='2007-10-09'))
 UNION ALL
 SELECT 'COVID 2020-02',
   (SELECT adj_close FROM s WHERE date='2020-02-19'),
   (SELECT MIN(adj_close) FROM s WHERE date BETWEEN '2020-02-20' AND '2020-12-31'),
   (SELECT date FROM s WHERE date='2020-02-19'),
   (SELECT date FROM s WHERE date='2020-03-23'),
   (SELECT MIN(date) FROM s WHERE date>'2020-03-23' AND adj_close>=(SELECT adj_close FROM s WHERE date='2020-02-19'))
) ORDER BY peak_to_trough_pct""")
INSIGHTS["crashes"] = cr.to_dict("records")

# 15 — crypto vs equity ---------------------------------------------------------------------
cv = q("crypto_vs_equity", """
SELECT symbol, ROUND(AVG(daily_return)*252*100,1) ann_ret_pct,
       ROUND(STDDEV(daily_return)*SQRT(252)*100,1) ann_vol_pct,
       ROUND(MIN(daily_return)*100,1) worst_day_pct, COUNT(*) days
FROM prices WHERE symbol IN ('BTC-USD','IDX_GSPC','GC=F','SPY') AND date>='2015-01-01'
GROUP BY symbol ORDER BY ann_vol_pct DESC""")
INSIGHTS["btc"] = cv[cv.symbol=="BTC-USD"].iloc[0].to_dict()

# 16 — market breadth by decade ----------------------------------------------------------------
mb = q("breadth_by_decade", f"""
SELECT FLOOR(YEAR(p.date)/10)*10 decade, COUNT(DISTINCT p.symbol) symbols_trading,
       COUNT(*) rows, ROUND(AVG(p.dollar_volume)/1e6,1) avg_dollar_vol_mn
FROM prices p WHERE {stocks} GROUP BY FLOOR(YEAR(p.date)/10)*10 ORDER BY decade""")

INSIGHTS["query_times_s"] = TIMES
with open(os.path.join(PROJ, "output", "metrics", "sql_insights.json"), "w") as f:
    json.dump(INSIGHTS, f, indent=2, default=str)
print(f"\nTOTAL SQL TIME: {sum(TIMES.values()):.0f}s over {len(TIMES)} queries")
spark.stop()
print("SQL_ANALYTICS_SUCCESS")
