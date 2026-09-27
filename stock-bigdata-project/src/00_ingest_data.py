#!/usr/bin/env python3
"""
====================================================================================
 STAGE 0 — DATA INGESTION  (Extract phase of ETL)
 Big Data Stock Market Analysis Project
------------------------------------------------------------------------------------
 Downloads real historical daily OHLCV data for all current S&P 500 companies
 (+ market indices, VIX, treasury yield, sector ETFs, Bitcoin for cross-asset
 variety) from Yahoo Finance into the RAW ZONE of the data lake.

 Raw-zone principles (data lake best practice):
   * Land data AS-IS (no cleaning) — immutability, full fidelity
   * Keep dividends & stock-split events (auto_adjust=False, actions=True)
   * One file per source entity → parallelizable ingestion (like Kafka topics
     landing into HDFS via Flume/Sqoop in production)
====================================================================================
"""
import os, sys, time, json, io, gzip
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import yfinance as yf

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # project root
RAW  = os.path.join(BASE, "data", "raw")
STOCK_DIR   = os.path.join(RAW, "stocks")
INDEX_DIR   = os.path.join(RAW, "indices")
META_DIR    = os.path.join(RAW, "metadata")
for d in (STOCK_DIR, INDEX_DIR, META_DIR):
    os.makedirs(d, exist_ok=True)

LIMIT_TICKERS = int(os.environ.get("LIMIT_TICKERS", "0"))   # 0 = all
MAX_WORKERS   = int(os.environ.get("DL_WORKERS", "6"))

# ---------------------------------------------------------------- constituents ---
def fetch_constituents():
    """S&P 500 constituent list (symbol, name, GICS sector) from the public
    'datasets/s-and-p-500-companies' registry on GitHub."""
    url = ("https://raw.githubusercontent.com/datasets/s-and-p-500-companies/"
           "main/data/constituents.csv")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    raw = urllib.request.urlopen(req, timeout=60).read().decode()
    df = pd.read_csv(io.StringIO(raw))
    df.to_csv(os.path.join(META_DIR, "constituents.csv"), index=False)
    print(f"[meta] constituents: {len(df)} companies, {df['GICS Sector'].nunique()} sectors")
    return df

# ------------------------------------------------------------------ downloader ---
def yfix(sym):
    """Yahoo uses '-' where share classes use '.' (BRK.B -> BRK-B)."""
    return sym.replace(".", "-")

def download_one(sym, outdir, retries=3, is_index=False):
    path = os.path.join(outdir, f"{sym.replace('^','IDX_').replace('=','_')}.csv")
    if os.path.exists(path) and os.path.getsize(path) > 200:
        return sym, "cached", 0
    for att in range(retries):
        try:
            df = yf.Ticker(yfix(sym)).history(period="max", auto_adjust=False,
                                              actions=True)
            if df is None or df.empty:
                return sym, "empty", 0
            df = df.reset_index()
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None).dt.strftime("%Y-%m-%d")
            else:  # intraday index has Datetime col
                df = df.rename(columns={"Datetime": "Date"})
                df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None).dt.strftime("%Y-%m-%d")
            df.to_csv(path, index=False)
            return sym, "ok", len(df)
        except Exception as e:
            if att == retries - 1:
                return sym, f"FAIL:{type(e).__name__}", 0
            time.sleep(2 + 3 * att)

def main():
    t0 = time.time()
    cons = fetch_constituents()
    syms = cons["Symbol"].astype(str).tolist()
    if LIMIT_TICKERS:
        syms = syms[:LIMIT_TICKERS]

    extras = {  # variety: indices, volatility, rates, sectors, crypto, commodity
        INDEX_DIR: ["^GSPC", "^IXIC", "^DJI", "^VIX", "^TNX", "^RUT",
                    "XLK", "XLF", "XLE", "XLV", "XLY", "XLP", "XLI", "XLU",
                    "XLB", "XLRE", "XLC", "SPY", "QQQ", "BTC-USD", "GC=F", "CL=F"],
    }
    ok = fail = rows = 0
    fails = []

    def run(pairs, tag):
        nonlocal ok, fail, rows
        with ThreadPoolExecutor(MAX_WORKERS) as ex:
            futs = {ex.submit(download_one, s, d): s for d, s in pairs}
            for i, fut in enumerate(as_completed(futs), 1):
                sym, status, n = fut.result()
                if status in ("ok", "cached"):
                    ok += 1; rows += n
                else:
                    fail += 1; fails.append((sym, status))
                if i % 25 == 0 or i == len(pairs):
                    print(f"[{tag}] {i}/{len(pairs)} ok={ok} fail={fail} rows={rows:,} "
                          f"elapsed={time.time()-t0:.0f}s", flush=True)

    run([(STOCK_DIR, s) for s in syms], "stocks")
    for d, lst in extras.items():
        run([(d, s) for s in lst], "extras")

    summary = {"tickers_ok": ok, "tickers_failed": fail, "total_rows": rows,
               "failed_list": fails, "seconds": round(time.time() - t0, 1)}
    with open(os.path.join(META_DIR, "ingest_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print("\n==== INGEST SUMMARY ====")
    print(json.dumps(summary, indent=2)[:1500])

if __name__ == "__main__":
    main()
