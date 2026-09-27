#!/usr/bin/env python3
"""Hadoop Streaming MAPPER (with map-side combining) — market activity by year.

Reads raw-zone CSV lines from HDFS via stdin
(Date,Open,High,Low,Close,Adj Close,Volume,Dividends,Stock Splits).

IMPORTANT DESIGN NOTE (learned the hard way — v1 emitted one record per input
row = 8.8M shuffle records and OOM'd the JVM heap):
This version performs MAP-SIDE COMBINATION (a.k.a. a combiner): partial sums
are accumulated in process memory and only ~240 distinct keys are emitted per
map task. Shuffle volume drops from ~200 MB to ~10 KB (99.99% reduction) —
the same optimization Hadoop's native Combiner provides for Java jobs.

Emits:  <year>_rows \t <row_count>
        <year>_vol  \t <total_volume>
"""
import sys
from collections import defaultdict

agg = defaultdict(float)   # key -> partial aggregate (fits: ~240 keys)
dirty = 0                  # malformed raw records (veracity accounting)

for line in sys.stdin:
    try:
        f = line.split(",")
        if len(f) < 7 or f[0].startswith("Date"):
            continue
        year = f[0][:4]
        if not ("1900" <= year <= "2030"):
            dirty += 1
            continue
        agg[f"{year}_rows"] += 1
        vol = f[6].strip()
        if vol and vol not in ("null", "None"):
            agg[f"{year}_vol"] += float(vol)
    except Exception:
        dirty += 1
        continue

for k in sorted(agg):
    v = agg[k]
    sys.stdout.write(f"{k}\t{int(v)}\n")
if dirty:
    sys.stdout.write(f"0000_dirty\t{dirty}\n")   # dirty-record counter for DQ report
