#!/usr/bin/env python3
"""Hadoop Streaming REDUCER — receives shuffle-sorted <year>_rows / <year>_vol
keys, performs the final aggregation and emits one row per (year, metric):
    year  metric  value
Shuffle-sort guarantees all values for a key arrive contiguously — the reducer
streams them in a single pass (O(1) memory, the essence of MapReduce scale-out).
"""
import sys

cur_key, total = None, 0.0

def emit(k, v):
    year, metric = k.split("_", 1)
    sys.stdout.write(f"{year}\t{metric}\t{int(v) if metric=='rows' else v:.0f}\n")

for line in sys.stdin:
    try:
        k, v = line.rstrip("\n").split("\t")
    except ValueError:
        continue
    if k != cur_key:
        if cur_key is not None:
            emit(cur_key, total)
        cur_key, total = k, 0.0
    total += float(v)
if cur_key is not None:
    emit(cur_key, total)
