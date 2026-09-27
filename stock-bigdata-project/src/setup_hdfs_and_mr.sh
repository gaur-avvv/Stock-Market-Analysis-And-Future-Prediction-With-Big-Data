#!/bin/bash
# ============================================================================
# HDFS + MapReduce phase — single-node pseudo-distributed cluster
# (NameNode + DataNode daemons, LocalJobRunner for MR tasks)
# NOTE: all paths are resolved relative to this script (sandbox-friendly).
# ============================================================================
set -e
source /opt/bigdata_env.sh
PROJ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJ"
echo "PROJ=$PROJ"

echo "===== [1] TRY SWAP (1GB RAM box) ====="
if [ ! -f /opt/swapfile ]; then fallocate -l 2G /opt/swapfile 2>/dev/null && chmod 600 /opt/swapfile && mkswap /opt/swapfile >/dev/null 2>&1 || true; fi
swapon /opt/swapfile 2>&1 && echo "SWAP ON" || echo "swap not permitted (expected) — using heap-tuned daemons instead"

echo; echo "===== [2] START HDFS DAEMONS ====="
jps | grep -q NameNode || hdfs --daemon start namenode
sleep 2
jps | grep -q DataNode || hdfs --daemon start datanode
sleep 4
jps | grep -E "NameNode|DataNode" || echo "DAEMON WARNING"
hdfs dfsadmin -report 2>/dev/null | grep -E "Live datanodes|Configured Capacity|DFS Used%|Present Capacity" | head -6

echo; echo "===== [3] CREATE DATA LAKE ZONES ON HDFS ====="
for z in /datalake/raw/stocks /datalake/raw/indices /datalake/raw/metadata /datalake/staging /datalake/curated /datalake/quality /datalake/mr_output; do
  hdfs dfs -mkdir -p $z
done
hdfs dfs -ls /datalake
hdfs dfs -ls /datalake/raw

echo; echo "===== [4] UPLOAD RAW ZONE TO HDFS (ingest -> landing) ====="
hdfs dfs -put -f $PROJ/data/raw/stocks/.    /datalake/raw/stocks/
hdfs dfs -put -f $PROJ/data/raw/indices/.   /datalake/raw/indices/
hdfs dfs -put -f $PROJ/data/raw/metadata/constituents.csv /datalake/raw/metadata/
echo "--- HDFS usage ---"
hdfs dfs -du -s -h /datalake/raw
hdfs dfs -count /datalake/raw/stocks
hdfs dfs -ls /datalake/raw/stocks | head -5
echo "--- sample read back from HDFS ---"
hdfs dfs -cat /datalake/raw/stocks/AAPL.csv | head -3

echo; echo "===== [5] MAPREDUCE JOB (Hadoop Streaming): market activity by year ====="
hdfs dfs -rm -r -f /datalake/mr_output/yearly_activity >/dev/null 2>&1 || true
time hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
  -input  "/datalake/raw/stocks/*.csv" \
  -output /datalake/mr_output/yearly_activity \
  -mapper  "python3 $PROJ/src/mapreduce/mapper.py" \
  -reducer "python3 $PROJ/src/mapreduce/reducer.py" \
  -jobconf mapreduce.job.name="stock_yearly_activity" 2>&1 | grep -E "packageJobJar|Streaming Job|map [0-9]+%|reduce [0-9]+%|Job job|completed successfully|FAIL|Elapsed" | tail -8

echo; echo "--- MR OUTPUT (first 12 + last 12 lines) ---"
hdfs dfs -cat /datalake/mr_output/yearly_activity/part-00000 | head -12
echo "..."
hdfs dfs -cat /datalake/mr_output/yearly_activity/part-00000 | tail -12
hdfs dfs -get /datalake/mr_output/yearly_activity/part-00000 $PROJ/output/tables/mr_yearly_activity.tsv
wc -l $PROJ/output/tables/mr_yearly_activity.tsv

echo; echo "===== [6] CLUSTER HEALTH: fsck ====="
hdfs fsck /datalake 2>/dev/null | tail -3

echo; echo "===== PHASE A COMPLETE ====="
awk '/MemAvailable/ {printf "MemAvailable: %.0f MB\n", $2/1024}' /proc/meminfo
