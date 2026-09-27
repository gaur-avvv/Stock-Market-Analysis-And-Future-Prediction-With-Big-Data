#!/usr/bin/env python3
"""
====================================================================================
 STAGE 6 — PUBLISH CURATED + EVIDENCE ZONE TO HDFS  (Python-driven HDFS ops)
------------------------------------------------------------------------------------
 Starts the single-node HDFS cluster (if down), publishes curated parquet, symbol
 dimension, DQ reports, SQL insight tables and captures cluster evidence
 (dfsadmin report, fsck, du, ls) into output/hdfs_evidence.txt for the report,
 then stops the daemons to release RAM for downstream stages.
====================================================================================
"""
import os, subprocess, time, json

PROJ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV = dict(os.environ)
ENV["JAVA_HOME"] = "/opt/arena-python/lib/python3.11/site-packages/jdk4py/java-runtime"
ENV["PATH"] = ENV["JAVA_HOME"] + "/bin:/opt/hadoop/bin:/opt/hadoop/sbin:" + ENV["PATH"]
ENV["HADOOP_HOME"] = "/opt/hadoop"
ENV["HADOOP_CONF_DIR"] = "/opt/hadoop/etc/hadoop"
ENV["HADOOP_CLIENT_OPTS"] = "-Xmx320m"

EVID = os.path.join(PROJ, "output", "hdfs_evidence.txt")

def sh(cmd, timeout=600, show=True):
    r = subprocess.run(cmd, shell=True, env=ENV, cwd=PROJ, capture_output=True,
                       text=True, timeout=timeout)
    out = (r.stdout or "") + (r.stderr or "")
    if show:
        print(out[:3000], flush=True)
    return out, r.returncode

def jps():
    out, _ = sh("jps", show=False)
    return out

def main():
    lines = [f"# HDFS EVIDENCE — captured {time.strftime('%Y-%m-%d %H:%M:%S')}\n"]
    def cap(title, cmd, timeout=600):
        out, rc = sh(cmd, timeout=timeout, show=False)
        block = f"\n$ {cmd}\n{out.strip()}\n[rc={rc}]\n"
        lines.append(f"\n{'='*70}\n## {title}\n{'='*70}\n{block}")
        print(f"--- {title} (rc={rc}) ---\n{out.strip()[:900]}\n", flush=True)

    if "NameNode" not in jps():
        sh("hdfs --daemon start namenode"); time.sleep(3)
    if "DataNode" not in jps():
        sh("hdfs --daemon start datanode"); time.sleep(5)
    sh("hdfs dfsadmin -safemode wait", timeout=180, show=False)

    cap("Live JVM daemons (jps)", "jps")
    cap("Cluster report (dfsadmin)", "hdfs dfsadmin -report")
    cap("Create curated/report zones",
        "hdfs dfs -mkdir -p /datalake/curated/prices /datalake/curated/symbol_dim "
        "/datalake/reports/quality /datalake/reports/analytics")
    cap("Publish curated parquet zone (local -> HDFS)",
        f"hdfs dfs -put -f {PROJ}/data/curated/prices/. /datalake/curated/prices/", timeout=900)
    cap("Publish symbol dimension",
        f"hdfs dfs -put -f {PROJ}/data/curated/symbol_dim/. /datalake/curated/symbol_dim/")
    cap("Publish DQ + SQL reports",
        f"hdfs dfs -put -f {PROJ}/data/quality/dq_report_raw.json /datalake/reports/quality/ && "
        f"hdfs dfs -put -f {PROJ}/output/tables/. /datalake/reports/analytics/ && "
        f"hdfs dfs -put -f {PROJ}/output/metrics/etl_metrics.json /datalake/reports/quality/")
    cap("Lake layout (top levels)", "hdfs dfs -ls -R /datalake | head -40")
    cap("Zone sizes", "hdfs dfs -du -s -h /datalake/raw /datalake/curated /datalake/mr_output /datalake/reports")
    cap("File/dir counts per zone",
        "hdfs dfs -count /datalake/raw/stocks /datalake/curated/prices /datalake/reports/analytics")
    cap("Sample curated partition listing",
        "hdfs dfs -ls /datalake/curated/prices/source_type=stock | head -8")
    cap("MR output still resident", "hdfs dfs -cat /datalake/mr_output/yearly_activity/part-00000 | head -6")
    cap("Filesystem health (fsck)", "hdfs fsck /datalake")

    with open(EVID, "w") as f:
        f.write("".join(lines))
    print(f"evidence → {EVID} ({os.path.getsize(EVID)/1024:.0f} KB)")

    # free RAM for downstream stages
    sh("hdfs --daemon stop datanode", show=False); sh("hdfs --daemon stop namenode", show=False)
    print("daemons stopped")
    print("HDFS_PUBLISH_SUCCESS")

if __name__ == "__main__":
    main()
