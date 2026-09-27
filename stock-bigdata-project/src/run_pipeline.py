#!/usr/bin/env python3
"""
====================================================================================
 PIPELINE ORCHESTRATOR — pure-Python workflow driver (Oozie/Airflow-style DAG)
------------------------------------------------------------------------------------
 Runs every stage as an isolated subprocess so each Spark JVM starts fresh with a
 small heap (hard requirement on this 1 GiB-cgroup node — a JVM's committed heap
 never shrinks, so long-lived multi-stage Spark apps get OOM-killed here).

 DAG:
   ingest  -> quality_audit -> staging -> curated(chunk 0..2) -> curated_verify
           -> hdfs_publish -> sql_analytics -> streaming -> ml -> benchmark
 Usage:  python3 run_pipeline.py [stage ...]      (default: all remaining)
====================================================================================
"""
import os, subprocess, sys, time

PROJ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

STAGES = {}

def stage(name):
    def deco(fn):
        STAGES[name] = fn
        return fn
    return deco

def run(desc, argv, timeout=1800):
    print(f"\n{'='*78}\n▶ {desc}\n  cmd: {' '.join(argv)}\n{'='*78}", flush=True)
    t0 = time.time()
    rc = subprocess.run(argv, cwd=PROJ, timeout=timeout).returncode
    print(f"◀ {desc} → rc={rc} in {time.time()-t0:.0f}s", flush=True)
    if rc != 0:
        raise SystemExit(f"STAGE FAILED: {desc}")

@stage("curated")
def curated():
    # transform+load in 3 chunk JVMs + 1 verify JVM (memory isolation)
    for i in range(3):
        run(f"curated chunk {i}/3", [PY, "src/01b_transform_load.py", "2chunk", str(i), "3"])
    run("curated verify + dimension", [PY, "src/01b_transform_load.py", "2verify"])

@stage("sql")
def sql():
    run("spark sql analytics", [PY, "src/02_spark_sql.py"])

@stage("streaming")
def streaming():
    run("structured streaming velocity demo", [PY, "src/03_streaming_demo.py"], timeout=900)

@stage("benchmark")
def benchmark():
    run("spark vs pandas benchmark", [PY, "src/05_benchmark.py"], timeout=900)

@stage("ml")
def ml():
    for phase in ("features", "train_rf", "train_lr", "predict", "backtest"):
        run(f"mllib phase {phase}", [PY, "src/04_ml_value.py", phase], timeout=1500)

@stage("viz")
def viz():
    run("market charts", [PY, "src/07_charts_market.py"], timeout=900)
    run("ops charts", [PY, "src/07_charts_ops.py"], timeout=900)
    run("diagrams", [PY, "src/07_diagrams.py"], timeout=900)

@stage("report")
def report():
    run("build pdf report", [PY, "report/build_report.py"], timeout=900)

if __name__ == "__main__":
    env = os.environ.copy()
    env.setdefault("JAVA_HOME", "/opt/arena-python/lib/python3.11/site-packages/jdk4py/java-runtime")
    env["PATH"] = env["JAVA_HOME"] + "/bin:" + env["PATH"]
    env["PYSPARK_PYTHON"] = PY
    os.environ.update(env)
    todo = sys.argv[1:] or list(STAGES)
    for name in todo:
        STAGES[name]()
    print("\nORCHESTRATOR: requested stages complete ✔")
