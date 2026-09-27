#!/usr/bin/env python3
"""
====================================================================================
 STAGE 4 — VALUE: Spark MLlib next-day direction model + strategy backtest
------------------------------------------------------------------------------------
 THREE isolated phases (each its own process — memory discipline on a 1 GiB node):
   features : universe=40 most-liquid names, 2005+ → leakage-free feature table
              (ret lags, MA ratios, vol21, range, dow, VIX regime, next-day label)
   train    : temporal split (train<=2022 / test>=2023), LogisticRegression +
              RandomForest (MLlib) → accuracy/AUC/confusion vs majority baseline,
              RF feature importance; RF PipelineModel persisted to disk
   predict  : fresh JVM loads the saved RF model, scores the test set, persists
              predictions as parquet
   backtest : pure pandas — P(up)>0.55 equal-weight strategy vs buy-and-hold
====================================================================================
"""
import os, sys, json, time, shutil
import numpy as np, pandas as pd
from pyspark.sql import SparkSession, functions as F, Window

PROJ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CURATED = "file://" + os.path.join(PROJ, "data", "curated", "prices")
SYMDIM  = "file://" + os.path.join(PROJ, "data", "curated", "symbol_dim")
FEATDIR_L = os.path.join(PROJ, "data", "ml", "features")
FEATDIR = "file://" + FEATDIR_L
MODELDIR_L = os.path.join(PROJ, "data", "ml", "rf_model")
PREDDIR_L  = os.path.join(PROJ, "data", "ml", "test_preds")
OUT     = os.path.join(PROJ, "output")
FEATS = ["ret_lag1","ret_lag2","ret_lag5","ma20_ratio","ma50_ratio","vol21","range1","dow_f","vix_ret"]

def mk(app, metaspace="192m"):
    s = (SparkSession.builder.appName(app).master("local[1]")
        .config("spark.driver.memory", "512m")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.local.dir", "/opt/sparktmp")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraJavaOptions",
            f"-XX:+UseG1GC -XX:MaxMetaspaceSize={metaspace} -XX:ReservedCodeCacheSize=48m "
            "-Xss384k -XX:MaxDirectMemorySize=32m -XX:+ExitOnOutOfMemoryError")
        .getOrCreate())
    s.sparkContext.setLogLevel("WARN")
    return s

# ================================================================ PHASE: FEATURES ==
def build_features():
    t0 = time.time(); spark = mk("StockLake_ML_Features")
    dim = spark.read.option("header","true").csv(SYMDIM)
    uni = [r.symbol for r in dim.filter("source_type='stock'")
           .orderBy(F.desc(F.col("total_dollar_volume").cast("double")))
           .select("symbol").limit(40).collect()]
    print(f"[ml] universe({len(uni)}): {uni[:10]} ...", flush=True)
    px = (spark.read.parquet(CURATED)
          .filter(F.col("symbol").isin(uni) & (F.col("year") >= 2005)))
    vix = (spark.read.parquet(CURATED).filter("symbol='IDX_VIX'")
           .select("date", F.col("daily_return").alias("vix_ret")))
    W = Window.partitionBy("symbol").orderBy("date")
    ds = (px
        .withColumn("ret_lag1", F.lag("daily_return",1).over(W))
        .withColumn("ret_lag2", F.lag("daily_return",2).over(W))
        .withColumn("ret_lag5", F.lag("daily_return",5).over(W))
        .withColumn("ma20_ratio", F.when(F.col("ma_20")>0, F.col("adj_close")/F.col("ma_20")-1))
        .withColumn("ma50_ratio", F.when(F.col("ma_50")>0, F.col("adj_close")/F.col("ma_50")-1))
        .withColumn("vol21", F.col("volatility_21d"))
        .withColumn("range1", F.col("intraday_range"))
        .withColumn("dow_f", F.col("dow").cast("double"))
        .withColumn("next_ret", F.lead("daily_return",1).over(W))
        .join(vix, on="date", how="left")
        .filter(F.col("next_ret").isNotNull() & F.col("ret_lag5").isNotNull() &
                F.col("vol21").isNotNull() & F.col("vix_ret").isNotNull() &
                F.col("ret_lag1").isNotNull() & F.col("ma20_ratio").isNotNull() &
                F.col("ma50_ratio").isNotNull() & F.col("range1").isNotNull())
        .withColumn("label", (F.col("next_ret") > 0).cast("int"))
        .select("symbol","date","year", *FEATS, "next_ret", "label"))
    shutil.rmtree(FEATDIR_L, ignore_errors=True)
    ds.coalesce(4).write.mode("overwrite").parquet(FEATDIR)
    n = spark.read.parquet(FEATDIR).count()
    print(f"[ml] feature table: {n:,} rows in {time.time()-t0:.0f}s", flush=True)
    spark.stop()

# =================================================================== PHASE: TRAIN ==
def _split(spark):
    ds = spark.read.parquet(FEATDIR)
    return (ds.filter(F.col("date") <  F.lit("2023-01-01").cast("date")),
            ds.filter(F.col("date") >= F.lit("2023-01-01").cast("date")))

def _merge_results(upd):
    mpath = os.path.join(OUT, "metrics", "ml_results.json")
    res = json.load(open(mpath)) if os.path.exists(mpath) else {}
    res.update(upd)
    json.dump(res, open(mpath, "w"), indent=2)

def train_rf():
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.classification import RandomForestClassifier
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
    t0 = time.time(); spark = mk("StockLake_ML_TrainRF", metaspace="224m")
    train, test = _split(spark)
    n_tr, n_te = train.count(), test.count()
    print(f"[ml] train={n_tr:,} test={n_te:,}", flush=True)
    asm = VectorAssembler().setInputCols(FEATS).setOutputCol("features").setHandleInvalid("keep")
    rf = RandomForestClassifier(numTrees=40, maxDepth=4, subsamplingRate=0.8, seed=7,
                                labelCol="label")
    fitted = Pipeline(stages=[asm, rf]).fit(train)
    preds = fitted.transform(test).withColumnRenamed("prediction", "pred")
    acc = MulticlassClassificationEvaluator(metricName="accuracy", labelCol="label",
                                            predictionCol="pred").evaluate(preds)
    roc = BinaryClassificationEvaluator(metricName="areaUnderROC", labelCol="label",
                                        rawPredictionCol="probability").evaluate(preds)
    cm = preds.groupBy("label", "pred").count().collect()
    base = test.groupBy("label").count().collect()
    rf_model = fitted.stages[-1]
    imp = sorted(zip(FEATS, [float(x) for x in rf_model.featureImportances]), key=lambda x: -x[1])
    _merge_results({
        "random_forest": {"accuracy": round(float(acc),4), "areaUnderROC": round(float(roc),4),
            "confusion": {f"y{int(r['label'])}_p{int(r['pred'])}": int(r["count"]) for r in cm}},
        "baseline_majority_accuracy": round(max(int(r["count"]) for r in base)/n_te, 4),
        "feature_importance": [{"feature":f,"importance":round(v,4)} for f,v in imp],
        "meta": {"train_rows": int(n_tr), "test_rows": int(n_te), "features": FEATS,
                 "split": "train<=2022 / test>=2023 (temporal, leakage-free)"}})
    print(f"[ml] random_forest: acc={acc:.4f} aucROC={roc:.4f}", flush=True)
    print("[ml] importance:", [(f, round(v,3)) for f, v in imp[:5]], flush=True)
    preds.select("date","symbol","next_ret","label").limit(1).collect()  # touch
    shutil.rmtree(MODELDIR_L, ignore_errors=True)
    fitted.write().overwrite().save("file://" + MODELDIR_L)
    print(f"[ml] RF model saved ({time.time()-t0:.0f}s)", flush=True)
    spark.stop(); print("TRAIN_RF_SUCCESS")

def train_lr():
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
    t0 = time.time(); spark = mk("StockLake_ML_TrainLR")
    train, test = _split(spark)
    asm = VectorAssembler().setInputCols(FEATS).setOutputCol("features").setHandleInvalid("keep")
    lr = LogisticRegression(maxIter=20, regParam=0.01, labelCol="label")
    fitted = Pipeline(stages=[asm, lr]).fit(train)
    preds = fitted.transform(test).withColumnRenamed("prediction", "pred")
    acc = MulticlassClassificationEvaluator(metricName="accuracy", labelCol="label",
                                            predictionCol="pred").evaluate(preds)
    roc = BinaryClassificationEvaluator(metricName="areaUnderROC", labelCol="label",
                                        rawPredictionCol="probability").evaluate(preds)
    cm = preds.groupBy("label", "pred").count().collect()
    _merge_results({"logistic_regression": {
        "accuracy": round(float(acc),4), "areaUnderROC": round(float(roc),4),
        "confusion": {f"y{int(r['label'])}_p{int(r['pred'])}": int(r["count"]) for r in cm}}})
    print(f"[ml] logistic_regression: acc={acc:.4f} aucROC={roc:.4f} ({time.time()-t0:.0f}s)", flush=True)
    spark.stop(); print("TRAIN_LR_SUCCESS")

# ================================================================== PHASE: PREDICT ==
def predict():
    from pyspark.ml import PipelineModel
    from pyspark.ml.functions import vector_to_array
    t0 = time.time(); spark = mk("StockLake_ML_Predict", metaspace="192m")
    model = PipelineModel.load("file://" + MODELDIR_L)
    test = spark.read.parquet(FEATDIR).filter(F.col("date") >= F.lit("2023-01-01").cast("date"))
    preds = model.transform(test)
    shutil.rmtree(PREDDIR_L, ignore_errors=True)
    (preds.select("date","symbol","next_ret","label",
          vector_to_array(F.col("probability"))[1].alias("p_up"))
     .coalesce(1).write.mode("overwrite").parquet("file://" + PREDDIR_L))
    n = spark.read.parquet("file://" + PREDDIR_L).count()
    print(f"[ml] predictions written: {n:,} rows in {time.time()-t0:.0f}s", flush=True)
    spark.stop(); print("PREDICT_SUCCESS")

# ================================================================= PHASE: BACKTEST ==
def backtest():
    """Pure-pandas (no JVM) strategy backtest from persisted test predictions."""
    t0=time.time()
    bt = pd.read_parquet(PREDDIR_L)
    yrs = None
    variants = {}
    for thr in (0.50, 0.55):
        bt["pos"] = (bt.p_up > thr).astype(float)
        g = bt.groupby("date")
        daily = pd.DataFrame({
            "strat_ret": g.apply(lambda d: float((d.next_ret*d.pos).mean()), include_groups=False),
            "bh_ret":    g.apply(lambda d: float(d.next_ret.mean()), include_groups=False),
            "exposure":  g.apply(lambda d: float(d.pos.mean()), include_groups=False),
            }).reset_index().sort_values("date")
        daily["strat_eq"] = (1+daily.strat_ret).cumprod(); daily["bh_eq"] = (1+daily.bh_ret).cumprod()
        if thr == 0.50:
            daily.to_csv(os.path.join(OUT,"tables","ml_backtest_daily.csv"), index=False)
        yrs = max(len(daily)/252, 0.01)
        variants[f"thr_{thr:.2f}"] = {
            "test_days": int(daily.date.nunique()),
            "strategy_total_return_pct": round(float(daily.strat_eq.iloc[-1]-1)*100,2),
            "buyhold_total_return_pct":  round(float(daily.bh_eq.iloc[-1]-1)*100,2),
            "strategy_ann_pct": round(float(daily.strat_eq.iloc[-1]**(1/yrs)-1)*100,2),
            "buyhold_ann_pct":  round(float(daily.bh_eq.iloc[-1]**(1/yrs)-1)*100,2),
            "strategy_sharpe": round(float(daily.strat_ret.mean()/max(daily.strat_ret.std(),1e-9)*np.sqrt(252)),2),
            "buyhold_sharpe":  round(float(daily.bh_ret.mean()/max(daily.bh_ret.std(),1e-9)*np.sqrt(252)),2),
            "avg_exposure": round(float(daily.exposure.mean()),3)}
    b = {"variants": variants,
        "note": "equal-weight 40-stock portfolio, long when P(up)>thr else flat, costs ignored",
        "backtest_seconds": round(time.time()-t0,1)}
    mpath = os.path.join(OUT,"metrics","ml_results.json")
    res = json.load(open(mpath)) if os.path.exists(mpath) else {}
    res["backtest"] = b
    json.dump(res, open(mpath,"w"), indent=2, default=str)
    print(json.dumps(b, indent=1)); print("BACKTEST_SUCCESS")

if __name__ == "__main__":
    {"features": build_features, "train_rf": train_rf, "train_lr": train_lr,
     "predict": predict, "backtest": backtest}[sys.argv[1]]()
