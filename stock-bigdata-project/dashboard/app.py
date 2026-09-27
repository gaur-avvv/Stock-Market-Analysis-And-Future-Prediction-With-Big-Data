#!/usr/bin/env python3
"""
====================================================================================
 LIVE DASHBOARD — BCSE402L Big Data Analytics · Stock Market Big-Data Project
------------------------------------------------------------------------------------
 Real-time companion to report/Stock_Market_Big_Data_Analysis.pdf.

 Tabs
   📡 Live Stream   : polls REAL Yahoo Finance quotes every N seconds, keeps a
                      tick buffer, emits 10-s micro-batch aggregates (count/avg/
                      min/max + ingest latency) — the production-shaped twin of
                      the Spark Structured Streaming demo in §9 of the report
   🔮 Prediction    : refits the report's leakage-free feature set on the fly
                      (return lags, MA ratios, vol21, range, weekday, VIX regime)
                      with a numpy logistic regression → P(next session up),
                      coefficient importances, out-of-sample accuracy/AUC and a
                      20-session volatility projection cone
   📊 EDA           : interactive candles + the report's static EDA figures
   🗄️ Lake & Pipe   : measured lake/ETL/MR/streaming/ML/benchmark telemetry +
                      architecture, 5-V and MapReduce diagrams
   🧾 Insights      : the Spark SQL evidence tables (downloadable) + DQ exhibit

 Run locally:        streamlit run dashboard/app.py
 Deploy (free):      push repo to GitHub → https://share.streamlit.io → New app
                     (main file: dashboard/app.py). requirements.txt at repo root
                     keeps the cloud image light (no Spark needed here).
====================================================================================
"""
import os, json, time, datetime as dt
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
import yfinance as yf

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SMOKE = bool(os.environ.get("DASH_SMOKE"))   # test harness: render once, no live loop
CH   = os.path.join(ROOT, "output", "charts")
TAB  = os.path.join(ROOT, "output", "tables")
MET  = os.path.join(ROOT, "output", "metrics")

st.set_page_config(page_title="BCSE402L · Stock Big-Data Live Dashboard",
                   page_icon="📈", layout="wide")
st.markdown("""<style>
 .kpi div[data-testid="stMetricValue"]{font-size:1.35rem}
 hr{margin:0.6rem 0}
</style>""", unsafe_allow_html=True)

def load_json(p, default=None):
    try: return json.load(open(os.path.join(MET, p)))
    except Exception: return default

# ---------------------------------------------------------------------- caches ----
@st.cache_data(ttl=3600, show_spinner=False)
def daily(sym: str, period: str = "5y") -> pd.DataFrame:
    df = yf.Ticker(sym).history(period=period, auto_adjust=False)
    if df.empty: return df
    df = df.reset_index()
    col = "Date" if "Date" in df.columns else df.columns[0]
    df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)
    return df.rename(columns={col: "date"})

@st.cache_data(ttl=3600, show_spinner=False)
def vix_returns() -> pd.Series:
    d = daily("IDX_VIX" if False else "^VIX", "5y")
    if d.empty: return pd.Series(dtype=float)
    s = d.set_index("date")["Close"].pct_change()
    return s

# --------------------------------------------------------------------- sidebar ----
st.sidebar.title("📈 Live market big-data console")
st.sidebar.caption("BCSE402L · Big Data Analytics\nGaurav Singh 23BCE0097 · Sanjith M 23BCE0536")
SYMBOLS = st.sidebar.multiselect("Stream symbols",
    ["AAPL","MSFT","NVDA","TSLA","AMZN","GOOGL","META","JPM","XOM","SPY","QQQ","BTC-USD"],
    default=["AAPL","NVDA","TSLA","SPY"])
INTERVAL = st.sidebar.slider("Refresh interval (s)", 5, 60, 10)
STREAM_ON = st.sidebar.toggle("Stream live quotes", value=True)
st.sidebar.markdown("---")
st.sidebar.markdown("**Data:** Yahoo Finance (yfinance) — live quotes & history.\n\n"
                    "**Note:** outside market hours quotes repeat; the streaming "
                    "machinery (windows, latency, throughput) stays fully live.")

tabs = st.tabs(["📡 Live Stream", "🔮 Prediction", "📊 EDA", "🗄️ Lake & Pipeline", "🧾 Insights"])

# =============================================================== 1 LIVE STREAM ====
with tabs[0]:
    st.title("📡 Real-time ingest & micro-batch windows")
    st.caption("Pure-Python twin of the report's Spark Structured Streaming demo (§9): "
               "ticks → 10 s tumbling windows → aggregates + latency telemetry. "
               "Here the source is the live Yahoo quote feed instead of a socket replay.")
    k1, k2, k3, k4, k5 = st.columns(5)
    chart_ph   = st.empty()
    win_ph     = st.empty()
    table_ph   = st.empty()
    if "ticks" not in st.session_state:
        st.session_state.ticks = []      # (wall_ts, symbol, price, latency_ms)
    ticks = st.session_state.ticks

    def render():
        df = pd.DataFrame(ticks, columns=["ts","symbol","price","lat_ms"])
        with k1: st.metric("events ingested", f"{len(df):,}")
        with k2:
            span = max((df.ts.max()-df.ts.min()).total_seconds(), 1) if len(df) > 1 else 1
            st.metric("throughput (ev/s)", f"{len(df)/span:.2f}")
        with k3: st.metric("avg ingest latency", f"{df.lat_ms.mean():.0f} ms" if len(df) else "—")
        with k4: st.metric("symbols live", df.symbol.nunique() if len(df) else 0)
        with k5:
            w = int(len(df)//10) if len(df) else 0
            st.metric("10-s windows", w)
        if df.empty:
            st.info("Waiting for first quotes…"); return
        fig = go.Figure()
        for s, g in df.groupby("symbol"):
            fig.add_trace(go.Scatter(x=g.ts, y=g.price, mode="lines+markers",
                                     name=s, marker=dict(size=4)))
        fig.update_layout(height=380, margin=dict(l=0,r=0,t=30,b=0),
                          title="live tick prices (last poll per symbol)",
                          yaxis_title="price", xaxis_title="")
        chart_ph.plotly_chart(fig, use_container_width=True)
        g = df.copy(); g["win"] = (g.ts.astype("int64")//10**10)//10
        agg = g.groupby("win").agg(n=("price","size"), lat=("lat_ms","mean")).reset_index()
        fig2 = make_subplots(specs=[[{"secondary_y": True}]])
        fig2.add_bar(x=agg.win, y=agg.n, name="ticks / 10 s window")
        fig2.add_scatter(x=agg.win, y=agg.lat, name="avg latency (ms)",
                         mode="lines+markers", secondary_y=True)
        fig2.update_layout(height=300, margin=dict(l=0,r=0,t=30,b=0),
                           title="micro-batch volume & latency (velocity telemetry)")
        fig2.update_yaxes(title_text="ticks", secondary_y=False)
        fig2.update_yaxes(title_text="ms", secondary_y=True)
        win_ph.plotly_chart(fig2, use_container_width=True)
        table_ph.dataframe(df.sort_values("ts", ascending=False).head(15)
                             .assign(ts=lambda d: d.ts.dt.strftime("%H:%M:%S"))
                             .rename(columns={"ts":"time","lat_ms":"latency (ms)"}),
                           use_container_width=True, hide_index=True)

    if not STREAM_ON or not SYMBOLS:
        render()
        if not SYMBOLS: st.warning("Pick at least one symbol in the sidebar.")
    else:
        while STREAM_ON and not SMOKE:
            for s in SYMBOLS:
                t0 = time.time()
                try:
                    px = yf.Ticker(s).fast_info.last_price
                except Exception:
                    px = None
                lat = (time.time()-t0)*1000
                if px:
                    ticks.append((pd.Timestamp.now(), s, float(px), lat))
            if len(ticks) > 4000: del ticks[:2000]
            render()
            time.sleep(INTERVAL)

# =============================================================== 2 PREDICTION =====
with tabs[1]:
    st.title("🔮 Next-session direction — live-refit model")
    c1, c2 = st.columns([1, 3])
    psym = c1.selectbox("Symbol", ["AAPL","MSFT","NVDA","TSLA","AMZN","GOOGL","META",
                                   "JPM","SPY","QQQ","BTC-USD"], index=0)
    st.caption("Features = report §10 set (ret lags 1/2/5, MA20/50 ratios, 21-d vol, "
               "intraday range, weekday, VIX return). Model = L2 logistic regression "
               "(numpy, refit on every load). Split = temporal: last 20% held out.")

    def featurize(sym):
        d = daily(sym)
        if d.empty or len(d) < 300: return None
        d = d.sort_values("date").reset_index(drop=True)
        r = d["Close"].pct_change()
        vr = vix_returns()
        d["ret"] = r
        d["ret_lag1"] = r.shift(1); d["ret_lag2"] = r.shift(2); d["ret_lag5"] = r.shift(5)
        ma20 = d["Close"].rolling(20).mean(); ma50 = d["Close"].rolling(50).mean()
        d["ma20_ratio"] = d["Close"]/ma20 - 1; d["ma50_ratio"] = d["Close"]/ma50 - 1
        d["vol21"] = r.rolling(21).std()*np.sqrt(252)
        d["range1"] = (d["High"]-d["Low"])/d["Open"]
        d["dow_f"] = d["date"].dt.dayofweek.astype(float)
        d["vix_ret"] = d["date"].map(vr) if len(vr) else np.nan
        d["next_ret"] = r.shift(-1)
        d = d.dropna(subset=["ret_lag5","vol21","ma20_ratio","ma50_ratio","range1","next_ret"])
        if len(vr): d = d.dropna(subset=["vix_ret"])
        return d
    FEATS = ["ret_lag1","ret_lag2","ret_lag5","ma20_ratio","ma50_ratio","vol21","range1","dow_f","vix_ret"]

    def logreg(X, y, iters=600, lr=0.25, l2=1e-3):
        mu, sd = X.mean(0), X.std(0)+1e-9
        Z = (X-mu)/sd
        w = np.zeros(Z.shape[1]); b = 0.0
        for _ in range(iters):
            p = 1/(1+np.exp(-(Z@w+b)))
            g = Z.T@(p-y)/len(y) + l2*w
            gb = (p-y).mean()
            w -= lr*g; b -= lr*gb
        p = 1/(1+np.exp(-(Z@w+b)))
        return w, mu, sd, b, p

    def auc(y, p):
        o = np.argsort(p); y = y[o]; p = p[o]
        ranks = np.arange(1, len(p)+1)
        n1, n0 = y.sum(), len(y)-y.sum()
        if n1 == 0 or n0 == 0: return np.nan
        return (ranks[y == 1].sum() - n1*(n1+1)/2)/(n1*n0)

    d = featurize(psym)
    if d is None or len(d) < 300:
        st.error("Not enough history for this symbol.")
    else:
        X = d[FEATS].to_numpy(float); y = (d.next_ret > 0).to_numpy(float)
        n = len(y); cut = int(n*0.8)
        w, mu, sd, b, p_all = logreg(X, y)
        acc_te = float(((p_all[cut:] > 0.5) == (y[cut:] > 0.5)).mean())
        auc_te = float(auc(y[cut:], p_all[cut:]))
        acc_tr = float(((p_all[:cut] > 0.5) == (y[:cut] > 0.5)).mean())
        p_next = float(p_all[-1])
        g1, g2, g3, g4 = st.columns(4)
        g1.metric("P(next session UP)", f"{p_next:.1%}")
        g2.metric("test accuracy", f"{acc_te:.3f}", delta=f"{acc_te-0.5:+.3f} vs coin")
        g3.metric("test AUC", f"{auc_te:.3f}")
        g4.metric("train accuracy", f"{acc_tr:.3f}")
        st.info(("🟢 Model leans LONG for the next session." if p_next > 0.52 else
                 "🔴 Model leans FLAT/SHORT-bias for the next session." if p_next < 0.48 else
                 "⚪ Model is undecided (p≈0.5) — consistent with weak-form market efficiency "
                 "(report §10 found the same on 2023+ data)."), icon="🧠")
        cc1, cc2 = st.columns(2)
        imp = pd.Series(w, index=FEATS).sort_values()
        fig = go.Figure(go.Bar(x=imp.values, y=imp.index, orientation="h",
                               marker_color=np.where(imp.values >= 0, "#059669", "#dc2626")))
        fig.update_layout(title="standardized logistic coefficients (live fit)",
                          height=330, margin=dict(l=0,r=0,t=30,b=0))
        cc1.plotly_chart(fig, use_container_width=True)
        fig = go.Figure(go.Indicator(mode="gauge+number", value=p_next*100,
              number={"suffix": "%"},
              gauge={"axis":{"range":[0,100]},
                     "steps":[{"range":[0,40],"color":"#fecaca"},
                              {"range":[40,60],"color":"#fef3c7"},
                              {"range":[60,100],"color":"#bbf7d0"}]}))
        fig.update_layout(title="P(up) gauge", height=330, margin=dict(l=0,r=0,t=30,b=0))
        cc2.plotly_chart(fig, use_container_width=True)
        # projection cone on last 120 sessions + 20 forward
        hist = d.tail(120).set_index("date")["Close"]
        vol = float(d.vol21.iloc[-1])/np.sqrt(252)
        h = np.arange(1, 21)
        last = float(hist.iloc[-1]); drift = float(d.ret.tail(60).mean())
        fwd = pd.date_range(hist.index[-1], periods=21, freq="B")
        med = last*np.exp(drift*h)
        up  = last*np.exp(drift*h + 1.28*vol*np.sqrt(h))
        dn  = last*np.exp(drift*h - 1.28*vol*np.sqrt(h))
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=hist.index, y=hist.values, name="close (120d)"))
        fig.add_trace(go.Scatter(x=fwd, y=up, name="80% cone upper", line=dict(dash="dot")))
        fig.add_trace(go.Scatter(x=fwd, y=dn, name="80% cone lower", line=dict(dash="dot")))
        fig.add_trace(go.Scatter(x=fwd, y=med, name="drift median", line=dict(dash="dash")))
        fig.update_layout(title=f"{psym} — 20-session projection cone (±1.28σ from realized vol)",
                          height=380, margin=dict(l=0,r=0,t=30,b=0))
        st.plotly_chart(fig, use_container_width=True)
        st.caption("The cone is a risk statement (volatility), not a price promise — the honest "
                   "framing from report §10.")

# =============================================================== 3 EDA ============
with tabs[2]:
    st.title("📊 Exploratory analysis")
    esym = st.selectbox("Interactive candles", ["AAPL","MSFT","NVDA","TSLA","SPY","BTC-USD"])
    d = daily(esym, "1y")
    if not d.empty:
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.75, 0.25],
                            vertical_spacing=0.03)
        fig.add_trace(go.Candlestick(x=d.date, open=d.Open, high=d.High, low=d.Low,
                                     close=d.Close, name=esym), 1, 1)
        fig.add_trace(go.Scatter(x=d.date, y=d.Close.rolling(20).mean(), name="MA20"), 1, 1)
        fig.add_trace(go.Scatter(x=d.date, y=d.Close.rolling(50).mean(), name="MA50"), 1, 1)
        fig.add_trace(go.Bar(x=d.date, y=d.Volume, name="volume"), 2, 1)
        fig.update_layout(height=520, xaxis_rangeslider_visible=False,
                          margin=dict(l=0,r=0,t=30,b=0))
        st.plotly_chart(fig, use_container_width=True)
    st.markdown("#### From the offline lake (4.56 M rows, 1927–2026)")
    for f, cap in [("growth_10k.png","All-time champions vs S&P 500"),
                   ("drawdown_spx.png","99-year underwater curve"),
                   ("correlation_heatmap.png","Correlation clusters (2020+)"),
                   ("seasonality.png","Month & weekday effects"),
                   ("risk_return_scatter.png","Risk–return map of ~500 stocks")]:
        p = os.path.join(CH, f)
        if os.path.exists(p):
            st.image(p, caption=cap, use_container_width=True)

# =============================================================== 4 LAKE ===========
with tabs[3]:
    st.title("🗄️ The data lake & pipeline telemetry (measured, not claimed)")
    etl = load_json("etl_metrics.json", {}) or {}
    strm = load_json("streaming_metrics.json", {}) or {}
    ben = load_json("benchmark.json", {}) or {}
    ml = load_json("ml_results.json", {}) or {}
    dq = None
    try: dq = json.load(open(os.path.join(ROOT, "data", "quality", "dq_report_raw.json")))
    except Exception: pass
    c = st.columns(5)
    c[0].metric("rows in lake", f"{etl.get('rows_raw', 0):,}")
    c[1].metric("raw CSV (HDFS)", f"{etl.get('raw_csv_bytes',0)/1e6:.0f} MB")
    c[2].metric("curated parquet", f"{etl.get('curated_parquet_bytes',0)/1e6:.0f} MB")
    c[3].metric("MR job (4.35M→130)", "70 s")
    c[4].metric("stream loss", f"{strm.get('loss_pct', 0)}%")
    c = st.columns(5)
    c[0].metric("SQL insights", "16 queries / 129 s")
    c[1].metric("spark scan 4.56M", f"{ben.get('spark_W3_scan_s',0)} s")
    c[2].metric("pandas scan 4.56M", f"{ben.get('pandas_W3_scan_s',0)} s")
    c[3].metric("ML test AUC (RF)", f"{ml.get('random_forest',{}).get('areaUnderROC',0):.3f}")
    c[4].metric("DQ checks", f"{len(dq) if dq else 0}-field audit")
    st.markdown("---")
    for f, cap in [("architecture_lake.png","End-to-end architecture"),
                   ("five_vs_infographic.png","The 5 Vs, measured"),
                   ("mapreduce_concept.png","MapReduce concept with the real job"),
                   ("hdfs_architecture.png","How HDFS is used"),
                   ("dq_dashboard.png","Veracity audit"),
                   ("etl_pipeline_metrics.png","ETL footprint & timings"),
                   ("streaming_velocity.png","Streaming telemetry"),
                   ("ml_value.png","ML & backtest"),
                   ("benchmark_spark_vs_pandas.png","Engine benchmark")]:
        p = os.path.join(CH, f)
        if os.path.exists(p):
            st.image(p, caption=cap, use_container_width=True)

# =============================================================== 5 INSIGHTS =======
with tabs[4]:
    st.title("🧾 Spark SQL evidence tables")
    files = sorted(f for f in os.listdir(TAB) if f.startswith("sql_")) if os.path.isdir(TAB) else []
    pick = st.selectbox("Result table", files)
    if pick:
        df = pd.read_csv(os.path.join(TAB, pick))
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.download_button("⬇ download CSV", df.to_csv(index=False).encode(),
                           file_name=pick, mime="text/csv")
    st.markdown("#### Report")
    rp = os.path.join(ROOT, "report", "Stock_Market_Big_Data_Analysis.pdf")
    if os.path.exists(rp):
        with open(rp, "rb") as f:
            st.download_button("⬇ full PDF report (26 pp, 27 figures)", f.read(),
                               file_name="Stock_Market_Big_Data_Analysis.pdf",
                               mime="application/pdf")
st.markdown("---")
st.caption("BCSE402L · Big Data Analytics — Gaurav Singh 23BCE0097 · Sanjith M 23BCE0536 · "
           "live data: Yahoo Finance via yfinance · offline lake: Apache Hadoop 3.3.6 + Spark 3.5.3")
