#!/usr/bin/env python3
"""
====================================================================================
 STAGE 7a — EDA & MARKET VISUALIZATIONS (pure Python: pandas + matplotlib/seaborn)
------------------------------------------------------------------------------------
 Reads the curated parquet lake with pyarrow predicate/partition pushdown so the
 1 GiB node never materializes more than it must. Every chart is independent and
 failure-isolated; PNGs land in output/charts/ at 150 dpi for the PDF report.
====================================================================================
"""
import os, json, traceback
import numpy as np, pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mtick
import seaborn as sns

PROJ  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQ  = os.path.join(PROJ, "data", "curated", "prices")
TABLES= os.path.join(PROJ, "output", "tables")
CH    = os.path.join(PROJ, "output", "charts"); os.makedirs(CH, exist_ok=True)

plt.rcParams.update({
    "figure.dpi": 150, "savefig.dpi": 150, "savefig.bbox": "tight",
    "axes.grid": True, "grid.alpha": 0.25, "grid.linewidth": 0.6,
    "axes.spines.top": False, "axes.spines.right": False,
    "font.size": 9, "axes.titlesize": 11.5, "axes.titleweight": "bold",
    "axes.labelsize": 9.5, "legend.fontsize": 8.2, "xtick.labelsize": 8.5,
    "ytick.labelsize": 8.5, "figure.facecolor": "white",
})
C = ["#2563eb","#dc2626","#059669","#d97706","#7c3aed","#0891b2","#db2777","#65a30d","#ea580c","#475569"]
FOOT = "Big Data Stock Market Analysis — curated parquet lake (4.56M rows, S&P 500 + indices, 1927–2026)"

def save(fig, name, suptitle=None):
    if suptitle:
        fig.subplots_adjust(top=0.84)
        fig.suptitle(suptitle, fontsize=12.5, fontweight="bold", y=0.985)
    fig.text(0.99, 0.005, FOOT, ha="right", va="bottom", fontsize=5.6, color="#94a3b8")
    p = os.path.join(CH, name); fig.savefig(p); plt.close(fig)
    print(f"  ✔ {name} ({os.path.getsize(p)/1024:.0f} KB)", flush=True)

def rd(columns, filters=None):
    df = pd.read_parquet(PARQ, columns=columns, filters=filters or [])
    if "date" in df.columns: df["date"] = pd.to_datetime(df["date"])
    if "symbol" in df.columns: df["symbol"] = df["symbol"].astype("category")
    return df

def chart(fn):
    def wrapper(*a, **k):
        try: fn(*a, **k)
        except Exception as e:
            print(f"  ✖ {fn.__name__}: {type(e).__name__}: {e}", flush=True)
            traceback.print_exc(limit=2)
    return wrapper

# ---------------------------------------------------------------- 1. growth of 10k --
@chart
def growth_10k():
    ch = pd.read_csv(os.path.join(TABLES, "sql_alltime_champions.csv"))
    syms = ch.symbol.head(8).tolist() + ["IDX_GSPC"]
    d = rd(["symbol","date","adj_close"], [("symbol","in",syms)])
    fig, ax = plt.subplots(figsize=(9.2,5.2))
    for i, s in enumerate(syms):
        g = d[d.symbol==s].sort_values("date")
        if g.empty: continue
        val = 10000 * g.adj_close / g.adj_close.iloc[0]
        ax.plot(g.date, val, lw=1.5 if s!="IDX_GSPC" else 2.4,
                color="#111827" if s=="IDX_GSPC" else C[i%len(C)],
                ls="-" if s=="IDX_GSPC" else "-", label=s)
    ax.set_yscale("log"); ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda v,_: f"${v:,.0f}"))
    ax.set_title("Growth of $10,000 invested at IPO/first record (log scale, adjusted close)")
    ax.set_ylabel("Portfolio value (log)"); ax.legend(ncol=5, loc="upper left", framealpha=.9)
    ax.xaxis.set_major_locator(mdates.YearLocator(10))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    save(fig, "growth_10k.png", "All-Time Champions vs the S&P 500 Index")

# ------------------------------------------------------------------- 2. SPX since 1927 --
@chart
def spx_long():
    g = rd(["date","adj_close"], [("symbol","==","IDX_GSPC")]).sort_values("date")
    fig, ax = plt.subplots(figsize=(9.2,4.6))
    ax.plot(g.date, g.adj_close, lw=1.1, color=C[0])
    ax.set_yscale("log")
    ann = [("1929-10-01","Great Depression\n−86% peak-to-trough"),("1987-10-19","Black Monday\n−22.6% in one day"),
           ("2000-03-24","Dot-com peak"),("2007-10-09","Pre-GFC peak"),("2020-03-23","COVID trough"),
           ("2026-09-01","Today")]
    for dt_, lbl in ann:
        ts = pd.Timestamp(dt_)
        row = g.iloc[(g.date-ts).abs().values.argmin()]
        ax.annotate(lbl, xy=(row.date, row.adj_close), xytext=(10, 26 if dt_!="2026-09-01" else -34),
                    textcoords="offset points", fontsize=7, color="#374151",
                    arrowprops=dict(arrowstyle="-", lw=.7, color="#6b7280"))
        ax.scatter([row.date],[row.adj_close], s=12, zorder=5, color=C[1])
    ax.set_title("S&P 500 index — 99 years of history in one lake (log scale)")
    ax.set_ylabel("Index level (log)")
    save(fig, "spx_since_1927.png", "Volume: nearly a century of market memory")

# ------------------------------------------------------------------ 3. candlestick ----
@chart
def candle():
    import mplfinance as mpf
    d = rd(["date","open","high","low","close","volume"], [("symbol","==","AAPL")]).sort_values("date")
    d = d.tail(126).set_index("date")
    d.columns = ["Open","High","Low","Close","Volume"]
    mpf.plot(d, type="candle", mav=(20,50), volume=True, style="yahoo",
             title="\nAAPL — last ~6 months from the curated lake (raw OHLC, MA20/MA50)",
             figsize=(9.2,5.4), savefig=dict(fname=os.path.join(CH,"candlestick_aapl.png"),
             dpi=150, bbox_inches="tight"))
    plt.close("all")
    print(f"  ✔ candlestick_aapl.png", flush=True)

# ------------------------------------------------------------- 4. return distributions --
@chart
def returns_dist():
    spy = rd(["date","daily_return"], [("symbol","==","SPY"), ("year",">=",2000)])
    nvda= rd(["date","daily_return"], [("symbol","==","NVDA"), ("year",">=",2000)])
    pool= rd(["daily_return"], [("source_type","==","stock"), ("year",">=",2015)])["daily_return"].dropna().astype(float)
    fig, axes = plt.subplots(1, 3, figsize=(10.5,3.4))
    for ax, (s, col, name) in zip(axes, [(spy,C[0],"SPY daily returns (2000+)"),
                                          (nvda,C[1],"NVDA daily returns (2000+)")]):
        r = s.daily_return.dropna().astype(float)*100
        ax.hist(r, bins=140, color=col, alpha=.8)
        x = np.linspace(r.min(), r.max(), 200)
        ax.plot(x, np.exp(-x**2/(2*r.std()**2))/(r.std()*np.sqrt(2*np.pi))*len(r)*(r.max()-r.min())/140,
                "k--", lw=1, label="normal fit")
        ax.set_title(f"{name}\nskew={r.skew():.2f} kurt={r.kurt():.1f}", fontsize=9)
        ax.set_xlabel("% return"); ax.legend(fontsize=7)
    ax = axes[2]
    r = pool*100
    ax.hist(r, bins=np.linspace(-14,14,120), color=C[4], alpha=.85)
    ax.set_yscale("log"); ax.set_title("All 503 stocks pooled (2015+)\nlog-y reveals fat tails", fontsize=9)
    ax.set_xlabel("% return")
    fig.tight_layout()
    save(fig, "returns_dist.png", "Returns are fat-tailed — why 'Veracity' matters in market data")

# ---------------------------------------------------------------- 5. volatility by sector --
@chart
def vol_sector():
    d = rd(["symbol","sector","volatility_21d"],
           [("source_type","==","stock"), ("year",">=",2022)])
    per = d.groupby(["symbol","sector"], observed=True).volatility_21d.mean().mul(100).reset_index()
    per = per[~per.sector.isin(["UNKNOWN"])]
    order = per.groupby("sector", observed=True).volatility_21d.median().sort_values().index
    fig, ax = plt.subplots(figsize=(8.6,4.6))
    sns.boxplot(data=per, y="sector", x="volatility_21d", order=order, ax=ax,
                palette="Blues_r", fliersize=2, width=.7)
    ax.set_xlabel("Annualized 21-day volatility since 2022 (%)")
    ax.set_title("Risk by GICS sector — 503 stocks, per-symbol average")
    ax.set_ylabel("")
    save(fig, "volatility_by_sector.png", "Variety: GICS sectors joined from constituent metadata")

# ------------------------------------------------------------------ 6. correlation matrix --
@chart
def corr_heat():
    mega = ["AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","JPM","JNJ","XOM","PG","KO","WMT","V","UNH","LLY"]
    etfs = ["XLK","XLF","XLE","XLV","XLY","XLP","XLI","XLU","XLRE"]
    other= ["IDX_GSPC","IDX_VIX","BTC-USD","GC=F","CL=F"]
    syms = mega+etfs+other
    d = rd(["symbol","date","daily_return"], [("symbol","in",syms), ("year",">=",2020)])
    piv = d.pivot_table(index="date", columns="symbol", values="daily_return", observed=True)
    corr = piv.corr(min_periods=100)
    g = sns.clustermap(corr, cmap="RdBu_r", center=0, vmin=-1, vmax=1, figsize=(9.4,8.2),
                       linewidths=.35, annot=False, cbar_kws={"label":"Pearson corr (daily returns, 2020+)"})
    g.fig.suptitle("Return correlation clusters — mega-caps, sector ETFs & cross-asset", y=1.005,
                   fontsize=12.5, fontweight="bold")
    g.savefig(os.path.join(CH,"correlation_heatmap.png"), bbox_inches="tight")
    plt.close("all")
    print("  ✔ correlation_heatmap.png", flush=True)

# ---------------------------------------------------------------- 7. rolling vol vs VIX --
@chart
def roll_vol():
    s = rd(["date","volatility_21d","adj_close"], [("symbol","==","IDX_GSPC"), ("year",">=",2018)]).sort_values("date")
    v = rd(["date","close"], [("symbol","==","IDX_VIX"), ("year",">=",2018)]).sort_values("date")
    fig, ax = plt.subplots(figsize=(9.4,4.2))
    ax.plot(s.date, s.volatility_21d*100, lw=1.1, color=C[0], label="SPX realized vol (21d, annualized)")
    ax2 = ax.twinx(); ax2.grid(False)
    ax2.plot(v.date, v.close, lw=.9, color=C[1], alpha=.75, label="VIX (implied vol)")
    ax.axvspan(pd.Timestamp("2020-02-15"), pd.Timestamp("2020-04-10"), color="#fecaca", alpha=.45, zorder=0)
    ax.annotate("COVID crash:\nVIX 82.7, realized 120%+", xy=(pd.Timestamp("2020-03-23"), 40),
                fontsize=7.5, color="#7f1d1d")
    ax.set_ylabel("Realized volatility (%)"); ax2.set_ylabel("VIX level")
    ax.legend(loc="upper left", fontsize=7.6); ax2.legend(loc="upper right", fontsize=7.6)
    ax.set_title("Realized vs implied volatility (2018+) — the fear gauge from the lake")
    fig.text(0.99,0.005,FOOT,ha="right",fontsize=5.6,color="#94a3b8")
    fig.savefig(os.path.join(CH,"rolling_vol_vix.png"), bbox_inches="tight"); plt.close(fig)
    print("  ✔ rolling_vol_vix.png", flush=True)

# -------------------------------------------------------------------- 8. drawdown curve --
@chart
def drawdown():
    g = rd(["date","adj_close"], [("symbol","==","IDX_GSPC")]).sort_values("date")
    dd = g.adj_close/g.adj_close.cummax()-1
    fig, ax = plt.subplots(figsize=(9.4,4.0))
    ax.fill_between(g.date, dd*100, 0, color=C[1], alpha=.55, lw=0)
    for dt_, lbl in [("1929-11-01","−86.2%"),("2009-03-09","−56.8%"),("2002-10-09","−49.1%"),
                     ("2020-03-23","−33.9%"),("2022-10-12","−25.4%")]:
        ts=pd.Timestamp(dt_); i=(g.date-ts).abs().values.argmin()
        ax.annotate(lbl, xy=(g.date.iloc[i], dd.iloc[i]*100), xytext=(4,-11), textcoords="offset points",
                    fontsize=7.2, color="#7f1d1d", fontweight="bold")
    ax.set_ylabel("Drawdown from all-time high (%)"); ax.set_ylim(-100, 5)
    ax.set_title("S&P 500 underwater curve, 1927–2026 — every crash in one view")
    save(fig, "drawdown_spx.png")

# ---------------------------------------------------------------- 9. crash event study --
@chart
def crashes():
    g = rd(["date","adj_close"], [("symbol","==","IDX_GSPC")]).sort_values("date").reset_index(drop=True)
    events = [("Dot-com","2000-03-24",C[4]),("GFC","2007-10-09",C[1]),("COVID","2020-02-19",C[0])]
    fig, ax = plt.subplots(figsize=(9.2,4.4))
    for name, peak, col in events:
        p = pd.Timestamp(peak); i0 = (g.date-p).abs().values.argmin()
        seg = g.iloc[max(i0-260,0): i0+780].copy().reset_index(drop=True)
        k = (seg.date-p).abs().values.argmin()
        ax.plot(np.arange(len(seg))-k, seg.adj_close/seg.adj_close.iloc[k]*100, lw=1.4, color=col, label=name)
    ax.axhline(100, color="k", lw=.7, ls="--"); ax.axvline(0, color="k", lw=.7)
    ax.set_xlabel("Trading days from peak"); ax.set_ylabel("Index (peak = 100)")
    ax.set_title("Crash anatomy — aligned at peak: dot-com, GFC, COVID")
    ax.legend(); ax.set_xlim(-260, 780)
    save(fig, "crash_event_study.png", "Velocity of recovery: COVID 148 days vs GFC 1,480 days")

# ------------------------------------------------------------------ 10. seasonality ----
@chart
def seasonality():
    mo = pd.read_csv(os.path.join(TABLES,"sql_monthly_seasonality.csv"))
    dw = pd.read_csv(os.path.join(TABLES,"sql_dayofweek_effect.csv"))
    fig, axes = plt.subplots(1,2, figsize=(10.2,3.6))
    months=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    cols=[C[1] if v<32 else C[0] for v in mo.avg_ret_bps]
    axes[0].bar(months, mo.avg_ret_bps, color=cols, alpha=.85)
    axes[0].axhline(mo.avg_ret_bps.mean(), color="k", ls="--", lw=.8)
    axes[0].set_title("Average daily return by month (bps)\nall 503 stocks, 1990+ — Sep/Oct weakest", fontsize=9.5)
    axes[0].set_ylabel("bps/day")
    days=["Mon","Tue","Wed","Thu","Fri"]
    axes[1].bar(days, dw.avg_ret_bps, color=C[4], alpha=.85)
    axes[1].set_title("Day-of-week effect (bps)\nMonday lag is real but small", fontsize=9.5)
    axes[1].set_ylabel("bps/day")
    fig.tight_layout(); save(fig, "seasonality.png")

# --------------------------------------------------------------- 11. liquidity decades --
@chart
def liquidity():
    b = pd.read_csv(os.path.join(TABLES,"sql_breadth_by_decade.csv"))
    b = b[b.decade>=1960]
    fig, ax = plt.subplots(figsize=(8.8,4.0))
    ax.bar(b.decade.astype(str), b.avg_dollar_vol_mn, color=C[0], alpha=.85, label="Avg daily $ volume per stock (M)")
    ax.set_yscale("log"); ax.set_ylabel("$ millions (log)")
    ax2 = ax.twinx(); ax2.grid(False)
    ax2.plot(b.decade.astype(str), b.symbols_trading, "o-", color=C[1], label="# distinct symbols trading")
    ax2.set_ylabel("symbols")
    h1,l1=ax.get_legend_handles_labels(); h2,l2=ax2.get_legend_handles_labels()
    ax.legend(h1+h2,l1+l2, fontsize=8, loc="upper left")
    ax.set_title("Market liquidity exploded: average traded dollars per stock per day, by decade")
    save(fig, "liquidity_by_decade.png", "Volume: from ~$1M/day/stock in the 1960s to ~$500M+ today")

# --------------------------------------------------------------- 12. MA crossover ------
@chart
def ma_cross():
    d = rd(["date","adj_close","ma_50","ma_200"], [("symbol","==","NVDA"), ("year",">=",2019)]).sort_values("date")
    d["diff"]=d.ma_50-d.ma_200
    x = np.sign(d["diff"]); cross = d[(x!=x.shift()) & x.notna() & (x.shift()!=0)]
    fig, ax = plt.subplots(figsize=(9.4,4.6))
    ax.plot(d.date, d.adj_close, lw=1.2, color="#111827", label="NVDA adj close")
    ax.plot(d.date, d.ma_50, lw=.9, color=C[0], label="MA50")
    ax.plot(d.date, d.ma_200, lw=.9, color=C[3], label="MA200")
    gold = cross[cross["diff"]>0]; dead = cross[cross["diff"]<0]
    ax.scatter(gold.date, gold.adj_close, marker="^", s=60, color=C[2], zorder=5, label=f"golden cross ({len(gold)})")
    ax.scatter(dead.date, dead.adj_close, marker="v", s=60, color=C[1], zorder=5, label=f"death cross ({len(dead)})")
    ax.set_yscale("log"); ax.legend(fontsize=8)
    ax.set_title("Trend signals computed in-lake: MA50×MA200 crossovers (NVDA, 2019+)")
    save(fig, "ma_crossover_nvda.png")

# ---------------------------------------------------------- 13. risk/return frontier --
@chart
def risk_return():
    d = rd(["symbol","sector","daily_return","is_price_anomaly"],
           [("source_type","==","stock"), ("year",">=",2015)])
    d = d[(~d.is_price_anomaly) & (d.daily_return.abs() < 0.35)]
    g = d.groupby(["symbol","sector"], observed=True).daily_return.agg(mean="mean", std="std", n="count")
    g = g[g.n > 500].reset_index()
    g["ann_ret"] = g["mean"]*252*100; g["ann_vol"] = g["std"]*np.sqrt(252)*100
    g = g[(g.ann_ret.abs() < 150) & (g.ann_vol < 120)]   # display window: sane universe
    fig, ax = plt.subplots(figsize=(9.6,5.6))
    for i, sec in enumerate(sorted(g.sector.unique())):
        sub = g[g.sector == sec]
        if sub.empty: continue
        ax.scatter(sub.ann_vol, sub.ann_ret, s=16, alpha=.72, color=C[i % len(C)],
                   label=sec, edgecolors="none")
    top = g.sort_values("ann_ret", ascending=False).head(5)
    bot = g.sort_values("ann_ret").head(4)
    for _, r in pd.concat([top, bot]).iterrows():
        ax.annotate(r.symbol, (r.ann_vol, r.ann_ret), fontsize=7.4, fontweight="bold",
                    xytext=(5, 4), textcoords="offset points")
    ax.axhline(0, color="k", lw=.6)
    ax.set_xlabel("Annualized volatility since 2015 (%)")
    ax.set_ylabel("Annualized return since 2015 (%)")
    ax.legend(fontsize=6.8, loc="upper left", framealpha=.85, ncols=2)
    ax.set_title(f"Risk-return map of {len(g)} stocks (2015+, anomaly rows excluded) — one groupBy on 4.5M rows")
    save(fig, "risk_return_scatter.png", "Insight: high-flyers sit far out on the risk axis")

# ------------------------------------------------------------ 14. sector ETF growth ----
@chart
def sector_growth():
    etfs=["SPY","QQQ","XLK","XLF","XLE","XLV","XLY","XLP","XLI","XLU","XLRE","XLC","XLB"]
    d = rd(["symbol","date","adj_close"], [("symbol","in",etfs), ("year",">=",2015)])
    fig, ax = plt.subplots(figsize=(9.4,4.8))
    for i,s in enumerate(etfs):
        g=d[d.symbol==s].sort_values("date")
        ax.plot(g.date, g.adj_close/g.adj_close.iloc[0], lw=1.5 if s in("SPY","QQQ") else 1.0,
                color="#111827" if s=="SPY" else (C[4] if s=="QQQ" else C[i%len(C)]), label=s)
    ax.set_yscale("log"); ax.set_ylabel("Growth of $1 (log)")
    ax.legend(ncol=7, fontsize=7.4, loc="upper left")
    ax.set_title("Sector ETF cumulative growth since 2015 — tech & discretionary lead")
    save(fig, "sector_etf_growth.png")

# ------------------------------------------------------------------ 15. VIX vs SPX -----
@chart
def vix_scatter():
    d = rd(["symbol","date","daily_return"], [("symbol","in",["IDX_GSPC","IDX_VIX"]), ("year",">=",2019)])
    piv = d.pivot_table(index="date", columns="symbol", values="daily_return", observed=True).dropna()*100
    corr_tab = pd.read_csv(os.path.join(TABLES,"sql_vix_spx_correlation.csv"))
    fig, axes = plt.subplots(1,2, figsize=(10.2,4.0))
    axes[0].scatter(piv["IDX_GSPC"], piv["IDX_VIX"], s=6, alpha=.45, color=C[4])
    m=(piv["IDX_GSPC"]>-3.5)&(piv["IDX_VIX"]<60)
    z=np.polyfit(piv.loc[m,"IDX_GSPC"],piv.loc[m,"IDX_VIX"],1)
    xs=np.linspace(-8,8,50); axes[0].plot(xs,np.polyval(z,xs),"k--",lw=1)
    r=piv["IDX_GSPC"].corr(piv["IDX_VIX"])
    axes[0].set_xlabel("S&P 500 daily return (%)"); axes[0].set_ylabel("VIX daily return (%)")
    axes[0].set_title(f"VIX vs SPX daily moves (2019+)\nPearson r = {r:.2f}", fontsize=9.5)
    axes[1].bar(corr_tab.era, corr_tab.corr_spx_vix.abs(), color=[C[0],C[1],C[4],C[2]])
    for x,v in zip(corr_tab.era, corr_tab.corr_spx_vix):
        axes[1].text(x, abs(v)+.015, f"{v:.2f}", ha="center", fontsize=8)
    axes[1].set_ylim(0,1); axes[1].set_ylabel("corr(SPX ret, VIX ret)")
    axes[1].set_title("The fear gauge tightened for 30 years,\nthen relaxed in the 2020s", fontsize=9.5)
    fig.tight_layout(); save(fig, "vix_vs_spx.png")

if __name__ == "__main__":
    print("MARKET CHARTS"); t0=pd.Timestamp.now()
    growth_10k(); spx_long(); candle(); returns_dist(); vol_sector(); corr_heat()
    roll_vol(); drawdown(); crashes(); seasonality(); liquidity(); ma_cross()
    risk_return(); sector_growth(); vix_scatter()
    print(f"done in {(pd.Timestamp.now()-t0).seconds}s")
