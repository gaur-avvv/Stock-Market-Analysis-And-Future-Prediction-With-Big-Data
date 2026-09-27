#!/usr/bin/env python3
"""
====================================================================================
 REPORT BUILDER — Stock Market Big Data Analysis (reportlab → PDF)
 Sections follow the course brief exactly:
   1 Introduction · 2 Dataset · 3 The 5 Vs · 4 Architecture & tools
   5 HDFS in this project · 6 MapReduce concept · 7 ETL & data quality
   8 EDA, insights & visualizations · 9 Velocity/streaming · 10 ML & value
   11 Performance · 12 Challenges & lessons · 13 Conclusion · Appendices
====================================================================================
"""
import os, json
import pandas as pd
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm, mm
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT
from reportlab.platypus import (BaseDocTemplate, PageTemplate, Frame, Paragraph, Spacer,
                                Image, Table, TableStyle, PageBreak, KeepTogether,
                                Preformatted, HRFlowable, CondPageBreak)
from reportlab.platypus.tableofcontents import TableOfContents
from PIL import Image as PILImage

PROJ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CH   = os.path.join(PROJ, "output", "charts")
TAB  = os.path.join(PROJ, "output", "tables")
MET  = os.path.join(PROJ, "output", "metrics")
OUTP = os.path.join(PROJ, "report", "Stock_Market_Big_Data_Analysis.pdf")

M_ETL = json.load(open(os.path.join(MET, "etl_metrics.json")))
M_DQ  = json.load(open(os.path.join(PROJ, "data", "quality", "dq_report_raw.json")))
M_STR = json.load(open(os.path.join(MET, "streaming_metrics.json")))
M_ML  = json.load(open(os.path.join(MET, "ml_results.json")))
M_BEN = json.load(open(os.path.join(MET, "benchmark.json")))
M_INS = json.load(open(os.path.join(MET, "sql_insights.json")))
M_ING = json.load(open(os.path.join(PROJ, "data", "raw", "metadata", "ingest_summary.json")))

# ---------------------------------------------------------------- styles -----------
SS = getSampleStyleSheet()
def st(name, **kw):
    base = kw.pop("parent", SS["Normal"])
    return ParagraphStyle(name, parent=base, **kw)
H1 = st("H1", fontSize=17, leading=21, textColor=colors.HexColor("#0f172a"),
        spaceBefore=14, spaceAfter=8, fontName="Helvetica-Bold")
H2 = st("H2", fontSize=12.5, leading=16, textColor=colors.HexColor("#1e3a5f"),
        spaceBefore=10, spaceAfter=5, fontName="Helvetica-Bold")
H3 = st("H3", fontSize=10.5, leading=13.5, textColor=colors.HexColor("#334155"),
        spaceBefore=7, spaceAfter=3, fontName="Helvetica-BoldOblique")
BODY = st("BODY", fontSize=9.2, leading=13.2, alignment=TA_JUSTIFY, spaceAfter=5,
          textColor=colors.HexColor("#111827"))
BULL = st("BULL", fontSize=9.0, leading=12.6, leftIndent=12, bulletIndent=3,
          spaceAfter=2.2, textColor=colors.HexColor("#111827"))
CAP = st("CAP", fontSize=7.8, leading=10, textColor=colors.HexColor("#475569"),
         alignment=TA_CENTER, spaceBefore=2, spaceAfter=8)
CODE = st("CODE", fontName="Courier", fontSize=6.9, leading=9, textColor=colors.HexColor("#0f172a"),
          backColor=colors.HexColor("#f1f5f9"), borderPadding=5, spaceAfter=7,
          leftIndent=4, rightIndent=4)
TCELL = st("TCELL", fontSize=7.4, leading=9.2)
TCELLB = st("TCELLB", fontSize=7.4, leading=9.2, fontName="Helvetica-Bold")
TITLE = st("TITLE", fontSize=26, leading=31, fontName="Helvetica-Bold",
           textColor=colors.white, alignment=TA_CENTER)
SUBT = st("SUBT", fontSize=12, leading=16, textColor=colors.HexColor("#bfdbfe"),
          alignment=TA_CENTER)

STORY = []
def P(t, s=BODY): STORY.append(Paragraph(t, s))
def SP(h=0.25): STORY.append(Spacer(1, h*cm))
def B(items):
    for it in items: STORY.append(Paragraph(it, BULL, bulletText="•"))
def H(t, lvl=1):
    STORY.append(CondPageBreak(3.2*cm))
    STORY.append(Paragraph(t, {1: H1, 2: H2, 3: H3}[lvl]))
    if lvl == 1: STORY.append(HRFlowable(width="100%", thickness=1.1,
                       color=colors.HexColor("#1e3a5f"), spaceAfter=6))

def img(name, width=17.2, caption=None, hold=False):
    p = os.path.join(CH, name)
    width = min(width, 17.0)
    w0, h0 = PILImage.open(p).size
    h = width * h0 / w0
    if h > 22.5:
        h = 22.5; width = h * w0 / h0
    flows = [Image(p, width=width*cm, height=h*cm)]
    if caption: flows.append(Paragraph(caption, CAP))
    if hold: return flows
    STORY.extend(flows)

def tbl(rows, widths=None, header=True, fs=7.4, align_right=(), ar=()):
    align_right = tuple(align_right) + tuple(ar)
    data = [[Paragraph(str(c), TCELLB if (header and i == 0) else TCELL) for c in r]
            for i, r in enumerate(rows)]
    t = Table(data, colWidths=widths, repeatRows=1 if header else 0, hAlign="LEFT")
    if len(rows) <= 16: t.splitByRow = 0
    style = [("GRID", (0,0), (-1,-1), 0.4, colors.HexColor("#cbd5e1")),
             ("VALIGN", (0,0), (-1,-1), "TOP"),
             ("TOPPADDING", (0,0), (-1,-1), 2.2), ("BOTTOMPADDING", (0,0), (-1,-1), 2.2),
             ("LEFTPADDING", (0,0), (-1,-1), 3.5), ("RIGHTPADDING", (0,0), (-1,-1), 3.5)]
    if header:
        style += [("BACKGROUND", (0,0), (-1,0), colors.HexColor("#1e3a5f")),
                  ("TEXTCOLOR", (0,0), (-1,0), colors.white)]
        for j in range(len(rows[0])):
            data[0][j] = Paragraph(str(rows[0][j]), st("TH", fontSize=fs, leading=fs+2,
                                   fontName="Helvetica-Bold", textColor=colors.white))
        t = Table(data, colWidths=widths, repeatRows=1, hAlign="LEFT")
        if len(rows) <= 16: t.splitByRow = 0
        t.setStyle(TableStyle(style))
    else:
        t.setStyle(TableStyle(style))
    for j in align_right:
        t.setStyle(TableStyle([("ALIGN", (j,0), (j,-1), "RIGHT")]))
    STORY.append(t); SP(0.25)
    return t

def df_tbl(df, cols=None, widths=None, n=None, fs=7.4, ar=()):
    d = df[cols] if cols else df
    if n: d = d.head(n)
    rows = [list(d.columns)] + d.astype(object).where(pd.notna(d.astype(object)), "").values.tolist()
    rows = [[("" if (c != c) else (f"{c:,}" if isinstance(c, int) else
            (f"{c:,.2f}" if isinstance(c, float) else str(c)))) for c in r] for r in rows[1:]], rows[0]
    body, head = rows
    return tbl([head]+body, widths=widths, fs=fs, align_right=ar)

def code(path, lo=None, hi=None, label=None):
    txt = open(path, encoding="utf-8", errors="replace").read().splitlines()
    txt = [l.replace("\t", "    ").replace("\r", "") for l in txt[(lo or 0):(hi or len(txt))]]
    if label: P(f"<b>{label}</b>", st("CL", fontSize=8, leading=10, spaceAfter=2))
    STORY.append(Preformatted("\n".join(txt), CODE))

# ---------------------------------------------------------------- document ---------
class Doc(BaseDocTemplate):
    def __init__(self, fn, **kw):
        super().__init__(fn, **kw)
        frame = Frame(self.leftMargin, self.bottomMargin, self.width, self.height, id="n")
        self.addPageTemplates([PageTemplate(id="all", frames=[frame], onPage=self.deco)])
    def deco(self, cv, doc):
        cv.saveState()
        cv.setFont("Helvetica", 7); cv.setFillColor(colors.HexColor("#64748b"))
        cv.drawString(2*cm, A4[1]-1.2*cm, "Big Data Analysis of Stock Markets")
        cv.drawRightString(A4[0]-2*cm, A4[1]-1.2*cm, "BCSE402L · Big Data Analytics")
        cv.setStrokeColor(colors.HexColor("#cbd5e1")); cv.setLineWidth(.6)
        cv.line(2*cm, A4[1]-1.35*cm, A4[0]-2*cm, A4[1]-1.35*cm)
        cv.line(2*cm, 1.45*cm, A4[0]-2*cm, 1.45*cm)
        cv.drawCentredString(A4[0]/2, 1.05*cm, f"page {doc.page}")
        cv.restoreState()
    def afterFlowable(self, fl):
        if isinstance(fl, Paragraph):
            s = fl.style.name
            if s == "H1": self.notify("TOCEntry", (0, fl.getPlainText(), self.page))
            elif s == "H2": self.notify("TOCEntry", (1, fl.getPlainText(), self.page))

doc = Doc(OUTP, pagesize=A4, leftMargin=2*cm, rightMargin=2*cm,
          topMargin=1.9*cm, bottomMargin=1.9*cm,
          title="Big Data Analysis of Stock Markets", author="Big Data Course Project")

# ================================================================ TITLE PAGE =======
STORY.append(Spacer(1, 2.2*cm))
cover = Table([[Paragraph("BIG DATA ANALYSIS OF<br/>GLOBAL STOCK MARKETS", TITLE)],
               [Paragraph("A complete data-lake pipeline on Apache Hadoop &amp; Apache Spark —<br/>"
                          "ETL · data quality · MapReduce · HDFS · Spark SQL · Structured Streaming · MLlib · 26 visualizations", SUBT)]],
              colWidths=[17*cm])
cover.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),colors.HexColor("#0f2a4a")),
    ("TOPPADDING",(0,0),(0,0),26),("BOTTOMPADDING",(0,1),(0,1),24),
    ("LEFTPADDING",(0,0),(-1,-1),14),("RIGHTPADDING",(0,0),(-1,-1),14),
    ("TOPPADDING",(0,1),(0,1),4),("BOTTOMPADDING",(0,0),(0,0),6)]))
STORY.append(cover); SP(1.0)
facts = [["Dataset", f"{M_ETL['rows_raw']:,} daily bars · {M_ETL['distinct_symbols']} symbols · "
          f"{M_ETL['date_min']} → {M_ETL['date_max']} (99 years)"],
         ["Raw footprint", f"{M_ETL['raw_csv_bytes']/1e6:.0f} MB CSV on HDFS (525 files)"],
         ["Curated footprint", f"{M_ETL['curated_parquet_bytes']/1e6:.0f} MB Snappy Parquet, "
          f"{M_ETL['parquet_part_files']} files, partitioned source_type × year"],
         ["Cluster", "Hadoop 3.3.6 (HDFS + Streaming MapReduce) · Spark 3.5.3 (SQL, MLlib, Streaming) · "
          "single node, 1 GiB cgroup, 2 vCPU"],
         ["Languages", "100% Python 3.11 (PySpark driver, orchestration, EDA, report)"],
         ["Analytics", "16 Spark SQL insight queries · 26 figures · ML direction model + backtest"],
         ["Data quality", f"{len(M_DQ)}-check veracity audit: {M_DQ['ohlc_integrity_violations']} OHLC breaks, "
          f"{M_DQ['true_anomalies_nonsplit']} extreme anomalies flagged & kept"],
         ["Data source", "Yahoo Finance daily-bar chart API via <font face='Courier'>yfinance</font> "
          "(finance.yahoo.com) · S&amp;P 500 constituent list + GICS sectors: "
          "github.com/datasets/s-and-p-500-companies · details &amp; URLs in §2.1"]]
tbl([["Item","Measured value"]]+facts, widths=[3.6*cm, 13.4*cm])
SP(0.8)
course = [["Course Name", "Big Data Analytics"],
          ["Course Code", "BCSE402L"],
          ["Submitted by", "Gaurav Singh — 23BCE0097"],
          ["", "Sanjith M — 23BCE0536"]]
ct = Table([[Paragraph(a, TCELLB), Paragraph(b, st("CB", fontSize=9.6, leading=13,
            fontName="Helvetica-Bold", textColor=colors.HexColor("#0f172a")))] for a, b in course],
           colWidths=[3.6*cm, 13.4*cm])
ct.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),colors.HexColor("#eef4fb")),
    ("GRID",(0,0),(-1,-1),0.5,colors.HexColor("#1e3a5f")),
    ("TOPPADDING",(0,0),(-1,-1),4),("BOTTOMPADDING",(0,0),(-1,-1),4),
    ("LEFTPADDING",(0,0),(-1,-1),6)]))
STORY.append(ct)
SP(0.5)
P("Every number in this report was measured on the cluster described above; every figure is generated by the "
  "pipeline code shipped in <font face='Courier'>src/</font>. The narrative follows the five Vs of big data and "
  "documents — honestly — every engineering failure (OOM kills, port conflicts, provider data artifacts) and its fix.",
  st("CV", fontSize=8.4, leading=11.5, alignment=TA_JUSTIFY, textColor=colors.HexColor("#334155")))
STORY.append(PageBreak())

# ================================================================ TOC ==============
P("Contents", st("H1x", fontSize=17, leading=21, textColor=colors.HexColor("#0f172a"), spaceBefore=14, spaceAfter=8, fontName="Helvetica-Bold"))
toc = TableOfContents()
toc.levelStyles = [st("TOC0", fontSize=10, leading=13.2, spaceBefore=1.5, fontName="Helvetica-Bold",
                      textColor=colors.HexColor("#1e3a5f"), leftIndent=0),
                   st("TOC1", fontSize=8.6, leading=11.2, leftIndent=14,
                      textColor=colors.HexColor("#334155"))]
STORY.append(toc)
SP(0.5)

# ================================================================ 1 INTRO ==========
H("1  Introduction")
P("Equity markets are one of the oldest and largest continuously-recorded data systems on the planet: "
  "every trading day, every listed instrument emits open/high/low/close prices, volume, corporate-action "
  "events and derived analytics. This project treats that record as what it is — a <b>big data problem</b> — "
  "and answers it with the canonical open-source stack: <b>Apache Hadoop</b> for distributed storage "
  "(HDFS) and batch parallelism (MapReduce), <b>Apache Spark</b> for data engineering (ETL), interactive "
  "analytics (Spark SQL), machine learning (MLlib) and stream processing (Structured Streaming), and "
  "<b>pure Python</b> for ingestion, orchestration, exploratory analysis and this report.")
P("<b>Objectives.</b> (i) Build an end-to-end, reproducible data lake from real market data; "
  "(ii) apply the 5-V lens (Volume, Velocity, Variety, Veracity, Value) with <i>measured</i> evidence "
  "rather than slogans; (iii) demonstrate the Hadoop primitives (HDFS layout, MapReduce shuffle/combine) "
  "on real jobs; (iv) extract economically meaningful insights through EDA and SQL; (v) quantify the "
  "engineering cost of doing all of this on a deliberately tiny 1 GiB node — the constraint that makes "
  "every design decision in this report explicit.")
P("<b>Method.</b> Data was ingested from the Yahoo Finance chart API for all 503 current S&amp;P 500 "
  "constituents plus 22 cross-asset instruments (index levels, VIX, 10-year yield, sector ETFs, Bitcoin, "
  "gold, crude) — full history, unadjusted and adjusted prices, dividends and splits. The raw CSVs landed "
  "immutably in an HDFS raw zone; a Hadoop Streaming MapReduce job aggregated them in place; a PySpark ETL "
  "audited quality, staged columnar parquet and loaded a curated, feature-engineered, partitioned table; "
  "Spark SQL, MLlib and a socket-based Structured Streaming demo consumed the lake; pandas + matplotlib + "
  "seaborn + mplfinance produced the 26 figures embedded here; reportlab assembled this document.")
P("<b>Headline results.</b>")
_hl = [f"<b>{M_ETL['rows_raw']:,} rows / {M_ETL['distinct_symbols']} symbols / 99 years</b> ingested in "
   f"{M_ING['seconds']:.0f} s with zero failures, and stored twice (CSV raw {M_ETL['raw_csv_bytes']/1e6:.0f} MB, "
   f"parquet curated {M_ETL['curated_parquet_bytes']/1e6:.0f} MB).",
   "A real MapReduce job over HDFS aggregated 4.35 M rows to 130 rows in 70 s after a map-side combiner cut "
   "shuffle volume by 99.99% (the naive version OOMed — documented in §6).",
   f"16 Spark SQL insight queries in {sum(M_INS['query_times_s'].values()):.0f} s produced the evidence tables of §8 "
   "(all-time champions, crash anatomy, seasonality, VIX coupling, liquidity growth).",
   f"Structured Streaming ingested {M_STR['producer_ticks']:,} synthetic-but-real-anchored ticks at "
   f"~{M_STR['throughput_events_per_s']:.0f} events/s with {M_STR['loss_pct']}% loss.",
   f"The ML honesty result: next-day direction is ~unpredictable (test AUC "
   f"{M_ML['random_forest']['areaUnderROC']:.3f}); the value of the platform shows up in risk analytics, "
   "data-quality discovery and speed of insight instead."]
B(_hl)
STORY.append(KeepTogether(img("five_vs_infographic.png", 17.2,
    "Figure 1.1 — The five Vs of this project, with measured values (detail in §3).", hold=True)))

# ================================================================ 2 DATASET =========
H("2  The dataset")
H("2.1  Source and coverage", 2)
P("Primary source: the Yahoo Finance daily-bar chart API (via the <font face='Courier'>yfinance</font> "
  "Python package), requested with <font face='Courier'>auto_adjust=False, actions=True</font> so the lake "
  "stores <i>both</i> raw OHLC and split/dividend-adjusted closes plus dividend and split events — the "
  "raw+events form is what makes later veracity analysis possible. Constituent list (symbols + GICS "
  "sectors) from the public <i>datasets/s-and-p-500-companies</i> registry.")
P("<b>Sources &amp; links (accessed 2026-09-24).</b>")
B(["<b>Price/volume/corporate-action data:</b> Yahoo Finance chart API, consumed through the "
   "<font face='Courier'>yfinance</font> Python package v1.7.0 — per-symbol endpoint "
   "<font face='Courier'>https://query1.finance.yahoo.com/v8/finance/chart/{symbol}</font> "
   "(package: <font face='Courier'>https://pypi.org/project/yfinance/</font>, source: "
   "<font face='Courier'>https://github.com/ranaroufi/yfinance</font>). Requested with "
   "<font face='Courier'>period='max', auto_adjust=False, actions=True</font>.",
   "<b>Constituents &amp; GICS sectors:</b> <font face='Courier'>https://github.com/datasets/s-and-p-500-companies</font> "
   "(file <font face='Courier'>data/constituents.csv</font>, mirrored in "
   "<font face='Courier'>data/raw/metadata/constituents.csv</font>).",
   "<b>Ingestion code:</b> <font face='Courier'>src/00_ingest_data.py</font> reproduces the full 4.56 M-row raw zone "
   "in ~2 minutes; the workspace ships a sample (see <font face='Courier'>data/raw/SAMPLE_NOTE.txt</font>).",
   "<b>Usage note:</b> data is used for academic study; the lake stores derived artefacts and the pipeline can "
   "re-download the raw series at any time."])
ov = pd.read_csv(os.path.join(TAB, "sql_market_overview.csv"))
df_tbl(ov, cols=["source_type","rows","symbols","first_date","last_date","avg_ann_vol"],
       widths=[3.4*cm,2.5*cm,2.0*cm,2.5*cm,2.5*cm,2.6*cm],
       n=8, ar=(1,2,5))
P("Table 2.1 — Lake contents by source type (rows, symbols, date span, mean annualized 21-day volatility).", CAP)
B(["<b>Span:</b> 1927-12-30 (first S&amp; composite record) → 2026-09-24 (ingestion day) — 99 years, "
   "covering the Great Depression, Black Monday, the dot-com bubble, the GFC, COVID and the 2022 bear market.",
   "<b>Granularity:</b> one row per instrument per trading session (crypto: calendar days, incl. weekends).",
   "<b>Known limitations (stated, not hidden):</b> the constituent list is <i>today's</i> S&amp;P 500, so the "
   "equity panel has survivorship bias — fine for pipeline/EDA teaching, wrong for index-backtest research; "
   "adjusted-close series carry provider re-basing artifacts (see §7 veracity exhibit)."])
H("2.2  Schema", 2)
sch = [["Date / date","string→date","session date (partition-friendly)"],
       ["Open High Low Close","double→float","raw session prices"],
       ["adj_close","double→float","split &amp; dividend adjusted close (used for returns)"],
       ["Volume","long","shares traded"],
       ["Dividends / stock_splits","double→float","corporate-action events per session"],
       ["symbol / source_type / security / sector","string","identity + GICS dimension (broadcast-joined)"],
       ["daily_return / log_return","float","adj-close based session returns"],
       ["ma_20 / ma_50 / ma_200","float","rolling means of adj_close (window functions)"],
       ["volatility_21d","float","21-day stdev of returns × √252 (annualized)"],
       ["dollar_volume / intraday_range","double/float","liquidity &amp; range analytics"],
       ["is_price_anomaly","bool","|return|&gt;50% and not a split day — flagged, never deleted"],
       ["year / month / dow","int/byte","calendar dimensions (partition keys)"]]
tbl([["Column","Type","Meaning"]]+sch, widths=[5.2*cm,2.6*cm,9.2*cm])
P("Table 2.2 — Curated table schema: 10 landed columns + 14 engineered columns.", CAP)
H("2.3  A raw record, verbatim", 2)
code(os.path.join(PROJ, "data", "raw", "stocks", "AAPL.csv"), 0, 4,
     label="data/raw/stocks/AAPL.csv (HDFS: /datalake/raw/stocks/AAPL.csv) — schema-on-read CSV")
P("Note the two event columns (<font face='Courier'>Dividends</font>, <font face='Courier'>Stock Splits</font>): "
  "they are why adjusted and raw prices diverge, and why §7 can distinguish a real crash from a corporate action.", BODY)

# ================================================================ 3 FIVE V ==========
H("3  The five Vs — and exactly how this project uses each")
P("The 5-V framework is usually quoted qualitatively. Here each V is tied to a pipeline component and a "
  "measured number; Figure 1.1 (repeated as Figure 3.1) summarizes them.")
H("3.1  Volume", 2)
B([f"<b>{M_ETL['rows_raw']:,} rows</b> landed; the lake holds <b>{(M_ETL['raw_csv_bytes']+M_ETL['curated_parquet_bytes'])/1e6:.0f} MB</b> "
   "across raw CSV + curated parquet on HDFS (Figure 5.2).",
   "Volume is what forces columnar storage: the curated parquet is "
   f"<b>{M_ETL['compression_ratio']}× smaller</b> than the CSV raw zone and supports column pruning "
   "(§11 benchmarks read only 3 of 24 columns for the scan workload).",
   f"Aggregation scale: {M_BEN['W1_groups']:,} (symbol, year) groups computed by one groupBy; the MapReduce "
   "job folded 4.35 M rows into 130 (year × metric) rows."])
H("3.2  Velocity", 2)
B([f"Batch ingest: {M_ING['total_rows']:,} rows downloaded in <b>{M_ING['seconds']:.0f} s</b> with 6 parallel producers.",
   f"Stream ingest (§9): {M_STR['producer_ticks']:,} ticks in {M_STR['duration_target_s']} s "
   f"(~{M_STR['tick_rate_target']}/s target, {M_STR['throughput_events_per_s']:.0f}/s achieved) through Spark "
   f"Structured Streaming micro-batches averaging {M_STR['avg_batch_s']} s with end-to-end latency "
   f"{M_STR['avg_latency_ms']/1000:.1f} s mean.",
   f"Analytic velocity: 16 SQL insight queries over 4.56 M rows in {sum(M_INS['query_times_s'].values()):.0f} s; "
   "the ETL turns raw→curated in ~11 min of wall time on one node."])
H("3.3  Variety", 2)
B(["<b>8 source types</b> in one lake: common stock, broad ETF, sector ETF, market index, volatility index, "
   "treasury yield, crypto, commodity — each with its own trading calendar (weekend rows are legitimate for "
   "BTC and would be a bug for NYSE names; the pipeline knows the difference).",
   "<b>Formats:</b> CSV (raw, schema-on-read) → Snappy Parquet (curated, typed, partitioned) → JSON/CSV "
   "(metrics, DQ reports, SQL result tables on HDFS).",
   "<b>Semantics:</b> prices, volumes, corporate-action events, GICS sector dimension, engineered features "
   "and model artefacts (MLlib pipeline on disk) all live in the same namespace."])
H("3.4  Veracity", 2)
B([f"A {len(M_DQ)}-check Spark audit runs <i>before</i> cleansing (§7): {M_DQ['null_rows_any_ohlcv']} null-OHLCV rows, "
   f"{M_DQ['ohlc_integrity_violations']} OHLC integrity violations (high&lt;low etc.), "
   f"{M_DQ['weekend_dates']} weekend rows, {M_DQ['zero_volume_stock_rows']:,} zero-volume stock rows, "
   f"{M_DQ['abs_return_gt_50pct']} extreme |return|&gt;50% moves of which {M_DQ['of_which_split_days']} split-confounded "
   f"and {M_DQ['true_anomalies_nonsplit']} true anomalies.",
   "Governance rule of the lake: <b>flag, never silently delete</b> — anomalies ride along as "
   "<font face='Courier'>is_price_anomaly</font> so every downstream chart can exclude or expose them.",
   "Real provider artifacts discovered and documented: Las Vegas Sands' adjusted-close re-basing around "
   "March 2009 produced consecutive −97% 'returns' — exactly the kind of trap naive pipelines ship to dashboards."])
H("3.5  Value", 2)
B(["Economic insights with numbers: Altria +2,981,661% total return since 1962 (dividends reinvested); "
   "NVDA 37.0% CAGR since 1999; GFC drawdown −56.8% took 1,480 days to recover vs COVID's −33.9% in 148 days; "
   "VIX↔S&amp;P correlation deepened from −0.63 (1990s) to −0.79 (2010s).",
   "An honest ML result (§10): daily direction is ~efficient (AUC≈0.50), and the report says so — the "
   "platform's value is risk analytics, data-quality discovery, and minutes-to-insight, not a crystal ball.",
   "Operational value: one reproducible Python DAG rebuilds the entire evidence base (data → lake → "
   "charts → this PDF)."])
img("five_vs_infographic.png", 17.2, "Figure 3.1 — The five Vs with measured values.")
vtab = [["V","Headline metric (measured)","Where it is handled","§"],
        ["Volume","4,564,958 rows · 730 MB lake · 18,502 group keys","HDFS zones + parquet partitioning + MR fold","3.1, 5, 6"],
        ["Velocity","116 s ingest · ~137 ticks/s stream · 129 s / 16 queries","parallel producers · Structured Streaming · Spark SQL","3.2, 9"],
        ["Variety","8 source types · 3 formats · 11 GICS sectors","schema-on-read + broadcast dimension join","2, 3.3"],
        ["Veracity","9-check audit · 88 anomalies flagged, kept","pre-cleansing Spark DQ stage + is_price_anomaly","3.4, 7"],
        ["Value","crash anatomy · seasonality · honest ML (AUC≈0.50)","Spark SQL · MLlib · backtest · this report","3.5, 8, 10"]]
tbl(vtab, widths=[1.6*cm,6.6*cm,6.6*cm,2.2*cm])
P("Table 3.1 — Five-V scoreboard: every claim above is backed by a measured artefact in "
  "<font face='Courier'>output/</font>.", CAP)

# ================================================================ 4 ARCH ============
H("4  Architecture, tools and orchestration")
img("architecture_lake.png", 17.4, "Figure 4.1 — End-to-end architecture: sources → HDFS raw → "
    "(MapReduce | Spark ETL) → curated parquet → consumers → report.")
tools = [["Apache Hadoop 3.3.6","HDFS NameNode+DataNode (pseudo-distributed), Hadoop Streaming MapReduce",
          "§5, §6"],
         ["Apache Spark 3.5.3 (PySpark)","ETL (DataFrames+window fns), Spark SQL, MLlib, Structured Streaming",
          "§7–§10"],
         ["Python 3.11","ingestion (yfinance), orchestration DAG, EDA, report generation","all"],
         ["pandas 3.0 / pyarrow 25","predicate-pushdown parquet reads for EDA; backtest; benchmarks","§8, §10, §11"],
         ["matplotlib / seaborn / mplfinance","26 publication figures incl. candlesticks & heatmaps","§8"],
         ["reportlab","this PDF (Platypus flowables, TOC, embedded assets)","§13"]]
tbl([["Component","Role in this project","Section"]]+tools, widths=[4.6*cm,10.2*cm,2.2*cm])
P("Table 4.1 — Tool stack and where each appears.", CAP)
H("4.1  Orchestration and the 1 GiB discipline", 2)
P("The node is a container limited to <b>1 GiB of memory including page cache</b>, with 2 vCPUs — no swap "
  "possible. That constraint shaped the architecture more than any aesthetic choice: a JVM's committed heap "
  "never shrinks, so long multi-stage Spark applications were replaced by a <b>pure-Python DAG runner</b> "
  "(<font face='Courier'>src/run_pipeline.py</font>) that launches <b>one fresh, heap-capped JVM per phase</b> "
  "(staging → transform chunk 0/1/2 → verify), each with G1GC, capped metaspace/code-cache/direct memory and "
  "<font face='Courier'>-XX:+ExitOnOutOfMemoryError</font> to fail loudly instead of thrashing. Streaming "
  "adds a watchdog thread that force-finalizes metrics if the JVM ever stalls. The result: a pipeline that "
  "would naively need ~8 GB runs reproducibly in 1 GB — and every trick is listed in §12.")
code(os.path.join(PROJ, "src", "run_pipeline.py"), 30, 46, label="src/run_pipeline.py — the DAG (excerpt)")

# ================================================================ 5 HDFS ============
H("5  How HDFS is used in this project")
P("HDFS is the project's system of record. A single-node pseudo-distributed cluster (NameNode RPC on "
  ":19000, DataNode :19010, web UI :19070, replication=1, JVM heaps 192/224 MB) hosts a four-zone data lake. "
  "Everything the Spark/MapReduce jobs read or write lives under <font face='Courier'>/datalake</font>.")
img("hdfs_architecture.png", 17.4, "Figure 5.1 — HDFS roles and the exact client operations used.")
zones = [["/datalake/raw/stocks","525 CSV files","479.2 MB","immutable landing zone; schema-on-read; "
          "input to MapReduce and Spark ETL"],
         ["/datalake/raw/indices","22 CSV files","(incl. above)","cross-asset variety: VIX, ^TNX, BTC, GC, CL"],
         ["/datalake/curated/prices","642 parquet files","251.2 MB","partitioned source_type × year; snappy; "
          "serves SQL/ML/EDA"],
         ["/datalake/mr_output/yearly_activity","part-00000","2.4 KB","MapReduce result (130 rows)"],
         ["/datalake/reports","dq + sql tables + metrics","102 KB","quality & insight artefacts"]]
tbl([["Zone","Files","Size","Purpose"]]+zones, widths=[4.6*cm,3.0*cm,2.2*cm,7.2*cm])
P("Table 5.1 — Data-lake zones on HDFS (measured with hdfs dfs -du / -count).", CAP)
H("5.1  The commands this project actually ran", 2)
code(os.path.join(PROJ, "src", "06_hdfs_publish.py"), 60, 74,
     label="Python-driven HDFS operations (excerpt of src/06_hdfs_publish.py)")
H("5.2  Cluster health evidence", 2)
ev = open(os.path.join(PROJ, "output", "hdfs_evidence.txt")).read()
def evblock(title):
    i = ev.find("## "+title)
    if i < 0: return None
    j = ev.find("## ", i+4); seg = ev[i:j if j>0 else len(ev)]
    lines = seg.splitlines()[3:]
    return "\n".join(lines[:14])
for t in ["Cluster report (dfsadmin)", "Filesystem health (fsck)"]:
    b = evblock(t)
    if b: code(os.path.join(PROJ,"output","hdfs_evidence.txt"),
               ev.splitlines().index([l for l in ev.splitlines() if l.strip()==("## "+t)][0])+2,
               ev.splitlines().index([l for l in ev.splitlines() if l.strip()==("## "+t)][0])+16,
               label="output/hdfs_evidence.txt — "+t)
img("hdfs_zones.png", 15.5, "Figure 5.2 — Zone sizes and file counts on HDFS.")
B(["<b>Write-once-read-many</b> fits the raw zone: files are landed once and never mutated — the "
   "reproducibility guarantee of the whole report.",
   "<b>Small-file awareness:</b> 525 CSVs of 0.4–2 MB are far below the 128 MB block size; fine for a teaching "
   "lake, and exactly why the curated zone re-expresses the same data as 642 larger columnar files.",
   "<b>fsck/dfsadmin as unit tests:</b> 0 under-replicated, 0 corrupt, 0 missing blocks after every publish."])

# ================================================================ 6 MAPREDUCE =======
H("6  The MapReduce concept — with a real job")
P("MapReduce splits a computation into an embarrassingly parallel <b>map</b> over input splits, a "
  "<b>shuffle&amp;sort</b> that groups intermediate records by key (the only network-heavy phase), and an "
  "associative <b>reduce</b> that folds each key's values. Hadoop adds the <b>combiner</b> optimization: "
  "run reduce-like logic inside the mapper to shrink the shuffle. Figure 6.1 walks through our actual job — "
  "<i>market activity per year</i> over the raw zone — including the failure of version 1 and the combiner "
  "fix in version 2.")
img("mapreduce_concept.png", 17.6, "Figure 6.1 — MapReduce explained through the project's real Hadoop "
    "Streaming job (Python mapper/reducer).")
H("6.1  Mapper and reducer (as shipped)", 2)
code(os.path.join(PROJ, "src", "mapreduce", "mapper.py"), 14, 45, label="src/mapreduce/mapper.py (core)")
code(os.path.join(PROJ, "src", "mapreduce", "reducer.py"), 8, 28, label="src/mapreduce/reducer.py (core)")
H("6.2  Launch command and measured job facts", 2)
P("<font face='Courier'>hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar "
  "-input \"/datalake/raw/stocks/*.csv\" -output /datalake/mr_output/yearly_activity "
  "-mapper \"python3 …/mapper.py\" -reducer \"python3 …/reducer.py\"</font>")
B(["Input: 525 CSV files, 4,354,506 data rows on HDFS (479 MB).",
   "v1 (one record per row): ~8.8 M shuffle records ≈ 200 MB → <b>JVM heap OOM after 172 s</b>.",
   "v2 (map-side combining): shuffle shrank to ~240 records (~10 KB, −99.99%) → "
   "<b>map 100% / reduce 100% in 70 s</b>, output 130 rows (65 years × {rows, volume}).",
   "Framework used: LocalJobRunner tasks + real HDFS I/O — the shuffle/sort and streaming-pipe mechanics "
   "are identical to a multi-node YARN run; only the transport (localhost) differs."])
img("mapreduce_yearly_output.png", 16.6, "Figure 6.2 — The job's output: trading rows and share volume per "
    "year, 1962–2026 (log scale). Volume grows ~250× from the 1960s to the 2020s.")
mr = pd.read_csv(os.path.join(TAB, "mr_yearly_activity.tsv"), sep="\t", names=["year","metric","value"])
piv = mr.pivot(index="year", columns="metric", values="value")
sel = piv.loc[[1962,1980,2000,2008,2020,2024,2025,2026]]
tbl([["year","trading rows","share volume"]]+
    [[str(int(r.year)), f"{int(r.rows):,}", f"{int(r.vol):,}"] for r in sel.reset_index().itertuples()],
    widths=[3*cm,4.5*cm,6*cm], ar=(1,2))
P("Table 6.1 — MapReduce output sample (part-00000 excerpt).", CAP)

# ================================================================ 7 ETL =============
H("7  Spark ETL pipeline & data quality (Veracity in action)")
P("The ETL is a five-step PySpark application executed as isolated phases (§4.1). Stage T2 — the veracity "
  "audit — runs <i>before</i> any cleansing so the report can show what the raw provider data actually "
  "contains; stages T3–T5 then cleanse, feature-engineer with window functions, and load a partitioned "
  "columnar table.")
img("dq_dashboard.png", 17.4, "Figure 7.1 — Veracity audit of 4,564,958 raw rows: what was found, what was "
    "kept, what was flagged.")
findings = [["null OHLCV rows","2","dropped (unusable)"],
            ["duplicate (symbol,date) keys","0","dedup stage kept as a measured no-op"],
            ["OHLC integrity violations","450","kept + listed in DQ report (provider ticks)"],
            ["weekend-dated rows","1,254","kept for crypto, dropped for exchange-traded types"],
            ["zero-volume stock rows","25,048","kept, flagged (halts/holidays)"],
            ["|adj return| > 50%","91","split-confounded: 3 · true anomalies: 88 → is_price_anomaly=True"]]
tbl([["Check","Rows found","Pipeline decision"]]+findings, widths=[5.2*cm,2.6*cm,9.2*cm])
P("Table 7.1 — Data-quality findings and governance decisions.", CAP)
H("7.1  Transform: windowed feature engineering (excerpt)", 2)
code(os.path.join(PROJ, "src", "01b_transform_load.py"), 112, 133,
     label="src/01b_transform_load.py — one exchange+sort feeds lag() and all rolling windows")
H("7.2  Load: partitioned columnar table", 2)
B([f"Curated table: <b>{M_ETL['rows_out']:,} rows</b>, {M_ETL['parquet_part_files']} Snappy parquet files, "
   "partitioned by (source_type, year) → partition pruning for every year-scoped query.",
   "Write-path fix worth remembering: <font face='Courier'>partitionBy</font> alone lets one write task keep "
   "~150 parquet writers open (one per directory) with 128 MB row-group buffers each → heap OOM. Sorting "
   "within partitions (<font face='Courier'>repartition(6, src, year).sortWithinPartitions(src, year)</font>) "
   "plus 32 MB row groups made each task hold ~1 writer.",
   f"Storage: CSV {M_ETL['raw_csv_bytes']/1e6:.0f} MB → parquet {M_ETL['curated_parquet_bytes']/1e6:.0f} MB "
   f"({M_ETL['compression_ratio']}×); symbol dimension table published alongside."])
img("etl_pipeline_metrics.png", 17.2, "Figure 7.2 — ETL footprint, chunk timings and lake composition.")

# ================================================================ 8 EDA =============
H("8  Exploratory analysis, insights & visualizations")
P("All figures below are generated from the curated parquet lake by "
  "<font face='Courier'>src/07_charts_market.py</font> (market) and "
  "<font face='Courier'>src/07_charts_ops.py</font> (operations), using pyarrow predicate pushdown so the "
  "1 GiB node only materializes what each chart needs. Captions carry the insight; tables carry the evidence.")
H("8.1  A century of market memory", 2)
img("spx_since_1927.png", 17.0, "Figure 8.1 — S&P index, 1927–2026, log scale, with the six eras annotated.")
img("drawdown_spx.png", 17.0, "Figure 8.2 — Underwater curve: every drawdown from all-time high in 99 years.")
B(["The index spent <b>22 years</b> (1929–1951) and <b>13 years</b> (2000–2013) below prior peaks — drawdown "
   "duration, not depth, is the risk retail investors actually feel.",
   f"Worst single session in the whole 4.56 M-row panel (anomalies excluded): "
   f"{M_INS['worst_day']['date']} {M_INS['worst_day']['symbol']} {M_INS['worst_day']['ret_pct']}%; best: "
   f"{M_INS['best_day']['date']} {M_INS['best_day']['symbol']} +{M_INS['best_day']['ret_pct']}% — both are "
   "corporate-action-adjacent records, which is itself a veracity lesson."])
H("8.2  Who created wealth — all-time champions", 2)
img("growth_10k.png", 17.0, "Figure 8.3 — $10,000 at first record, log scale: staples & home-improvement "
    "compounders beat the index by 2–3 orders of magnitude (dividends reinvested).")
ch = pd.read_csv(os.path.join(TAB, "sql_alltime_champions.csv"))
df_tbl(ch, cols=["symbol","security","sector","first_dt","total_return_pct","cagr_pct"],
       widths=[1.8*cm,4.4*cm,3.6*cm,2.2*cm,2.8*cm,2.2*cm], n=8, ar=(4,5))
P("Table 8.1 — Top-8 total-return champions since first record (adjusted close, dividends reinvested).", CAP)
H("8.3  Microstructure: candles, returns, risk", 2)
img("candlestick_aapl.png", 16.4, "Figure 8.4 — Candlestick + MA20/50 + volume straight from the lake "
    "(mplfinance).")
img("returns_dist.png", 17.0, "Figure 8.5 — Daily returns are fat-tailed: the pooled log-y histogram has "
    "mass far beyond any normal fit — the statistical core of the Veracity story.")
img("risk_return_scatter.png", 16.6, "Figure 8.6 — Risk–return map of ~500 stocks since 2015; annotated "
    "names are spin-off-affected records (HII, LVS) — kept, flagged, visible.")
img("volatility_by_sector.png", 15.6, "Figure 8.7 — Annualized volatility by GICS sector since 2022.")
sec = pd.read_csv(os.path.join(TAB, "sql_sector_scoreboard.csv"))
df_tbl(sec, widths=[4.4*cm,1.9*cm,3.0*cm,2.9*cm,3.0*cm], n=11, ar=(1,2,3,4))
P("Table 8.2 — Sector scoreboard since 2015: mean daily return (bps), mean annualized vol, cumulative "
    "dollar volume (trillions).", CAP)
H("8.4  Correlation, fear and cross-asset structure", 2)
img("correlation_heatmap.png", 15.0, "Figure 8.8 — Hierarchically clustered correlation matrix (2020+): "
    "equity mega-caps and sector ETFs form one block; VIX, oil and BTC sit outside it.")
img("rolling_vol_vix.png", 17.0, "Figure 8.9 — Realized vs implied volatility; COVID spike shaded.")
vx = pd.read_csv(os.path.join(TAB, "sql_vix_spx_correlation.csv"))
df_tbl(vx, widths=[3*cm,3*cm,4*cm], n=4, ar=(1,2))
P("Table 8.3 — VIX↔S&P daily-return correlation by era: the hedge got stronger for three decades.", CAP)
img("vix_vs_spx.png", 16.6, "Figure 8.10 — VIX vs S&P daily moves (2020+) and the era-wise correlation bars.")
H("8.5  Crash anatomy", 2)
img("crash_event_study.png", 16.8, "Figure 8.11 — Dot-com, GFC and COVID aligned at their peaks: same "
    "shape, radically different recovery clocks.")
cw = pd.read_csv(os.path.join(TAB, "sql_crash_windows.csv"))
df_tbl(cw, widths=[3.4*cm,3.6*cm,2.8*cm,3.6*cm], n=3, ar=(1,2,3))
P("Table 8.4 — Peak-to-trough and recovery durations (S&P, adjusted).", CAP)
H("8.6  Calendar effects & liquidity growth", 2)
img("seasonality.png", 16.6, "Figure 8.12 — Month and weekday effects across 503 stocks since 1990: "
    "Sep/Oct weakest, Apr/Nov strongest; Monday lag real but tiny.")
img("liquidity_by_decade.png", 16.0, "Figure 8.13 — Average traded dollars per stock per day by decade "
    "(log): market depth grew ~500× since the 1960s.")
img("sector_etf_growth.png", 16.6, "Figure 8.14 — Sector ETF growth since 2015 (log): technology and "
    "discretionary lead; energy lags until the 2022 spike.")
H("8.7  Signals computed in-lake", 2)
img("ma_crossover_nvda.png", 16.8, "Figure 8.15 — MA50×MA200 golden/death crosses for NVDA since 2019, "
    "computed by window functions on the curated table.")
gc = pd.read_csv(os.path.join(TAB, "sql_golden_cross_count.csv"))
df_tbl(gc, widths=[3*cm,4*cm,4*cm], n=6, ar=(1,2))
P("Table 8.5 — Golden/death cross counts since 2000 for the most-liquid names.", CAP)
dv = pd.read_csv(os.path.join(TAB, "sql_dividend_leaders.csv"))
df_tbl(dv, cols=["symbol","security","sector","div_per_share"], widths=[2.2*cm,5.6*cm,4.4*cm,3.2*cm], n=6, ar=(3,))
P("Table 8.6 — Dividend leaders: cumulative dividends per share since listing.", CAP)
an = pd.read_csv(os.path.join(TAB, "sql_anomaly_exhibit.csv"))
df_tbl(an, cols=["date","symbol","security","ret_pct"], widths=[2.6*cm,2.2*cm,6.6*cm,2.6*cm], n=6, ar=(3,))
P("Table 8.7 — Veracity exhibit: the largest flagged anomalies (spin-off/re-basing artifacts), kept in the "
    "lake with is_price_anomaly=True.", CAP)

# ================================================================ 9 STREAM ==========
H("9  Velocity: Spark Structured Streaming over a live tick feed")
P("A pure-Python producer replays a random-walk tick stream for the 40 most liquid S&amp;P names — anchored "
  "at their <i>real</i> last closing prices from the lake — over a TCP socket; Spark Structured Streaming "
  "parses the JSON, applies event-time tumbling windows (10 s) with a 30 s watermark, aggregates per symbol "
  "and sinks window statistics with latency telemetry. In production the socket would be a Kafka topic; the "
  "pipeline shape is identical.")
img("streaming_velocity.png", 16.8, "Figure 9.1 — Ingest per window and end-to-end latency during the demo.")
sm = [["events produced", f"{M_STR['producer_ticks']:,}"],
      ["events processed (deduplicated)", f"{M_STR['spark_processed_events_dedup']:,}"],
      ["loss", f"{M_STR['loss_pct']}%"],
      ["micro-batches (10 s trigger)", f"{M_STR['batches']} (avg {M_STR['avg_batch_s']} s, max {M_STR['max_batch_s']} s)"],
      ["window × symbol aggregates", f"{M_STR['final_windows']}"],
      ["mean / p95 end-to-end latency", f"{M_STR['avg_latency_ms']:.0f} ms / {M_STR['p95_latency_ms']:.0f} ms"],
      ["duplicate window emissions (update mode)", f"{M_STR['duplicate_window_emissions']} → sink must upsert by (window, symbol)"]]
tbl([["Streaming metric","Measured"]]+sm, widths=[8.4*cm,8.6*cm])
P("Table 9.1 — Streaming demo metrics.", CAP)
B(["<b>Lesson 1 — watermarks matter:</b> ticks arrive out of order across symbols; the 30 s watermark lets "
   "late ticks land in the right window instead of corrupting it.",
   "<b>Lesson 2 — update mode re-emits:</b> revised windows arrive at the sink more than once "
   f"({M_STR['duplicate_window_emissions']} extra emissions); a production sink must upsert on the window key, "
   "or counts double (we caught this in our own telemetry).",
   "<b>Lesson 3 — backpressure is honest:</b> on 2 vCPUs one micro-batch ran 3.8 s against a 10 s trigger; "
   "the trigger interval is the system's real SLA."])

# ================================================================ 10 ML =============
H("10  Value: machine learning & an honest backtest")
P("Question: can lake features predict next-day direction well enough to beat buy-and-hold? Universe: the 40 "
  "most liquid names since 2005 (209,150 feature rows). Features: return lags 1/2/5, MA20/MA50 ratios, 21-day "
  "volatility, intraday range, weekday, and the VIX daily return as a market-regime feature. Label: next-day "
  "adjusted return &gt; 0. Split: <b>temporal</b> — train ≤ 2022, test ≥ 2023 (no shuffle, no leakage).")
mm = [["logistic regression", f"{M_ML['logistic_regression']['accuracy']:.4f}", f"{M_ML['logistic_regression']['areaUnderROC']:.4f}"],
      ["random forest (40 trees, depth 4)", f"{M_ML['random_forest']['accuracy']:.4f}", f"{M_ML['random_forest']['areaUnderROC']:.4f}"],
      ["majority-class baseline", f"{M_ML['baseline_majority_accuracy']:.4f}", "—"]]
tbl([["Model (test 2023+)","Accuracy","AUC (ROC)"]]+mm, widths=[7.5*cm,4*cm,4*cm], ar=(1,2))
P("Table 10.1 — Out-of-sample performance: a hair above coin-flip, exactly as market efficiency predicts.", CAP)
img("ml_value.png", 17.4, "Figure 10.1 — Feature importance, accuracy vs baseline, and the gated-strategy "
    "backtest against buy-and-hold.")
bt = M_ML["backtest"]["variants"]
tbl([["Backtest (932 test days)","strategy (P&gt;0.50)","buy & hold"],
     ["total return", f"{bt['thr_0.50']['strategy_total_return_pct']}%", f"{bt['thr_0.50']['buyhold_total_return_pct']}%"],
     ["annualized", f"{bt['thr_0.50']['strategy_ann_pct']}%", f"{bt['thr_0.50']['buyhold_ann_pct']}%"],
     ["Sharpe", f"{bt['thr_0.50']['strategy_sharpe']}", f"{bt['thr_0.50']['buyhold_sharpe']}"],
     ["avg exposure", f"{bt['thr_0.50']['avg_exposure']:.0%}", "100%"]],
    widths=[6*cm,5.5*cm,5.5*cm], ar=(1,2))
P("Table 10.2 — Equal-weight 40-stock portfolio, long only when P(up)&gt;0.50 (costs ignored).", CAP)
B(["The dominant feature is <b>the VIX's own return</b> (importance "
   f"{M_ML['feature_importance'][0]['importance']:.2f}) — regime, not stock skill.",
   "A stricter gate (P&gt;0.55) never triggers (0.2% exposure): forest probabilities hug 0.5 — the model "
   "knows it doesn't know. Reporting that <i>is</i> the value: a big-data platform whose job is to test "
   "hypotheses fast, including the hypothesis that a signal doesn't exist.",
   "Where the lake's value actually compounds: §7 data-quality discovery, §8 risk analytics, §9 latency "
   "telemetry — all re-runnable in minutes on new data."])

# ================================================================ 11 PERF ===========
H("11  Performance: Spark vs pandas on one node")
img("benchmark_spark_vs_pandas.png", 15.6, "Figure 11.1 — Identical workloads on the 4.56 M-row curated "
    "table, single 1 GiB node.")
bb = [["W1 groupBy(symbol, year) → avg close, count", f"{M_BEN['spark_W1_agg_s']:.1f} s", f"{M_BEN['pandas_W1_agg_s']:.1f} s"],
      ["W2 filter date≥2024 → sector agg", f"{M_BEN['spark_W2_filter_agg_s']:.1f} s", f"{M_BEN['pandas_W2_filter_agg_s']:.1f} s"],
      ["W3 full count(scan)", f"{M_BEN['spark_W3_scan_s']:.1f} s", f"{M_BEN['pandas_W3_scan_s']:.1f} s"]]
tbl([["Workload (4.56 M rows parquet)","Spark 3.5 local[1]","pandas 3.0 + pyarrow"]]+bb,
    widths=[8.6*cm,4.2*cm,4.2*cm], ar=(1,2))
P("Table 11.1 — Wall-clock comparison.", CAP)
P("Read honestly: on a single laptop-class node, pandas+pyarrow wins these in-memory aggregations — Spark "
  "pays JVM start-up, task scheduling and shuffle bookkeeping that a 4.5 M-row dataset doesn't amortize. "
  "Spark's advantages are structural, not local: it scales past one node's RAM/disk, tolerates worker "
  "failure, expresses streaming and ML in the same engine, and — decisively for this project — it was the "
  "only engine here that could <i>build</i> the lake (CSV parse + windowed transforms over 479 MB) without "
  "exceeding the memory ceiling, because its spill-to-disk machinery is designed for exactly that. Engine "
  "choice must follow data gravity, not fashion.")

# ================================================================ 12 LESSONS ========
H("12  Challenges & lessons learned (the honest appendix)")
lessons = [
 ("Port 9000 was already taken by the sandbox", "HDFS RPC moved to :19000 (DN :19010, UI :19070); configs "
  "and every script parameterized accordingly."),
 ("Spark forbids -Xmx inside extraJavaOptions", "Heap stays in spark.driver.memory; native knobs "
  "(metaspace, code cache, direct memory, -Xss) go in extraJavaOptions."),
 ("countDistinct(struct(*all_columns)) OOM-killed the JVM", "Replaced with a spill-friendly "
  "groupBy(symbol,date).count() duplicate probe; full-row exact-dup detection folded into key-uniqueness."),
 ("partitionBy writes opened ~150 parquet writers per task", "repartition+sortWithinPartitions by the "
  "partition columns + 32 MB row groups → ~1 writer per task; OOM gone."),
 ("MLlib + whole-stage codegen blew the 160 MB metaspace cap", "Raised metaspace, disabled whole-stage "
  "codegen for ML (unnecessary at this scale), and split train/predict into separate JVMs."),
 ("The kernel SIGKILLed long Spark apps (cgroup 1 GiB incl. page cache)", "One fresh JVM per pipeline "
  "phase via a Python DAG runner; chunked the windowed transform by pmod(hash(symbol),3) so each symbol's "
  "history stayed whole."),
 ("Spark's drop() is case-insensitive", "drop(\"Date\") silently killed the new lowercase date column; "
  "now convert-in-place then rename."),
 ("A filtered pandas scan of the staging parquet took ~8 minutes", "Reading 40 CSV tails (~2 s) replaced "
  "it for the streaming anchors; lesson: know your engines' scan strategies."),
 ("The streaming query never terminated after socket EOF once", "Watchdog thread + incremental metrics "
  "flush + os._exit finalization guarantee bounded runtime and always-usable telemetry."),
 ("Provider data lies, politely", "LVS adjusted-close re-basing and spin-off gaps produce ±50–97% fake "
  "'returns'; the flag-don't-delete policy (is_price_anomaly) keeps them visible in every exhibit."),
]
tbl([["#","Problem","Resolution"]]+[[str(i+1), a, b] for i, (a, b) in enumerate(lessons)],
    widths=[0.8*cm,7.2*cm,9.0*cm])
P("Table 12.1 — Failure log and fixes; each entry is reproducible from the shipped code and logs.", CAP)

# ================================================================ 13 CONCL ==========
H("13  Conclusion & future work")
P("This project delivered a complete, reproducible big-data lifecycle over real market data: ingestion of "
  "4.56 M rows across 525 instruments and 8 asset types; an HDFS data lake with raw/curated/report zones "
  "verified by fsck; a real MapReduce aggregation with a documented combiner optimization; a Spark ETL with "
  "a pre-cleansing veracity audit and a partitioned columnar curated table; 16 SQL insight queries; a "
  "Structured Streaming velocity demo with lossless ingest; an honest ML study; 26 publication figures; and "
  "this report — all orchestrated by pure Python on a 1 GiB node, which forced every architectural decision "
  "to justify itself.")
P("<b>Future work.</b> Replace the socket feed with Kafka (KRaft) and add exactly-once sinks; move to a "
  "multi-node YARN/Kubernetes cluster to measure true scale-out (the §11 benchmark is the baseline to beat); "
  "adopt Delta Lake/Iceberg for ACID table semantics and time travel over the curated zone; extend the "
  "equity panel with delisted constituents to remove survivorship bias; add intraday bars to push Volume two "
  "orders of magnitude higher; and serve the insight layer through Airflow-scheduled refreshes with data "
  "contracts on the raw zone.")
SP(0.4)
P("<b>Reproducibility.</b> <font face='Courier'>src/00_ingest_data.py</font> (download) → "
  "<font face='Courier'>src/setup_hdfs_and_mr.sh</font> (HDFS+MR) → "
  "<font face='Courier'>src/run_pipeline.py curated sql streaming ml benchmark</font> → "
  "<font face='Courier'>src/06_hdfs_publish.py</font> → <font face='Courier'>src/07_charts_*.py</font>, "
  "<font face='Courier'>src/07_diagrams.py</font> → <font face='Courier'>report/build_report.py</font>. "
  "All metrics JSONs, SQL result CSVs and HDFS evidence ship in <font face='Courier'>output/</font>.")

# ================================================================ APPENDIX =========
H("Appendix A  Visualization asset inventory")
assets = sorted(os.listdir(CH))
rows = [["Asset #","File (output/charts/)","Kind"]]
kinds = {"architecture_lake.png":"diagram","hdfs_architecture.png":"diagram","mapreduce_concept.png":"diagram",
         "five_vs_infographic.png":"infographic"}
fig_n = 1
for a in assets:
    if not a.endswith(".png"): continue
    rows.append([str(fig_n), a, kinds.get(a, "chart")]); fig_n += 1
tbl(rows, widths=[1.6*cm,9.4*cm,6*cm])
H("Appendix B  References")
B(["Apache Hadoop 3.3.6 documentation — HDFS architecture, Streaming MapReduce, fsck/dfsadmin. "
   "<font face='Courier'>https://hadoop.apache.org/docs/r3.3.6/</font>",
   "Apache Spark 3.5.3 documentation — DataFrames, window functions, MLlib, Structured Streaming. "
   "<font face='Courier'>https://spark.apache.org/docs/3.5.3/</font>",
   "Yahoo Finance chart API via yfinance (Python) — daily bars, dividends & splits. "
   "<font face='Courier'>https://query1.finance.yahoo.com/v8/finance/chart/{symbol}</font> · "
   "<font face='Courier'>https://pypi.org/project/yfinance/</font> · "
   "<font face='Courier'>https://github.com/ranaroufi/yfinance</font>",
   "datasets/s-and-p-500-companies (GitHub) — constituent symbols and GICS sectors. "
   "<font face='Courier'>https://github.com/datasets/s-and-p-500-companies</font>",
   "McKinsey et al. — the 5-V framing of big data (volume, velocity, variety, veracity, value).",
   "reportlab Platypus — programmatic PDF composition used for this document."])

# ---- layout hygiene: no split code blocks, no orphan table rows/captions ----
def _nlines(fl):
    return len(fl.lines)
fixed = []
i = 0
while i < len(STORY):
    fl = STORY[i]
    if isinstance(fl, Paragraph) and fl.style.name == "BULL":
        j = i
        while j < len(STORY) and isinstance(STORY[j], Paragraph) \
              and STORY[j].style.name == "BULL" and (j - i) < 12:
            j += 1
        if j - i > 1:
            fixed.append(KeepTogether(STORY[i:j])); i = j; continue
    if isinstance(fl, Preformatted) and _nlines(fl) <= 34:
        fixed.append(KeepTogether([fl])); i += 1; continue
    if isinstance(fl, Table) and len(fl._cellvalues) <= 16 and i+1 < len(STORY) \
       and isinstance(STORY[i+1], Paragraph) and STORY[i+1].style.name == "CAP":
        fixed.append(KeepTogether([fl, STORY[i+1]])); i += 2; continue
    fixed.append(fl); i += 1
STORY[:] = fixed

doc.multiBuild(STORY)
print("REPORT BUILT:", OUTP, f"{os.path.getsize(OUTP)/1e6:.1f} MB")
