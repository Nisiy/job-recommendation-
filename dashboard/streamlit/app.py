import re
import pathlib
import os
import pickle
import collections
from collections import Counter
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ── Load .env ─────────────────────────────────────────────────────────────────
for _env_path in [pathlib.Path("/opt/airflow/.env"), pathlib.Path(".env")]:
    if _env_path.exists():
        load_dotenv(_env_path)
        break
else:
    load_dotenv()

# ── AI model ──────────────────────────────────────────────────────────────────
AI_AVAILABLE = False
AI_PIPELINE  = None
AI_LE        = None
AI_SALARY    = None
_load_error  = None

for _p in [
    pathlib.Path("/opt/airflow/model_bundle.pkl"),
    pathlib.Path("/opt/airflow/model_bundle_fixed.pkl"),
    pathlib.Path("/opt/airflow/model_bundle_compat.pkl"),
    pathlib.Path("model_bundle.pkl"),
]:
    if _p.exists() and _p.is_file():
        try:
            _bundle      = pickle.load(open(_p, "rb"))
            AI_PIPELINE  = _bundle["job_pipeline"]
            AI_LE        = _bundle["label_encoder"]
            AI_SALARY    = _bundle["salary_model"]
            AI_AVAILABLE = True
            break
        except Exception as e:
            _load_error = str(e)

st.set_page_config(page_title="Job Pipeline Monitor", page_icon="🚀", layout="wide")

# ── Theme / CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
html, body, [class*="css"], [class*="st-"], .stApp,
.stTextInput input, .stSelectbox div, .stTextArea textarea,
.stRadio label, .stCheckbox label, .stSlider, button {
    font-family: 'Inter', sans-serif !important;
}
.stApp, .stApp > div, [data-testid="stAppViewContainer"],
[data-testid="stAppViewContainer"] > section {
    background-color: #0a0f1e !important;
    color: #e2e8f0 !important;
}
[data-testid="stMain"], [data-testid="block-container"] {
    background-color: #0a0f1e !important;
    color: #e2e8f0 !important;
}
[data-testid="stSidebar"],
[data-testid="stSidebar"] > div,
[data-testid="stSidebar"] > div > div,
section[data-testid="stSidebar"] {
    background-color: #080f1e !important;
    border-right: 1px solid #1e293b !important;
}
[data-testid="stSidebar"] * { color: #e2e8f0 !important; }
[data-testid="stSidebar"] .stRadio label {
    padding: 6px 10px; border-radius: 8px;
    cursor: pointer; transition: background 0.2s;
}
[data-testid="stSidebar"] .stRadio label:hover { background: #1e293b; }
[data-testid="metric-container"] {
    background: linear-gradient(135deg, #1e293b 0%, #162032 100%) !important;
    border: 1px solid #334155 !important; border-radius: 14px !important;
    padding: 1.2rem 1.4rem !important; box-shadow: 0 4px 20px rgba(0,0,0,0.3) !important;
    transition: border-color 0.2s, transform 0.2s !important;
}
[data-testid="metric-container"]:hover {
    border-color: #7c8bff !important; transform: translateY(-2px) !important;
}
[data-testid="metric-container"] label {
    color: #7c8bff !important; font-size: 0.72rem !important;
    font-weight: 600 !important; letter-spacing: 0.08em !important;
    text-transform: uppercase !important;
}
[data-testid="stMetricValue"] { color: #fff !important; font-size: 1.6rem !important; font-weight: 700 !important; }
[data-testid="stMetricDelta"] { color: #22d3a5 !important; }
.stTextInput input, .stTextArea textarea {
    background-color: #1e293b !important; color: #e2e8f0 !important;
    border: 1px solid #334155 !important; border-radius: 10px !important;
}
.stTextInput input:focus, .stTextArea textarea:focus {
    border-color: #7c8bff !important; box-shadow: 0 0 0 2px rgba(124,139,255,0.2) !important;
}
.stSelectbox > div > div {
    background-color: #1e293b !important; color: #e2e8f0 !important;
    border: 1px solid #334155 !important; border-radius: 10px !important;
}
[data-testid="stSelectbox"] div[data-baseweb="select"] > div {
    background-color: #1e293b !important; color: #e2e8f0 !important;
}
.stButton button {
    border-radius: 10px !important; font-weight: 600 !important;
    transition: transform 0.2s, box-shadow 0.2s !important;
    background: linear-gradient(135deg, #4a4aff, #7c8bff) !important;
    color: white !important; border: none !important;
}
.stButton button:hover {
    transform: translateY(-1px) !important;
    box-shadow: 0 4px 16px rgba(124,139,255,0.3) !important;
}
[data-testid="stDataFrame"], [data-testid="dataframe-container"] {
    border-radius: 12px !important; overflow: hidden !important;
    border: 1px solid #334155 !important;
}
.dvn-scroller { background-color: #1e293b !important; }
[data-testid="stTabs"] [data-baseweb="tab-list"] {
    background-color: #1e293b !important; border-radius: 10px !important;
}
[data-testid="stTabs"] [data-baseweb="tab"] { color: #94a3b8 !important; }
[data-testid="stTabs"] [aria-selected="true"] { color: #7c8bff !important; }
[data-testid="stExpander"] {
    background-color: #1e293b !important; border: 1px solid #334155 !important;
    border-radius: 10px !important;
}
[data-testid="stExpander"] summary { color: #e2e8f0 !important; }
.stMarkdown, .stCaption, p, span, label { color: #e2e8f0 !important; }
.stCaption { color: #94a3b8 !important; font-size: 0.78rem !important; }
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: #0a0f1e; }
::-webkit-scrollbar-thumb { background: #334155; border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: #7c8bff; }
.section {
    color: #7c8bff; font-size: 0.68rem; font-weight: 600;
    letter-spacing: 0.14em; text-transform: uppercase;
    border-bottom: 1px solid #1e293b; padding-bottom: 6px; margin: 2rem 0 1.2rem;
}
.job-card {
    background: linear-gradient(135deg, #1e293b 0%, #162032 100%);
    border: 1px solid #334155; border-radius: 14px;
    padding: 1.2rem 1.4rem; margin-bottom: 0.9rem;
    transition: border-color 0.2s, transform 0.2s, box-shadow 0.2s;
    box-shadow: 0 2px 12px rgba(0,0,0,0.2);
}
.job-card:hover {
    border-color: #7c8bff; transform: translateY(-2px);
    box-shadow: 0 8px 24px rgba(124,139,255,0.15);
}
.job-title { font-size: 1.05rem; font-weight: 600; color: #e2e8f0; margin-bottom: 0.3rem; }
.job-meta  { font-size: 0.82rem; color: #94a3b8; margin-bottom: 0.6rem; }
.job-badge {
    display: inline-block; background: #0a0f1e; border: 1px solid #334155;
    border-radius: 6px; padding: 3px 10px; font-size: 0.72rem;
    color: #94a3b8; margin-right: 6px; margin-bottom: 4px;
}
.job-link {
    display: inline-block; margin-top: 0.6rem;
    background: linear-gradient(135deg, #4a4aff, #7c8bff);
    color: white !important; padding: 5px 16px; border-radius: 8px;
    font-size: 0.78rem; font-weight: 500; text-decoration: none; transition: opacity 0.2s;
}
.job-link:hover { opacity: 0.85; }
.ai-result-card {
    background: linear-gradient(135deg, #1e293b 0%, #162032 100%);
    border: 1px solid #334155; border-radius: 14px;
    padding: 1.4rem 1.6rem; margin-bottom: 1rem; text-align: center;
    box-shadow: 0 4px 20px rgba(0,0,0,0.25); transition: border-color 0.2s;
}
.ai-result-card:hover { border-color: #7c8bff; }
.ai-result-label {
    font-size: 0.68rem; color: #7c8bff; letter-spacing: 0.12em;
    text-transform: uppercase; font-weight: 600; margin-bottom: 0.5rem;
}
.ai-result-value { font-size: 1.5rem; font-weight: 700; color: #e2e8f0; }
.page-header {
    background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
    border: 1px solid #334155; border-radius: 16px;
    padding: 1.5rem 2rem; margin-bottom: 1.5rem;
}
.page-header h1 { margin: 0; font-size: 1.6rem; color: #e2e8f0; }
.page-header p  { margin: 0; font-size: 0.85rem; color: #94a3b8; }
.info-banner {
    background: rgba(124,139,255,0.08); border: 1px solid rgba(124,139,255,0.3);
    border-radius: 10px; padding: 0.8rem 1.2rem;
    font-size: 0.82rem; color: #a5b4fc; margin-bottom: 1rem;
}
.service-card {
    background: linear-gradient(135deg, #1e293b 0%, #162032 100%);
    border: 1px solid #334155; border-radius: 14px;
    padding: 1.2rem 1.4rem; margin-bottom: 0.9rem;
    transition: border-color 0.2s, transform 0.2s;
}
.service-card:hover { border-color: #7c8bff; transform: translateY(-2px); }
.service-title { font-size: 1rem; font-weight: 600; color: #e2e8f0; margin-bottom: 0.3rem; }
.service-url { font-size: 0.82rem; color: #7c8bff; }
</style>
""", unsafe_allow_html=True)

# ── Base plot layout
PLOT_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#e2e8f0", family="Inter"),
    margin=dict(t=30, b=20, l=10, r=10),
    legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor="#334155", borderwidth=1),
)
XAXIS_DEFAULT = dict(gridcolor="#1e293b", linecolor="#334155", zerolinecolor="#1e293b")
YAXIS_DEFAULT = dict(gridcolor="#1e293b", linecolor="#334155", zerolinecolor="#1e293b")

def plot_layout(**overrides):
    layout = dict(**PLOT_LAYOUT)
    layout.update(overrides)
    return layout

COLOR = {
    "success":         "#22d3a5",
    "failed":          "#f87171",
    "running":         "#fbbf24",
    "queued":          "#7c8bff",
    "up_for_retry":    "#fb923c",
    "skipped":         "#94a3b8",
    "upstream_failed": "#f97316",
}

JOB_TABLE_KEYWORDS = ["job","listing","posting","vacancy","bongthom","work","career","position"]

# ── Styler compatibility: pandas <2.1 uses applymap, >=2.1 uses map
def _style_state(val):
    if isinstance(val, str) and val in COLOR:
        return f"color: {COLOR[val]}"
    return ""

def style_state_col(styler, subset=("state",)):
    try:
        return styler.map(_style_state, subset=list(subset))
    except AttributeError:
        return styler.applymap(_style_state, subset=list(subset))

# ══════════════════════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════════════════════
def _db_url() -> str:
    host     = os.getenv("DB_HOST") or os.getenv("STREAMLIT_DB_HOST") or "airflow-postgres"
    port     = os.getenv("DB_PORT") or os.getenv("STREAMLIT_DB_PORT") or "5432"
    user     = os.getenv("DB_USER",     "airflow")
    password = os.getenv("DB_PASSWORD", "airflow")
    dbname   = os.getenv("DB_NAME",     "airflow")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"

@st.cache_resource(show_spinner=False)
def get_engine():
    try:
        url    = _db_url()
        engine = create_engine(url, pool_pre_ping=True, pool_recycle=300,
                               connect_args={"connect_timeout": 10})
        with engine.connect() as c:
            c.execute(text("SELECT 1"))
        return engine
    except Exception as e:
        st.error(f"❌ DB connection failed: {e}")
        return None

@st.cache_data(ttl=30, show_spinner=False)
def query(sql: str) -> pd.DataFrame:
    try:
        eng = get_engine()
        if eng is None:
            return pd.DataFrame()
        with eng.connect() as conn:
            df = pd.read_sql(text(sql), conn)
        return df
    except Exception as ex:
        st.warning(f"Query error: {ex}")
        return pd.DataFrame()

def safe_metric(sql, col="n", default="—"):
    try:
        df = query(sql)
        if df.empty or col not in df.columns:
            return default
        v = df[col].iloc[0]
        return v if v is not None else default
    except Exception:
        return default

# FIX: raw table data — bypass cache, use direct engine connection to avoid stale reads
def query_raw(sql: str) -> pd.DataFrame:
    """Direct query without cache — used for raw data preview so it's always fresh."""
    try:
        eng = get_engine()
        if eng is None:
            return pd.DataFrame()
        with eng.connect() as conn:
            df = pd.read_sql(text(sql), conn)
        # Coerce object columns to string to avoid Arrow serialization errors in st.dataframe
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].astype(str).replace({"None": "", "nan": "", "<NA>": ""})
        return df
    except Exception as ex:
        st.error(f"❌ Raw query failed: {ex}")
        return pd.DataFrame()

# ── AI / job helpers ──────────────────────────────────────────────────────────
@st.cache_data(ttl=60, show_spinner=False)
def ai_get_columns(table):
    df = query(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name='{table}'
        ORDER BY ordinal_position
    """)
    return df["column_name"].tolist() if not df.empty else []

def ai_fuzzy_find(columns, keywords):
    for kw in keywords:
        for col in columns:
            if kw in col.lower():
                return col
    return None

def ai_search_jobs(predicted_job, skills_input, table, title_col, limit=60):
    all_cols = ai_get_columns(table)
    if not all_cols:
        return pd.DataFrame()
    raw      = predicted_job.split() + [s.strip() for s in skills_input.split(",")]
    keywords = list({k.lower() for k in raw if len(k) > 2})
    if not keywords:
        return pd.DataFrame()
    text_hints  = ["title","name","position","role","job","post","category","desc","detail","type","field"]
    search_cols = list(dict.fromkeys(
        ([title_col] if title_col else []) +
        [c for c in all_cols if any(h in c.lower() for h in text_hints)]
    )) or all_cols[:5]
    conditions = [
        f'LOWER("{col}"::text) LIKE \'%%{kw.replace(chr(39), chr(39)*2)}%%\''
        for col in search_cols for kw in keywords
    ]
    df = query(f'SELECT * FROM "{table}" WHERE {" OR ".join(conditions)} LIMIT {limit}')
    if df.empty:
        df = query(f'SELECT * FROM "{table}" LIMIT {limit}')
    return df

def ai_score(row, predicted_job, skills_input):
    kws  = {k.lower() for k in (predicted_job + " " + skills_input).replace(",", " ").split() if len(k) > 2}
    text = " ".join(str(v) for v in row.values).lower()
    hits = sum(1 for kw in kws if kw in text)
    return min(100, int(hits / max(len(kws), 1) * 100))

def ai_safe_val(row, col):
    if not col:
        return ""
    v = row.get(col, "")
    return "" if pd.isna(v) or str(v).strip().lower() in ("nan","none","null","") else str(v).strip()

def render_job_card(title, company, loc, source, dead, url, score=None):
    badges = ""
    if score is not None:
        color = "#22d3a5" if score >= 60 else "#fbbf24" if score >= 30 else "#94a3b8"
        badges += (
            f'<span style="display:inline-block;background:rgba(34,211,165,0.1);'
            f'border:1px solid {color};border-radius:6px;padding:3px 10px;'
            f'font-size:0.72rem;color:{color};margin-right:6px;font-weight:600;">'
            f'🎯 {score}% match</span>'
        )
    if source: badges += f'<span class="job-badge">🌐 {source}</span>'
    if loc:    badges += f'<span class="job-badge">📍 {loc}</span>'
    if dead:   badges += f'<span class="job-badge">⏰ {dead}</span>'
    link_btn = f'<a class="job-link" href="{url}" target="_blank">View Job →</a>' if url else ""
    st.markdown(f"""
    <div class="job-card">
        <div class="job-title">{title}</div>
        <div class="job-meta">🏢 {company if company else "—"}</div>
        <div style="margin-bottom:4px">{badges}</div>
        {link_btn}
    </div>""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
engine = get_engine()

# Strip trailing slashes from env URLs to avoid double-slash issues
def _clean_url(url: str) -> str:
    return url.rstrip("/")

AIRFLOW_URL  = _clean_url(os.getenv("AIRFLOW_WEBSERVER_URL", "http://localhost:8081"))
KAFKA_UI_URL = _clean_url(os.getenv("KAFKA_UI_URL",          "http://localhost:8090"))
PGADMIN_URL  = _clean_url(os.getenv("PGADMIN_URL",           "http://localhost:5050"))
SPARK_URL    = _clean_url(os.getenv("SPARK_UI_URL",          "http://localhost:8082"))

with st.sidebar:
    st.markdown("## 🚀 Job Pipeline")
    st.markdown("---")
    if engine:
        st.markdown('<span style="color:#22d3a5;font-weight:600;">🟢 Database Connected</span>', unsafe_allow_html=True)
    else:
        st.markdown('<span style="color:#f87171;font-weight:600;">🔴 Database Disconnected</span>', unsafe_allow_html=True)
    if AI_AVAILABLE:
        st.markdown('<span style="color:#7c8bff;font-weight:600;">🤖 AI Model Ready</span>', unsafe_allow_html=True)
    else:
        st.markdown('<span style="color:#fbbf24;font-weight:600;">⚠️ AI Unavailable</span>', unsafe_allow_html=True)
    st.markdown("---")
    page = st.radio("Navigation", [
        "📊 Overview",
        "📰 Job Feed",
        "💼 Job Market",
        "🧠 Skills & Jobs",
        "🤖 AI Recommender",
        "📋 DAG Runs",
        "⚙️ Tasks",
        "📈 Performance",
        "📡 Kafka",
        "✈️ Airflow",
        "🔗 pgAdmin",
    ], label_visibility="collapsed")
    st.markdown("---")
    st.markdown("**🔗 Quick Links**")
    st.markdown(f'<a href="{AIRFLOW_URL}"  target="_blank" style="color:#7c8bff;text-decoration:none;font-size:0.82rem;">✈️ Airflow UI</a>',  unsafe_allow_html=True)
    st.markdown(f'<a href="{KAFKA_UI_URL}" target="_blank" style="color:#7c8bff;text-decoration:none;font-size:0.82rem;">📡 Kafka UI</a>',    unsafe_allow_html=True)
    st.markdown(f'<a href="{PGADMIN_URL}"  target="_blank" style="color:#7c8bff;text-decoration:none;font-size:0.82rem;">🗄️ pgAdmin</a>',      unsafe_allow_html=True)
    st.markdown(f'<a href="{SPARK_URL}"    target="_blank" style="color:#7c8bff;text-decoration:none;font-size:0.82rem;">⚡ Spark UI</a>',     unsafe_allow_html=True)
    st.markdown("---")
    st.caption(f"🕐 Updated: {datetime.now().strftime('%H:%M:%S')}")
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()
    st.markdown("---")
    st.caption("Job Pipeline Monitor v2.4")

# ── DB gate ───────────────────────────────────────────────────────────────────
if not engine:
    host = os.getenv("DB_HOST") or os.getenv("STREAMLIT_DB_HOST") or "airflow-postgres"
    port = os.getenv("DB_PORT") or os.getenv("STREAMLIT_DB_PORT") or "5432"
    st.error("❌ Cannot connect to the database.")
    st.info(
        "**Check these env vars in your `.env` / `docker-compose.yml`:**\n\n"
        f"- `DB_HOST`     → `{host}`\n"
        f"- `DB_PORT`     → `{port}`\n"
        f"- `DB_NAME`     → `{os.getenv('DB_NAME','airflow')}`\n"
        f"- `DB_USER`     → `{os.getenv('DB_USER','airflow')}`\n"
        "- `DB_PASSWORD` → (hidden)\n\n"
        "Make sure containers are running: `docker compose up -d`\n\n"
        "Then click **🔄 Refresh Data** in the sidebar."
    )
    # Do NOT call st.stop() here — just let the page end naturally.
    # This keeps the sidebar interactive so the user can refresh.

# ══════════════════════════════════════════════════════
# 📊 OVERVIEW
# ══════════════════════════════════════════════════════
elif page == "📊 Overview":
    st.markdown("""
    <div class="page-header">
        <h1>📊 Pipeline Overview</h1>
        <p>Real-time Airflow pipeline metrics and status</p>
    </div>""", unsafe_allow_html=True)
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("Total Runs",    safe_metric("SELECT COUNT(*) n FROM dag_run"))
    c2.metric("✅ Success",    safe_metric("SELECT COUNT(*) n FROM dag_run WHERE state='success'"))
    c3.metric("❌ Failed",     safe_metric("SELECT COUNT(*) n FROM dag_run WHERE state='failed'"))
    c4.metric("⏳ Running",    safe_metric("SELECT COUNT(*) n FROM dag_run WHERE state='running'"))
    c5.metric("📦 Total DAGs", safe_metric("SELECT COUNT(DISTINCT dag_id) n FROM dag_run"))
    st.markdown('<p class="section">Run Status Distribution</p>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    status_df = query("SELECT state, COUNT(*) count FROM dag_run GROUP BY state")
    if not status_df.empty:
        fig = px.pie(status_df, names="state", values="count", hole=0.6,
                     color="state", color_discrete_map=COLOR)
        fig.update_traces(textposition="outside", textinfo="label+percent",
                          marker=dict(line=dict(color="#0a0f1e", width=2)))
        fig.update_layout(**plot_layout(title="Status Breakdown"))
        col1.plotly_chart(fig, use_container_width=True)
    trend_df = query("""
        SELECT DATE(execution_date) d, state, COUNT(*) count FROM dag_run
        WHERE execution_date >= NOW() - INTERVAL '30 days'
        GROUP BY d, state ORDER BY d
    """)
    if not trend_df.empty:
        fig2 = px.bar(trend_df, x="d", y="count", color="state",
                      color_discrete_map=COLOR, barmode="stack", title="30-Day Run Trend")
        fig2.update_layout(**plot_layout(xaxis=XAXIS_DEFAULT, yaxis=YAXIS_DEFAULT))
        col2.plotly_chart(fig2, use_container_width=True)
    st.markdown('<p class="section">Top DAGs by Activity</p>', unsafe_allow_html=True)
    top = query("""
        SELECT dag_id,
               SUM(CASE WHEN state='success' THEN 1 ELSE 0 END) success,
               SUM(CASE WHEN state='failed'  THEN 1 ELSE 0 END) failed,
               SUM(CASE WHEN state='running' THEN 1 ELSE 0 END) running,
               COUNT(*) total
        FROM dag_run GROUP BY dag_id ORDER BY total DESC LIMIT 10
    """)
    if not top.empty:
        fig3 = px.bar(top, x="dag_id", y=["success","failed","running"],
                      color_discrete_map={"success":"#22d3a5","failed":"#f87171","running":"#fbbf24"},
                      barmode="group", title="Top 10 DAGs")
        fig3.update_layout(**plot_layout(
            xaxis=dict(tickangle=-20, **XAXIS_DEFAULT),
            yaxis=YAXIS_DEFAULT,
        ))
        st.plotly_chart(fig3, use_container_width=True)
    st.markdown('<p class="section">Recent DAG Runs</p>', unsafe_allow_html=True)
    recent = query("SELECT dag_id, state, execution_date, run_type FROM dag_run ORDER BY execution_date DESC LIMIT 10")
    if not recent.empty:
        st.dataframe(recent, use_container_width=True, height=300)

# ══════════════════════════════════════════════════════
# 📰 JOB FEED
# ══════════════════════════════════════════════════════
elif page == "📰 Job Feed":
    st.markdown("""
    <div class="page-header">
        <h1>📰 Live Job Feed</h1>
        <p>Browse all scraped job listings with filters</p>
    </div>""", unsafe_allow_html=True)
    all_tables = query("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name")
    table_list = all_tables["table_name"].tolist() if not all_tables.empty else []
    job_tables = [t for t in table_list if any(k in t.lower() for k in JOB_TABLE_KEYWORDS)]
    if not job_tables and not table_list:
        st.info("No job tables found. Run your scrapers first.")
    else:
        chosen_table = st.selectbox("📋 Select Job Table", job_tables if job_tables else table_list)
        if chosen_table:
            with st.spinner("Loading job listings…"):
                df = query_raw(f'SELECT * FROM "{chosen_table}" LIMIT 5000')
            if df.empty:
                st.warning("Table is empty — run your scrapers to populate it.")
            else:
                df.columns  = [c.lower().strip() for c in df.columns]
                cols        = df.columns.tolist()
                title_col   = ai_fuzzy_find(cols, ["title","position","role","name","job","post","vacancy"])
                company_col = ai_fuzzy_find(cols, ["company","employer","organization","org","firm"])
                loc_col     = ai_fuzzy_find(cols, ["location","city","province","region","place","area"])
                source_col  = ai_fuzzy_find(cols, ["source","website","portal","platform","site"])
                dead_col    = ai_fuzzy_find(cols, ["deadline","closing","expiry","expire","due"])
                url_col     = ai_fuzzy_find(cols, ["url","link","href","apply"])
                st.markdown(f'<div class="info-banner">📦 <strong>{len(df):,}</strong> total jobs in <code>{chosen_table}</code></div>', unsafe_allow_html=True)
                st.markdown('<p class="section">🔍 Filters</p>', unsafe_allow_html=True)
                f1,f2,f3,f4 = st.columns(4)
                search       = f1.text_input("Search title", placeholder="e.g. Developer")
                source_opts  = ["All"] + sorted(df[source_col].dropna().unique().tolist()) if source_col else ["All"]
                sel_source   = f2.selectbox("Source", source_opts)
                company_opts = ["All"] + sorted(df[company_col].dropna().unique().tolist()) if company_col else ["All"]
                sel_company  = f3.selectbox("Company", company_opts)
                loc_opts     = ["All"] + sorted(df[loc_col].dropna().unique().tolist()) if loc_col else ["All"]
                sel_loc      = f4.selectbox("Location", loc_opts)
                per_page     = st.select_slider("Jobs per page", options=[10,20,30,50], value=20)
                filtered = df.copy()
                if search and title_col:
                    filtered = filtered[filtered[title_col].str.contains(search, case=False, na=False)]
                if sel_source  != "All" and source_col:  filtered = filtered[filtered[source_col]  == sel_source]
                if sel_company != "All" and company_col: filtered = filtered[filtered[company_col] == sel_company]
                if sel_loc     != "All" and loc_col:     filtered = filtered[filtered[loc_col]     == sel_loc]
                st.markdown(f'<p class="section">{len(filtered):,} jobs match your filters</p>', unsafe_allow_html=True)
                if filtered.empty:
                    st.warning("No jobs match your filters. Try adjusting the search.")
                else:
                    total_pages = max(1, -(-len(filtered) // per_page))
                    _, col_pg, _ = st.columns([1,2,1])
                    page_num = col_pg.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1)
                    st.caption(f"Page {page_num} of {total_pages}")
                    for _, row in filtered.iloc[(page_num-1)*per_page : page_num*per_page].iterrows():
                        render_job_card(
                            title   = ai_safe_val(row, title_col)   or "Untitled",
                            company = ai_safe_val(row, company_col) or "Unknown",
                            loc     = ai_safe_val(row, loc_col),
                            source  = ai_safe_val(row, source_col),
                            dead    = ai_safe_val(row, dead_col),
                            url     = ai_safe_val(row, url_col),
                        )

# ══════════════════════════════════════════════════════
# 💼 JOB MARKET
# ══════════════════════════════════════════════════════
elif page == "💼 Job Market":
    st.markdown("""
    <div class="page-header">
        <h1>💼 Job Market Insights</h1>
        <p>Analytics from your scraped job listings</p>
    </div>""", unsafe_allow_html=True)
    all_tables   = query("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name")
    table_list   = all_tables["table_name"].tolist() if not all_tables.empty else []
    job_tables   = [t for t in table_list if any(k in t.lower() for k in JOB_TABLE_KEYWORDS)]
    if not job_tables and not table_list:
        st.info("No job tables found.")
    else:
        chosen_table = st.selectbox("Job listings table", job_tables if job_tables else table_list)
        if chosen_table:
            with st.spinner("Loading market data…"):
                df = query_raw(f'SELECT * FROM "{chosen_table}" LIMIT 5000')
            if df.empty:
                st.warning("Table is empty.")
            else:
                df.columns  = [c.lower().strip() for c in df.columns]
                cols        = df.columns.tolist()
                source_col  = ai_fuzzy_find(cols, ["source","website","portal","platform","site"])
                title_col   = ai_fuzzy_find(cols, ["title","position","role","name","job","post","vacancy"])
                company_col = ai_fuzzy_find(cols, ["company","employer","organization","org","firm"])
                loc_col     = ai_fuzzy_find(cols, ["location","city","province","region","place","area"])
                dead_col    = ai_fuzzy_find(cols, ["deadline","closing","expiry","expire","due"])
                c1,c2,c3,c4 = st.columns(4)
                c1.metric("Total Jobs",  f"{len(df):,}")
                c2.metric("Sources",     df[source_col].nunique()  if source_col  else "N/A")
                c3.metric("Companies",   df[company_col].nunique() if company_col else "N/A")
                c4.metric("Locations",   df[loc_col].nunique()     if loc_col     else "N/A")
                if source_col:
                    st.markdown('<p class="section">Jobs by Source</p>', unsafe_allow_html=True)
                    src_df = df[source_col].value_counts().reset_index()
                    src_df.columns = ["source","count"]
                    col1,col2 = st.columns(2)
                    fig_src = px.pie(src_df, names="source", values="count", hole=0.55,
                                     color_discrete_sequence=px.colors.qualitative.Bold, title="Source Distribution")
                    fig_src.update_traces(marker=dict(line=dict(color="#0a0f1e", width=2)))
                    fig_src.update_layout(**plot_layout())
                    col1.plotly_chart(fig_src, use_container_width=True)
                    fig_src2 = px.bar(src_df, x="source", y="count", color="count",
                                      color_continuous_scale="Blues", title="Jobs per Source")
                    fig_src2.update_layout(**plot_layout(xaxis=XAXIS_DEFAULT, yaxis=YAXIS_DEFAULT))
                    col2.plotly_chart(fig_src2, use_container_width=True)
                if company_col:
                    st.markdown('<p class="section">Top Companies Hiring</p>', unsafe_allow_html=True)
                    top_n   = st.slider("Show top N companies", 5, 30, 15)
                    comp_df = df[company_col].value_counts().head(top_n).reset_index()
                    comp_df.columns = ["company","count"]
                    col1,col2 = st.columns([2,1])
                    fig_comp = px.bar(comp_df, x="count", y="company", orientation="h",
                                      color="count", color_continuous_scale="Teal", title="Top Hiring Companies")
                    fig_comp.update_layout(**plot_layout(
                        yaxis=dict(autorange="reversed", **YAXIS_DEFAULT),
                        xaxis=XAXIS_DEFAULT,
                    ))
                    col1.plotly_chart(fig_comp, use_container_width=True)
                    fig_tree = px.treemap(comp_df, path=["company"], values="count",
                                          color="count", color_continuous_scale="Blues", title="Company Treemap")
                    fig_tree.update_layout(**plot_layout())
                    col2.plotly_chart(fig_tree, use_container_width=True)
                if loc_col:
                    st.markdown('<p class="section">Jobs by Location</p>', unsafe_allow_html=True)
                    loc_df = df[loc_col].value_counts().head(20).reset_index()
                    loc_df.columns = ["location","count"]
                    fig_loc = px.bar(loc_df, x="location", y="count", color="count",
                                     color_continuous_scale="Purples", title="Top Locations")
                    fig_loc.update_layout(**plot_layout(
                        xaxis=dict(tickangle=-30, **XAXIS_DEFAULT),
                        yaxis=YAXIS_DEFAULT,
                    ))
                    st.plotly_chart(fig_loc, use_container_width=True)
                if title_col:
                    st.markdown('<p class="section">Most Common Job Title Keywords</p>', unsafe_allow_html=True)
                    stopwords = {"and","or","for","the","of","in","a","an","to","with","at","by",
                                 "on","is","as","be","are","from","&","its","–","-",""}
                    words = (df[title_col].dropna()
                             .str.lower()
                             .str.replace(r"[^\w\s]","",regex=True)
                             .str.split().explode())
                    word_freq = words[~words.isin(stopwords)].value_counts().head(25).reset_index()
                    word_freq.columns = ["keyword","count"]
                    fig_kw = px.bar(word_freq, x="count", y="keyword", orientation="h",
                                    color="count", color_continuous_scale="Viridis", title="Top Keywords in Job Titles")
                    fig_kw.update_layout(**plot_layout(
                        yaxis=dict(autorange="reversed", **YAXIS_DEFAULT),
                        xaxis=XAXIS_DEFAULT,
                    ))
                    st.plotly_chart(fig_kw, use_container_width=True)
                if company_col and source_col:
                    st.markdown('<p class="section">Company vs Source</p>', unsafe_allow_html=True)
                    top_companies = df[company_col].value_counts().head(15).index
                    cross = df[df[company_col].isin(top_companies)].groupby([source_col, company_col]).size().reset_index(name="count")
                    if not cross.empty:
                        fig_cross = px.bar(cross, x=company_col, y="count", color=source_col,
                                           barmode="stack", color_discrete_sequence=px.colors.qualitative.Bold,
                                           title="Top Companies by Source")
                        fig_cross.update_layout(**plot_layout(
                            xaxis=dict(tickangle=-30, **XAXIS_DEFAULT),
                            yaxis=YAXIS_DEFAULT,
                        ))
                        st.plotly_chart(fig_cross, use_container_width=True)
                if dead_col:
                    st.markdown('<p class="section">Deadline Timeline</p>', unsafe_allow_html=True)
                    dead_df = df[[dead_col]].copy()
                    dead_df[dead_col] = pd.to_datetime(dead_df[dead_col], errors="coerce")
                    dead_df = dead_df.dropna()
                    if not dead_df.empty:
                        dead_df["month"] = dead_df[dead_col].dt.to_period("M").astype(str)
                        timeline = dead_df["month"].value_counts().sort_index().reset_index()
                        timeline.columns = ["month","count"]
                        fig_tl = px.area(timeline, x="month", y="count",
                                         color_discrete_sequence=["#7c8bff"], title="Jobs Expiring by Month")
                        fig_tl.update_layout(**plot_layout(xaxis=XAXIS_DEFAULT, yaxis=YAXIS_DEFAULT))
                        st.plotly_chart(fig_tl, use_container_width=True)
                st.markdown('<p class="section">Search & Browse Listings</p>', unsafe_allow_html=True)
                search   = st.text_input("🔍 Search by keyword")
                filtered = (df[df[title_col].str.contains(search, case=False, na=False)]
                            if (search and title_col) else df)
                st.caption(f"{len(filtered):,} listings")
                st.dataframe(filtered, use_container_width=True, height=400)

# ══════════════════════════════════════════════════════
# 🧠 SKILLS & JOBS
# ══════════════════════════════════════════════════════
elif page == "🧠 Skills & Jobs":
    st.markdown("""
    <div class="page-header">
        <h1>🧠 Skills & Job Insights</h1>
        <p>Explore skill and candidate data from your database</p>
    </div>""", unsafe_allow_html=True)
    tables     = query("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name")
    table_list = tables["table_name"].tolist() if not tables.empty else []
    skill_tables = [t for t in table_list if any(k in t.lower()
                    for k in ["skill","recommend","candidate","match","score","profile"])]
    if not table_list:
        st.info("No tables found.")
    else:
        chosen = st.selectbox("Select table", skill_tables if skill_tables else table_list)
        if chosen:
            with st.spinner("Loading data…"):
                df = query_raw(f'SELECT * FROM "{chosen}" LIMIT 1000')
            if df.empty:
                st.warning("Table is empty.")
            else:
                st.markdown(f'<div class="info-banner">📦 <strong>{len(df):,}</strong> rows in <code>{chosen}</code></div>', unsafe_allow_html=True)
                cat_cols = df.select_dtypes(include="object").columns.tolist()
                num_cols = df.select_dtypes(include="number").columns.tolist()
                if cat_cols:
                    st.markdown('<p class="section">Categorical Analysis</p>', unsafe_allow_html=True)
                    col1,col2 = st.columns([2,1])
                    chosen_cat = col1.selectbox("Group by column", cat_cols)
                    chart_type = col2.radio("Chart type", ["Bar","Pie","Treemap"], horizontal=True)
                    count_df   = df[chosen_cat].value_counts().reset_index()
                    count_df.columns = [chosen_cat,"count"]
                    if chart_type == "Bar":
                        fig = px.bar(count_df.head(20), x=chosen_cat, y="count", color="count", color_continuous_scale="Blues")
                    elif chart_type == "Pie":
                        fig = px.pie(count_df.head(15), names=chosen_cat, values="count", hole=0.45)
                    else:
                        fig = px.treemap(count_df.head(30), path=[chosen_cat], values="count", color="count", color_continuous_scale="Blues")
                    fig.update_layout(**plot_layout(xaxis=XAXIS_DEFAULT, yaxis=YAXIS_DEFAULT))
                    st.plotly_chart(fig, use_container_width=True)
                if num_cols:
                    st.markdown('<p class="section">Numeric Distribution</p>', unsafe_allow_html=True)
                    c1,c2 = st.columns(2)
                    chosen_num  = c1.selectbox("Select column", num_cols)
                    chart_type2 = c2.radio("Chart", ["Histogram","Box","Violin"], horizontal=True)
                    if chart_type2 == "Histogram":
                        fig_n = px.histogram(df, x=chosen_num, nbins=30, color_discrete_sequence=["#7c8bff"])
                    elif chart_type2 == "Box":
                        fig_n = px.box(df, y=chosen_num, color_discrete_sequence=["#7c8bff"])
                    else:
                        fig_n = px.violin(df, y=chosen_num, color_discrete_sequence=["#7c8bff"], box=True)
                    fig_n.update_layout(**plot_layout(xaxis=XAXIS_DEFAULT, yaxis=YAXIS_DEFAULT))
                    st.plotly_chart(fig_n, use_container_width=True)
                    if len(num_cols) >= 2:
                        st.markdown('<p class="section">Correlation Heatmap</p>', unsafe_allow_html=True)
                        corr = df[num_cols].corr()
                        fig_corr = px.imshow(corr, color_continuous_scale="Blues", text_auto=True, aspect="auto")
                        fig_corr.update_layout(**plot_layout())
                        st.plotly_chart(fig_corr, use_container_width=True)
                st.markdown('<p class="section">Raw Data</p>', unsafe_allow_html=True)
                search_raw = st.text_input("🔍 Filter rows")
                if search_raw:
                    mask = df.apply(lambda r: r.astype(str).str.contains(search_raw, case=False).any(), axis=1)
                    st.dataframe(df[mask], use_container_width=True, height=350)
                else:
                    st.dataframe(df, use_container_width=True, height=350)

# ══════════════════════════════════════════════════════
# 🤖 AI RECOMMENDER
# ══════════════════════════════════════════════════════
elif page == "🤖 AI Recommender":
    st.markdown("""
    <div class="page-header">
        <h1>🤖 AI Job & Salary Recommender</h1>
        <p>Enter your skills → get AI predictions + live job matches</p>
    </div>""", unsafe_allow_html=True)
    if not AI_AVAILABLE:
        st.warning(f"⚠️ AI model unavailable — {'Error: ' + _load_error if _load_error else 'model file not found.'} Job search still works below!")
    all_tables = query("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name")
    table_list = all_tables["table_name"].tolist() if not all_tables.empty else []
    job_tables = [t for t in table_list if any(k in t.lower() for k in JOB_TABLE_KEYWORDS)]
    cfg_col, main_col = st.columns([1,3])
    with cfg_col:
        st.markdown('<p class="section">⚙️ Settings</p>', unsafe_allow_html=True)
        selected_table  = st.selectbox("Job table", job_tables if job_tables else table_list, key="ai_table")
        real_cols       = ai_get_columns(selected_table) if selected_table else []
        real_cols_lower = [c.lower() for c in real_cols]
        if real_cols_lower:
            st.caption("**Columns:** " + " · ".join(real_cols_lower))
            title_override = st.selectbox("Job title column", ["(auto-detect)"] + real_cols_lower, key="ai_title_col")
        else:
            title_override = "(auto-detect)"
        max_matches = st.slider("Max results", 5, 30, 10, key="ai_max")
        min_score   = st.slider("Min match score %", 0, 80, 0, key="ai_min_score")
    with main_col:
        st.subheader("Enter Your Skills")
        skills_input = st.text_area(
            "Skills", placeholder="e.g. Python, SQL, Data Analysis, Machine Learning, Communication",
            height=120, label_visibility="collapsed", key="ai_skills",
        )
        col_btn1, col_btn2 = st.columns([2,1])
        run_predict = col_btn1.button("🚀 Predict & Find Matching Jobs", type="primary", key="ai_predict", use_container_width=True)
        show_debug  = col_btn2.checkbox("Show debug info", key="ai_debug")
    if run_predict:
        if not skills_input.strip():
            st.warning("Please enter your skills first!")
        else:
            skills      = [s.strip() for s in skills_input.split(",") if s.strip()]
            skills_text = " ".join(skills)
            if AI_AVAILABLE:
                try:
                    predicted_job = AI_PIPELINE.predict([skills_text])[0]
                    job_encoded   = AI_LE.transform([predicted_job])[0]
                    base_salary   = float(AI_SALARY.predict([[job_encoded]])[0])
                    base_salary   = max(300.0, min(3000.0, base_salary))
                    salary_min    = max(300.0, base_salary - 300)
                    salary_max    = min(3000.0, base_salary + 500)
                    level         = "High" if base_salary >= 2000 else "Medium" if base_salary >= 1000 else "Low"
                    level_icon    = "🟢" if level == "High" else "🟡" if level == "Medium" else "🔴"
                    st.success("✅ AI prediction complete!")
                    r1,r2,r3 = st.columns(3)
                    r1.markdown(f'<div class="ai-result-card"><div class="ai-result-label">🧑‍💼 Predicted Job Role</div><div class="ai-result-value">{predicted_job}</div></div>', unsafe_allow_html=True)
                    r2.markdown(f'<div class="ai-result-card"><div class="ai-result-label">💰 Salary Range (USD/mo)</div><div class="ai-result-value">${round(salary_min):,} – ${round(salary_max):,}</div></div>', unsafe_allow_html=True)
                    r3.markdown(f'<div class="ai-result-card"><div class="ai-result-label">📊 Salary Level</div><div class="ai-result-value">{level_icon} {level}</div></div>', unsafe_allow_html=True)
                    st.markdown(f'<div class="info-banner">📌 <strong>Skills entered:</strong> {", ".join(skills)}</div>', unsafe_allow_html=True)
                except Exception as e:
                    predicted_job = skills_text
                    st.error(f"AI prediction failed: {e}")
            else:
                predicted_job = skills_text
                st.info(f"🔍 Searching for: {', '.join(skills)}")
            st.markdown("---")
            st.subheader("🔍 Matching Live Job Listings")
            if not selected_table:
                st.info("No job tables found.")
            else:
                title_col = title_override if title_override != "(auto-detect)" else \
                            ai_fuzzy_find(real_cols_lower, ["title","position","role","name","job","post","vacancy"])
                if show_debug:
                    with st.expander("🛠️ Debug Info"):
                        kws = list({k.lower() for k in predicted_job.split() +
                                    [s.strip() for s in skills_input.split(",")] if len(k) > 2})
                        st.write(f"**Table:** `{selected_table}`")
                        st.write(f"**Columns:** `{real_cols_lower}`")
                        st.write(f"**Title col:** `{title_col}`")
                        st.write(f"**Keywords:** `{kws}`")
                with st.spinner("🔍 Searching job listings …"):
                    df = ai_search_jobs(predicted_job, skills_input, selected_table, title_col or "", max_matches * 3)
                if df.empty:
                    st.warning("Table is empty — run your scrapers to populate it.")
                else:
                    df.columns = [c.lower().strip() for c in df.columns]
                    cols   = df.columns.tolist()
                    t_col  = ai_fuzzy_find(cols, ["title","position","role","job_name","name","post","vacancy"])
                    co_col = ai_fuzzy_find(cols, ["company","employer","organization","org","firm","institute"])
                    lo_col = ai_fuzzy_find(cols, ["location","city","province","region","place","area","district"])
                    sc_col = ai_fuzzy_find(cols, ["source","website","portal","platform","site","origin"])
                    dl_col = ai_fuzzy_find(cols, ["deadline","closing","expiry","expire","due","end_date"])
                    ur_col = ai_fuzzy_find(cols, ["url","link","href","apply"])
                    if t_col is None:
                        str_cols = df.select_dtypes(include="object").columns.tolist()
                        t_col    = str_cols[0] if str_cols else cols[0]
                    df["_score"] = df.apply(lambda r: ai_score(r, predicted_job, skills_input), axis=1)
                    df = df[df["_score"] >= min_score].sort_values("_score", ascending=False).head(max_matches)
                    if df.empty:
                        st.warning(f"No jobs found with match score ≥ {min_score}%. Try lowering the minimum score.")
                    else:
                        st.markdown(f'<p class="section">{len(df)} jobs found — ranked by relevance</p>', unsafe_allow_html=True)
                        for _, row in df.iterrows():
                            render_job_card(
                                title   = ai_safe_val(row, t_col)  or "No title",
                                company = ai_safe_val(row, co_col),
                                loc     = ai_safe_val(row, lo_col),
                                source  = ai_safe_val(row, sc_col),
                                dead    = ai_safe_val(row, dl_col),
                                url     = ai_safe_val(row, ur_col),
                                score   = int(row.get("_score", 0)),
                            )
                        with st.expander("📋 Raw Matched Data"):
                            show_cols = [c for c in [t_col, co_col, lo_col, sc_col, dl_col] if c]
                            st.dataframe(
                                df[show_cols + ["_score"]].rename(columns={"_score":"match %"})
                                if show_cols else df.drop(columns=["_score"]),
                                use_container_width=True, height=300,
                            )
                        st.markdown('<p class="section">📊 Match Statistics</p>', unsafe_allow_html=True)
                        m1,m2,m3,m4 = st.columns(4)
                        m1.metric("Matches Found",    len(df))
                        m2.metric("Avg Match Score",  f"{df['_score'].mean():.0f}%")
                        m3.metric("Top Score",        f"{df['_score'].max():.0f}%")
                        m4.metric("Min Score Filter", f"{min_score}%")
                        fig_score = px.histogram(df, x="_score", nbins=10, color_discrete_sequence=["#7c8bff"],
                                                 title="Match Score Distribution", labels={"_score":"Match Score (%)"})
                        fig_score.update_layout(**plot_layout(xaxis=XAXIS_DEFAULT, yaxis=YAXIS_DEFAULT))
                        st.plotly_chart(fig_score, use_container_width=True)

# ══════════════════════════════════════════════════════
# 📋 DAG RUNS
# ══════════════════════════════════════════════════════
elif page == "📋 DAG Runs":
    st.markdown("""
    <div class="page-header">
        <h1>📋 DAG Runs</h1>
        <p>View and filter all Airflow DAG run history</p>
    </div>""", unsafe_allow_html=True)
    col1,col2,col3 = st.columns(3)
    dag   = col1.text_input("Filter DAG ID")
    state = col2.selectbox("State", ["All","success","failed","running","queued","up_for_retry"])
    limit = col3.select_slider("Max rows", options=[50,100,200,500], value=200)
    where = []
    if dag:            where.append(f"dag_id ILIKE '%{dag}%'")
    if state != "All": where.append(f"state='{state}'")
    w  = ("WHERE " + " AND ".join(where)) if where else ""
    df = query(f"""
        SELECT dag_id, run_id, state, execution_date, start_date, end_date, run_type
        FROM dag_run {w} ORDER BY execution_date DESC LIMIT {limit}
    """)
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Showing",      len(df))
    c2.metric("Unique DAGs",  df["dag_id"].nunique() if not df.empty else 0)
    if not df.empty and "state" in df.columns:
        c3.metric("Success Rate", f"{(df['state']=='success').mean()*100:.1f}%")
        c4.metric("Failed",       int((df['state']=='failed').sum()))
    st.markdown(f'<p class="section">{len(df)} runs</p>', unsafe_allow_html=True)
    if not df.empty:
        st.dataframe(style_state_col(df.style), use_container_width=True, height=500)
        st.markdown('<p class="section">State Breakdown</p>', unsafe_allow_html=True)
        state_counts = df["state"].value_counts().reset_index()
        state_counts.columns = ["state","count"]
        fig_state = px.bar(state_counts, x="state", y="count", color="state", color_discrete_map=COLOR)
        fig_state.update_layout(**plot_layout(xaxis=XAXIS_DEFAULT, yaxis=YAXIS_DEFAULT))
        st.plotly_chart(fig_state, use_container_width=True)

# ══════════════════════════════════════════════════════
# ⚙️ TASKS
# ══════════════════════════════════════════════════════
elif page == "⚙️ Tasks":
    st.markdown("""
    <div class="page-header">
        <h1>⚙️ Task Instances</h1>
        <p>Detailed view of all Airflow task executions</p>
    </div>""", unsafe_allow_html=True)
    col1,col2 = st.columns(2)
    dag   = col1.text_input("Filter DAG ID")
    state = col2.selectbox("State", ["All","success","failed","running","queued","up_for_retry","skipped"])
    where = []
    if dag:            where.append(f"dag_id ILIKE '%{dag}%'")
    if state != "All": where.append(f"state='{state}'")
    w  = ("WHERE " + " AND ".join(where)) if where else ""
    df = query(f"""
        SELECT dag_id, task_id, state,
               ROUND(EXTRACT(EPOCH FROM (end_date - start_date))::numeric, 2) AS duration_sec,
               start_date, end_date
        FROM task_instance {w} ORDER BY start_date DESC LIMIT 300
    """)
    if not df.empty:
        c1,c2,c3,c4 = st.columns(4)
        c1.metric("Total Tasks",  len(df))
        c2.metric("Success Rate", f"{(df['state']=='success').mean()*100:.1f}%")
        dur_clean = df["duration_sec"].dropna()
        c3.metric("Avg Duration", f"{dur_clean.mean():.1f}s" if not dur_clean.empty else "—")
        c4.metric("Max Duration", f"{dur_clean.max():.1f}s"  if not dur_clean.empty else "—")
        col1,col2 = st.columns(2)
        if not dur_clean.empty:
            fig1 = px.histogram(df.dropna(subset=["duration_sec"]), x="duration_sec",
                                color="state", color_discrete_map=COLOR, nbins=30,
                                title="Task Duration Distribution (sec)")
            fig1.update_layout(**plot_layout(xaxis=XAXIS_DEFAULT, yaxis=YAXIS_DEFAULT))
            col1.plotly_chart(fig1, use_container_width=True)
        task_state = df.groupby(["task_id","state"]).size().reset_index(name="count")
        fig2 = px.bar(task_state, x="task_id", y="count", color="state",
                      color_discrete_map=COLOR, barmode="stack", title="Task State Breakdown")
        fig2.update_layout(**plot_layout(
            xaxis=dict(tickangle=-30, **XAXIS_DEFAULT),
            yaxis=YAXIS_DEFAULT,
        ))
        col2.plotly_chart(fig2, use_container_width=True)
        st.markdown('<p class="section">Task Table</p>', unsafe_allow_html=True)
        st.dataframe(df, use_container_width=True, height=350)
    else:
        st.info("No task instances found.")

# ══════════════════════════════════════════════════════
# 📈 PERFORMANCE
# ══════════════════════════════════════════════════════
elif page == "📈 Performance":
    st.markdown("""
    <div class="page-header">
        <h1>📈 Pipeline Performance</h1>
        <p>Duration analytics and success rates for all DAGs</p>
    </div>""", unsafe_allow_html=True)
    st.markdown('<p class="section">Avg Task Duration by DAG (seconds)</p>', unsafe_allow_html=True)
    dur = query("""
        SELECT dag_id,
               ROUND(AVG(EXTRACT(EPOCH FROM (end_date-start_date)))::numeric,2) avg_sec,
               ROUND(MAX(EXTRACT(EPOCH FROM (end_date-start_date)))::numeric,2) max_sec,
               ROUND(MIN(EXTRACT(EPOCH FROM (end_date-start_date)))::numeric,2) min_sec,
               COUNT(*) task_count
        FROM task_instance WHERE state='success' AND end_date IS NOT NULL
        GROUP BY dag_id ORDER BY avg_sec DESC LIMIT 15
    """)
    if not dur.empty:
        fig = px.bar(dur, x="dag_id", y=["avg_sec","max_sec"],
                     color_discrete_map={"avg_sec":"#7c8bff","max_sec":"#f87171"},
                     barmode="group", title="Average vs Max Duration")
        fig.update_layout(**plot_layout(
            xaxis=dict(tickangle=-20, **XAXIS_DEFAULT),
            yaxis=YAXIS_DEFAULT,
        ))
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(dur, use_container_width=True)
    st.markdown('<p class="section">Daily Success Rate (last 30 days)</p>', unsafe_allow_html=True)
    rate = query("""
        SELECT DATE(execution_date) d,
               ROUND(100.0 * SUM(CASE WHEN state='success' THEN 1 ELSE 0 END) / COUNT(*), 1) rate,
               COUNT(*) total_runs
        FROM dag_run WHERE execution_date >= NOW() - INTERVAL '30 days'
        GROUP BY d ORDER BY d
    """)
    if not rate.empty:
        col1,col2 = st.columns(2)
        fig2 = px.line(rate, x="d", y="rate", markers=True,
                       color_discrete_sequence=["#22d3a5"], title="Daily Success Rate (%)")
        fig2.update_layout(**plot_layout(xaxis=XAXIS_DEFAULT, yaxis=YAXIS_DEFAULT))
        fig2.add_hline(y=90, line_dash="dot", line_color="#f87171", annotation_text="90% target")
        col1.plotly_chart(fig2, use_container_width=True)
        fig3 = px.bar(rate, x="d", y="total_runs", color_discrete_sequence=["#7c8bff"], title="Daily Run Volume")
        fig3.update_layout(**plot_layout(xaxis=XAXIS_DEFAULT, yaxis=YAXIS_DEFAULT))
        col2.plotly_chart(fig3, use_container_width=True)
    st.markdown('<p class="section">Slowest Tasks</p>', unsafe_allow_html=True)
    slow = query("""
        SELECT dag_id, task_id,
               ROUND(AVG(EXTRACT(EPOCH FROM (end_date-start_date)))::numeric,2) avg_sec
        FROM task_instance WHERE state='success' AND end_date IS NOT NULL
        GROUP BY dag_id, task_id ORDER BY avg_sec DESC LIMIT 10
    """)
    if not slow.empty:
        fig4 = px.bar(slow, x="avg_sec", y="task_id", orientation="h",
                      color="avg_sec", color_continuous_scale="Reds",
                      title="Top 10 Slowest Tasks (avg sec)")
        fig4.update_layout(**plot_layout(
            yaxis=dict(autorange="reversed", **YAXIS_DEFAULT),
            xaxis=XAXIS_DEFAULT,
        ))
        st.plotly_chart(fig4, use_container_width=True)

# ══════════════════════════════════════════════════════
# 📡 KAFKA
# ══════════════════════════════════════════════════════
elif page == "📡 Kafka":
    st.markdown("""
    <div class="page-header">
        <h1>📡 Kafka Monitor</h1>
        <p>Real-time Kafka broker and topic monitoring</p>
    </div>""", unsafe_allow_html=True)
    KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka:29092")
    KAFKA_UI   = _clean_url(os.getenv("KAFKA_UI_URL", "http://localhost:8090"))
    st.markdown(f'<div class="info-banner">🔗 Kafka UI: <a href="{KAFKA_UI}" target="_blank" style="color:#7c8bff">{KAFKA_UI}</a></div>', unsafe_allow_html=True)
    try:
        from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
        from kafka.errors import NoBrokersAvailable
        try:
            admin  = KafkaAdminClient(bootstrap_servers=KAFKA_HOST,
                                      client_id="streamlit-monitor", request_timeout_ms=5000)
            topics = admin.list_topics()
            admin.close()
            c1,c2,c3 = st.columns(3)
            c1.metric("🟢 Status", "Online")
            c2.metric("📦 Topics", len(topics))
            c3.metric("🖥️ Broker", KAFKA_HOST)
            st.markdown('<p class="section">Topics</p>', unsafe_allow_html=True)
            rows = []
            for topic in sorted(topics):
                try:
                    consumer   = KafkaConsumer(topic, bootstrap_servers=KAFKA_HOST,
                                               auto_offset_reset="earliest",
                                               consumer_timeout_ms=500, enable_auto_commit=False)
                    partitions = consumer.partitions_for_topic(topic) or set()
                    tp_list    = [TopicPartition(topic, p) for p in partitions]
                    # FIX: wrap beginning_offsets in try/except — it can hang on older brokers
                    try:
                        end_off = consumer.end_offsets(tp_list)
                        beg_off = consumer.beginning_offsets(tp_list)
                        total   = sum(end_off[tp]-beg_off[tp] for tp in tp_list)
                    except Exception:
                        total = "?"
                    consumer.close()
                    rows.append({"topic":topic,"partitions":len(partitions),"messages":total})
                except Exception:
                    rows.append({"topic":topic,"partitions":"?","messages":"?"})
            if rows:
                tdf = pd.DataFrame(rows)
                st.dataframe(tdf, use_container_width=True)
                numeric_tdf = tdf[pd.to_numeric(tdf["messages"], errors="coerce").notna()].copy()
                numeric_tdf["messages"] = numeric_tdf["messages"].astype(int)
                if not numeric_tdf.empty:
                    fig_k = px.bar(numeric_tdf, x="topic", y="messages",
                                   color="messages", color_continuous_scale="Teal", title="Messages per Topic")
                    fig_k.update_layout(**plot_layout(xaxis=XAXIS_DEFAULT, yaxis=YAXIS_DEFAULT))
                    st.plotly_chart(fig_k, use_container_width=True)
            st.markdown('<p class="section">Preview Latest Messages</p>', unsafe_allow_html=True)
            if topics:
                sel_topic = st.selectbox("Select topic", sorted(topics))
                max_msgs  = st.slider("Max messages", 5, 100, 20)
                if st.button("📥 Fetch Messages"):
                    with st.spinner("Fetching messages…"):
                        try:
                            consumer = KafkaConsumer(
                                sel_topic, bootstrap_servers=KAFKA_HOST,
                                auto_offset_reset="latest", consumer_timeout_ms=3000,
                                enable_auto_commit=False,
                                value_deserializer=lambda v: v.decode("utf-8", errors="replace"),
                            )
                            msgs = []
                            for msg in consumer:
                                msgs.append({
                                    "partition": msg.partition,
                                    "offset":    msg.offset,
                                    "timestamp": datetime.fromtimestamp(msg.timestamp/1000).strftime("%H:%M:%S"),
                                    "value":     str(msg.value)[:300],
                                })
                                if len(msgs) >= max_msgs:
                                    break
                            consumer.close()
                            if msgs:
                                st.dataframe(pd.DataFrame(msgs), use_container_width=True, height=300)
                            else:
                                st.info("No recent messages in this topic.")
                        except Exception as e:
                            st.error(f"Failed to fetch messages: {e}")
        except NoBrokersAvailable:
            st.error(f"❌ Kafka broker not reachable at `{KAFKA_HOST}`")
            st.info("Make sure your Docker containers are running.")
        except Exception as e:
            st.error(f"❌ Kafka error: {e}")
    except ImportError:
        st.error("kafka-python not installed in this container.")
        st.code("pip install kafka-python")

# ══════════════════════════════════════════════════════
# ✈️ AIRFLOW
# ══════════════════════════════════════════════════════
elif page == "✈️ Airflow":
    st.markdown("""
    <div class="page-header">
        <h1>✈️ Airflow Monitor</h1>
        <p>Live DAG stats pulled straight from the Airflow metadata database</p>
    </div>""", unsafe_allow_html=True)
    st.markdown(
        f'<div class="info-banner">🔗 Airflow UI: '
        f'<a href="{AIRFLOW_URL}" target="_blank" style="color:#7c8bff">{AIRFLOW_URL}</a>'
        f' &nbsp;|&nbsp; user: <code>admin</code> &nbsp;|&nbsp; pass: <code>admin</code></div>',
        unsafe_allow_html=True,
    )
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("Total DAGs",      safe_metric("SELECT COUNT(DISTINCT dag_id) n FROM dag_run"))
    c2.metric("Total Runs",      safe_metric("SELECT COUNT(*) n FROM dag_run"))
    c3.metric("✅ Success",      safe_metric("SELECT COUNT(*) n FROM dag_run WHERE state='success'"))
    c4.metric("❌ Failed",       safe_metric("SELECT COUNT(*) n FROM dag_run WHERE state='failed'"))
    c5.metric("⏳ Running Now",  safe_metric("SELECT COUNT(*) n FROM dag_run WHERE state='running'"))
    st.markdown('<p class="section">Latest 20 Runs</p>', unsafe_allow_html=True)
    latest = query("""
        SELECT dag_id, run_id, state, run_type,
               execution_date, start_date, end_date
        FROM dag_run ORDER BY execution_date DESC LIMIT 20
    """)
    if not latest.empty:
        st.dataframe(style_state_col(latest.style), use_container_width=True, height=350)
    else:
        st.info("No DAG runs found yet.")
    st.markdown('<p class="section">Per-DAG Summary</p>', unsafe_allow_html=True)
    dag_summary = query("""
        SELECT dag_id,
               COUNT(*) total_runs,
               SUM(CASE WHEN state='success' THEN 1 ELSE 0 END) success,
               SUM(CASE WHEN state='failed'  THEN 1 ELSE 0 END) failed,
               SUM(CASE WHEN state='running' THEN 1 ELSE 0 END) running,
               MAX(execution_date) last_run
        FROM dag_run
        GROUP BY dag_id ORDER BY total_runs DESC
    """)
    if not dag_summary.empty:
        col1, col2 = st.columns([3,2])
        fig_dag = px.bar(dag_summary.head(15), x="dag_id",
                         y=["success","failed","running"],
                         color_discrete_map={"success":"#22d3a5","failed":"#f87171","running":"#fbbf24"},
                         barmode="stack", title="Run States per DAG")
        fig_dag.update_layout(**plot_layout(
            xaxis=dict(tickangle=-25, **XAXIS_DEFAULT),
            yaxis=YAXIS_DEFAULT,
        ))
        col1.plotly_chart(fig_dag, use_container_width=True)
        fig_pie = px.pie(dag_summary, names="dag_id", values="total_runs", hole=0.5,
                         title="Share of Runs by DAG",
                         color_discrete_sequence=px.colors.qualitative.Bold)
        fig_pie.update_traces(marker=dict(line=dict(color="#0a0f1e", width=2)))
        fig_pie.update_layout(**plot_layout())
        col2.plotly_chart(fig_pie, use_container_width=True)
        st.dataframe(dag_summary, use_container_width=True, height=300)
    st.markdown('<p class="section">Task Success Rates</p>', unsafe_allow_html=True)
    task_rates = query("""
        SELECT dag_id, task_id,
               COUNT(*) total,
               ROUND(100.0 * SUM(CASE WHEN state='success' THEN 1 ELSE 0 END) / COUNT(*), 1) success_pct,
               ROUND(AVG(EXTRACT(EPOCH FROM (end_date - start_date)))::numeric, 1) avg_sec
        FROM task_instance
        WHERE end_date IS NOT NULL
        GROUP BY dag_id, task_id
        ORDER BY success_pct ASC
        LIMIT 20
    """)
    if not task_rates.empty:
        fig_tr = px.bar(task_rates, x="task_id", y="success_pct",
                        color="success_pct", color_continuous_scale="RdYlGn",
                        range_color=[0,100], title="Task Success % (lowest first)",
                        hover_data=["dag_id","total","avg_sec"])
        fig_tr.update_layout(**plot_layout(
            xaxis=dict(tickangle=-30, **XAXIS_DEFAULT),
            yaxis=YAXIS_DEFAULT,
        ))
        fig_tr.add_hline(y=90, line_dash="dot", line_color="#f87171", annotation_text="90% target")
        st.plotly_chart(fig_tr, use_container_width=True)
    st.markdown('<p class="section">30-Day Run Trend</p>', unsafe_allow_html=True)
    trend = query("""
        SELECT DATE(execution_date) d, state, COUNT(*) cnt
        FROM dag_run
        WHERE execution_date >= NOW() - INTERVAL '30 days'
        GROUP BY d, state ORDER BY d
    """)
    if not trend.empty:
        fig_trend = px.area(trend, x="d", y="cnt", color="state",
                            color_discrete_map=COLOR, title="Daily Run Volume (30d)")
        fig_trend.update_layout(**plot_layout(xaxis=XAXIS_DEFAULT, yaxis=YAXIS_DEFAULT))
        st.plotly_chart(fig_trend, use_container_width=True)
    running_df = query("SELECT dag_id, run_id, start_date FROM dag_run WHERE state='running' ORDER BY start_date DESC")
    if not running_df.empty:
        st.markdown('<p class="section">⏳ Currently Running DAGs</p>', unsafe_allow_html=True)
        st.dataframe(running_df, use_container_width=True)

# ══════════════════════════════════════════════════════
# 🔗 PGADMIN
# ══════════════════════════════════════════════════════
elif page == "🔗 pgAdmin":
    st.markdown("""
    <div class="page-header">
        <h1>🔗 pgAdmin & Database Explorer</h1>
        <p>Full table browser, raw data viewer, and job title similarity visualiser</p>
    </div>""", unsafe_allow_html=True)
    st.markdown(
        f'<div class="info-banner">🔗 pgAdmin: <a href="{PGADMIN_URL}" target="_blank" style="color:#7c8bff">{PGADMIN_URL}</a>'
        f' &nbsp;|&nbsp; Email: <code>admin@admin.com</code>'
        f' &nbsp;|&nbsp; Password: <code>admin</code></div>',
        unsafe_allow_html=True,
    )
    c1,c2,c3 = st.columns(3)
    c1.metric("Tables",       safe_metric("SELECT COUNT(*) n FROM information_schema.tables WHERE table_schema='public'"))
    c2.metric("DB Size",      safe_metric(f"SELECT pg_size_pretty(pg_database_size('{os.getenv('DB_NAME','airflow')}')) size", col="size"))
    c3.metric("Active Conns", safe_metric("SELECT COUNT(*) n FROM pg_stat_activity WHERE state='active'"))
    st.markdown('<p class="section">All Tables</p>', unsafe_allow_html=True)
    tbl = query("""
        SELECT t.table_name,
               pg_size_pretty(pg_total_relation_size(quote_ident(t.table_name))) AS size,
               (SELECT reltuples::bigint FROM pg_class WHERE relname = t.table_name) AS approx_rows,
               (SELECT COUNT(*) FROM information_schema.columns c
                WHERE c.table_name = t.table_name AND c.table_schema='public') AS columns
        FROM information_schema.tables t
        WHERE t.table_schema='public' ORDER BY t.table_name
    """)
    if not tbl.empty:
        search_tbl   = st.text_input("🔍 Filter tables")
        filtered_tbl = tbl[tbl["table_name"].str.contains(search_tbl, case=False)] if search_tbl else tbl
        st.dataframe(filtered_tbl, use_container_width=True, height=320)
    all_tables = tbl["table_name"].tolist() if not tbl.empty else []

    # ── Raw data preview — FIX: use query_raw + spinner + safe column handling ──
    st.markdown('<p class="section">Raw Table Data</p>', unsafe_allow_html=True)
    if all_tables:
        col_tbl, col_lim, col_off = st.columns([3,1,1])
        sel_tbl    = col_tbl.selectbox("Table", all_tables, key="pg_sel_tbl")
        row_limit  = col_lim.number_input("Rows", min_value=5, max_value=500, value=50, step=25, key="pg_row_limit")
        row_offset = col_off.number_input("Offset", min_value=0, value=0, step=50, key="pg_row_offset")
        if sel_tbl:
            with st.spinner(f"Loading data from `{sel_tbl}`…"):
                preview = query_raw(f'SELECT * FROM "{sel_tbl}" LIMIT {row_limit} OFFSET {row_offset}')
            total_rows_df = query(f'SELECT COUNT(*) n FROM "{sel_tbl}"')
            total_rows = int(total_rows_df["n"].iloc[0]) if not total_rows_df.empty else "?"
            st.caption(f"Showing {len(preview)} rows (offset {row_offset}) · ~{total_rows:,} total rows · {len(preview.columns) if not preview.empty else 0} columns")
            if not preview.empty:
                all_cols = preview.columns.tolist()
                sel_cols = st.multiselect("Show columns", all_cols, default=all_cols, key="pg_cols")
                show_df  = preview[sel_cols] if sel_cols else preview
                search_raw_input = st.text_input("🔍 Search within results", key="pg_search_raw")
                if search_raw_input:
                    mask = show_df.apply(lambda r: r.astype(str).str.contains(search_raw_input, case=False).any(), axis=1)
                    show_df = show_df[mask]
                    st.caption(f"{len(show_df)} rows match '{search_raw_input}'")
                # FIX: reset index so dataframe renders cleanly
                st.dataframe(show_df.reset_index(drop=True), use_container_width=True, height=420)
                with st.expander("📐 Column types & null counts"):
                    type_info = pd.DataFrame({
                        "column":   preview.columns,
                        "dtype":    [str(d) for d in preview.dtypes],
                        "non_null": preview.count().values,
                        "null_pct": [f"{preview[c].isna().mean()*100:.1f}%" for c in preview.columns],
                        "unique":   [preview[c].nunique() for c in preview.columns],
                    })
                    st.dataframe(type_info, use_container_width=True, hide_index=True)
            else:
                st.info("Table is empty or could not be loaded.")
    else:
        st.info("No tables found in the public schema.")

    # ── Job title similarity visualiser ──────────────────────────────────────
    st.markdown('<p class="section">🔍 Job Title Similarity Explorer</p>', unsafe_allow_html=True)
    st.caption("Pick a job table to see which titles are most similar — keyword overlap bubble chart + bar chart.")
    job_tables_pg = [t for t in all_tables if any(k in t.lower() for k in JOB_TABLE_KEYWORDS)]
    if not job_tables_pg:
        st.info("No job tables detected. Similarity visualiser will appear once your scrapers run.")
    else:
        sim_col1, sim_col2 = st.columns([2,1])
        sim_table = sim_col1.selectbox("Job table for similarity", job_tables_pg, key="pg_sim_table")
        sim_top_n = sim_col2.slider("Top N titles to analyse", 20, 150, 60, key="pg_sim_n")
        if sim_table:
            with st.spinner("Analysing titles…"):
                sim_df = query_raw(f'SELECT * FROM "{sim_table}" LIMIT {sim_top_n * 3}')
            if not sim_df.empty:
                sim_df.columns = [c.lower().strip() for c in sim_df.columns]
                title_col_sim = ai_fuzzy_find(sim_df.columns.tolist(),
                                              ["title","position","role","name","job","post","vacancy"])
                if not title_col_sim:
                    st.warning("Could not detect a title column in this table.")
                else:
                    titles_raw = sim_df[title_col_sim].dropna().astype(str).str.strip()
                    titles_raw = titles_raw[titles_raw.str.len() > 2].unique()[:sim_top_n]
                    stopwords = {"and","or","for","the","of","in","a","an","to","with","at","by",
                                 "on","is","as","be","are","from","&","its","–","-","","officer",
                                 "manager","senior","junior","assistant","specialist","staff"}
                    all_words = []
                    for t in titles_raw:
                        words = re.sub(r"[^\w\s]","",t.lower()).split()
                        all_words.extend([w for w in words if w not in stopwords and len(w) > 2])
                    word_counts = Counter(all_words)
                    top_words   = pd.DataFrame(word_counts.most_common(30), columns=["keyword","count"])
                    top_kw_set  = set(top_words["keyword"])
                    def dominant_kw(title_str):
                        words = re.sub(r"[^\w\s]","",title_str.lower()).split()
                        hits  = [w for w in words if w in top_kw_set and w not in stopwords]
                        if not hits:
                            return "other"
                        return max(hits, key=lambda w: word_counts[w])
                    title_df = pd.DataFrame({"title": titles_raw})
                    title_df["keyword"] = title_df["title"].apply(dominant_kw)
                    title_df["count"]   = title_df["keyword"].map(word_counts).fillna(1)
                    tab1, tab2, tab3 = st.tabs(["🔵 Bubble Chart", "📊 Keyword Bar", "🗂️ Title Groups"])
                    with tab1:
                        bubble_df = (title_df.groupby("keyword")
                                     .agg(titles=("title","count"), weight=("count","mean"))
                                     .reset_index()
                                     .sort_values("titles", ascending=False)
                                     .head(20))
                        bubble_df["x"] = range(len(bubble_df))
                        fig_bubble = px.scatter(
                            bubble_df, x="x", y="titles",
                            size="titles", color="keyword",
                            hover_name="keyword",
                            size_max=60,
                            title="Job title keyword clusters (bubble size = number of titles)",
                            labels={"x":"Keyword rank","titles":"# titles containing keyword"},
                        )
                        fig_bubble.update_layout(**plot_layout(
                            xaxis=dict(showticklabels=False, **XAXIS_DEFAULT),
                            yaxis=YAXIS_DEFAULT,
                        ))
                        st.plotly_chart(fig_bubble, use_container_width=True)
                    with tab2:
                        fig_bar = px.bar(
                            top_words, x="count", y="keyword", orientation="h",
                            color="count", color_continuous_scale="Teal",
                            title="Most common keywords across all job titles",
                        )
                        fig_bar.update_layout(**plot_layout(
                            yaxis=dict(autorange="reversed", **YAXIS_DEFAULT),
                            xaxis=XAXIS_DEFAULT,
                        ))
                        st.plotly_chart(fig_bar, use_container_width=True)
                    with tab3:
                        st.caption("Each group shows titles sharing the same dominant keyword.")
                        groups = title_df.groupby("keyword")["title"].apply(list).reset_index()
                        groups = groups[groups["keyword"] != "other"].sort_values(
                            "keyword", key=lambda s: s.map(word_counts), ascending=False
                        )
                        for _, row in groups.head(12).iterrows():
                            kw     = row["keyword"]
                            titles = row["title"]
                            with st.expander(f"**{kw}** — {len(titles)} titles"):
                                for t in sorted(titles):
                                    st.markdown(f"- {t}")
            else:
                st.warning("Selected table is empty.")

    # ── Quick SQL runner ──────────────────────────────────────────────────────
    st.markdown('<p class="section">⚡ Quick SQL Runner</p>', unsafe_allow_html=True)
    default_sql = "SELECT * FROM information_schema.tables WHERE table_schema='public' LIMIT 10"
    custom_sql  = st.text_area("SQL query (SELECT only)", value=default_sql, height=100, key="pg_sql")
    if st.button("▶ Run Query", key="pg_run_sql"):
        if custom_sql.strip().upper().startswith("SELECT"):
            with st.spinner("Running…"):
                result = query_raw(custom_sql)
            if result.empty:
                st.info("Query returned no rows.")
            else:
                st.caption(f"{len(result)} rows × {len(result.columns)} columns")
                st.dataframe(result.reset_index(drop=True), use_container_width=True, height=350)
        else:
            st.error("Only SELECT statements are allowed for safety.")