import streamlit as st
import pickle
import pathlib
import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# ── Safe model loading ────────────────────────────────────────────────────────
_SEARCH_PATHS = [
    pathlib.Path("/opt/airflow/model_bundle.pkl"),
    pathlib.Path("/opt/airflow/model_bundle_fixed.pkl"),
    pathlib.Path("/opt/airflow/model_bundle_compat.pkl"),
    pathlib.Path("model_bundle.pkl"),
    pathlib.Path("model_bundle_fixed.pkl"),
]

AI_AVAILABLE  = False
job_pipeline  = None
le            = None
salary_model  = None
_load_error   = None

for _path in _SEARCH_PATHS:
    if _path.exists():
        try:
            bundle       = pickle.load(open(_path, "rb"))
            job_pipeline = bundle["job_pipeline"]
            le           = bundle["label_encoder"]
            salary_model = bundle["salary_model"]
            AI_AVAILABLE = True
            break
        except Exception as e:
            _load_error = str(e)
            continue

st.set_page_config(page_title="AI Job & Salary Recommender", page_icon="💼", layout="wide")

st.markdown("""
<style>
    [data-testid="stSidebar"] { background: #0f172a; }
    [data-testid="stSidebar"] * { color: #e2e8f0 !important; }
    .stApp { background: #0f172a; color: #e2e8f0; }
    div[data-testid="metric-container"] {
        background: #1e293b; border: 1px solid #334155;
        border-radius: 10px; padding: 1rem;
    }
    div[data-testid="metric-container"] label { color: #7c8bff !important; font-size: 0.75rem; }
    div[data-testid="stMetricValue"] { color: #fff !important; }
    .section {
        color: #7c8bff; font-size: 0.7rem; letter-spacing: 0.12em;
        text-transform: uppercase; border-bottom: 1px solid #334155;
        padding-bottom: 4px; margin: 1.5rem 0 1rem;
    }
    .result-card {
        background: #1e293b; border: 1px solid #334155;
        border-radius: 12px; padding: 1.2rem 1.5rem;
        margin-bottom: 1rem; text-align: center;
    }
    .result-label { font-size: 0.72rem; color: #7c8bff; letter-spacing: 0.1em;
                    text-transform: uppercase; margin-bottom: 0.4rem; }
    .result-value { font-size: 1.4rem; font-weight: 700; color: #e2e8f0; }
    .job-card {
        background: #1e293b; border: 1px solid #334155;
        border-radius: 12px; padding: 1rem 1.2rem;
        margin-bottom: 0.8rem;
    }
    .job-title  { font-size: 1rem; font-weight: 600; color: #e2e8f0; margin-bottom: 0.3rem; }
    .job-meta   { font-size: 0.8rem; color: #94a3b8; margin-bottom: 0.5rem; }
    .job-badge {
        display: inline-block; background: #0f172a; border: 1px solid #334155;
        border-radius: 6px; padding: 2px 8px; font-size: 0.72rem;
        color: #7c8bff; margin-right: 6px;
    }
    .job-link {
        display: inline-block; margin-top: 0.5rem;
        background: linear-gradient(135deg, #4a4aff, #7c8bff);
        color: white !important; padding: 4px 14px;
        border-radius: 6px; font-size: 0.78rem; text-decoration: none;
    }
    .match-score {
        display: inline-block; background: #22d3a520;
        border: 1px solid #22d3a5; border-radius: 6px;
        padding: 2px 8px; font-size: 0.72rem; color: #22d3a5; margin-right: 6px;
    }
</style>
""", unsafe_allow_html=True)


# ── DB ────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    try:
        url = (
            f"postgresql+psycopg2://"
            f"{os.getenv('DB_USER','airflow')}:{os.getenv('DB_PASSWORD','airflow')}"
            f"@{os.getenv('STREAMLIT_DB_HOST','localhost')}:"
            f"{os.getenv('STREAMLIT_DB_PORT','5433')}/"
            f"{os.getenv('DB_NAME','airflow')}"
        )
        eng = create_engine(url)
        with eng.connect() as c:
            c.execute(text("SELECT 1"))
        return eng
    except Exception:
        return None


def run_query(sql: str) -> pd.DataFrame:
    eng = get_engine()
    if eng is None:
        return pd.DataFrame()
    try:
        return pd.read_sql(sql, eng)
    except Exception as e:
        st.error(f"SQL error: {e}\n\nQuery: {sql[:300]}")
        return pd.DataFrame()


def get_columns(table: str) -> list:
    df = run_query(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name='{table}'
        ORDER BY ordinal_position
    """)
    return df["column_name"].tolist() if not df.empty else []


def fuzzy_find(columns: list, keywords: list):
    for kw in keywords:
        for col in columns:
            if kw in col:
                return col
    return None


def find_job_tables() -> list:
    df = run_query("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='public' ORDER BY table_name
    """)
    if df.empty:
        return []
    kw = ["job","listing","posting","vacancy","bongthom","work","career"]
    return [t for t in df["table_name"].tolist() if any(k in t.lower() for k in kw)]


def search_similar_jobs(predicted_job: str, skills_input: str,
                        table: str, title_col: str, limit: int = 60) -> pd.DataFrame:
    all_cols = get_columns(table)
    if not all_cols:
        return pd.DataFrame()
    raw = predicted_job.split() + [s.strip() for s in skills_input.split(",")]
    keywords = list({k.lower() for k in raw if len(k) > 2})
    if not keywords:
        return pd.DataFrame()
    text_hints = ["title","name","position","role","job","post","category",
                  "desc","detail","summary","type","field","sector"]
    search_cols = list(dict.fromkeys(
        ([title_col] if title_col else []) +
        [c for c in all_cols if any(h in c.lower() for h in text_hints)]
    ))
    if not search_cols:
        search_cols = all_cols[:5]
    conditions = []
    for col in search_cols:
        for kw in keywords:
            safe = kw.replace("'", "''")
            conditions.append(f'LOWER("{col}"::text) LIKE \'%%{safe}%%\'')
    where = " OR ".join(conditions)
    sql   = f'SELECT * FROM "{table}" WHERE {where} LIMIT {limit}'
    df    = run_query(sql)
    if df.empty:
        df = run_query(f'SELECT * FROM "{table}" LIMIT {limit}')
    return df


def score_row(row: pd.Series, predicted_job: str, skills_input: str) -> int:
    keywords = {k.lower() for k in
                (predicted_job + " " + skills_input).replace(",", " ").split()
                if len(k) > 2}
    text = " ".join(str(v) for v in row.values).lower()
    hits = sum(1 for kw in keywords if kw in text)
    return min(100, int(hits / max(len(keywords), 1) * 100))


# ── Sidebar ───────────────────────────────────────────────────────────────────
engine = get_engine()
with st.sidebar:
    st.markdown("## 💼 AI Recommender")
    st.markdown("🟢 **DB Connected**" if engine else "🔴 **DB Offline**")
    if AI_AVAILABLE:
        st.markdown("🤖 **Model Loaded**")
    else:
        st.markdown("⚠️ **Model Unavailable**")
    st.markdown("---")

    selected_table      = None
    title_col_override  = None
    max_matches         = 10

    if engine:
        job_tables = find_job_tables()
        if job_tables:
            selected_table = st.selectbox("🗄️ Job listings table", job_tables)
            real_cols       = get_columns(selected_table)
            real_cols_lower = [c.lower() for c in real_cols]
            st.markdown("**Columns in this table:**")
            st.caption(" · ".join(real_cols_lower) if real_cols_lower else "none found")
            title_col_override = st.selectbox(
                "Which column is the job title?",
                ["(auto-detect)"] + real_cols_lower,
            )
        else:
            st.caption("No job tables found. Run your scrapers first.")
        max_matches = st.slider("Max jobs to show", 5, 30, 10)
    st.markdown("---")
    if st.button("🔄 Clear cache"):
        st.cache_data.clear()
        st.rerun()


# ── Main ──────────────────────────────────────────────────────────────────────
st.title("💼 AI Job & Salary Recommender")
st.write("Enter your skills → AI prediction + **real matching jobs** from the live database.")
st.markdown("---")

# Show model status warning but don't stop the app
if not AI_AVAILABLE:
    st.warning(
        f"⚠️ AI model could not be loaded. "
        f"{'Error: ' + _load_error if _load_error else 'Model file not found.'} "
        f"Job search still works below!"
    )

st.subheader("🧠 Enter Your Skills")
skills_input = st.text_area(
    "Type skills separated by commas",
    placeholder="Example: English, Teaching, Communication, Python",
)

if st.button("Predict & Find Matching Jobs 🚀", type="primary"):
    if not skills_input.strip():
        st.warning("Please enter your skills first!")
        st.stop()

    skills      = [s.strip() for s in skills_input.split(",") if s.strip()]
    skills_text = " ".join(skills)

    # ── AI prediction (only if model available) ───────────────────────────────
    if AI_AVAILABLE:
        predicted_job = job_pipeline.predict([skills_text])[0]
        job_encoded   = le.transform([predicted_job])[0]
        base_salary   = float(salary_model.predict([[job_encoded]])[0])
        base_salary   = max(300.0, min(3000.0, base_salary))
        salary_min    = max(300.0, base_salary - 300)
        salary_max    = min(3000.0, base_salary + 500)
        level         = "Low" if base_salary < 1000 else ("Medium" if base_salary < 2000 else "High")

        st.success("✅ Prediction complete!")
        c1, c2, c3 = st.columns(3)
        c1.markdown(
            f'<div class="result-card"><div class="result-label">🧑‍💼 Predicted Job</div>'
            f'<div class="result-value">{predicted_job}</div></div>',
            unsafe_allow_html=True)
        c2.markdown(
            f'<div class="result-card"><div class="result-label">💰 Salary Range (USD)</div>'
            f'<div class="result-value">${round(salary_min)} – ${round(salary_max)}</div></div>',
            unsafe_allow_html=True)
        c3.markdown(
            f'<div class="result-card"><div class="result-label">📊 Salary Level</div>'
            f'<div class="result-value">{"🟢" if level=="High" else "🟡" if level=="Medium" else "🔴"} {level}</div></div>',
            unsafe_allow_html=True)

        st.markdown('<p class="section">📌 Input Summary</p>', unsafe_allow_html=True)
        st.info(f"**Skills entered:** {', '.join(skills)}")
    else:
        # No model — use skills as the search query directly
        predicted_job = skills_text
        st.info(f"**Searching jobs for skills:** {', '.join(skills)}")

    # ── Live job matches ──────────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("🔍 Matching Live Job Listings")

    if not engine:
        st.info("💡 Database offline — start Docker to see live matches.")
        st.stop()
    if not selected_table:
        st.info("No job-listing tables found. Run your scrapers first.")
        st.stop()

    real_cols       = get_columns(selected_table)
    real_cols_lower = [c.lower() for c in real_cols]

    if title_col_override and title_col_override != "(auto-detect)":
        title_col = title_col_override
    else:
        title_col = fuzzy_find(real_cols_lower,
                               ["title","position","role","name","job","post","vacancy"])

    with st.expander("🛠️ Debug — actual columns & detected mappings"):
        st.write(f"**Table:** `{selected_table}`")
        st.write(f"**All columns:** `{real_cols_lower}`")
        st.write(f"**Detected title column:** `{title_col}`")
        kws = list({k.lower() for k in predicted_job.split() +
                    [s.strip() for s in skills_input.split(",")] if len(k) > 2})
        st.write(f"**Keywords used:** `{kws}`")

    with st.spinner("Searching …"):
        df = search_similar_jobs(predicted_job, skills_input,
                                 selected_table, title_col or "", max_matches * 3)

    if df.empty:
        st.warning("Table is empty. Run your scrapers to populate it.")
        st.stop()

    df.columns = [c.lower().strip() for c in df.columns]
    cols = df.columns.tolist()

    t_col  = fuzzy_find(cols, ["title","position","role","job_name","name","post","vacancy"])
    co_col = fuzzy_find(cols, ["company","employer","organization","org","firm","institute"])
    lo_col = fuzzy_find(cols, ["location","city","province","region","place","area","district"])
    sc_col = fuzzy_find(cols, ["source","website","portal","platform","site","origin"])
    dl_col = fuzzy_find(cols, ["deadline","closing","expiry","expire","due","end_date"])
    ur_col = fuzzy_find(cols, ["url","link","href","apply","apply_url","apply_link"])

    if t_col is None:
        str_cols = df.select_dtypes(include="object").columns.tolist()
        t_col = str_cols[0] if str_cols else cols[0]

    df["_score"] = df.apply(lambda r: score_row(r, predicted_job, skills_input), axis=1)
    df = df.sort_values("_score", ascending=False).head(max_matches)

    st.markdown(
        f'<p class="section">{len(df)} jobs found — ranked by relevance to "{predicted_job}"</p>',
        unsafe_allow_html=True
    )

    for _, row in df.iterrows():
        def safe(col):
            v = row.get(col, "") if col else ""
            return "" if pd.isna(v) or str(v).strip().lower() in ("nan","none","") else str(v).strip()

        title   = safe(t_col)  or "No title"
        company = safe(co_col) or "—"
        loc     = safe(lo_col)
        source  = safe(sc_col)
        dead    = safe(dl_col)
        url     = safe(ur_col)
        score   = int(row.get("_score", 0))

        badges  = f'<span class="match-score">🎯 {score}% match</span>'
        if source: badges += f'<span class="job-badge">🌐 {source}</span>'
        if loc:    badges += f'<span class="job-badge">📍 {loc}</span>'
        if dead:   badges += f'<span class="job-badge">⏰ {dead}</span>'
        link_btn = (f'<a class="job-link" href="{url}" target="_blank">View Job →</a>'
                    if url else "")

        st.markdown(f"""
        <div class="job-card">
            <div class="job-title">{title}</div>
            <div class="job-meta">🏢 {company}</div>
            <div>{badges}</div>
            {link_btn}
        </div>""", unsafe_allow_html=True)

    with st.expander("📋 Raw matched data (all columns)"):
        st.dataframe(df.drop(columns=["_score"]), use_container_width=True, height=300)

    st.markdown('<p class="section">📊 Match Stats</p>', unsafe_allow_html=True)
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Matches found",   len(df))
    m2.metric("Avg match score", f"{df['_score'].mean():.0f}%")
    m3.metric("Top score",        f"{df['_score'].max():.0f}%")
    m4.metric("Table",            selected_table)

    st.markdown("---")
    st.caption("💡 Head over to **app.py → Job Feed** to browse all listings with full filters.")