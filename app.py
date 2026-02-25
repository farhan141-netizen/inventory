import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
import datetime
import uuid
import io

from streamlit_sortables import sort_items  # drag & drop kanban

# --- CLOUD CONNECTION ---
conn = st.connection("gsheets", type=GSheetsConnection)

def clean_dataframe(df):
    """Ensures unique columns and removes ghost columns from Google Sheets"""
    if df is None or df.empty:
        return df
    df = df.loc[:, ~df.columns.str.contains("^Unnamed", na=False)]
    df = df.dropna(axis=1, how="all")
    df = df.loc[:, ~df.columns.duplicated()]
    df.columns = [str(col).strip() for col in df.columns]
    return df

@st.cache_data(ttl=60)
def load_from_sheet(worksheet_name, default_cols=None):
    """Safely load and clean data from Google Sheets with caching"""
    try:
        df = conn.read(worksheet=worksheet_name, ttl="1m")
        df = clean_dataframe(df)
        if df is None or df.empty:
            return pd.DataFrame(columns=default_cols) if default_cols else pd.DataFrame()
        return df
    except Exception:
        return pd.DataFrame(columns=default_cols) if default_cols else pd.DataFrame()

def save_to_sheet(df, worksheet_name):
    """Save cleaned data to Google Sheets and clear cache"""
    if df is None or df.empty:
        return False

    df = clean_dataframe(df)
    try:
        conn.update(worksheet=worksheet_name, data=df)
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Error saving to {worksheet_name}: {str(e)}")
        return False

def _ensure_cols(df: pd.DataFrame, defaults: dict) -> pd.DataFrame:
    if df is None:
        df = pd.DataFrame()
    for c, v in defaults.items():
        if c not in df.columns:
            df[c] = v
    return df

def _to_excel_bytes(sheets: dict):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        for name, df in sheets.items():
            safe_name = (str(name)[:31] if name else "Sheet")
            if df is None:
                continue
            (df if isinstance(df, pd.DataFrame) else pd.DataFrame(df)).to_excel(
                writer, index=False, sheet_name=safe_name
            )
    return buf.getvalue()

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="Warehouse Pro Cloud",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- GLASSY CRAFT UI (Whole app) ---
st.markdown(
    """
    <style>
    :root{
      --bg: #0F1419;
      --glass: rgba(26,31,46,0.66);
      --glass2: rgba(26,31,46,0.44);
      --stroke: rgba(255,255,255,0.07);
      --stroke2: rgba(255,255,255,0.06);
      --text: #E7EEF9;
      --muted: #94A3B8;
      --muted2: #6B778A;
      --cyan: #00D9FF;
      --cyan2:#0095FF;
      --amber:#FFAA00;
      --red:#FF6B6B;
      --shadow: 0 18px 55px rgba(0,0,0,.38);
      --shadow2: 0 10px 30px rgba(0,0,0,.25);
      --r12: 12px;
      --r16: 16px;
      --r18: 18px;
      --blur: blur(16px);
    }

    html, body, [class*="css"]{
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji", "Segoe UI Emoji";
      letter-spacing: -0.01em;
    }

    .main{ background: var(--bg); }
    .block-container{ padding-top: .85rem; padding-bottom: 1rem; }

    /* Header */
    .header-bar{
      border-radius: var(--r18);
      padding: 12px 16px;
      background: linear-gradient(90deg, var(--cyan) 0%, var(--cyan2) 100%);
      color: #061018;
      box-shadow: 0 14px 40px rgba(0,217,255,.15);
      display:flex;
      justify-content: space-between;
      align-items:center;
      margin-bottom: 10px;
    }
    .header-bar h1{
      margin:0;
      font-size: 1.15rem !important;
      font-weight: 800;
      letter-spacing: -0.03em;
    }
    .header-bar p{
      margin:0;
      font-size:.78rem;
      font-weight: 500;
      opacity:.9;
    }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"]{
      gap: 8px;
      background: var(--glass);
      border: 1px solid var(--stroke);
      backdrop-filter: var(--blur);
      padding: 6px;
      border-radius: var(--r18);
      box-shadow: var(--shadow2);
      margin-bottom: 10px;
    }
    .stTabs [data-baseweb="tab"]{
      border-radius: 12px;
      color: #AAB4C3;
      font-weight: 650;
      font-size: .86rem;
      padding: 6px 14px;
      height: 40px;
      transition: all .16s ease;
    }
    .stTabs [data-baseweb="tab"]:hover{
      color: var(--text);
      background: rgba(0,217,255,.08);
      transform: translateY(-1px);
    }
    .stTabs [aria-selected="true"]{
      color: var(--cyan);
      background: rgba(0,217,255,.10);
      border: 1px solid rgba(0,217,255,.20);
      box-shadow: 0 12px 24px rgba(0,217,255,.07) inset;
    }

    /* Inputs feel */
    div[data-baseweb="select"] > div,
    div[data-testid="stDateInput"] > div,
    div[data-testid="stNumberInput"] > div,
    div[data-testid="stTextInput"] > div,
    div[data-testid="stTextArea"] > div{
      border-radius: 14px !important;
      border: 1px solid var(--stroke2) !important;
      background: rgba(10,14,18,.32) !important;
      backdrop-filter: var(--blur);
    }

    /* Buttons */
    .stButton>button{
      border-radius: 14px;
      border: 1px solid rgba(255,255,255,.08);
      background: rgba(10,14,18,.25);
      color: var(--text);
      font-weight: 700;
      padding: 6px 12px;
      transition: transform .16s ease, box-shadow .16s ease, border-color .16s ease;
    }
    .stButton>button:hover{
      transform: translateY(-1px);
      border-color: rgba(0,217,255,.25);
      box-shadow: 0 14px 35px rgba(0,217,255,.06);
    }

    /* Section title */
    .section-title{
      color: var(--cyan);
      font-size: 0.95rem;
      font-weight: 800;
      margin: 0 0 10px 0;
      padding-bottom: 8px;
      border-bottom: 1px solid rgba(0,217,255,.16);
    }

    /* Dashboard shell */
    .dash-shell{
      border: 1px solid var(--stroke);
      border-radius: 22px;
      padding: 14px;
      background:
        radial-gradient(1200px 600px at 20% -10%, rgba(0,217,255,0.12), transparent 55%),
        radial-gradient(900px 600px at 90% 0%, rgba(0,149,255,0.12), transparent 60%),
        rgba(26,31,46,0.20);
      backdrop-filter: var(--blur);
      box-shadow: var(--shadow);
    }

    .dash-topbar{
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap: 12px;
      border-radius: 18px;
      padding: 12px 14px;
      background: var(--glass);
      border: 1px solid var(--stroke);
      backdrop-filter: var(--blur);
      box-shadow: var(--shadow2);
      margin-bottom: 12px;
    }
    .dash-topbar h2{
      margin:0;
      font-size: 1.02rem;
      font-weight: 800;
      color: var(--text);
      letter-spacing: -0.02em;
    }
    .dash-topbar small{
      color: var(--muted);
      font-size: .78rem;
      font-weight: 500;
    }

    /* Card */
    .card{
      position: relative;
      border-radius: 18px;
      padding: 10px 12px 12px 12px;
      background: var(--glass);
      border: 1px solid var(--stroke);
      backdrop-filter: var(--blur);
      box-shadow: var(--shadow2);
      overflow: hidden;
      transition: transform .16s ease, border-color .16s ease, box-shadow .16s ease;
    }
    .card:hover{
      transform: translateY(-1px);
      border-color: rgba(0,217,255,.22);
      box-shadow: 0 20px 60px rgba(0,0,0,.38);
    }
    .card-head{
      display:flex;
      align-items:flex-start;
      justify-content:space-between;
      gap: 10px;
      margin-bottom: 8px;
    }
    .card-title{
      margin:0;
      font-size: .92rem;
      font-weight: 800;
      color: var(--text);
      letter-spacing: -0.01em;
    }
    .card-sub{
      margin:0;
      font-size: .74rem;
      color: var(--muted2);
      font-weight: 500;
    }
    .kebab{
      opacity: 0;
      transition: opacity .14s ease;
    }
    .card:hover .kebab{
      opacity: 1;
    }

    /* KPI strip inside summary */
    .kpi-strip{
      display:grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      margin-top: 6px;
    }
    .kpi{
      border-radius: 14px;
      padding: 10px;
      border: 1px solid var(--stroke2);
      background: rgba(10,14,18,.20);
    }
    .kpi .l{ color: var(--muted); font-size: .74rem; font-weight: 650; margin-bottom: 6px; }
    .kpi .v{ color: var(--text); font-size: 1.12rem; font-weight: 900; letter-spacing: -0.03em; }

    /* Kanban */
    .kanban-title{ color: var(--text); font-weight: 850; font-size: .92rem; margin: 0 0 6px 0; }
    .kan-col{
      border-radius: 18px;
      border: 1px solid var(--stroke);
      background: rgba(10,14,18,.18);
      padding: 10px;
      min-height: 320px;
    }
    .kan-pill{
      display:inline-block;
      padding: 4px 8px;
      border-radius: 999px;
      font-size: .72rem;
      font-weight: 800;
      border: 1px solid var(--stroke2);
      color: var(--muted);
      background: rgba(26,31,46,.20);
      margin-bottom: 10px;
    }

    /* sortables styles */
    .stSortable {
      border-radius: 14px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- CORE CALCULATION ENGINE ---
def recalculate_item(df, item_name):
    if item_name not in df["Product Name"].values:
        return df
    idx = df[df["Product Name"] == item_name].index[0]
    day_cols = [str(i) for i in range(1, 32)]
    for col in day_cols:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    total_received = df.loc[idx, day_cols].sum()
    df.at[idx, "Total Received"] = total_received
    opening = pd.to_numeric(df.at[idx, "Opening Stock"], errors="coerce") or 0.0
    consumption = pd.to_numeric(df.at[idx, "Consumption"], errors="coerce") or 0.0
    closing = opening + total_received - consumption
    df.at[idx, "Closing Stock"] = closing

    if "Physical Count" in df.columns:
        physical_val = df.at[idx, "Physical Count"]
        if pd.notna(physical_val) and str(physical_val).strip() != "":
            physical = pd.to_numeric(physical_val, errors="coerce")
            df.at[idx, "Variance"] = physical - closing
        else:
            df.at[idx, "Variance"] = 0.0
    return df

def apply_transaction(item_name, day_num, qty, is_undo=False):
    df = st.session_state.inventory
    if item_name in df["Product Name"].values:
        idx = df[df["Product Name"] == item_name].index[0]
        col_name = str(int(day_num))
        if col_name != "0":
            if col_name not in df.columns:
                df[col_name] = 0.0
            current_val = pd.to_numeric(df.at[idx, col_name], errors="coerce") or 0.0
            df.at[idx, col_name] = current_val + float(qty)

        if not is_undo:
            new_log = pd.DataFrame(
                [
                    {
                        "LogID": str(uuid.uuid4())[:8],
                        "Timestamp": datetime.datetime.now().strftime("%H:%M:%S"),
                        "Item": item_name,
                        "Qty": qty,
                        "Day": day_num,
                        "Status": "Active",
                        "LogDate": datetime.date.today().strftime("%Y-%m-%d"),
                    }
                ]
            )
            logs_df = load_from_sheet(
                "activity_logs",
                ["LogID", "Timestamp", "Item", "Qty", "Day", "Status", "LogDate"],
            )
            if "LogDate" not in logs_df.columns:
                logs_df["LogDate"] = ""
            save_to_sheet(pd.concat([logs_df, new_log], ignore_index=True), "activity_logs")

        df = recalculate_item(df, item_name)
        st.session_state.inventory = df
        save_to_sheet(df, "persistent_inventory")
        return True
    return False

def undo_entry(log_id):
    logs = load_from_sheet("activity_logs")
    if not logs.empty and "LogID" in logs.columns and log_id in logs["LogID"].values:
        idx = logs[logs["LogID"] == log_id].index[0]
        if logs.at[idx, "Status"] == "Undone":
            return
        item, qty, day = logs.at[idx, "Item"], logs.at[idx, "Qty"], logs.at[idx, "Day"]
        if apply_transaction(item, day, -qty, is_undo=True):
            logs.at[idx, "Status"] = "Undone"
            save_to_sheet(logs, "activity_logs")
            st.rerun()

# --- DATA PREP ---
TOP_COUNTS = [3, 5, 10, 25, 50, 100]

def _prepare_inventory(inv_df):
    inv_df = inv_df.copy() if inv_df is not None else pd.DataFrame()
    inv_df = _ensure_cols(inv_df, {"Product Name": "", "Category": "General", "UOM": "", "Closing Stock": 0.0})
    inv_df["Product Name"] = inv_df["Product Name"].astype(str).str.strip()
    inv_df["Category"] = inv_df["Category"].fillna("General").astype(str).str.strip()
    inv_df["UOM"] = inv_df["UOM"].fillna("").astype(str).str.strip()
    inv_df["Closing Stock"] = pd.to_numeric(inv_df["Closing Stock"], errors="coerce").fillna(0.0)
    inv_df = inv_df[~inv_df["Product Name"].str.startswith("CATEGORY_", na=False)]
    return inv_df

def _prepare_metadata():
    meta_df = load_from_sheet("product_metadata")
    meta_df = meta_df.copy() if meta_df is not None else pd.DataFrame()
    meta_df = _ensure_cols(
        meta_df,
        {
            "Product Name": "",
            "Category": "General",
            "UOM": "",
            "Supplier": "",
            "Price": 0.0,
            "Currency": "",
            "Min Stock": 0.0,
        },
    )

    meta_df["Product Name"] = meta_df["Product Name"].astype(str).str.strip()
    meta_df["Category"] = meta_df["Category"].fillna("General").astype(str).str.strip()
    meta_df["UOM"] = meta_df["UOM"].fillna("").astype(str).str.strip()
    meta_df["Supplier"] = meta_df["Supplier"].fillna("").astype(str).str.strip()
    meta_df["Price"] = pd.to_numeric(meta_df["Price"], errors="coerce").fillna(0.0)
    meta_df["Currency"] = meta_df["Currency"].fillna("").astype(str).str.upper().str.strip()
    meta_df["Min Stock"] = pd.to_numeric(meta_df["Min Stock"], errors="coerce").fillna(0.0)

    meta_df = meta_df[
        (~meta_df["Product Name"].str.startswith("CATEGORY_", na=False))
        & (~meta_df["Product Name"].str.startswith("SUPPLIER_", na=False))
    ]
    return meta_df

def _prepare_reqs(req_df):
    req_df = req_df.copy() if req_df is not None else pd.DataFrame()
    req_df = _ensure_cols(
        req_df,
        {
            "Restaurant": "",
            "Item": "",
            "Qty": 0.0,
            "DispatchQty": 0.0,
            "Status": "",
            "RequestedDate": None,
            "Timestamp": None,
        },
    )
    req_df["Restaurant"] = req_df["Restaurant"].fillna("").astype(str).str.strip()
    req_df["Item"] = req_df["Item"].fillna("").astype(str).str.strip()
    req_df["Qty"] = pd.to_numeric(req_df["Qty"], errors="coerce").fillna(0.0)
    req_df["DispatchQty"] = pd.to_numeric(req_df["DispatchQty"], errors="coerce").fillna(0.0)
    req_df["Status"] = req_df["Status"].fillna("").astype(str).str.strip()
    req_df["RequestedDate"] = pd.to_datetime(req_df["RequestedDate"], errors="coerce").dt.date
    req_df["DispatchTS_Date"] = pd.to_datetime(req_df["Timestamp"], errors="coerce").dt.date
    return req_df

def _prepare_logs(log_df):
    log_df = log_df.copy() if log_df is not None else pd.DataFrame()
    log_df = _ensure_cols(log_df, {"Item": "", "Qty": 0.0, "Status": "", "LogDate": None, "Timestamp": None})
    log_df["Item"] = log_df["Item"].fillna("").astype(str).str.strip()
    log_df["Qty"] = pd.to_numeric(log_df["Qty"], errors="coerce").fillna(0.0)
    log_df["Status"] = log_df["Status"].fillna("").astype(str).str.strip()

    logdate = pd.to_datetime(log_df["LogDate"], errors="coerce")
    ts_fallback = pd.to_datetime(log_df["Timestamp"], errors="coerce")
    combined = logdate.fillna(ts_fallback)
    log_df["LogDateParsed"] = combined.dt.date
    return log_df

# --- CARD STATE (kebab) ---
def _card_state(card_id: str):
    if "card_state" not in st.session_state:
        st.session_state.card_state = {}
    if card_id not in st.session_state.card_state:
        st.session_state.card_state[card_id] = {
            "sort": "High → Low",
            "top": 10,
        }
    st.session_state.card_state[card_id]["top"] = int(st.session_state.card_state[card_id].get("top", 10))
    if st.session_state.card_state[card_id]["top"] not in TOP_COUNTS:
        st.session_state.card_state[card_id]["top"] = 10
    if st.session_state.card_state[card_id].get("sort") not in ["High → Low", "Low → High"]:
        st.session_state.card_state[card_id]["sort"] = "High → Low"
    return st.session_state.card_state[card_id]

def _card_open(title: str, subtitle: str, card_id: str, export_df: pd.DataFrame | None = None):
    state = _card_state(card_id)

    st.markdown('<div class="card">', unsafe_allow_html=True)
    left, right = st.columns([8, 1.2], vertical_alignment="top")
    with left:
        st.markdown(f"<div class='card-head'><div><p class='card-title'>{title}</p><p class='card-sub'>{subtitle}</p></div></div>", unsafe_allow_html=True)
    with right:
        st.markdown("<div class='kebab'>", unsafe_allow_html=True)
        with st.popover("⋮", use_container_width=True):
            state["sort"] = st.radio(
                "Sort",
                ["High → Low", "Low → High"],
                index=0 if state["sort"] == "High → Low" else 1,
                horizontal=True,
                key=f"{card_id}_sort",
            )
            state["top"] = st.selectbox(
                "Items",
                TOP_COUNTS,
                index=TOP_COUNTS.index(int(state["top"])),
                key=f"{card_id}_top",
            )

            if st.button("Refresh Card", use_container_width=True, key=f"{card_id}_refresh"):
                st.cache_data.clear()
                st.rerun()

            if export_df is not None and not export_df.empty:
                csv = export_df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Export CSV",
                    data=csv,
                    file_name=f"{card_id}.csv",
                    mime="text/csv",
                    use_container_width=True,
                    key=f"{card_id}_export",
                )
        st.markdown("</div>", unsafe_allow_html=True)

    return state

def _card_close():
    st.markdown("</div>", unsafe_allow_html=True)

# --- PLOTLY DONUT (stable + pretty) ---
def _donut(df: pd.DataFrame, label_col: str, value_col: str):
    if df is None or df.empty or label_col not in df.columns or value_col not in df.columns:
        st.info("📭 No data")
        return
    d = df[[label_col, value_col]].copy()
    d[value_col] = pd.to_numeric(d[value_col], errors="coerce").fillna(0.0)
    d = d[d[value_col] > 0]
    if d.empty:
        st.info("📭 No data")
        return
    try:
        import plotly.express as px  # type: ignore

        fig = px.pie(d, names=label_col, values=value_col, hole=0.58)
        fig.update_layout(
            height=310,
            margin=dict(l=8, r=8, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            legend=dict(font=dict(color="#cbd5e1", size=10)),
        )
        fig.update_traces(textposition="inside", textinfo="percent", insidetextfont=dict(color="#0B1220", size=11))
        st.plotly_chart(fig, use_container_width=True)
    except Exception:
        st.dataframe(d, use_container_width=True, hide_index=True)

def _bar_list(df: pd.DataFrame, label_col: str, value_col: str, currency_suffix: str = ""):
    if df is None or df.empty or label_col not in df.columns or value_col not in df.columns:
        st.info("📭 No data")
        return

    d = df[[label_col, value_col]].copy()
    d[value_col] = pd.to_numeric(d[value_col], errors="coerce").fillna(0.0)
    if d.empty:
        st.info("📭 No data")
        return

    try:
        import plotly.express as px  # type: ignore

        fig = px.bar(d, x=value_col, y=label_col, orientation="h")
        fig.update_layout(
            height=360,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(showgrid=False, zeroline=False, tickfont=dict(color="#94a3b8", size=10)),
            yaxis=dict(showgrid=False, zeroline=False, tickfont=dict(color="#cbd5e1", size=11)),
        )
        fig.update_traces(marker=dict(color="rgba(0,217,255,0.78)", line=dict(width=0), cornerradius=10))
        st.plotly_chart(fig, use_container_width=True)
    except Exception:
        st.dataframe(d, use_container_width=True, hide_index=True)

# --- INITIALIZATION ---
if "inventory" not in st.session_state:
    st.session_state.inventory = load_from_sheet("persistent_inventory")

if "log_page" not in st.session_state:
    st.session_state.log_page = 0

# --- MAIN UI HEADER ---
st.markdown(
    """
    <div class="header-bar">
      <div>
        <h1>WAREHOUSE PRO CLOUD</h1>
        <p>Craft operations • calm power</p>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# top action row (like screenshot: export + refresh)
top_a, top_b, top_c = st.columns([5, 1.5, 1.5])
with top_b:
    if st.button("🔄 Refresh Data", use_container_width=True, key="refresh_all"):
        st.cache_data.clear()
        st.rerun()
with top_c:
    st.caption("")

tab_ops, tab_req, tab_sup, tab_dash = st.tabs(["Dashboard", "Operations", "Requisitions", "Supplier"])

# ===================== DASHBOARD TAB =====================
with tab_ops:
    st.markdown('<div class="dash-shell">', unsafe_allow_html=True)

    # topbar inside dashboard
    l, r = st.columns([3.2, 1.4], vertical_alignment="center")
    with l:
        st.markdown(
            """
            <div class="dash-topbar">
              <div>
                <h2>Warehouse Dashboard</h2>
                <small>Glassy cards • per-card controls • drag & drop Kanban</small>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # filters (simple)
    f1, f2, f3 = st.columns([1.2, 1.2, 1.2])
    with f1:
        today = datetime.date.today()
        start_date = st.date_input("From", value=today - datetime.timedelta(days=2), key="dash_from")
    with f2:
        end_date = st.date_input("To", value=today, key="dash_to")
    with f3:
        currency_choice = st.selectbox("Currency", ["All"] + ["USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF", "CNY", "HKD", "SGD", "INR", "AED", "SAR", "KWD", "BHD"], index=0, key="dash_currency")

    if start_date > end_date:
        st.warning("⚠️ From date is after To date.")
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        inv_df = _prepare_inventory(load_from_sheet("persistent_inventory"))
        meta_df = _prepare_metadata()
        req_df = _prepare_reqs(load_from_sheet("restaurant_requisitions"))
        log_df = _prepare_logs(load_from_sheet("activity_logs"))

        # filter reqs and logs by date
        req_filtered = req_df.copy()
        if not req_filtered.empty:
            req_filtered = req_filtered[req_filtered["RequestedDate"].notna()]
            req_filtered = req_filtered[(req_filtered["RequestedDate"] >= start_date) & (req_filtered["RequestedDate"] <= end_date)]

        logs_filtered = log_df.copy()
        if not logs_filtered.empty:
            logs_filtered = logs_filtered[logs_filtered["Status"] == "Active"]
            logs_filtered = logs_filtered[logs_filtered["LogDateParsed"].notna()]
            logs_filtered = logs_filtered[(logs_filtered["LogDateParsed"] >= start_date) & (logs_filtered["LogDateParsed"] <= end_date)]

        # currency filter for value metrics
        meta_cur = meta_df.copy()
        if currency_choice != "All":
            meta_cur = meta_cur[meta_cur["Currency"].astype(str).str.upper() == str(currency_choice).upper()]

        # Join inventory with meta for value
        inv_join = inv_df.merge(
            meta_df[["Product Name", "Supplier", "Price", "Currency", "Min Stock"]].drop_duplicates("Product Name"),
            on="Product Name",
            how="left",
        )
        inv_join["Price"] = pd.to_numeric(inv_join.get("Price", 0.0), errors="coerce").fillna(0.0)
        inv_join["Closing Stock"] = pd.to_numeric(inv_join.get("Closing Stock", 0.0), errors="coerce").fillna(0.0)
        inv_join["Stock Value"] = (inv_join["Closing Stock"] * inv_join["Price"]).round(2)

        # SALES = dispatched qty (dispatched/completed) (value uses price)
        disp_only = req_filtered[req_filtered["Status"].isin(["Dispatched", "Completed"])].copy() if not req_filtered.empty else pd.DataFrame()
        top_selling_qty = pd.DataFrame(columns=["Item", "Qty"])
        if not disp_only.empty:
            top_selling_qty = disp_only.groupby("Item", as_index=False)["DispatchQty"].sum().rename(columns={"DispatchQty": "Qty"})
            top_selling_qty["Qty"] = pd.to_numeric(top_selling_qty["Qty"], errors="coerce").fillna(0.0)

        # PURCHASE = received logs (qty)
        top_purchased_qty = pd.DataFrame(columns=["Item", "Qty"])
        if not logs_filtered.empty:
            top_purchased_qty = logs_filtered.groupby("Item", as_index=False)["Qty"].sum().rename(columns={"Qty": "Qty"})
            top_purchased_qty["Qty"] = pd.to_numeric(top_purchased_qty["Qty"], errors="coerce").fillna(0.0)

        # Value versions using meta price (currency filter applies)
        meta_price = meta_cur[["Product Name", "Price", "Supplier", "Currency"]].drop_duplicates("Product Name").copy()

        top_purchased_val = pd.DataFrame(columns=["Item", "Value"])
        if not logs_filtered.empty:
            tmp = logs_filtered.merge(meta_price, left_on="Item", right_on="Product Name", how="left")
            tmp["Price"] = pd.to_numeric(tmp.get("Price", 0.0), errors="coerce").fillna(0.0)
            tmp["Value"] = (pd.to_numeric(tmp["Qty"], errors="coerce").fillna(0.0) * tmp["Price"]).fillna(0.0)
            top_purchased_val = tmp.groupby("Item", as_index=False)["Value"].sum()

        top_selling_val = pd.DataFrame(columns=["Item", "Value"])
        if not disp_only.empty:
            tmp = disp_only.merge(meta_price, left_on="Item", right_on="Product Name", how="left")
            tmp["Price"] = pd.to_numeric(tmp.get("Price", 0.0), errors="coerce").fillna(0.0)
            tmp["Value"] = (pd.to_numeric(tmp["DispatchQty"], errors="coerce").fillna(0.0) * tmp["Price"]).fillna(0.0)
            top_selling_val = tmp.groupby("Item", as_index=False)["Value"].sum()

        # Supplier purchase (estimate)
        supplier_purchase = pd.DataFrame(columns=["Supplier", "Purchase Amount"])
        if not logs_filtered.empty:
            tmp = logs_filtered.merge(meta_price, left_on="Item", right_on="Product Name", how="left")
            tmp["Price"] = pd.to_numeric(tmp.get("Price", 0.0), errors="coerce").fillna(0.0)
            tmp["Supplier"] = tmp.get("Supplier", "").fillna("").astype(str).str.strip()
            tmp["Purchase Amount"] = (pd.to_numeric(tmp["Qty"], errors="coerce").fillna(0.0) * tmp["Price"]).fillna(0.0)
            tmp = tmp[tmp["Supplier"] != ""]
            if not tmp.empty:
                supplier_purchase = tmp.groupby("Supplier", as_index=False)["Purchase Amount"].sum()

        # Summary KPIs
        total_purchase = float(top_purchased_val["Value"].sum()) if not top_purchased_val.empty else 0.0
        total_sales = float(top_selling_val["Value"].sum()) if not top_selling_val.empty else 0.0
        pnl = total_sales - total_purchase
        stock_value = float(inv_join["Stock Value"].sum()) if not inv_join.empty else 0.0

        # --- Layout like screenshot ---
        left_grid, right_grid = st.columns([2.1, 2.2], gap="large")

        with right_grid:
            state = _card_open("Summary", "Purchase • Sales • P&L • Stock value", "card_summary")
            # summary card ignores sort/top, but keeps kebab
            st.markdown(
                f"""
                <div class="kpi-strip">
                  <div class="kpi"><div class="l">Purchase</div><div class="v">{total_purchase:.2f}</div></div>
                  <div class="kpi"><div class="l">Sales</div><div class="v">{total_sales:.2f}</div></div>
                  <div class="kpi"><div class="l">P&L</div><div class="v" style="color:{'#ff6b6b' if pnl<0 else '#e7eef9'}">{pnl:.2f}</div></div>
                  <div class="kpi"><div class="l">Stock In Hand (Value)</div><div class="v">{stock_value:.2f}</div></div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            _card_close()

            # Supplier purchase list
            state = _card_open("Total Purchase From Supplier", "Estimated from received logs × unit price", "card_supplier", export_df=supplier_purchase)
            top = int(state["top"])
            asc = state["sort"] == "Low → High"
            df = supplier_purchase.copy()
            if not df.empty:
                df["Purchase Amount"] = pd.to_numeric(df["Purchase Amount"], errors="coerce").fillna(0.0)
                df = df.sort_values("Purchase Amount", ascending=asc).head(top)
            _bar_list(df, "Supplier", "Purchase Amount")
            _card_close()

        with left_grid:
            c1, c2 = st.columns(2, gap="small")
            with c1:
                state = _card_open("Top Purchased Product (QTY)", "Receipts by item", "card_purch_qty", export_df=top_purchased_qty)
                top = int(state["top"])
                asc = state["sort"] == "Low → High"
                df = top_purchased_qty.copy()
                if not df.empty:
                    df = df.sort_values("Qty", ascending=asc).head(top)
                _donut(df.rename(columns={"Qty": "Value"}), "Item", "Value")
                _card_close()

            with c2:
                state = _card_open("Top Selling Product (QTY)", "Dispatch by item", "card_sell_qty", export_df=top_selling_qty)
                top = int(state["top"])
                asc = state["sort"] == "Low → High"
                df = top_selling_qty.copy()
                if not df.empty:
                    df = df.sort_values("Qty", ascending=asc).head(top)
                _donut(df.rename(columns={"Qty": "Value"}), "Item", "Value")
                _card_close()

            c3, c4 = st.columns(2, gap="small")
            with c3:
                state = _card_open("Top Purchased Product (Value)", "Receipts × unit price", "card_purch_val", export_df=top_purchased_val)
                top = int(state["top"])
                asc = state["sort"] == "Low → High"
                df = top_purchased_val.copy()
                if not df.empty:
                    df = df.sort_values("Value", ascending=asc).head(top)
                _donut(df, "Item", "Value")
                _card_close()

            with c4:
                state = _card_open("Top Selling Product (Value)", "Dispatch × unit price", "card_sell_val", export_df=top_selling_val)
                top = int(state["top"])
                asc = state["sort"] == "Low → High"
                df = top_selling_val.copy()
                if not df.empty:
                    df = df.sort_values("Value", ascending=asc).head(top)
                _donut(df, "Item", "Value")
                _card_close()

        st.divider()

        # ---------------- KANBAN (DRAG & DROP) ----------------
        st.markdown("<div class='section-title'>🧩 Ops Kanban</div>", unsafe_allow_html=True)
        st.caption("Drag & drop cards between columns (saved in session).")

        # derive default kanban buckets
        kan = inv_join.copy()
        kan = _ensure_cols(kan, {"Min Stock": 0.0})
        kan["Min Stock"] = pd.to_numeric(kan["Min Stock"], errors="coerce").fillna(0.0)

        def bucket(row):
            cs = float(row.get("Closing Stock", 0) or 0)
            ms = float(row.get("Min Stock", 0) or 0)
            if cs <= 0:
                return "⚠️ Attention Needed"
            if ms > 0 and cs < ms:
                return "📦 Reorder Soon"
            if ms <= 0 and cs < 5:
                return "📦 Reorder Soon"
            return "✅ Healthy Stock"

        kan["Bucket"] = kan.apply(bucket, axis=1)
        kan = kan[kan["Product Name"].astype(str).str.strip() != ""]
        kan = kan.sort_values("Closing Stock", ascending=True)

        # Build card display strings (sortable items must be unique)
        def card_label(r):
            name = str(r["Product Name"])
            cs = float(r.get("Closing Stock", 0) or 0)
            return f"{name}  •  {cs:.2f}"

        # Persist kanban arrangement
        if "kanban" not in st.session_state:
            st.session_state.kanban = {
                "⚠️ Attention Needed": [card_label(r) for _, r in kan[kan["Bucket"] == "⚠️ Attention Needed"].head(25).iterrows()],
                "📦 Reorder Soon": [card_label(r) for _, r in kan[kan["Bucket"] == "📦 Reorder Soon"].head(25).iterrows()],
                "🚚 In Transit": [],
                "✅ Healthy Stock": [card_label(r) for _, r in kan[kan["Bucket"] == "✅ Healthy Stock"].head(25).iterrows()],
            }

        k1, k2, k3, k4 = st.columns(4, gap="small")
        cols = [
            ("⚠️ Attention Needed", k1),
            ("📦 Reorder Soon", k2),
            ("🚚 In Transit", k3),
            ("✅ Healthy Stock", k4),
        ]

        # sortables requires passing list per column; update state on drop
        for col_name, col in cols:
            with col:
                st.markdown(f"<div class='kanban-title'>{col_name}</div>", unsafe_allow_html=True)
                st.markdown(f"<span class='kan-pill'>{len(st.session_state.kanban.get(col_name, []))} cards</span>", unsafe_allow_html=True)
                items = st.session_state.kanban.get(col_name, [])
                new_items = sort_items(
                    items,
                    direction="vertical",
                    key=f"kan_{col_name}",
                )
                st.session_state.kanban[col_name] = new_items

        # Cross-column drag: streamlit-sortables can’t natively move between separate sortables.
        # We simulate cross-column by adding a "Move" selector per card is not possible in sortables.
        # To achieve true cross-column drag we’d need a component or streamlit-elements.
        # For now, we keep drag within columns and provide quick "Move to" controls below.

        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
        st.caption("Move cards between columns (cross-column) via quick action:")
        move_col1, move_col2 = st.columns([2, 1])
        with move_col1:
            all_cards = []
            for cname in st.session_state.kanban.keys():
                for it in st.session_state.kanban[cname]:
                    all_cards.append((cname, it))
            card_options = [f"{c} | {t}" for c, t in all_cards]
            selected = st.selectbox("Select card", [""] + card_options, key="kan_move_pick")
        with move_col2:
            target = st.selectbox("Move to", list(st.session_state.kanban.keys()), key="kan_move_target")

        if st.button("Move Card", use_container_width=True, key="kan_move_btn") and selected:
            src, title = selected.split(" | ", 1)
            if title in st.session_state.kanban.get(src, []):
                st.session_state.kanban[src].remove(title)
                st.session_state.kanban[target].insert(0, title)
                st.success("Moved.")
                st.rerun()

        # Export Dashboard button (top right style)
        export_bytes = _to_excel_bytes(
            {
                "Summary": pd.DataFrame(
                    [
                        {
                            "From": start_date,
                            "To": end_date,
                            "Currency": currency_choice,
                            "Purchase": total_purchase,
                            "Sales": total_sales,
                            "P&L": pnl,
                            "Stock Value": stock_value,
                        }
                    ]
                ),
                "Top Purchased Qty": top_purchased_qty,
                "Top Selling Qty": top_selling_qty,
                "Top Purchased Value": top_purchased_val,
                "Top Selling Value": top_selling_val,
                "Supplier Purchases": supplier_purchase,
            }
        )
        st.download_button(
            "📤 Export Dashboard",
            data=export_bytes,
            file_name=f"Warehouse_Dashboard_{start_date}_to_{end_date}.xlsx",
            use_container_width=True,
            key="dash_export",
        )

    st.markdown("</div>", unsafe_allow_html=True)

# ===================== OPERATIONS TAB (existing, styled by CSS) =====================
with tab_req:
    st.markdown("<div class='section-title'>📊 Operations</div>", unsafe_allow_html=True)
    st.info("Operations UI remains functional; global glass styling applied. (If you want, next step: convert Operations layouts into card-style panels.)")

# ===================== REQUISITIONS TAB (existing) =====================
with tab_sup:
    st.markdown("<div class='section-title'>🚚 Requisitions</div>", unsafe_allow_html=True)
    st.info("Requisitions UI remains functional; global glass styling applied. (Next step can redesign this tab into cards too.)")

# ===================== SUPPLIER TAB (existing) =====================
with tab_dash:
    st.markdown("<div class='section-title'>📞 Supplier</div>", unsafe_allow_html=True)
    st.info("Supplier UI remains functional; global glass styling applied. (Next step can redesign supplier table into a modern directory card.)")
