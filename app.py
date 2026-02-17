import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
import datetime
import uuid
import io

# -------------------------------------------------
# PAGE CONFIG
# -------------------------------------------------

st.set_page_config(
    page_title="Warehouse Pro Cloud v9",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# -------------------------------------------------
# ULTRA MODERN CSS (NO FEATURE CHANGES)
# -------------------------------------------------

st.markdown("""
<style>

/* Global */
.main {
    background: linear-gradient(135deg,#020617,#0f172a);
    color:white;
}

/* Header */
.modern-header {
    padding:18px;
    border-radius:14px;
    background: linear-gradient(90deg,#06b6d4,#3b82f6,#6366f1);
    box-shadow: 0 0 25px rgba(0,0,0,0.4);
    margin-bottom:15px;
}

/* KPI cards */
.kpi-card {
    background: linear-gradient(145deg,#020617,#111827);
    padding:16px;
    border-radius:14px;
    box-shadow:0 0 20px rgba(0,0,0,0.4);
    border:1px solid rgba(255,255,255,0.05);
    text-align:center;
    transition:0.3s;
}

.kpi-card:hover {
    transform:scale(1.03);
    box-shadow:0 0 25px rgba(59,130,246,0.6);
}

/* Glass cards */
.glass-card {
    background: rgba(255,255,255,0.04);
    backdrop-filter: blur(12px);
    border-radius:14px;
    padding:14px;
    border:1px solid rgba(255,255,255,0.05);
    margin-bottom:12px;
}

/* Buttons */
.stButton>button {
    background:linear-gradient(90deg,#06b6d4,#3b82f6);
    border:none;
    border-radius:10px;
    color:white;
    font-weight:600;
    padding:10px;
    transition:0.25s;
}

.stButton>button:hover {
    transform:scale(1.05);
    box-shadow:0 0 15px #3b82f6;
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg,#020617,#020617);
}

/* Activity log */
.log-card {
    padding:8px;
    border-radius:8px;
    margin-bottom:6px;
    background:#020617;
    border-left:4px solid #22c55e;
}

.log-undone {
    border-left:4px solid red;
    opacity:0.6;
}

</style>
""", unsafe_allow_html=True)

# -------------------------------------------------
# GOOGLE SHEETS CONNECTION (UNCHANGED)
# -------------------------------------------------

conn = st.connection("gsheets", type=GSheetsConnection)

# -------------------------------------------------
# DATA CLEAN FUNCTIONS (UNCHANGED)
# -------------------------------------------------

def clean_dataframe(df):

    if df is None or df.empty:
        return df

    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df = df.dropna(axis=1, how='all')
    df = df.loc[:, ~df.columns.duplicated()]
    df.columns = [str(col).strip() for col in df.columns]

    return df


def load_from_sheet(name, default_cols=None):

    try:
        df = conn.read(worksheet=name, ttl="2s")
        df = clean_dataframe(df)

        if df is None or df.empty:
            return pd.DataFrame(columns=default_cols)

        return df

    except:
        return pd.DataFrame(columns=default_cols)


def save_to_sheet(df, name):

    df = clean_dataframe(df)
    conn.update(worksheet=name, data=df)
    st.cache_data.clear()

# -------------------------------------------------
# CALC ENGINE (UNCHANGED)
# -------------------------------------------------

def recalculate_item(df, item):

    if item not in df["Product Name"].values:
        return df

    idx = df[df["Product Name"] == item].index[0]

    day_cols = [str(i) for i in range(1,32)]

    for col in day_cols:

        if col not in df.columns:
            df[col] = 0

        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    total = df.loc[idx, day_cols].sum()

    df.at[idx, "Total Received"] = total

    opening = pd.to_numeric(df.at[idx, "Opening Stock"], errors="coerce") or 0
    consumption = pd.to_numeric(df.at[idx, "Consumption"], errors="coerce") or 0

    closing = opening + total - consumption

    df.at[idx, "Closing Stock"] = closing

    if "Physical Count" in df.columns:

        phys = df.at[idx, "Physical Count"]

        if phys not in [None,""]:
            df.at[idx, "Variance"] = pd.to_numeric(phys) - closing

    return df


# -------------------------------------------------
# TRANSACTION SYSTEM (UNCHANGED)
# -------------------------------------------------

def apply_transaction(item, day, qty, is_undo=False):

    df = st.session_state.inventory

    idx = df[df["Product Name"] == item].index[0]

    col = str(day)

    if col not in df.columns:
        df[col] = 0

    df.at[idx, col] += qty

    if not is_undo:

        log = pd.DataFrame([{

            "LogID": str(uuid.uuid4())[:8],
            "Timestamp": datetime.datetime.now().strftime("%H:%M:%S"),
            "Item": item,
            "Qty": qty,
            "Day": day,
            "Status": "Active"

        }])

        logs = load_from_sheet("activity_logs",
            ["LogID","Timestamp","Item","Qty","Day","Status"])

        save_to_sheet(pd.concat([logs,log]), "activity_logs")

    df = recalculate_item(df,item)

    st.session_state.inventory = df

    save_to_sheet(df,"persistent_inventory")

    return True


def undo_entry(log_id):

    logs = load_from_sheet("activity_logs")

    idx = logs[logs["LogID"]==log_id].index[0]

    item = logs.at[idx,"Item"]
    qty = logs.at[idx,"Qty"]
    day = logs.at[idx,"Day"]

    apply_transaction(item,day,-qty,True)

    logs.at[idx,"Status"]="Undone"

    save_to_sheet(logs,"activity_logs")

    st.rerun()

# -------------------------------------------------
# SESSION INIT (UNCHANGED)
# -------------------------------------------------

if "inventory" not in st.session_state:

    st.session_state.inventory = load_from_sheet("persistent_inventory")

# -------------------------------------------------
# HEADER
# -------------------------------------------------

st.markdown(
    '<div class="modern-header"><h2>📦 Warehouse Pro Cloud v9 Ultra Modern</h2></div>',
    unsafe_allow_html=True)

# -------------------------------------------------
# KPI DASHBOARD (NEW UI ONLY)
# -------------------------------------------------

df = st.session_state.inventory

c1,c2,c3,c4 = st.columns(4)

total_items = len(df)
total_stock = df["Closing Stock"].sum() if not df.empty else 0
variance = df["Variance"].sum() if "Variance" in df.columns else 0
low_stock = len(df[df["Closing Stock"]<5]) if not df.empty else 0

c1.markdown(f'<div class="kpi-card">Products<br><h2>{total_items}</h2></div>',unsafe_allow_html=True)
c2.markdown(f'<div class="kpi-card">Stock<br><h2>{total_stock:.0f}</h2></div>',unsafe_allow_html=True)
c3.markdown(f'<div class="kpi-card">Low Stock<br><h2>{low_stock}</h2></div>',unsafe_allow_html=True)
c4.markdown(f'<div class="kpi-card">Variance<br><h2>{variance:.1f}</h2></div>',unsafe_allow_html=True)

# -------------------------------------------------
# TABS (UNCHANGED FEATURES)
# -------------------------------------------------

tab_ops, tab_req, tab_sup = st.tabs(
    ["Operations","Requisitions","Suppliers"]
)

# -------------------------------------------------
# OPERATIONS TAB (FULL FEATURES)
# -------------------------------------------------

with tab_ops:

    st.markdown('<div class="glass-card">',unsafe_allow_html=True)

    st.subheader("Daily Receipt")

    if not df.empty:

        c1,c2,c3 = st.columns(3)

        item = c1.selectbox(
            "Item",
            [""]+sorted(df["Product Name"].tolist())
        )

        day = c2.number_input("Day",1,31,datetime.datetime.now().day)

        qty = c3.number_input("Qty",0.0)

        if st.button("Confirm Receipt"):

            if item and qty>0:

                apply_transaction(item,day,qty)

                st.rerun()

    st.markdown('</div>',unsafe_allow_html=True)

    st.markdown('<div class="glass-card">',unsafe_allow_html=True)

    st.subheader("Live Stock Status")

    display_cols=[
        "Product Name","UOM","Opening Stock",
        "Total Received","Consumption",
        "Closing Stock","Physical Count","Variance"
    ]

    edited = st.data_editor(df[display_cols],use_container_width=True)

    if st.button("Save Changes"):

        df.update(edited)

        save_to_sheet(df,"persistent_inventory")

        st.rerun()

    st.markdown('</div>',unsafe_allow_html=True)

    # Activity log UI only updated visually

    st.markdown('<div class="glass-card">',unsafe_allow_html=True)

    st.subheader("Recent Activity")

    logs = load_from_sheet("activity_logs")

    if not logs.empty:

        for _,row in logs[::-1].head(10).iterrows():

            cls = "log-undone" if row["Status"]=="Undone" else "log-card"

            colA,colB = st.columns([4,1])

            colA.markdown(
                f'<div class="{cls}">{row["Item"]} : {row["Qty"]} ({row["Timestamp"]})</div>',
                unsafe_allow_html=True)

            if row["Status"]!="Undone":

                if colB.button("Undo",key=row["LogID"]):

                    undo_entry(row["LogID"])

    st.markdown('</div>',unsafe_allow_html=True)

# -------------------------------------------------
# REQUISITIONS TAB (UNCHANGED)
# -------------------------------------------------

with tab_req:

    meta = load_from_sheet("product_metadata")

    item = st.selectbox(
        "Product",
        [""]+meta["Product Name"].tolist()
        if not meta.empty else [""]
    )

    qty = st.number_input("Order Qty",0.0)

    if st.button("Add to Order"):

        orders = load_from_sheet(
            "orders_db",
            ["Product Name","Qty","Supplier","Status"]
        )

        sup = meta[meta["Product Name"]==item]["Supplier"].values[0]

        save_to_sheet(
            pd.concat([orders,pd.DataFrame([{
                "Product Name":item,
                "Qty":qty,
                "Supplier":sup,
                "Status":"Pending"
            }])]),
            "orders_db"
        )

        st.rerun()

    st.dataframe(load_from_sheet("orders_db"))

# -------------------------------------------------
# SUPPLIERS TAB (UNCHANGED)
# -------------------------------------------------

with tab_sup:

    meta = load_from_sheet("product_metadata")

    edited = st.data_editor(meta,num_rows="dynamic")

    if st.button("Save Directory"):

        save_to_sheet(edited,"product_metadata")

        st.rerun()

# -------------------------------------------------
# SIDEBAR (UNCHANGED)
# -------------------------------------------------

with st.sidebar:

    st.title("Cloud Control")

    if st.button("Reset Cache"):

        st.cache_data.clear()

        st.rerun()
