import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
import datetime
import uuid
import io

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

# --- PAGE CONFIG ---
st.set_page_config(page_title="Warehouse Pro Cloud v8.6", layout="wide", initial_sidebar_state="expanded")

# --- GLASSY UI THEME (Attio/Linear-inspired) ---
st.markdown(
    """
    <style>
    /* ===== Fonts ===== */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&family=JetBrains+Mono:wght@300;400;500&display=swap');

    :root{
        --bg: #0F1419;
        --panel: rgba(26, 31, 46, 0.62);
        --panel-2: rgba(26, 31, 46, 0.42);
        --border: rgba(255,255,255,0.08);
        --border-2: rgba(255,255,255,0.06);
        --text: rgba(224,231,255,0.92);
        --muted: rgba(136,146,176,0.95);
        --muted2: rgba(136,146,176,0.7);
        --accent: #00d9ff;
        --accent2: #0095ff;
        --warn: #ffaa00;
        --danger: #ff6b6b;
        --good: #2ee59d;
        --shadow: 0 18px 55px rgba(0,0,0,0.45);
        --radius: 14px;
        --radius-sm: 10px;
        --blur: 16px;
    }

    html, body, [class*="css"], [data-testid="stAppViewContainer"]{
        font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif !important;
    }
    code, pre, .stMarkdown code {
        font-family: "JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace !important;
        font-weight: 400 !important;
    }

    /* ===== Base layout ===== */
    .block-container{ padding-top: 0.9rem; padding-bottom: 1.0rem; max-width: 1400px; }
    .main { background: var(--bg); }
    [data-testid="stAppViewContainer"]{ background: var(--bg); }
    [data-testid="stHeader"]{ background: transparent; }
    [data-testid="stToolbar"]{ visibility: hidden; height: 0px; }
    footer{ visibility: hidden; }

    /* ===== Scrollbars ===== */
    ::-webkit-scrollbar { width: 10px; height: 10px; }
    ::-webkit-scrollbar-track { background: rgba(255,255,255,0.04); border-radius: 99px; }
    ::-webkit-scrollbar-thumb { background: rgba(0,217,255,0.18); border-radius: 99px; }
    ::-webkit-scrollbar-thumb:hover { background: rgba(0,217,255,0.28); }

    /* ===== Floating header ===== */
    .wp-header{
        background: linear-gradient(90deg, rgba(0,217,255,0.18) 0%, rgba(0,149,255,0.10) 100%);
        border: 1px solid var(--border);
        backdrop-filter: blur(var(--blur));
        -webkit-backdrop-filter: blur(var(--blur));
        border-radius: var(--radius);
        padding: 14px 18px;
        margin-bottom: 12px;
        box-shadow: var(--shadow);
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    .wp-header .title{
        font-size: 14px;
        letter-spacing: 0.12em;
        font-weight: 600;
        color: rgba(224,231,255,0.9);
        text-transform: uppercase;
    }
    .wp-header .subtitle{
        font-size: 12px;
        font-weight: 400;
        color: var(--muted);
        margin-top: 2px;
    }
    .wp-pill{
        display: inline-flex;
        align-items: center;
        gap: 8px;
        background: rgba(0,217,255,0.10);
        border: 1px solid rgba(0,217,255,0.18);
        color: var(--text);
        padding: 8px 12px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 500;
    }

    /* ===== Tabs ===== */
    .stTabs [data-baseweb="tab-list"]{
        gap: 6px;
        background: var(--panel);
        backdrop-filter: blur(var(--blur));
        -webkit-backdrop-filter: blur(var(--blur));
        padding: 6px;
        border-radius: var(--radius);
        border: 1px solid var(--border);
        box-shadow: 0 10px 25px rgba(0,0,0,0.25);
        margin-bottom: 12px;
    }
    .stTabs [data-baseweb="tab"]{
        padding: 6px 14px;
        font-weight: 500;
        color: var(--muted);
        border-radius: 12px;
        font-size: 13px;
        height: 40px;
        transition: all 160ms ease;
    }
    .stTabs [data-baseweb="tab"]:hover{
        color: rgba(224,231,255,0.95);
        background: rgba(255,255,255,0.04);
        transform: translateY(-1px);
    }
    .stTabs [aria-selected="true"]{
        color: var(--accent);
        background: rgba(0,217,255,0.10);
        border: 1px solid rgba(0,217,255,0.22);
        box-shadow: 0 8px 22px rgba(0,217,255,0.08);
    }

    /* ===== Section titles ===== */
    .section-title{
        color: var(--accent);
        font-size: 13px;
        font-weight: 600;
        margin: 8px 0 8px;
        display: block;
        border-bottom: 1px solid rgba(0,217,255,0.18);
        padding-bottom: 6px;
        letter-spacing: 0.01em;
    }

    /* ===== Glass cards ===== */
    .glass-card{
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: var(--radius);
        box-shadow: var(--shadow);
        backdrop-filter: blur(var(--blur));
        -webkit-backdrop-filter: blur(var(--blur));
        padding: 14px 14px 12px 14px;
        position: relative;
        overflow: hidden;
    }
    .glass-card:hover{
        border-color: rgba(0,217,255,0.20);
        box-shadow: 0 18px 65px rgba(0,0,0,0.55);
    }
    .glass-card::before{
        content:"";
        position:absolute;
        inset:-2px;
        background: radial-gradient(600px 220px at 25% 0%,
            rgba(0,217,255,0.10),
            rgba(0,0,0,0) 60%);
        opacity: 0.9;
        pointer-events:none;
    }
    .card-title{
        font-size: 13px;
        font-weight: 600;
        color: rgba(224,231,255,0.92);
        margin: 0 0 10px 0;
        letter-spacing: 0.01em;
        display:flex;
        align-items:center;
        justify-content:space-between;
        gap: 10px;
    }
    .card-title .meta{
        font-size: 11px;
        color: var(--muted);
        font-weight: 400;
    }

    /* ===== KPI cards ===== */
    .kpi-grid{ display:grid; grid-template-columns: repeat(4, 1fr); gap: 10px; }
    .kpi{
        background: rgba(255,255,255,0.03);
        border: 1px solid var(--border-2);
        border-radius: 14px;
        padding: 12px 12px 10px;
        position: relative;
        overflow:hidden;
        min-height: 74px;
    }
    .kpi .label{
        font-size: 11px;
        color: var(--muted);
        font-weight: 500;
        letter-spacing: 0.01em;
        display:flex;
        align-items:center;
        justify-content:space-between;
        gap: 10px;
    }
    .kpi .value{
        margin-top: 8px;
        font-size: 22px;
        font-weight: 600;
        color: var(--text);
        letter-spacing: 0.01em;
        font-family: "JetBrains Mono", ui-monospace, monospace !important;
    }
    .kpi .sub{
        margin-top: 2px;
        font-size: 11px;
        color: var(--muted2);
        font-weight: 400;
    }
    .kpi.good .value{ color: rgba(46,229,157,0.95); }
    .kpi.bad .value{ color: rgba(255,107,107,0.95); }
    .kpi.accent{
        border-color: rgba(0,217,255,0.20);
        background: rgba(0,217,255,0.06);
    }

    /* ===== Kebab (menu trigger) ===== */
    .kebab-wrap{ display:flex; align-items:center; gap:8px; }
    .kebab-hint{
        font-size: 11px;
        color: var(--muted2);
        font-weight: 400;
    }

    /* ===== Requisition item box ===== */
    .req-box{
        background: rgba(26, 47, 63, 0.65);
        border-left: 3px solid var(--warn);
        padding: 8px 10px;
        margin: 6px 0;
        border-radius: 10px;
        font-size: 13px;
        line-height: 1.35;
        border: 1px solid rgba(255,255,255,0.06);
        backdrop-filter: blur(var(--blur));
        -webkit-backdrop-filter: blur(var(--blur));
    }

    /* ===== Activity list ===== */
    .log-container{
        max-height: 320px;
        overflow-y: auto;
        padding-right: 6px;
        border-radius: var(--radius);
        background: var(--panel-2);
        border: 1px solid var(--border-2);
        backdrop-filter: blur(var(--blur));
        -webkit-backdrop-filter: blur(var(--blur));
    }
    .log-row{
        display:flex;
        justify-content:space-between;
        align-items:center;
        background: rgba(26,31,46,0.62);
        padding: 8px 10px;
        border-radius: 12px;
        margin: 6px;
        border-left: 3px solid var(--accent);
        border: 1px solid rgba(255,255,255,0.06);
        transition: all 150ms ease;
    }
    .log-row:hover{ transform: translateY(-1px); border-color: rgba(0,217,255,0.18); }
    .log-row-undone{ border-left: 3px solid var(--danger); opacity: 0.55; }
    .log-info{ font-size: 12px; color: var(--text); line-height: 1.25; }
    .log-time{ font-size: 11px; color: var(--muted); margin-left: 6px; }

    /* ===== Sidebar ===== */
    [data-testid="stSidebar"]{
        background: rgba(12,16,22,0.85);
        border-right: 1px solid rgba(255,255,255,0.06);
        backdrop-filter: blur(var(--blur));
        -webkit-backdrop-filter: blur(var(--blur));
    }
    .sidebar-title{
        color: rgba(224,231,255,0.92);
        font-weight: 600;
        font-size: 13px;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        margin-bottom: 8px;
    }

    /* ===== Buttons (global) ===== */
    .stButton>button, .stDownloadButton>button{
        border-radius: 12px !important;
        font-size: 12px !important;
        padding: 6px 12px !important;
        border: 1px solid rgba(255,255,255,0.10) !important;
        background: rgba(255,255,255,0.04) !important;
        color: rgba(224,231,255,0.92) !important;
        transition: all 150ms ease !important;
    }
    .stButton>button:hover, .stDownloadButton>button:hover{
        border-color: rgba(0,217,255,0.22) !important;
        background: rgba(0,217,255,0.08) !important;
        transform: translateY(-1px);
    }
    .stButton>button:focus{ box-shadow: 0 0 0 3px rgba(0,217,255,0.12) !important; }

    /* Primary buttons */
    .stButton>button[kind="primary"], .stButton>button[data-testid="baseButton-primary"]{
        background: linear-gradient(90deg, rgba(0,217,255,0.25), rgba(0,149,255,0.18)) !important;
        border-color: rgba(0,217,255,0.28) !important;
    }

    /* Inputs */
    .stTextInput input, .stNumberInput input, .stSelectbox div[role="combobox"], .stDateInput input{
        background: rgba(255,255,255,0.03) !important;
        border: 1px solid rgba(255,255,255,0.10) !important;
        border-radius: 12px !important;
        color: rgba(224,231,255,0.92) !important;
        font-size: 12px !important;
    }
    .stTextInput label, .stNumberInput label, .stSelectbox label, .stDateInput label{
        font-size: 11px !important;
        color: var(--muted) !important;
        font-weight: 500 !important;
    }

    /* Dataframe / editor */
    [data-testid="stDataFrame"]{
        border-radius: var(--radius) !important;
        border: 1px solid rgba(255,255,255,0.08) !important;
        overflow:hidden !important;
    }

    /* Skeleton shimmer helper */
    .skeleton{
        background: linear-gradient(90deg, rgba(255,255,255,0.04) 0%, rgba(255,255,255,0.07) 50%, rgba(255,255,255,0.04) 100%);
        background-size: 200% 100%;
        animation: shimmer 1.1s ease-in-out infinite;
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 12px;
        height: 12px;
        margin: 8px 0;
    }
    @keyframes shimmer{
        0%{ background-position: 200% 0; }
        100%{ background-position: -200% 0; }
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
            # LogDate added for dashboard filtering (new logs only)
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
            logs_df = load_from_sheet("activity_logs", ["LogID", "Timestamp", "Item", "Qty", "Day", "Status", "LogDate"])
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

# --- MODALS ---
@st.dialog("🗂️ Manage Categories")
def manage_categories_modal():
    st.subheader("🗂️ Category Manager")

    meta_df = load_from_sheet("product_metadata")
    existing_categories = []
    if not meta_df.empty and "Category" in meta_df.columns:
        all_cats = meta_df["Category"].dropna().unique().tolist()
        existing_categories = sorted(
            [cat for cat in all_cats if not str(cat).startswith("CATEGORY_") and cat != "Supplier_Master" and cat != "General"]
        )

    tab1, tab2, tab3 = st.tabs(["➕ Add", "✏️ Modify", "🗑️ Delete"])

    with tab1:
        st.subheader("Add New Category")
        category_name = st.text_input("📌 Category Name", placeholder="e.g., Vegetables, Grains, Dairy", key="cat_add_name")
        description = st.text_area("📝 Description", placeholder="Brief description of this category", height=60, key="cat_add_desc")

        if st.button("✅ Add Category", use_container_width=True, type="primary", key="add_cat_confirm"):
            if not category_name or not category_name.strip():
                st.error("❌ Please fill in Category Name")
                return

            category_name = category_name.strip()
            if category_name in existing_categories:
                st.error(f"❌ Category '{category_name}' already exists!")
                return

            new_category = pd.DataFrame(
                [
                    {
                        "Product Name": f"CATEGORY_{category_name}",
                        "UOM": "",
                        "Supplier": "",
                        "Contact": "",
                        "Email": "",
                        "Category": category_name,
                        "Lead Time": "",
                        "Price": 0,
                        "Currency": "",
                    }
                ]
            )
            meta_df = pd.concat([meta_df, new_category], ignore_index=True)

            if save_to_sheet(meta_df, "product_metadata"):
                st.success(f"✅ Category '{category_name}' added successfully!")
                st.balloons()
                st.rerun()
            else:
                st.error("❌ Failed to save category")

    with tab2:
        st.subheader("Modify Category")
        if existing_categories:
            selected_cat = st.selectbox("Select Category to Modify", existing_categories, key="cat_modify_select")

            cat_records = meta_df[meta_df["Category"] == selected_cat]
            current_desc = ""
            if not cat_records.empty:
                current_desc = cat_records.iloc[0].get("Product Name", "").replace(f"CATEGORY_{selected_cat}", "").strip()

            new_name = st.text_input("📌 New Category Name", value=selected_cat, key="cat_new_name")
            new_desc = st.text_area("📝 New Description", value=current_desc, height=60, key="cat_new_desc")

            if st.button("✅ Update Category", use_container_width=True, type="primary", key="modify_cat_confirm"):
                if not new_name or not new_name.strip():
                    st.error("❌ Please fill in Category Name")
                    return

                new_name = new_name.strip()
                if new_name != selected_cat and new_name in existing_categories:
                    st.error(f"❌ Category '{new_name}' already exists!")
                    return

                meta_df.loc[meta_df["Category"] == selected_cat, "Category"] = new_name
                for idx in meta_df[meta_df["Category"] == new_name].index:
                    if str(meta_df.at[idx, "Product Name"]).startswith("CATEGORY_"):
                        meta_df.at[idx, "Product Name"] = f"CATEGORY_{new_name}"

                if save_to_sheet(meta_df, "product_metadata"):
                    st.success(f"✅ Category '{selected_cat}' renamed to '{new_name}'!")
                    st.balloons()
                    st.rerun()
                else:
                    st.error("❌ Failed to update category")
        else:
            st.info("📭 No categories to modify")

    with tab3:
        st.subheader("Delete Category")
        if existing_categories:
            selected_cat = st.selectbox("Select Category to Delete", existing_categories, key="cat_delete_select")

            cat_usage = meta_df[meta_df["Category"] == selected_cat]
            product_count = len(cat_usage[~cat_usage["Product Name"].str.startswith("CATEGORY_", na=False)])

            if product_count > 0:
                st.warning(f"⚠️ This category is used by {product_count} product(s). Products will be reassigned to 'General'.")

            if st.button("🗑️ Delete Category", use_container_width=True, type="secondary", key="delete_cat_confirm"):
                meta_df.loc[
                    (meta_df["Category"] == selected_cat) & (~meta_df["Product Name"].str.startswith("CATEGORY_", na=False)), "Category"
                ] = "General"
                meta_df = meta_df[~meta_df["Product Name"].str.startswith(f"CATEGORY_{selected_cat}", na=False)]

                if save_to_sheet(meta_df, "product_metadata"):
                    st.success(f"✅ Category '{selected_cat}' deleted successfully!")
                    st.balloons()
                    st.rerun()
                else:
                    st.error("❌ Failed to delete category")
        else:
            st.info("📭 No categories to delete")

@st.dialog("➕ Add New Product")
def add_item_modal():
    st.subheader("📦 Product Details")
    col1, col2 = st.columns(2)
    with col1:
        name = st.text_input("📦 Item Name", placeholder="e.g., Tomato, Rice", key="item_name_input")
        uom = st.selectbox("📏 Unit of Measure", ["pcs", "kg", "box", "ltr", "pkt", "can", "bot", "bag", "carton"], key="uom_select")
    with col2:
        opening = st.number_input("📊 Opening Stock", min_value=0.0, value=0.0, key="opening_input")

        meta_df = load_from_sheet("product_metadata")
        category_list = ["General"]
        if not meta_df.empty and "Category" in meta_df.columns:
            all_cats = meta_df["Category"].dropna().unique().tolist()
            user_cats = [cat for cat in all_cats if not str(cat).startswith("CATEGORY_") and cat != "Supplier_Master"]
            if user_cats:
                category_list = sorted(set(user_cats))
            if "General" not in category_list:
                category_list.insert(0, "General")
        category = st.selectbox("🗂️ Category", category_list, key="cat_select")

    col3, col4 = st.columns(2)
    with col3:
        price = st.number_input("💵 Unit Price", min_value=0.0, value=0.0, step=0.01, key="price_input")
    with col4:
        currency = st.text_input("💱 Currency", value="USD", placeholder="e.g., USD, INR", key="currency_input")

    st.divider()
    st.subheader("🏭 Supplier Details")

    meta_df = load_from_sheet("product_metadata")
    existing_suppliers = []
    if not meta_df.empty and "Supplier" in meta_df.columns:
        all_suppliers = meta_df["Supplier"].dropna().unique().tolist()
        existing_suppliers = sorted([s for s in all_suppliers if s and str(s).strip()])

    supplier_choice = st.radio("Supplier Option:", ["Select Existing Supplier", "Create New Supplier"], horizontal=True, key="supp_choice")

    supplier = None
    contact = ""
    email = ""
    lead_time = ""

    if supplier_choice == "Select Existing Supplier":
        if existing_suppliers:
            supplier = st.selectbox("🏪 Choose Supplier", existing_suppliers, key="supp_select")
            if supplier:
                supplier_rows = meta_df[meta_df["Supplier"] == supplier]
                if not supplier_rows.empty:
                    current_data = supplier_rows.iloc[0]
                    contact = current_data.get("Contact", "")
                    email = current_data.get("Email", "")
                    lead_time = current_data.get("Lead Time", "")
                    st.info(f"✅ **Contact:** {contact}\n\n📧 **Email:** {email}\n\n⏱️ **Lead Time:** {lead_time}")
        else:
            st.warning("⚠️ No suppliers found. Please create a new one.")
            supplier = None
    else:
        supplier = st.text_input("🏪 New Supplier Name", placeholder="e.g., ABC Trading", key="new_supp_input")
        contact = st.text_input("📞 Contact / Phone", placeholder="e.g., +1-234-567-8900", key="contact_input")
        email = st.text_input("📧 Email", placeholder="e.g., supplier@abc.com", key="email_input")
        lead_time = st.text_input("🕐 Lead Time (days)", placeholder="e.g., 2-3", key="lead_time_input")

    if st.button("✅ Create Product", use_container_width=True, type="primary", key="create_prod_btn"):
        if not name or not name.strip():
            st.error("❌ Please fill in Product Name")
            return
        if not supplier or not supplier.strip():
            st.error("❌ Please fill in Supplier Name")
            return

        name = name.strip()
        supplier = supplier.strip()

        new_row = {str(i): 0.0 for i in range(1, 32)}
        new_row.update(
            {
                "Product Name": name,
                "UOM": uom,
                "Opening Stock": opening,
                "Total Received": 0.0,
                "Consumption": 0.0,
                "Closing Stock": opening,
                "Physical Count": None,
                "Variance": 0.0,
                "Category": category,
            }
        )
        st.session_state.inventory = pd.concat([st.session_state.inventory, pd.DataFrame([new_row])], ignore_index=True)
        save_to_sheet(st.session_state.inventory, "persistent_inventory")

        supplier_meta = pd.DataFrame(
            [
                {
                    "Product Name": name,
                    "UOM": uom,
                    "Supplier": supplier,
                    "Contact": contact,
                    "Email": email,
                    "Category": category,
                    "Lead Time": lead_time,
                    "Price": price,
                    "Currency": currency,
                }
            ]
        )
        meta_df = load_from_sheet("product_metadata")
        meta_df = pd.concat([meta_df, supplier_meta], ignore_index=True)
        save_to_sheet(meta_df, "product_metadata")

        st.success(f"✅ Product '{name}' created with supplier '{supplier}' at {currency} {price}!")
        st.balloons()
        st.rerun()

@st.dialog("➕ Add New Supplier")
def add_supplier_modal():
    st.subheader("🏭 Add New Supplier")

    supplier_name = st.text_input("🏪 Supplier Name", placeholder="e.g., ABC Trading", key="add_supp_name")
    contact = st.text_input("📞 Contact / Phone", placeholder="e.g., +1-234-567-8900", key="add_supp_contact")
    email = st.text_input("📧 Email", placeholder="e.g., supplier@abc.com", key="add_supp_email")

    if st.button("✅ Add Supplier", use_container_width=True, type="primary", key="add_supp_btn"):
        if not supplier_name or not supplier_name.strip():
            st.error("❌ Please fill in Supplier Name")
            return

        supplier_name = supplier_name.strip()
        meta_df = load_from_sheet("product_metadata")

        if not meta_df.empty and "Supplier" in meta_df.columns:
            existing = meta_df[meta_df["Supplier"] == supplier_name]
            if not existing.empty:
                st.error(f"❌ Supplier '{supplier_name}' already exists!")
                return

        supplier_entry = pd.DataFrame(
            [
                {
                    "Product Name": f"SUPPLIER_{supplier_name}",
                    "Supplier": supplier_name,
                    "Contact": contact,
                    "Email": email,
                    "Category": "Supplier_Master",
                    "UOM": "",
                    "Price": 0,
                    "Currency": "",
                    "Lead Time": "",
                }
            ]
        )

        meta_df = pd.concat([meta_df, supplier_entry], ignore_index=True)

        if save_to_sheet(meta_df, "product_metadata"):
            st.success(f"✅ Supplier '{supplier_name}' added successfully!")
            st.balloons()
            st.rerun()
        else:
            st.error("❌ Failed to save supplier")

@st.dialog("✏️ Update Supplier Details")
def update_supplier_modal(supplier_name):
    st.subheader(f"Update Supplier: {supplier_name}")

    meta_df = load_from_sheet("product_metadata")
    supplier_data = meta_df[meta_df["Supplier"] == supplier_name]

    if supplier_data.empty:
        st.error("Supplier not found")
        return

    current_data = supplier_data.iloc[0]

    contact = st.text_input("📞 Contact / Phone", value=str(current_data.get("Contact", "")), placeholder="e.g., +1-234-567-8900", key="upd_contact")
    email = st.text_input("📧 Email", value=str(current_data.get("Email", "")), placeholder="e.g., supplier@abc.com", key="upd_email")
    lead_time = st.text_input("🕐 Lead Time (days)", value=str(current_data.get("Lead Time", "")), placeholder="e.g., 2-3", key="upd_lead_time")

    if st.button("✅ Update Supplier", use_container_width=True, type="primary", key="upd_supp_btn"):
        meta_df.loc[meta_df["Supplier"] == supplier_name, "Contact"] = contact
        meta_df.loc[meta_df["Supplier"] == supplier_name, "Email"] = email
        meta_df.loc[meta_df["Supplier"] == supplier_name, "Lead Time"] = lead_time

        if save_to_sheet(meta_df, "product_metadata"):
            st.success(f"✅ Supplier '{supplier_name}' updated successfully!")
            st.balloons()
            st.rerun()
        else:
            st.error("❌ Failed to update supplier")

@st.dialog("📂 Archive Explorer")
def archive_explorer_modal():
    hist_df = load_from_sheet("monthly_history")
    if not hist_df.empty and "Month_Period" in hist_df.columns:
        selected_month = st.selectbox(
            "📅 Select Month Period",
            options=sorted(hist_df["Month_Period"].unique().tolist(), reverse=True),
            key="arch_month",
        )
        month_data = hist_df[hist_df["Month_Period"] == selected_month].drop(columns=["Month_Period"])
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
            month_data.to_excel(writer, index=False, sheet_name="Archive")
        st.download_button(
            label=f"📥 Download {selected_month}",
            data=buf.getvalue(),
            file_name=f"Inventory_{selected_month}.xlsx",
            use_container_width=True,
            type="primary",
        )
    else:
        st.info("📭 No records found.")

@st.dialog("🔒 Close Month & Rollover")
def close_month_modal():
    st.warning("⚠️ Physical Counts will become new Opening Stocks.")
    month_label = st.text_input("📅 Month Label", value=datetime.datetime.now().strftime("%b %Y"), key="month_label_input")
    if st.button("✅ Confirm Monthly Close", type="primary", use_container_width=True, key="close_month_btn"):
        df = st.session_state.inventory.copy()
        hist_df = load_from_sheet("monthly_history")
        archive_df = df.copy()
        archive_df["Month_Period"] = month_label
        save_to_sheet(pd.concat([hist_df, archive_df], ignore_index=True), "monthly_history")

        new_df = df.copy()
        for i in range(1, 32):
            new_df[str(i)] = 0.0
        for idx, row in new_df.iterrows():
            phys = row.get("Physical Count")
            new_df.at[idx, "Opening Stock"] = pd.to_numeric(phys) if pd.notna(phys) and str(phys).strip() != "" else row["Closing Stock"]

        new_df["Total Received"] = 0.0
        new_df["Consumption"] = 0.0
        new_df["Closing Stock"] = new_df["Opening Stock"]
        new_df["Physical Count"] = None
        new_df["Variance"] = 0.0
        save_to_sheet(new_df, "persistent_inventory")
        st.rerun()

# --- DASHBOARD HELPERS ---
TOP_15_CURRENCIES_PLUS_BHD = [
    "USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF", "CNY", "HKD", "SGD",
    "INR", "AED", "SAR", "KWD", "BHD",
]

def _currency_filtered_meta(meta_df, selected_currency):
    if meta_df is None or meta_df.empty:
        return meta_df
    if selected_currency == "All":
        return meta_df
    if "Currency" not in meta_df.columns:
        return meta_df.iloc[0:0]
    return meta_df[meta_df["Currency"].astype(str).str.upper() == str(selected_currency).upper()]

def _prepare_metadata():
    meta_df = load_from_sheet("product_metadata")
    if meta_df.empty:
        return meta_df

    for col in [
        "Product Name",
        "Category",
        "Price",
        "Currency",
        "Lead Time",
        "Reorder Qty",
        "Min Safety Stock",
        "Min Stock",
        "UOM",
        "Supplier",
    ]:
        if col not in meta_df.columns:
            meta_df[col] = None

    meta_df["Product Name"] = meta_df["Product Name"].astype(str).str.strip()
    meta_df["Category"] = meta_df["Category"].fillna("General").astype(str).str.strip()

    meta_df = meta_df[
        (~meta_df["Product Name"].str.startswith("CATEGORY_", na=False))
        & (~meta_df["Product Name"].str.startswith("SUPPLIER_", na=False))
    ]

    meta_df["Price"] = pd.to_numeric(meta_df["Price"], errors="coerce").fillna(0.0)
    meta_df["Lead Time"] = pd.to_numeric(meta_df["Lead Time"], errors="coerce").fillna(0.0)

    if "Reorder Qty" in meta_df.columns:
        meta_df["Reorder Qty"] = pd.to_numeric(meta_df["Reorder Qty"], errors="coerce").fillna(0.0)
    if "Min Safety Stock" in meta_df.columns:
        meta_df["Min Safety Stock"] = pd.to_numeric(meta_df["Min Safety Stock"], errors="coerce").fillna(0.0)
    if "Min Stock" in meta_df.columns:
        meta_df["Min Stock"] = pd.to_numeric(meta_df["Min Stock"], errors="coerce").fillna(0.0)

    meta_df["Currency"] = meta_df["Currency"].fillna("").astype(str).str.upper().str.strip()
    meta_df["UOM"] = meta_df["UOM"].fillna("").astype(str).str.strip()
    meta_df["Supplier"] = meta_df["Supplier"].fillna("").astype(str).str.strip()
    return meta_df

def _prepare_inventory(inv_df):
    if inv_df is None or inv_df.empty:
        return inv_df
    inv_df = inv_df.copy()
    for col in ["Product Name", "Category", "Closing Stock", "UOM"]:
        if col not in inv_df.columns:
            inv_df[col] = None
    inv_df["Product Name"] = inv_df["Product Name"].astype(str).str.strip()
    inv_df["Category"] = inv_df["Category"].fillna("General").astype(str).str.strip()
    inv_df["Closing Stock"] = pd.to_numeric(inv_df["Closing Stock"], errors="coerce").fillna(0.0)
    inv_df["UOM"] = inv_df["UOM"].fillna("").astype(str).str.strip()
    inv_df = inv_df[~inv_df["Product Name"].str.startswith("CATEGORY_", na=False)]
    return inv_df

def _prepare_reqs(req_df):
    if req_df is None or req_df.empty:
        return req_df
    df = req_df.copy()
    for col in ["Restaurant", "Item", "Qty", "DispatchQty", "Status", "RequestedDate", "Timestamp"]:
        if col not in df.columns:
            df[col] = None
    df["Restaurant"] = df["Restaurant"].fillna("").astype(str).str.strip()
    df["Item"] = df["Item"].fillna("").astype(str).str.strip()
    df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0.0)
    df["DispatchQty"] = pd.to_numeric(df["DispatchQty"], errors="coerce").fillna(0.0)
    df["Status"] = df["Status"].fillna("").astype(str).str.strip()
    df["RequestedDate"] = pd.to_datetime(df["RequestedDate"], errors="coerce").dt.date
    df["DispatchTS_Date"] = pd.to_datetime(df["Timestamp"], errors="coerce").dt.date
    return df

def _prepare_logs(log_df):
    """
    forces LogDateParsed as python datetime.date to compare with st.date_input values.
    """
    if log_df is None or log_df.empty:
        return log_df
    df = log_df.copy()
    for col in ["Item", "Qty", "Status", "LogDate", "Timestamp"]:
        if col not in df.columns:
            df[col] = None

    df["Item"] = df["Item"].fillna("").astype(str).str.strip()
    df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0.0)
    df["Status"] = df["Status"].fillna("").astype(str).str.strip()

    logdate = pd.to_datetime(df["LogDate"], errors="coerce")
    ts_fallback = pd.to_datetime(df["Timestamp"], errors="coerce")
    combined = logdate.fillna(ts_fallback)

    df["LogDateParsed"] = combined.dt.date  # python date
    return df

def _to_excel_bytes(sheets: dict):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        for name, df in sheets.items():
            safe_name = (str(name)[:31] if name else "Sheet")
            if df is None:
                continue
            (df if isinstance(df, pd.DataFrame) else pd.DataFrame(df)).to_excel(writer, index=False, sheet_name=safe_name)
    return buf.getvalue()

def _make_bar_chart(df, x_col, y_col):
    """
    Streamlit bar_chart may reorder categories (often alphabetically).
    Use Plotly for stable ordering and correct High→Low / Low→High display.
    """
    if df is None or df.empty or x_col not in df.columns or y_col not in df.columns:
        st.info("📭 No data for chart.")
        return

    chart_df = df[[x_col, y_col]].copy()
    chart_df[y_col] = pd.to_numeric(chart_df[y_col], errors="coerce").fillna(0.0)

    try:
        import plotly.express as px  # type: ignore

        fig = px.bar(chart_df, x=x_col, y=y_col)
        fig.update_traces(marker_color="rgba(0,217,255,0.65)", marker_line_color="rgba(0,217,255,0.0)")
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="rgba(224,231,255,0.92)", family="Inter", size=12),
            xaxis=dict(
                categoryorder="array",
                categoryarray=chart_df[x_col].tolist(),
                showgrid=False,
                zeroline=False,
                tickfont=dict(size=10, color="rgba(136,146,176,0.95)"),
            ),
            yaxis=dict(showgrid=False, zeroline=False, tickfont=dict(size=10, color="rgba(136,146,176,0.95)")),
            margin=dict(l=10, r=10, t=10, b=10),
            height=360,
        )
        st.plotly_chart(fig, use_container_width=True)
    except Exception:
        chart_df = chart_df.set_index(x_col)
        st.bar_chart(chart_df, y=y_col)

def _make_pie_chart(df, label_col, value_col, top_n=None):
    """
    Pie should respect the Top-N user selection (top_n).
    If top_n is None, default to 10 (sane default for readability).
    """
    if df is None or df.empty or label_col not in df.columns or value_col not in df.columns:
        st.info("📭 No data for chart.")
        return

    if top_n is None:
        top_n = 10

    pie_df = df[[label_col, value_col]].copy()
    pie_df[value_col] = pd.to_numeric(pie_df[value_col], errors="coerce").fillna(0.0)

    pie_df = pie_df[pie_df[value_col] > 0].head(int(top_n))

    if pie_df.empty:
        st.info("📭 No non-zero values for pie chart.")
        return

    try:
        import plotly.express as px  # type: ignore
        fig = px.pie(pie_df, names=label_col, values=value_col, hole=0.38)
        fig.update_traces(textposition="inside", textinfo="percent+label")
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="rgba(224,231,255,0.92)", family="Inter", size=12),
            margin=dict(l=10, r=10, t=20, b=10),
            height=360,
            legend=dict(font=dict(size=10, color="rgba(136,146,176,0.95)")),
        )
        st.plotly_chart(fig, use_container_width=True)
    except Exception:
        st.info("Plotly not installed. Showing table instead (install: `pip install plotly`).")
        st.dataframe(pie_df, use_container_width=True, hide_index=True)

def _init_card_state(card_id: str, default_sort: str = "High → Low", default_topn: int = 10, default_view: str = "Quantity"):
    """
    Per-card settings stored in st.session_state.dash_cards[card_id]
    """
    if "dash_cards" not in st.session_state:
        st.session_state.dash_cards = {}

    if card_id not in st.session_state.dash_cards:
        st.session_state.dash_cards[card_id] = {
            "sort": default_sort,
            "topn": default_topn,
            "view": default_view,  # Quantity/Value (used by some cards)
        }

def _card_controls(card_id: str, allow_view_mode: bool = False):
    """
    Kebab menu per card (⋮): sort, item count, optional view mode.
    """
    _init_card_state(card_id)
    state = st.session_state.dash_cards[card_id]

    with st.popover("⋮", use_container_width=False):
        st.caption("Card settings")
        state["sort"] = st.selectbox(
            "Sort order",
            options=["High → Low", "Low → High"],
            index=0 if state["sort"] == "High → Low" else 1,
            key=f"{card_id}_sort",
        )
        state["topn"] = st.selectbox(
            "Item count",
            options=[3, 5, 10, 25, 50, 100],
            index=[3, 5, 10, 25, 50, 100].index(state["topn"]) if state["topn"] in [3, 5, 10, 25, 50, 100] else 2,
            key=f"{card_id}_topn",
        )
        if allow_view_mode:
            state["view"] = st.selectbox(
                "View mode",
                options=["Quantity", "Value"],
                index=0 if state["view"] == "Quantity" else 1,
                key=f"{card_id}_view",
            )

        if st.button("Refresh card", key=f"{card_id}_refresh_btn"):
            st.cache_data.clear()
            st.rerun()

    # Ensure the dict reflects latest widget keys
    state["sort"] = st.session_state.get(f"{card_id}_sort", state["sort"])
    state["topn"] = st.session_state.get(f"{card_id}_topn", state["topn"])
    if allow_view_mode:
        state["view"] = st.session_state.get(f"{card_id}_view", state["view"])

    st.session_state.dash_cards[card_id] = state
    return state

def _sum_purchase_from_logs(logs_filtered: pd.DataFrame, meta_df: pd.DataFrame):
    """
    Purchase inferred from activity_logs (received qty) * unit price.
    Supplier comes from product_metadata.
    """
    if logs_filtered is None or logs_filtered.empty:
        return pd.DataFrame(columns=["Supplier", "Purchase Amount"])

    if meta_df is None or meta_df.empty:
        return pd.DataFrame(columns=["Supplier", "Purchase Amount"])

    # Join logs Item -> meta Product Name
    join_df = pd.merge(
        logs_filtered.rename(columns={"Item": "Product Name"})[["Product Name", "Qty"]],
        meta_df[["Product Name", "Supplier", "Price", "Currency"]],
        on="Product Name",
        how="left",
    )
    join_df["Price"] = pd.to_numeric(join_df["Price"], errors="coerce").fillna(0.0)
    join_df["Qty"] = pd.to_numeric(join_df["Qty"], errors="coerce").fillna(0.0)
    join_df["Supplier"] = join_df["Supplier"].fillna("Unknown").astype(str).str.strip().replace("", "Unknown")

    join_df["Purchase Amount"] = (join_df["Qty"] * join_df["Price"]).fillna(0.0)

    out = (
        join_df.groupby("Supplier", as_index=False)["Purchase Amount"]
        .sum()
        .sort_values("Purchase Amount", ascending=False)
    )
    return out

# --- INITIALIZATION ---
if "inventory" not in st.session_state:
    st.session_state.inventory = load_from_sheet("persistent_inventory")
if "log_page" not in st.session_state:
    st.session_state.log_page = 0

# --- HEADER ---
st.markdown(
    """
    <div class="wp-header">
        <div>
            <div class="title">Warehouse Pro Cloud</div>
            <div class="subtitle">v8.6 · Calm operational command center</div>
        </div>
        <div class="wp-pill">☁️ Live · Google Sheets</div>
    </div>
    """,
    unsafe_allow_html=True,
)

# --- Refresh row (kept, but styled by global CSS) ---
col_refresh, col_empty = st.columns([1, 5])
with col_refresh:
    if st.button("🔄 Refresh Data", use_container_width=True, key="refresh_all"):
        st.cache_data.clear()
        st.rerun()

tab_ops, tab_req, tab_sup, tab_dash = st.tabs(["📊 Operations", "🚚 Requisitions", "📞 Suppliers", "📊 Dashboard"])

# ===================== OPERATIONS TAB =====================
with tab_ops:
    col_receipt_main, col_quick_main = st.columns([3, 1])

    with col_receipt_main:
        st.markdown('<span class="section-title">📥 Daily Receipt Portal</span>', unsafe_allow_html=True)
        if not st.session_state.inventory.empty:
            c1, c2, c3, c4 = st.columns([2, 0.8, 0.8, 1])
            with c1:
                sel_item = st.selectbox(
                    "🔍 Item",
                    options=[""] + sorted(st.session_state.inventory["Product Name"].unique().tolist()),
                    key="receipt_item",
                    label_visibility="collapsed",
                )
            with c2:
                day_in = st.number_input("Day", 1, 31, datetime.datetime.now().day, key="receipt_day", label_visibility="collapsed")
            with c3:
                qty_in = st.number_input("Qty", min_value=0.0, key="receipt_qty", label_visibility="collapsed")
            with c4:
                if st.button("✅ Confirm", use_container_width=True, type="primary", key="receipt_confirm"):
                    if sel_item and qty_in > 0:
                        apply_transaction(sel_item, day_in, qty_in)
                        st.rerun()
        else:
            st.info("Initialize inventory first.")

    with col_quick_main:
        st.markdown('<span class="section-title">⚙️ Actions</span>', unsafe_allow_html=True)
        ac1, ac2, ac3, ac4 = st.columns(4)
        with ac1:
            if st.button("➕ Item", use_container_width=True, help="New Product", key="btn_add_item"):
                add_item_modal()
        with ac2:
            if st.button("🗂️ Cat", use_container_width=True, help="Manage Categories", key="btn_add_cat"):
                manage_categories_modal()
        with ac3:
            if st.button("📂 Exp", use_container_width=True, help="Explorer", key="btn_exp"):
                archive_explorer_modal()
        with ac4:
            if st.button("🔒 Close", use_container_width=True, type="primary", help="Close Month", key="btn_close"):
                close_month_modal()

    st.markdown("<hr>", unsafe_allow_html=True)

    log_col, stat_col = st.columns([1.2, 2.8])

    with log_col:
        st.markdown('<span class="section-title">📜 Activity</span>', unsafe_allow_html=True)
        logs = load_from_sheet("activity_logs")
        if not logs.empty:
            full_logs = logs.iloc[::-1]
            items_per_page = 6
            total_pages = (len(full_logs) - 1) // items_per_page + 1
            start_idx = st.session_state.log_page * items_per_page
            end_idx = start_idx + items_per_page
            current_logs = full_logs.iloc[start_idx:end_idx]

            st.markdown('<div class="log-container">', unsafe_allow_html=True)
            for _, row in current_logs.iterrows():
                is_undone = row.get("Status", "") == "Undone"
                row_class = "log-row-undone" if is_undone else ""

                c_row = st.container()
                c_txt, c_undo = c_row.columns([4, 1])
                with c_txt:
                    h_item, h_qty, h_day, h_time = row.get("Item", ""), row.get("Qty", ""), row.get("Day", ""), row.get("Timestamp", "")
                    l_html = (
                        f'<div class="log-row {row_class}"><div class="log-info"><b>{h_item}</b><br>'
                        f'{h_qty} | D{h_day} <span class="log-time">{h_time}</span></div></div>'
                    )
                    st.markdown(l_html, unsafe_allow_html=True)
                with c_undo:
                    if (not is_undone) and str(row.get("LogID", "")).strip():
                        if st.button("↩", key=f"rev_{row['LogID']}", use_container_width=True):
                            undo_entry(row["LogID"])
            st.markdown("</div>", unsafe_allow_html=True)

            p_prev, p_next = st.columns(2)
            with p_prev:
                if st.button("◀", disabled=st.session_state.log_page == 0, use_container_width=True, key="log_prev"):
                    st.session_state.log_page -= 1
                    st.rerun()
            with p_next:
                if st.button("▶", disabled=st.session_state.log_page >= total_pages - 1, use_container_width=True, key="log_next"):
                    st.session_state.log_page += 1
                    st.rerun()
        else:
            st.caption("📭 No logs.")

    with stat_col:
        st.markdown('<span class="section-title">📊 Live Stock Status</span>', unsafe_allow_html=True)
        df_status = st.session_state.inventory.copy()
        disp_cols = ["Product Name", "Category", "UOM", "Opening Stock", "Total Received", "Closing Stock", "Consumption", "Physical Count", "Variance"]
        for col in disp_cols:
            if col not in df_status.columns:
                df_status[col] = 0.0

        edited_df = st.data_editor(
            df_status[disp_cols],
            height=300,
            use_container_width=True,
            disabled=["Product Name", "Category", "UOM", "Total Received", "Closing Stock", "Variance"],
            hide_index=True,
        )

        sc1, sc2, sc3 = st.columns(3)
        with sc1:
            if st.button("💾 Update Stock", use_container_width=True, type="primary", key="update_stock"):
                df_status.update(edited_df)
                for item in df_status["Product Name"]:
                    df_status = recalculate_item(df_status, item)
                save_to_sheet(df_status, "persistent_inventory")
                st.rerun()
        with sc2:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
                df_status[disp_cols].to_excel(writer, index=False, sheet_name="Summary")
            st.download_button("📥 Summary", data=buf.getvalue(), file_name="Summary.xlsx", use_container_width=True, key="dl_summary")
        with sc3:
            day_cols = [str(i) for i in range(1, 32)]
            existing_day_cols = [col for col in day_cols if col in df_status.columns]
            full_cols = ["Product Name", "Category", "UOM", "Opening Stock"] + existing_day_cols + [
                "Total Received",
                "Consumption",
                "Closing Stock",
                "Physical Count",
                "Variance",
            ]
            full_cols = [col for col in full_cols if col in df_status.columns]

            if full_cols:
                buf_f = io.BytesIO()
                with pd.ExcelWriter(buf_f, engine="xlsxwriter") as writer:
                    df_status[full_cols].to_excel(writer, index=False, sheet_name="Details")
                st.download_button("📂 Details", data=buf_f.getvalue(), file_name="Full_Report.xlsx", use_container_width=True, key="dl_details")
            else:
                st.warning("⚠️ No data columns available for export")

    with st.expander("📈 Weekly Par Analysis", expanded=False):
        df_hist = load_from_sheet("monthly_history")
        if not df_hist.empty and not st.session_state.inventory.empty:
            df_hist["Consumption"] = pd.to_numeric(df_hist["Consumption"], errors="coerce").fillna(0)
            avg_cons = df_hist.groupby("Product Name")["Consumption"].mean().reset_index()
            df_par = pd.merge(
                st.session_state.inventory[["Product Name", "UOM", "Closing Stock"]],
                avg_cons,
                on="Product Name",
                how="left",
            ).fillna(0)
            df_par["Weekly Usage"] = (df_par["Consumption"] / 4.33).round(2)
            df_par["Min (50%)"] = (df_par["Weekly Usage"] * 0.5).round(2)
            df_par["Max (150%)"] = (df_par["Weekly Usage"] * 1.5).round(2)
            st.dataframe(df_par, use_container_width=True, hide_index=True, height=250)
        else:
            st.info("Historical data required.")

# ===================== REQUISITIONS TAB =====================
with tab_req:
    st.markdown('<span class="section-title">🚚 Restaurant Requisitions</span>', unsafe_allow_html=True)

    if st.button("🔄 Refresh Requisitions", use_container_width=True, key="refresh_reqs"):
        st.cache_data.clear()
        st.rerun()

    all_reqs = load_from_sheet(
        "restaurant_requisitions",
        ["ReqID", "Restaurant", "Item", "Qty", "Status", "DispatchQty", "Timestamp", "RequestedDate", "FollowupSent"],
    )

    if not all_reqs.empty:
        if "FollowupSent" not in all_reqs.columns:
            all_reqs["FollowupSent"] = False

        status_filter = st.selectbox("Filter by Status", ["All", "Pending", "Dispatched", "Completed"], key="req_status_filter", label_visibility="collapsed")

        display_reqs = all_reqs if status_filter == "All" else all_reqs[all_reqs["Status"] == status_filter]

        if not display_reqs.empty:
            display_reqs = display_reqs.copy()
            display_reqs["RequestedDate"] = pd.to_datetime(display_reqs["RequestedDate"], errors="coerce")
            display_reqs = display_reqs[display_reqs["RequestedDate"].notna()]

            if not display_reqs.empty:
                display_reqs = display_reqs.sort_values("RequestedDate", ascending=False)
                unique_dates = sorted(display_reqs["RequestedDate"].unique(), reverse=True)

                for req_date in unique_dates:
                    try:
                        date_str = pd.Timestamp(req_date).strftime("%d/%m/%Y")
                    except Exception:
                        date_str = "Unknown Date"

                    date_reqs = display_reqs[display_reqs["RequestedDate"] == req_date]

                    with st.expander(f"📅 {date_str} ({len(date_reqs)} items)", expanded=False):
                        restaurants = date_reqs["Restaurant"].unique()

                        for restaurant in restaurants:
                            rest_reqs = date_reqs[date_reqs["Restaurant"] == restaurant]
                            st.write(f"🏪 **{restaurant}** - {len(rest_reqs)} items")

                            for idx, row in rest_reqs.iterrows():
                                item_name = row["Item"]
                                req_qty = float(row["Qty"])
                                status = row["Status"]
                                dispatch_qty = float(row.get("DispatchQty", 0))
                                req_id = row["ReqID"]
                                remaining_qty = req_qty - dispatch_qty
                                followup_sent = row.get("FollowupSent", False)

                                stock_info = st.session_state.inventory[st.session_state.inventory["Product Name"] == item_name]
                                available_qty = float(stock_info["Closing Stock"].values[0]) if not stock_info.empty else 0.0

                                status_color = "🟡" if status == "Pending" else "🟠" if status == "Dispatched" else "🔵"
                                followup_text = " ⚠️" if followup_sent else ""

                                st.markdown(
                                    f"""
                                    <div class="req-box">
                                        <b>{status_color} {item_name}</b> | Req:{req_qty} | Got:{dispatch_qty} | Rem:{remaining_qty} | Avail:{available_qty}{followup_text}
                                    </div>
                                    """,
                                    unsafe_allow_html=True,
                                )

                                if status == "Pending":
                                    c1, c2, c3 = st.columns([2, 1, 1])
                                    with c1:
                                        default_dispatch = min(req_qty, available_qty)
                                        dispatch_qty_input = st.number_input(
                                            "Dispatch",
                                            min_value=0.0,
                                            max_value=available_qty,
                                            value=default_dispatch,
                                            key=f"dispatch_{req_id}",
                                            label_visibility="collapsed",
                                        )
                                    with c2:
                                        if st.button("🚀 Dispatch", key=f"dispatch_btn_{req_id}", use_container_width=True):
                                            if dispatch_qty_input > 0:
                                                all_reqs.at[idx, "DispatchQty"] = dispatch_qty_input
                                                all_reqs.at[idx, "Status"] = "Dispatched"
                                                all_reqs.at[idx, "Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                                if save_to_sheet(all_reqs, "restaurant_requisitions"):
                                                    st.success(f"✅ Dispatched {dispatch_qty_input}")
                                                    st.cache_data.clear()
                                                    st.rerun()
                                    with c3:
                                        if st.button("❌ Cancel", key=f"cancel_btn_{req_id}", use_container_width=True):
                                            all_reqs = all_reqs.drop(idx)
                                            save_to_sheet(all_reqs, "restaurant_requisitions")
                                            st.warning("❌ Cancelled")
                                            st.rerun()

                                elif status == "Dispatched":
                                    c1, c2 = st.columns(2)
                                    with c1:
                                        st.caption(f"✅ Dispatched: {dispatch_qty} | Rem: {remaining_qty}")
                                    with c2:
                                        if st.button("🚩 Follow-up", key=f"followup_{idx}", use_container_width=True):
                                            all_reqs.at[idx, "FollowupSent"] = True
                                            all_reqs.at[idx, "Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                            save_to_sheet(all_reqs, "restaurant_requisitions")
                                            st.success("✅ Follow-up sent!")
                                            st.rerun()
            else:
                st.info("📭 No valid dates found in requisitions")
        else:
            st.info(f"📭 No {status_filter.lower()} requisitions found")
    else:
        st.info("📭 No requisitions yet")

# ===================== SUPPLIERS TAB =====================
with tab_sup:
    st.markdown('<span class="section-title">📞 Supplier Directory</span>', unsafe_allow_html=True)

    col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 3])
    with col_btn1:
        if st.button("➕ Add Supplier", use_container_width=True, key="btn_add_supp"):
            add_supplier_modal()

    with col_btn2:
        meta_df = load_from_sheet("product_metadata")
        if not meta_df.empty and "Supplier" in meta_df.columns:
            all_suppliers = meta_df["Supplier"].dropna().unique().tolist()
            suppliers_list = sorted([s for s in all_suppliers if s and str(s).strip()])
            if suppliers_list:
                selected_supplier = st.selectbox("Select Supplier", suppliers_list, label_visibility="collapsed", key="upd_supp_select")
                if st.button("✏️ Update", use_container_width=True, key="btn_upd_supp"):
                    update_supplier_modal(selected_supplier)

    st.divider()

    meta = load_from_sheet("product_metadata")
    search = st.text_input("🔍 Filter...", placeholder="Item or Supplier...", key="sup_search")

    if not meta.empty:
        filtered = meta[
            ~meta["Product Name"].str.startswith("CATEGORY_", na=False)
            & ~meta["Product Name"].str.startswith("SUPPLIER_", na=False)
        ]
        if search:
            filtered = filtered[
                filtered["Product Name"].str.lower().str.contains(search.lower(), na=False)
                | filtered["Supplier"].str.lower().str.contains(search.lower(), na=False)
            ]
    else:
        filtered = meta

    if not filtered.empty:
        display_cols = [
            "Product Name",
            "Category",
            "Supplier",
            "Contact",
            "Email",
            "Price",
            "Currency",
            "Lead Time",
            "UOM",
            "Min Stock",
            "Reorder Qty",
            "Min Safety Stock",
        ]
        available_cols = [col for col in display_cols if col in filtered.columns]
        filtered_display = filtered[available_cols]
    else:
        filtered_display = filtered

    edited_meta = st.data_editor(filtered_display, num_rows="dynamic", use_container_width=True, hide_index=True, height=400, key="sup_editor")
    if st.button("💾 Save Directory", use_container_width=True, type="primary", key="save_sup_dir"):
        save_to_sheet(edited_meta, "product_metadata")
        st.rerun()

# ===================== DASHBOARD TAB =====================
with tab_dash:
    st.markdown('<span class="section-title">📊 Warehouse Dashboard</span>', unsafe_allow_html=True)

    # Keep global view + filters (do not remove anything), but the per-card Top/Sort is now in kebab menus
    v1, v2, v3, v4, v5 = st.columns([1.2, 1.2, 1.4, 1.2, 1.2])
    with v1:
        if st.button("🔄 Refresh", use_container_width=True, key="dash_refresh"):
            st.cache_data.clear()
            st.rerun()
    with v2:
        dashboard_view = st.selectbox(
            "View",
            options=["Tables", "Bar Charts", "Pie Charts"],
            index=0,
            key="dash_view_mode",
            label_visibility="collapsed",
        )
    with v3:
        reqs_tmp = load_from_sheet("restaurant_requisitions", ["Restaurant"])
        restaurants = []
        if not reqs_tmp.empty and "Restaurant" in reqs_tmp.columns:
            restaurants = sorted([r for r in reqs_tmp["Restaurant"].dropna().astype(str).str.strip().unique().tolist() if r])
        restaurant_filter = st.selectbox("Restaurant", options=["All"] + restaurants, index=0, key="dash_restaurant", label_visibility="collapsed")
    with v4:
        # Keep this global Top as a fallback (legacy); cards use kebab menu topn unless you want to remove later.
        legacy_top_n = st.selectbox("Top", options=[10, 25, 50, 100], index=0, key="dash_topn", label_visibility="collapsed")
    with v5:
        currency_choice = st.selectbox("Currency", options=["All"] + TOP_15_CURRENCIES_PLUS_BHD, index=0, key="dash_currency", label_visibility="collapsed")

    d1, d2, d3 = st.columns([1.6, 1.6, 2.8])
    with d1:
        today = datetime.date.today()
        default_start = today - datetime.timedelta(days=30)
        start_date = st.date_input("Start", value=default_start, key="dash_start", label_visibility="collapsed")
    with d2:
        end_date = st.date_input("End", value=today, key="dash_end", label_visibility="collapsed")
    with d3:
        dispatch_date_basis = st.selectbox("Dispatch date", ["RequestedDate", "Dispatch Timestamp"], key="dash_dispatch_basis", label_visibility="collapsed")

    if start_date > end_date:
        st.warning("⚠️ Start date is after end date. Please fix the date range.")
    else:
        start_date = pd.to_datetime(start_date).date()
        end_date = pd.to_datetime(end_date).date()

        # Data
        inv_df = _prepare_inventory(load_from_sheet("persistent_inventory"))
        meta_df = _prepare_metadata()
        req_df = _prepare_reqs(load_from_sheet("restaurant_requisitions"))
        log_df = _prepare_logs(load_from_sheet("activity_logs"))

        meta_cur = _currency_filtered_meta(meta_df, currency_choice)

        meta_all = meta_df.copy() if meta_df is not None else pd.DataFrame()
        if meta_all is None or meta_all.empty:
            meta_all = pd.DataFrame(columns=["Product Name", "UOM", "Category", "Price", "Currency", "Supplier"])

        inv_join = pd.merge(
            inv_df if inv_df is not None else pd.DataFrame(),
            meta_all[["Product Name", "Category", "UOM"]].drop_duplicates("Product Name"),
            on="Product Name",
            how="left",
        )
        inv_join = pd.merge(
            inv_join,
            (
                meta_cur[["Product Name", "Price", "Currency"]].drop_duplicates("Product Name")
                if meta_cur is not None and not meta_cur.empty
                else pd.DataFrame(columns=["Product Name", "Price", "Currency"])
            ),
            on="Product Name",
            how="left",
        )

        if "UOM_x" in inv_join.columns and "UOM_y" in inv_join.columns:
            inv_join["UOM"] = inv_join["UOM_y"].fillna("").astype(str).str.strip()
            inv_join.loc[inv_join["UOM"] == "", "UOM"] = inv_join["UOM_x"].fillna("").astype(str).str.strip()
            inv_join = inv_join.drop(columns=["UOM_x", "UOM_y"])
        else:
            if "UOM" not in inv_join.columns:
                inv_join["UOM"] = ""

        if "Category_x" in inv_join.columns and "Category_y" in inv_join.columns:
            inv_join["Category"] = inv_join["Category_y"].fillna("General").astype(str).str.strip()
            inv_join.loc[inv_join["Category"] == "", "Category"] = inv_join["Category_x"].fillna("General").astype(str).str.strip()
            inv_join = inv_join.drop(columns=["Category_x", "Category_y"])

        inv_join["Price"] = pd.to_numeric(inv_join.get("Price", 0), errors="coerce").fillna(0.0)
        inv_join["Closing Stock"] = pd.to_numeric(inv_join.get("Closing Stock", 0), errors="coerce").fillna(0.0)
        inv_join["Stock Value"] = (inv_join["Closing Stock"] * inv_join["Price"]).round(2)

        # Requisitions filter
        req_filtered = req_df.copy() if req_df is not None and not req_df.empty else pd.DataFrame(
            columns=["Restaurant", "Item", "Qty", "DispatchQty", "Status", "RequestedDate", "DispatchTS_Date"]
        )
        if not req_filtered.empty:
            if restaurant_filter != "All":
                req_filtered = req_filtered[req_filtered["Restaurant"] == restaurant_filter]

            date_col = "DispatchTS_Date" if dispatch_date_basis == "Dispatch Timestamp" else "RequestedDate"
            req_filtered = req_filtered[req_filtered[date_col].notna()]
            req_filtered = req_filtered[(req_filtered[date_col] >= start_date) & (req_filtered[date_col] <= end_date)]

        # Logs filter (Received)
        logs_filtered = log_df.copy() if log_df is not None and not log_df.empty else pd.DataFrame(columns=["Item", "Qty", "Status", "LogDateParsed"])
        if not logs_filtered.empty:
            logs_filtered = logs_filtered[logs_filtered["Status"] == "Active"]
            logs_filtered = logs_filtered[logs_filtered["LogDateParsed"].notna()]
            logs_filtered = logs_filtered[
                (logs_filtered["LogDateParsed"] >= start_date) & (logs_filtered["LogDateParsed"] <= end_date)
            ]

        total_ordered_qty = float(req_filtered["Qty"].sum()) if not req_filtered.empty else 0.0
        total_dispatched_qty = (
            float(req_filtered[req_filtered["Status"].isin(["Dispatched", "Completed"])]["DispatchQty"].sum()) if not req_filtered.empty else 0.0
        )
        total_received_qty = float(logs_filtered["Qty"].sum()) if not logs_filtered.empty else 0.0

        stock_inhand_qty = float(inv_join["Closing Stock"].sum()) if not inv_join.empty else 0.0
        stock_inhand_value = float(inv_join["Stock Value"].sum()) if not inv_join.empty else 0.0

        # --- Dashboard layout inspired by your screenshot (Image #5) ---
        left_col, right_col = st.columns([1.35, 1.65], gap="large")

        # ===== Left: 2x2 cards for Top Purchased/Selling (Qty/Value) =====
        with left_col:
            # Card 1: Top Purchased Product (QTY) -> uses logs (received)
            st.markdown('<div class="glass-card">', unsafe_allow_html=True)
            st.markdown('<div class="card-title">Top Purchased Product (QTY) <span class="meta">from receipts</span></div>', unsafe_allow_html=True)
            s = _card_controls("card_purchased_qty", allow_view_mode=False)
            asc = (s["sort"] == "Low → High")
            topn = int(s["topn"])

            purchased_qty = pd.DataFrame(columns=["Item", "Received Qty"])
            if not logs_filtered.empty:
                purchased_qty = (
                    logs_filtered.groupby("Item", as_index=False)["Qty"]
                    .sum()
                    .rename(columns={"Qty": "Received Qty"})
                    .sort_values("Received Qty", ascending=asc)
                    .head(topn)
                )

            if dashboard_view == "Tables":
                st.dataframe(purchased_qty, use_container_width=True, hide_index=True, height=320)
            elif dashboard_view == "Bar Charts":
                _make_bar_chart(purchased_qty, "Item", "Received Qty")
            else:
                _make_pie_chart(purchased_qty, "Item", "Received Qty", top_n=topn)
            st.markdown('</div>', unsafe_allow_html=True)

            # Card 2: Top Selling Product (QTY) -> requisitions dispatched qty
            st.markdown('<div class="glass-card" style="margin-top:12px;">', unsafe_allow_html=True)
            st.markdown('<div class="card-title">Top Selling Product (QTY) <span class="meta">from dispatch</span></div>', unsafe_allow_html=True)
            s = _card_controls("card_selling_qty", allow_view_mode=False)
            asc = (s["sort"] == "Low → High")
            topn = int(s["topn"])

            selling_qty = pd.DataFrame(columns=["Item", "Dispatched Qty"])
            if not req_filtered.empty:
                disp_only = req_filtered[req_filtered["Status"].isin(["Dispatched", "Completed"])]
                selling_qty = (
                    disp_only.groupby("Item", as_index=False)["DispatchQty"]
                    .sum()
                    .rename(columns={"DispatchQty": "Dispatched Qty"})
                    .sort_values("Dispatched Qty", ascending=asc)
                    .head(topn)
                )

            if dashboard_view == "Tables":
                st.dataframe(selling_qty, use_container_width=True, hide_index=True, height=320)
            elif dashboard_view == "Bar Charts":
                _make_bar_chart(selling_qty, "Item", "Dispatched Qty")
            else:
                _make_pie_chart(selling_qty, "Item", "Dispatched Qty", top_n=topn)
            st.markdown('</div>', unsafe_allow_html=True)

            # Card 3: Top Purchased Product (Value)
            st.markdown('<div class="glass-card" style="margin-top:12px;">', unsafe_allow_html=True)
            st.markdown('<div class="card-title">Top Purchased Product (Value) <span class="meta">Qty × Price</span></div>', unsafe_allow_html=True)
            s = _card_controls("card_purchased_val", allow_view_mode=False)
            asc = (s["sort"] == "Low → High")
            topn = int(s["topn"])

            purchased_val = pd.DataFrame(columns=["Item", "Purchase Value"])
            if not logs_filtered.empty and meta_df is not None and not meta_df.empty:
                tmp = pd.merge(
                    logs_filtered.rename(columns={"Item": "Product Name"})[["Product Name", "Qty"]],
                    meta_df[["Product Name", "Price"]],
                    on="Product Name",
                    how="left",
                )
                tmp["Qty"] = pd.to_numeric(tmp["Qty"], errors="coerce").fillna(0.0)
                tmp["Price"] = pd.to_numeric(tmp["Price"], errors="coerce").fillna(0.0)
                tmp["Purchase Value"] = tmp["Qty"] * tmp["Price"]
                purchased_val = (
                    tmp.groupby("Product Name", as_index=False)["Purchase Value"]
                    .sum()
                    .rename(columns={"Product Name": "Item"})
                    .sort_values("Purchase Value", ascending=asc)
                    .head(topn)
                )

            if dashboard_view == "Tables":
                st.dataframe(purchased_val, use_container_width=True, hide_index=True, height=320)
            elif dashboard_view == "Bar Charts":
                _make_bar_chart(purchased_val, "Item", "Purchase Value")
            else:
                _make_pie_chart(purchased_val, "Item", "Purchase Value", top_n=topn)
            st.markdown('</div>', unsafe_allow_html=True)

            # Card 4: Top Selling Product (Value)
            st.markdown('<div class="glass-card" style="margin-top:12px;">', unsafe_allow_html=True)
            st.markdown('<div class="card-title">Top Selling Product (Value) <span class="meta">DispatchQty × Price</span></div>', unsafe_allow_html=True)
            s = _card_controls("card_selling_val", allow_view_mode=False)
            asc = (s["sort"] == "Low → High")
            topn = int(s["topn"])

            selling_val = pd.DataFrame(columns=["Item", "Sales Value"])
            if not req_filtered.empty and meta_df is not None and not meta_df.empty:
                disp_only = req_filtered[req_filtered["Status"].isin(["Dispatched", "Completed"])][["Item", "DispatchQty"]].copy()
                disp_only["DispatchQty"] = pd.to_numeric(disp_only["DispatchQty"], errors="coerce").fillna(0.0)

                tmp = pd.merge(
                    disp_only.rename(columns={"Item": "Product Name"}),
                    meta_df[["Product Name", "Price"]],
                    on="Product Name",
                    how="left",
                )
                tmp["Price"] = pd.to_numeric(tmp["Price"], errors="coerce").fillna(0.0)
                tmp["Sales Value"] = tmp["DispatchQty"] * tmp["Price"]
                selling_val = (
                    tmp.groupby("Product Name", as_index=False)["Sales Value"]
                    .sum()
                    .rename(columns={"Product Name": "Item"})
                    .sort_values("Sales Value", ascending=asc)
                    .head(topn)
                )

            if dashboard_view == "Tables":
                st.dataframe(selling_val, use_container_width=True, hide_index=True, height=320)
            elif dashboard_view == "Bar Charts":
                _make_bar_chart(selling_val, "Item", "Sales Value")
            else:
                _make_pie_chart(selling_val, "Item", "Sales Value", top_n=topn)
            st.markdown('</div>', unsafe_allow_html=True)

        # ===== Right: Summary KPI + Total Purchase From Supplier =====
        with right_col:
            # Summary card (like screenshot)
            purchase_total_val = float(purchased_val["Purchase Value"].sum()) if not purchased_val.empty else float(_sum_purchase_from_logs(logs_filtered, meta_df)["Purchase Amount"].sum()) if meta_df is not None else 0.0
            sales_total_val = float(selling_val["Sales Value"].sum()) if not selling_val.empty else 0.0
            pnl = sales_total_val - purchase_total_val

            st.markdown('<div class="glass-card">', unsafe_allow_html=True)
            st.markdown('<div class="card-title">Summary <span class="meta">live</span></div>', unsafe_allow_html=True)
            _card_controls("card_summary", allow_view_mode=False)

            # KPI grid
            st.markdown('<div class="kpi-grid">', unsafe_allow_html=True)
            st.markdown(
                f"""
                <div class="kpi accent">
                    <div class="label">Purchase <span>🧾</span></div>
                    <div class="value">{purchase_total_val:.2f}</div>
                    <div class="sub">{("All currencies" if currency_choice=="All" else currency_choice)}</div>
                </div>
                <div class="kpi">
                    <div class="label">Sales <span>🪙</span></div>
                    <div class="value">{sales_total_val:.2f}</div>
                    <div class="sub">{("All currencies" if currency_choice=="All" else currency_choice)}</div>
                </div>
                <div class="kpi {"good" if pnl>=0 else "bad"}">
                    <div class="label">P&L <span>📈</span></div>
                    <div class="value">{pnl:.2f}</div>
                    <div class="sub">Sales − Purchase</div>
                </div>
                <div class="kpi accent">
                    <div class="label">Stock In Hand (Value) <span>📦</span></div>
                    <div class="value">{stock_inhand_value:.2f}</div>
                    <div class="sub">{("All currencies" if currency_choice=="All" else currency_choice)}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            st.markdown('</div>', unsafe_allow_html=True)  # kpi-grid
            st.markdown('</div>', unsafe_allow_html=True)  # glass-card

            # Total Purchase From Supplier (NEW)
            st.markdown('<div class="glass-card" style="margin-top:12px;">', unsafe_allow_html=True)
            st.markdown('<div class="card-title">Total Purchase From Supplier <span class="meta">Qty × Price</span></div>', unsafe_allow_html=True)
            s = _card_controls("card_supplier_purchase", allow_view_mode=False)
            asc = (s["sort"] == "Low → High")
            topn = int(s["topn"])

            supplier_df = _sum_purchase_from_logs(logs_filtered, meta_df)
            if not supplier_df.empty:
                supplier_df = supplier_df.sort_values("Purchase Amount", ascending=asc).head(topn)

            if dashboard_view == "Tables":
                st.dataframe(supplier_df, use_container_width=True, hide_index=True, height=360)
            else:
                # Render as bar chart even in Pie view for readability (supplier list style)
                if supplier_df.empty:
                    st.info("📭 No supplier purchase data.")
                else:
                    bar = supplier_df.rename(columns={"Supplier": "Supplier", "Purchase Amount": "Amount"})
                    _make_bar_chart(bar, "Supplier", "Amount")

            st.markdown('</div>', unsafe_allow_html=True)

            # Export button preserved (uses legacy_top_n datasets)
            st.markdown('<div class="glass-card" style="margin-top:12px;">', unsafe_allow_html=True)
            st.markdown('<div class="card-title">Export <span class="meta">dashboard</span></div>', unsafe_allow_html=True)

            # Legacy datasets for export (kept)
            export_bytes = _to_excel_bytes(
                {
                    "KPIs": pd.DataFrame(
                        [
                            {
                                "Start": start_date,
                                "End": end_date,
                                "Restaurant": restaurant_filter,
                                "Currency": currency_choice,
                                "Ordered Qty": total_ordered_qty,
                                "Dispatched Qty": total_dispatched_qty,
                                "Received Qty": total_received_qty,
                                "Stock In Hand Qty": stock_inhand_qty,
                                "Stock In Hand Value": stock_inhand_value,
                                "Purchase Total (approx)": purchase_total_val,
                                "Sales Total (approx)": sales_total_val,
                                "P&L (approx)": pnl,
                                "Dispatch Date Basis": dispatch_date_basis,
                            }
                        ]
                    ),
                    "Top Purchased Qty": purchased_qty,
                    "Top Selling Qty": selling_qty,
                    "Top Purchased Value": purchased_val,
                    "Top Selling Value": selling_val,
                    "Supplier Purchase": supplier_df,
                    "Stock In Hand Qty (Top)": inv_join[["Product Name", "UOM", "Closing Stock"]].sort_values("Closing Stock", ascending=False).head(int(legacy_top_n)) if not inv_join.empty else pd.DataFrame(),
                    "Stock In Hand Value (Top)": inv_join[["Product Name", "UOM", "Closing Stock", "Price", "Currency", "Stock Value"]].sort_values("Stock Value", ascending=False).head(int(legacy_top_n)) if not inv_join.empty else pd.DataFrame(),
                }
            )
            st.download_button(
                "📥 Export Dashboard (Excel)",
                data=export_bytes,
                file_name=f"Warehouse_Dashboard_{start_date}_to_{end_date}.xlsx",
                use_container_width=True,
                key="dash_export_excel",
                type="primary",
            )
            st.markdown(
                '<div class="dash-note">Note: Value metrics use the selected currency filter (no exchange-rate conversion). Items with Price=0 contribute 0 to value.</div>',
                unsafe_allow_html=True,
            )
            st.markdown('</div>', unsafe_allow_html=True)

    # --- Kanban (lightweight, Streamlit-native) ---
    # True drag & drop across columns needs a component (e.g., streamlit-elements / sortable).
    # This is a "pretty" MVP Kanban that keeps state and lets you move items via dropdowns (no feature removed).
    st.markdown('<span class="section-title">🧩 Operational Kanban (MVP)</span>', unsafe_allow_html=True)
    st.caption("Drag & drop across columns requires a Streamlit component; this MVP uses per-card move controls and preserves state.")

    if "kanban" not in st.session_state:
        st.session_state.kanban = {
            "⚠️ Attention Needed": [],
            "📦 Reorder Soon": [],
            "🚚 In Transit": [],
            "✅ Healthy Stock": [],
        }

    # Seed with low stock items (only once)
    if "kanban_seeded" not in st.session_state:
        st.session_state.kanban_seeded = True
        if inv_df is not None and not inv_df.empty:
            low_items = inv_df.sort_values("Closing Stock", ascending=True).head(8)["Product Name"].astype(str).tolist()
            st.session_state.kanban["⚠️ Attention Needed"] = [{"id": str(uuid.uuid4())[:8], "title": it, "note": "Low stock"} for it in low_items]

    kc1, kc2, kc3, kc4 = st.columns(4, gap="medium")
    columns = ["⚠️ Attention Needed", "📦 Reorder Soon", "🚚 In Transit", "✅ Healthy Stock"]
    col_map = dict(zip(columns, [kc1, kc2, kc3, kc4]))

    for col_name in columns:
        with col_map[col_name]:
            st.markdown(f'<div class="glass-card"><div class="card-title">{col_name} <span class="meta">{len(st.session_state.kanban[col_name])} cards</span></div>', unsafe_allow_html=True)

            cards = st.session_state.kanban[col_name]
            if not cards:
                st.markdown('<div class="skeleton"></div><div class="skeleton" style="width:70%"></div>', unsafe_allow_html=True)
            else:
                               else:
                    for i, c in enumerate(cards):
                        # --- Defensive: ensure each card is a dict ---
                        if isinstance(c, dict):
                            card = c
                        else:
                            # Handle legacy/bad state where cards might be strings or other objects
                            card = {"id": str(uuid.uuid4())[:8], "title": str(c), "note": ""}

                        title = str(card.get("title", "") or "")
                        cid = str(card.get("id", "") or "")
                        note = str(card.get("note", "") or "")

                        st.markdown(
                            f"""
                            <div class="kpi" style="margin-bottom:10px;">
                                <div class="label">{title} <span style="color:rgba(136,146,176,0.9); font-size:11px;">#{cid}</span></div>
                                <div class="sub">{note}</div>
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )

                        # Move control (uses safe cid)
                        new_col = st.selectbox(
                            "Move to",
                            options=columns,
                            index=columns.index(col_name),
                            key=f"kan_move_{col_name}_{cid}",
                            label_visibility="collapsed",
                        )
                        if new_col != col_name:
                            st.session_state.kanban[col_name] = [
                                x
                                for x in st.session_state.kanban[col_name]
                                if (x.get("id") if isinstance(x, dict) else None) != cid
                            ]
                            st.session_state.kanban[new_col].append(card)
                            st.success("Moved")
                            st.rerun()

                    # Move control
                    new_col = st.selectbox(
                        "Move to",
                        options=columns,
                        index=columns.index(col_name),
                        key=f"kan_move_{col_name}_{cid}",
                        label_visibility="collapsed",
                    )
                    if new_col != col_name:
                        # Remove from current column
                        st.session_state.kanban[col_name] = [
                            x
                            for x in st.session_state.kanban[col_name]
                            if (x.get("id") if isinstance(x, dict) else None) != cid
                        ]
                        # Add to new column
                        st.session_state.kanban[new_col].append(card)
                        st.success("Moved")
                        st.rerun()
                    # Move control
                    new_col = st.selectbox(
                        "Move to",
                        options=columns,
                        index=columns.index(col_name),
                        key=f"kan_move_{col_name}_{c['id']}",
                        label_visibility="collapsed",
                    )
                    if new_col != col_name:
                        # Move card
                        st.session_state.kanban[col_name] = [x for x in st.session_state.kanban[col_name] if x["id"] != c["id"]]
                        st.session_state.kanban[new_col].append(c)
                        st.success("Moved")
                        st.rerun()

            # Add card
            with st.expander("➕ Add card", expanded=False):
                title = st.text_input("Title", key=f"kan_new_title_{col_name}")
                note = st.text_input("Note", key=f"kan_new_note_{col_name}")
                if st.button("Add", key=f"kan_add_{col_name}", use_container_width=True):
                    if title.strip():
                        st.session_state.kanban[col_name].append({"id": str(uuid.uuid4())[:8], "title": title.strip(), "note": note.strip()})
                        st.balloons()
                        st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)

# ===================== SIDEBAR =====================
with st.sidebar:
    st.markdown('<h2 class="sidebar-title">☁️ Data Management</h2>', unsafe_allow_html=True)

    if st.button("🔄 Refresh All Data", use_container_width=True, key="refresh_sidebar"):
        st.cache_data.clear()
        st.rerun()

    st.divider()

    with st.expander("📦 Inventory Master Sync"):
        inv_file = st.file_uploader("Upload XLSX/CSV", type=["csv", "xlsx"], key="inv_upload")
        if inv_file:
            try:
                raw = pd.read_excel(inv_file, skiprows=4, header=None) if inv_file.name.endswith(".xlsx") else pd.read_csv(inv_file, skiprows=4, header=None)
                new_inv = pd.DataFrame()
                new_inv["Product Name"] = raw[1]
                new_inv["UOM"] = raw[2]
                new_inv["Opening Stock"] = pd.to_numeric(raw[3], errors="coerce").fillna(0.0)
                for i in range(1, 32):
                    new_inv[str(i)] = 0.0
                new_inv["Total Received"] = 0.0
                new_inv["Consumption"] = 0.0
                new_inv["Closing Stock"] = new_inv["Opening Stock"]
                new_inv["Category"] = "General"

                if st.button("🚀 Push Inventory", type="primary", use_container_width=True, key="push_inv"):
                    save_to_sheet(new_inv.dropna(subset=["Product Name"]), "persistent_inventory")
                    st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

    with st.expander("📞 Supplier Metadata Sync"):
        meta_file = st.file_uploader("Upload Product Data", type=["csv", "xlsx"], key="meta_upload")
        if meta_file:
            try:
                new_meta = pd.read_excel(meta_file) if meta_file.name.endswith(".xlsx") else pd.read_csv(meta_file)
                if st.button("🚀 Push Metadata", type="primary", use_container_width=True, key="push_meta"):
                    save_to_sheet(new_meta, "product_metadata")
                    st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

    st.markdown("<hr>", unsafe_allow_html=True)
    if st.button("🗑️ Clear Cache", use_container_width=True, key="clear_cache"):
        st.cache_data.clear()
        st.rerun()
