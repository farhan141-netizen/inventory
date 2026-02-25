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

# --- GLOBAL THEME / CRAFT UI CSS ---
st.markdown(
    """
    <style>
    /* --- Base --- */
    :root{
      --bg: #0F1419;
      --panel: rgba(26, 31, 46, 0.68);
      --panel-2: rgba(26, 31, 46, 0.50);
      --stroke: rgba(255,255,255,0.07);
      --stroke-2: rgba(255,255,255,0.06);
      --text: #e6edf7;
      --muted: #9aa7b8;
      --muted2: #6f7a8a;
      --cyan: #00d9ff;
      --cyan2: #0095ff;
      --amber: #ffaa00;
      --red: #ff6b6b;
      --shadow: 0 18px 50px rgba(0,0,0,0.35);
      --shadow2: 0 10px 30px rgba(0,0,0,0.25);
      --r16: 16px;
      --r12: 12px;
      --blur: blur(16px);
    }

    * { box-sizing: border-box; }
    .main { background: var(--bg); }
    .block-container { padding-top: 0.85rem; padding-bottom: 1rem; }

    /* --- Header --- */
    .header-bar {
        background: linear-gradient(90deg, var(--cyan) 0%, var(--cyan2) 100%);
        border-radius: 14px;
        padding: 12px 18px;
        color: white;
        margin-bottom: 10px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        box-shadow: 0 10px 35px rgba(0, 217, 255, 0.18);
    }
    .header-bar h1 { font-size: 1.25em !important; margin: 0; font-weight: 800; letter-spacing: -0.02em; }
    .header-bar p { font-size: 0.8em; margin: 0; opacity: 0.92; }

    /* --- Tabs (glassy) --- */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: var(--panel);
        padding: 6px;
        border-radius: 14px;
        margin-bottom: 10px;
        border: 1px solid var(--stroke);
        backdrop-filter: var(--blur);
    }
    .stTabs [data-baseweb="tab"] {
        padding: 6px 14px;
        font-weight: 600;
        color: #aab4c3;
        border-radius: 10px;
        font-size: 0.86em;
        height: 40px;
        transition: all .18s ease;
    }
    .stTabs [data-baseweb="tab"]:hover { color: var(--text); background: rgba(0,217,255,0.08); }
    .stTabs [aria-selected="true"] {
        color: var(--cyan);
        background: rgba(0,217,255,0.10);
        border: 1px solid rgba(0,217,255,0.22);
        box-shadow: 0 10px 25px rgba(0,217,255,0.07) inset;
    }

    /* --- Existing components kept --- */
    .log-container {
        max-height: 300px;
        overflow-y: auto;
        padding-right: 5px;
        border-radius: 14px;
        background: var(--panel-2);
        border: 1px solid var(--stroke-2);
        backdrop-filter: var(--blur);
    }
    .log-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        background: rgba(26, 31, 46, 0.92);
        padding: 6px 10px;
        border-radius: 12px;
        margin-bottom: 6px;
        border-left: 3px solid var(--cyan);
        border: 1px solid var(--stroke-2);
    }
    .log-row-undone { border-left: 3px solid var(--red); opacity: 0.55; }
    .log-info { font-size: 0.78rem; color: var(--text); line-height: 1.15; }
    .log-time { font-size: 0.67rem; color: #9aa7b8; margin-left: 6px; }

    .section-title {
        color: var(--cyan);
        font-size: 1em;
        font-weight: 750;
        margin-bottom: 8px;
        margin-top: 2px;
        padding-bottom: 6px;
        border-bottom: 1px solid rgba(0,217,255,0.18);
        display: block;
        letter-spacing: -0.01em;
    }
    .sidebar-title { color: var(--cyan); font-weight: 800; font-size: 0.95em; margin-bottom: 8px; }

    .stButton>button { border-radius: 10px; font-size: 0.84em; padding: 5px 10px; transition: all 0.18s ease; }
    .stButton>button:hover { transform: translateY(-1px); }

    .req-box {
        background: rgba(26, 47, 63, 0.65);
        border-left: 3px solid var(--amber);
        padding: 8px 10px;
        margin: 6px 0;
        border-radius: 12px;
        font-size: 0.88em;
        line-height: 1.3;
        border: 1px solid var(--stroke-2);
        backdrop-filter: var(--blur);
    }

    /* --- DASHBOARD CRAFT SHELL --- */
    .dash-shell{
      border: 1px solid var(--stroke);
      border-radius: 18px;
      padding: 14px;
      background: radial-gradient(1200px 600px at 20% -10%, rgba(0,217,255,0.10), transparent 55%),
                  radial-gradient(900px 600px at 90% 0%, rgba(0,149,255,0.10), transparent 60%),
                  rgba(26, 31, 46, 0.22);
      backdrop-filter: var(--blur);
      box-shadow: var(--shadow);
    }

    .dash-topbar{
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap: 12px;
      border-radius: 16px;
      padding: 12px 14px;
      background: var(--panel);
      border: 1px solid var(--stroke);
      backdrop-filter: var(--blur);
      box-shadow: var(--shadow2);
      margin-bottom: 12px;
    }

    .dash-greet{
      display:flex;
      align-items:center;
      gap: 10px;
    }
    .dash-dot{
      width:10px;height:10px;border-radius:999px;
      background: rgba(45,182,124,1);
      box-shadow: 0 0 0 5px rgba(45,182,124,0.12);
      display:inline-block;
      flex: 0 0 auto;
    }
    .dash-greet h2{
      margin:0;
      font-size: 1.05rem;
      letter-spacing: -0.02em;
      font-weight: 700;
      color: var(--text);
    }
    .dash-greet p{
      margin:0;
      font-size: 0.78rem;
      font-weight: 400;
      color: var(--muted);
    }

    .dash-card{
      position: relative;
      border-radius: 18px;
      padding: 12px 12px 10px 12px;
      background: var(--panel);
      border: 1px solid var(--stroke);
      backdrop-filter: var(--blur);
      box-shadow: var(--shadow2);
      overflow: hidden;
      transition: transform .16s ease, box-shadow .16s ease, border-color .16s ease;
    }
    .dash-card:hover{
      transform: translateY(-1px);
      border-color: rgba(0,217,255,0.18);
      box-shadow: 0 20px 55px rgba(0,0,0,0.35);
    }

    .dash-card-header{
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap: 10px;
      margin-bottom: 8px;
    }
    .dash-card-title{
      display:flex;
      flex-direction:column;
      gap: 2px;
      min-width: 0;
    }
    .dash-card-title h3{
      margin:0;
      font-size: 0.92rem;
      font-weight: 700;
      color: var(--text);
      letter-spacing: -0.01em;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .dash-card-title small{
      color: var(--muted2);
      font-size: 0.74rem;
      font-weight: 400;
    }

    /* kebab visibility on hover */
    .kebab-wrap{
      opacity: 0.0;
      transition: opacity .14s ease;
    }
    .dash-card:hover .kebab-wrap{ opacity: 1.0; }

    /* KPI style */
    .kpi-row{
      display:grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 10px;
      margin: 10px 0 12px 0;
    }
    .kpi{
      border-radius: 16px;
      padding: 12px;
      background: rgba(15,20,25,0.34);
      border: 1px solid var(--stroke);
      backdrop-filter: var(--blur);
    }
    .kpi .label{ font-size: 0.74rem; color: var(--muted); font-weight: 500; margin-bottom: 6px; }
    .kpi .value{ font-size: 1.25rem; font-weight: 800; color: var(--text); letter-spacing: -0.03em; }
    .kpi .sub{ margin-top: 6px; font-size: 0.72rem; color: var(--muted2); font-weight: 400; }

    @media (max-width: 1200px){
      .kpi-row{ grid-template-columns: repeat(3, minmax(0, 1fr)); }
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

# --- MODALS (unchanged) ---
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

            new_name = st.text_input("📌 New Category Name", value=selected_cat, key="cat_new_name")

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
    """forces LogDateParsed as python datetime.date to compare with st.date_input values."""
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

    df["LogDateParsed"] = combined.dt.date
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

def _plotly_bar_keep_order(df, x_col, y_col, height=360):
    if df is None or df.empty or x_col not in df.columns or y_col not in df.columns:
        st.info("📭 No data for chart.")
        return
    d = df[[x_col, y_col]].copy()
    d[y_col] = pd.to_numeric(d[y_col], errors="coerce").fillna(0.0)

    try:
        import plotly.express as px  # type: ignore

        fig = px.bar(d, x=x_col, y=y_col)
        fig.update_layout(
            height=height,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(
                categoryorder="array",
                categoryarray=d[x_col].tolist(),
                showgrid=False,
                zeroline=False,
                tickfont=dict(color="#cbd5e1", size=10),
            ),
            yaxis=dict(showgrid=False, zeroline=False, tickfont=dict(color="#94a3b8", size=10)),
        )
        fig.update_traces(marker=dict(color="rgba(0,217,255,0.78)", line=dict(width=0), cornerradius=6))
        st.plotly_chart(fig, use_container_width=True)
    except Exception:
        d = d.set_index(x_col)
        st.bar_chart(d, y=y_col)

def _plotly_pie(df, label_col, value_col, top_n):
    if df is None or df.empty or label_col not in df.columns or value_col not in df.columns:
        st.info("📭 No data for chart.")
        return

    pie_df = df[[label_col, value_col]].copy()
    pie_df[value_col] = pd.to_numeric(pie_df[value_col], errors="coerce").fillna(0.0)
    pie_df = pie_df[pie_df[value_col] > 0].head(int(top_n))

    if pie_df.empty:
        st.info("📭 No non-zero values for pie chart.")
        return

    try:
        import plotly.express as px  # type: ignore

        fig = px.pie(pie_df, names=label_col, values=value_col, hole=0.42)
        fig.update_layout(
            height=360,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            legend=dict(font=dict(color="#cbd5e1", size=10)),
        )
        st.plotly_chart(fig, use_container_width=True)
    except Exception:
        st.dataframe(pie_df, use_container_width=True, hide_index=True)

def _ensure_card_state(card_id: str, defaults: dict):
    if "dash_cards" not in st.session_state:
        st.session_state.dash_cards = {}
    if card_id not in st.session_state.dash_cards:
        st.session_state.dash_cards[card_id] = defaults.copy()
    else:
        for k, v in defaults.items():
            st.session_state.dash_cards[card_id].setdefault(k, v)
    return st.session_state.dash_cards[card_id]

def _card_frame(title: str, subtitle: str, card_id: str, allow_export_df=None):
    """
    Creates a glassy card header with kebab popover.
    Returns (state, refresh_clicked, export_clicked, container)
    """
    defaults = {
        "sort": "High → Low",
        "top": 10,
        "view": "Quantity",
    }
    state = _ensure_card_state(card_id, defaults)

    refresh_clicked = False
    export_clicked = False

    st.markdown('<div class="dash-card">', unsafe_allow_html=True)
    header_left, header_right = st.columns([6, 1.2], vertical_alignment="center")
    with header_left:
        st.markdown(
            f"""
            <div class="dash-card-header">
              <div class="dash-card-title">
                <h3>{title}</h3>
                <small>{subtitle}</small>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with header_right:
        st.markdown('<div class="kebab-wrap">', unsafe_allow_html=True)
        with st.popover("⋮", use_container_width=True):
            state["sort"] = st.radio("Sort Order", ["High → Low", "Low → High"], horizontal=True, index=0 if state["sort"] == "High → Low" else 1, key=f"{card_id}_sort")
            state["top"] = st.selectbox("Item Count", [3, 5, 10, 25, 50, 100], index=[3, 5, 10, 25, 50, 100].index(int(state["top"])), key=f"{card_id}_top")
            state["view"] = st.radio("View Mode", ["Quantity", "Value"], horizontal=True, index=0 if state["view"] == "Quantity" else 1, key=f"{card_id}_view")

            refresh_clicked = st.button("Refresh Card", use_container_width=True, key=f"{card_id}_refresh_btn")
            if allow_export_df is not None:
                export_clicked = st.button("Export Card", use_container_width=True, key=f"{card_id}_export_btn")
        st.markdown("</div>", unsafe_allow_html=True)

    return state, refresh_clicked, export_clicked

# --- INITIALIZATION ---
if "inventory" not in st.session_state:
    st.session_state.inventory = load_from_sheet("persistent_inventory")

if "log_page" not in st.session_state:
    st.session_state.log_page = 0

# --- MAIN UI ---
st.markdown(
    """
    <div class="header-bar">
        <h1>📦 Warehouse Pro Cloud</h1>
        <p>v8.6 | Multi-Restaurant Distribution Hub</p>
    </div>
""",
    unsafe_allow_html=True,
)

# --- REFRESH BUTTON IN HEADER ---
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

# ===================== DASHBOARD TAB (CRAFT COMMAND CENTER) =====================
with tab_dash:
    st.markdown('<div class="dash-shell">', unsafe_allow_html=True)

    # Topbar
    left, right = st.columns([3.2, 1.2], vertical_alignment="center")
    with left:
        st.markdown(
            f"""
            <div class="dash-topbar">
              <div class="dash-greet">
                <span class="dash-dot"></span>
                <div>
                  <h2>Warehouse Command Center</h2>
                  <p>Calm power • glassy minimalism • stable charts</p>
                </div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with right:
        if st.button("🔄 Refresh", use_container_width=True, key="dash_refresh"):
            st.cache_data.clear()
            st.rerun()

    # Global filters (still present)
    v1, v2, v3, v4, v5 = st.columns([1.4, 1.1, 1.4, 1.0, 1.1])
    with v1:
        reqs_tmp = load_from_sheet("restaurant_requisitions", ["Restaurant"])
        restaurants = []
        if not reqs_tmp.empty and "Restaurant" in reqs_tmp.columns:
            restaurants = sorted([r for r in reqs_tmp["Restaurant"].dropna().astype(str).str.strip().unique().tolist() if r])
        restaurant_filter = st.selectbox("Restaurant", options=["All"] + restaurants, index=0, key="dash_restaurant")
    with v2:
        global_sort_dir = st.selectbox("Sort", ["High → Low", "Low → High"], key="dash_sort")
    with v3:
        dispatch_date_basis = st.selectbox("Dispatch date", ["RequestedDate", "Dispatch Timestamp"], key="dash_dispatch_basis")
    with v4:
        global_top_n = st.selectbox("Top", options=[10, 25, 50, 100], index=0, key="dash_topn")
    with v5:
        currency_choice = st.selectbox("Currency", options=["All"] + TOP_15_CURRENCIES_PLUS_BHD, index=0, key="dash_currency")

    d1, d2 = st.columns(2)
    with d1:
        today = datetime.date.today()
        default_start = today - datetime.timedelta(days=30)
        start_date = st.date_input("Start", value=default_start, key="dash_start")
    with d2:
        end_date = st.date_input("End", value=today, key="dash_end")

    if start_date > end_date:
        st.warning("⚠️ Start date is after end date. Please fix the date range.")
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        start_date = pd.to_datetime(start_date).date()
        end_date = pd.to_datetime(end_date).date()

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
            meta_all[["Product Name", "Category", "UOM", "Supplier"]].drop_duplicates("Product Name"),
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
            logs_filtered = logs_filtered[(logs_filtered["LogDateParsed"] >= start_date) & (logs_filtered["LogDateParsed"] <= end_date)]

        total_ordered_qty = float(req_filtered["Qty"].sum()) if not req_filtered.empty else 0.0
        total_dispatched_qty = float(req_filtered[req_filtered["Status"].isin(["Dispatched", "Completed"])]["DispatchQty"].sum()) if not req_filtered.empty else 0.0
        total_received_qty = float(logs_filtered["Qty"].sum()) if not logs_filtered.empty else 0.0

        stock_inhand_qty = float(inv_join["Closing Stock"].sum()) if not inv_join.empty else 0.0
        stock_inhand_value = float(inv_join["Stock Value"].sum()) if not inv_join.empty else 0.0
        net_flow = total_received_qty - total_dispatched_qty

        # Supplier purchase estimation (received logs × unit price) grouped by supplier
        supplier_purchase = pd.DataFrame(columns=["Supplier", "Currency", "Purchase Amount"])
        if not logs_filtered.empty and meta_df is not None and not meta_df.empty:
            meta_for_join = meta_df.copy()
            if currency_choice != "All":
                meta_for_join = meta_for_join[meta_for_join["Currency"].astype(str).str.upper() == str(currency_choice).upper()]
            meta_for_join = meta_for_join[["Product Name", "Supplier", "Price", "Currency"]].drop_duplicates("Product Name")

            tmp = logs_filtered.merge(meta_for_join, left_on="Item", right_on="Product Name", how="left")
            tmp["Price"] = pd.to_numeric(tmp.get("Price", 0), errors="coerce").fillna(0.0)
            tmp["Qty"] = pd.to_numeric(tmp.get("Qty", 0), errors="coerce").fillna(0.0)
            tmp["Supplier"] = tmp.get("Supplier", "").fillna("").astype(str).str.strip()
            tmp["Currency"] = tmp.get("Currency", "").fillna("").astype(str).str.upper().str.strip()
            tmp["Purchase Amount"] = (tmp["Qty"] * tmp["Price"]).fillna(0.0)

            tmp = tmp[tmp["Supplier"] != ""]
            if not tmp.empty:
                supplier_purchase = (
                    tmp.groupby(["Supplier", "Currency"], as_index=False)["Purchase Amount"]
                    .sum()
                    .sort_values("Purchase Amount", ascending=False)
                )

        # KPI Row
        st.markdown('<div class="kpi-row">', unsafe_allow_html=True)

        def kpi(label, value, sub=""):
            st.markdown(
                f"""
                <div class="kpi">
                  <div class="label">{label}</div>
                  <div class="value">{value}</div>
                  <div class="sub">{sub}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        kpi("Ordered Qty", f"{total_ordered_qty:.2f}")
        kpi("Dispatched Qty", f"{total_dispatched_qty:.2f}", "Consumption")
        kpi("Received Qty", f"{total_received_qty:.2f}")
        kpi("Net Flow", f"{net_flow:.2f}", "In - Out")
        kpi("Stock In Hand (Qty)", f"{stock_inhand_qty:.2f}")
        kpi("Stock In Hand (Value)", f"{stock_inhand_value:.2f}", ("All currencies" if currency_choice == "All" else currency_choice))
        st.markdown("</div>", unsafe_allow_html=True)

        # Top-N datasets computed with global defaults (cards can override)
        global_ascending = global_sort_dir == "Low → High"

        top_ordered = pd.DataFrame(columns=["Item", "Ordered Qty"])
        if not req_filtered.empty:
            top_ordered = (
                req_filtered.groupby("Item", as_index=False)["Qty"]
                .sum()
                .rename(columns={"Qty": "Ordered Qty"})
                .sort_values("Ordered Qty", ascending=global_ascending)
                .head(global_top_n)
            )

        top_dispatched = pd.DataFrame(columns=["Item", "Dispatched Qty"])
        if not req_filtered.empty:
            disp_only = req_filtered[req_filtered["Status"].isin(["Dispatched", "Completed"])]
            top_dispatched = (
                disp_only.groupby("Item", as_index=False)["DispatchQty"]
                .sum()
                .rename(columns={"DispatchQty": "Dispatched Qty"})
                .sort_values("Dispatched Qty", ascending=global_ascending)
                .head(global_top_n)
            )

        top_received = pd.DataFrame(columns=["Item", "Received Qty"])
        if not logs_filtered.empty:
            top_received = (
                logs_filtered.groupby("Item", as_index=False)["Qty"]
                .sum()
                .rename(columns={"Qty": "Received Qty"})
                .sort_values("Received Qty", ascending=global_ascending)
                .head(global_top_n)
            )

        top_stock_qty = pd.DataFrame(columns=["Product Name", "UOM", "Closing Stock"])
        if not inv_join.empty:
            top_stock_qty = (
                inv_join[["Product Name", "UOM", "Closing Stock"]]
                .sort_values("Closing Stock", ascending=global_ascending)
                .head(global_top_n)
            )

        top_stock_val = pd.DataFrame(columns=["Product Name", "UOM", "Closing Stock", "Price", "Currency", "Stock Value"])
        if not inv_join.empty:
            val_df = inv_join.copy()
            val_df = val_df[pd.to_numeric(val_df["Price"], errors="coerce").fillna(0.0) > 0]
            top_stock_val = (
                val_df[["Product Name", "UOM", "Closing Stock", "Price", "Currency", "Stock Value"]]
                .sort_values("Stock Value", ascending=global_ascending)
                .head(global_top_n)
            )

        # Dashboard View selector (kept simple, can be global)
        view_mode = st.selectbox("Dashboard View", ["Tables", "Bar Charts", "Pie Charts"], index=0, key="dash_view_mode")

        # --- Card grid row 1 ---
        c1, c2, c3 = st.columns([1.4, 1.0, 1.0], gap="small")

        # Card: Ordered
        with c1:
            state, refresh_clicked, export_clicked = _card_frame(
                "Ordered",
                "Top items ordered in selected period",
                "card_ordered",
                allow_export_df=top_ordered,
            )
            if refresh_clicked:
                st.cache_data.clear()
                st.rerun()

            card_top = int(state["top"])
            asc = state["sort"] == "Low → High"
            df = top_ordered.sort_values("Ordered Qty", ascending=asc).head(card_top)

            if export_clicked:
                st.download_button(
                    "Download CSV",
                    data=df.to_csv(index=False).encode("utf-8"),
                    file_name="ordered_card.csv",
                    mime="text/csv",
                    use_container_width=True,
                )

            if view_mode == "Tables":
                st.dataframe(df, use_container_width=True, hide_index=True, height=320)
            elif view_mode == "Bar Charts":
                _plotly_bar_keep_order(df, "Item", "Ordered Qty", height=340)
            else:
                _plotly_pie(df, "Item", "Ordered Qty", top_n=card_top)

            st.markdown("</div>", unsafe_allow_html=True)

        # Card: Dispatched
        with c2:
            state, refresh_clicked, export_clicked = _card_frame(
                "Dispatched",
                "Consumption via requisition dispatch",
                "card_dispatched",
                allow_export_df=top_dispatched,
            )
            if refresh_clicked:
                st.cache_data.clear()
                st.rerun()

            card_top = int(state["top"])
            asc = state["sort"] == "Low → High"
            df = top_dispatched.sort_values("Dispatched Qty", ascending=asc).head(card_top)

            if export_clicked:
                st.download_button(
                    "Download CSV",
                    data=df.to_csv(index=False).encode("utf-8"),
                    file_name="dispatched_card.csv",
                    mime="text/csv",
                    use_container_width=True,
                )

            if view_mode == "Tables":
                st.dataframe(df, use_container_width=True, hide_index=True, height=320)
            elif view_mode == "Bar Charts":
                _plotly_bar_keep_order(df, "Item", "Dispatched Qty", height=340)
            else:
                _plotly_pie(df, "Item", "Dispatched Qty", top_n=card_top)

            st.markdown("</div>", unsafe_allow_html=True)

        # Card: Received
        with c3:
            state, refresh_clicked, export_clicked = _card_frame(
                "Received",
                "Inbound receipts (activity logs)",
                "card_received",
                allow_export_df=top_received,
            )
            if refresh_clicked:
                st.cache_data.clear()
                st.rerun()

            card_top = int(state["top"])
            asc = state["sort"] == "Low → High"
            df = top_received.sort_values("Received Qty", ascending=asc).head(card_top)

            if export_clicked:
                st.download_button(
                    "Download CSV",
                    data=df.to_csv(index=False).encode("utf-8"),
                    file_name="received_card.csv",
                    mime="text/csv",
                    use_container_width=True,
                )

            if view_mode == "Tables":
                st.dataframe(df, use_container_width=True, hide_index=True, height=320)
            elif view_mode == "Bar Charts":
                _plotly_bar_keep_order(df, "Item", "Received Qty", height=340)
            else:
                _plotly_pie(df, "Item", "Received Qty", top_n=card_top)

            st.markdown("</div>", unsafe_allow_html=True)

        # --- Card grid row 2 ---
        c4, c5, c6 = st.columns([1.2, 1.2, 1.2], gap="small")

        # Card: Stock Qty
        with c4:
            state, refresh_clicked, export_clicked = _card_frame(
                "Stock In Hand (Qty)",
                "Closing stock by item",
                "card_stock_qty",
                allow_export_df=top_stock_qty,
            )
            if refresh_clicked:
                st.cache_data.clear()
                st.rerun()

            card_top = int(state["top"])
            asc = state["sort"] == "Low → High"
            df = top_stock_qty.sort_values("Closing Stock", ascending=asc).head(card_top)

            if export_clicked:
                st.download_button(
                    "Download CSV",
                    data=df.to_csv(index=False).encode("utf-8"),
                    file_name="stock_qty_card.csv",
                    mime="text/csv",
                    use_container_width=True,
                )

            if view_mode == "Tables":
                st.dataframe(df, use_container_width=True, hide_index=True, height=320)
            elif view_mode == "Bar Charts":
                _plotly_bar_keep_order(df, "Product Name", "Closing Stock", height=340)
            else:
                _plotly_pie(df, "Product Name", "Closing Stock", top_n=card_top)

            st.markdown("</div>", unsafe_allow_html=True)

        # Card: Stock Value
        with c5:
            state, refresh_clicked, export_clicked = _card_frame(
                "Stock In Hand (Value)",
                "Closing stock × unit price (no FX conversion)",
                "card_stock_val",
                allow_export_df=top_stock_val,
            )
            if refresh_clicked:
                st.cache_data.clear()
                st.rerun()

            card_top = int(state["top"])
            asc = state["sort"] == "Low → High"
            df = top_stock_val.sort_values("Stock Value", ascending=asc).head(card_top)

            if export_clicked:
                st.download_button(
                    "Download CSV",
                    data=df.to_csv(index=False).encode("utf-8"),
                    file_name="stock_value_card.csv",
                    mime="text/csv",
                    use_container_width=True,
                )

            # view toggle for this card: qty vs value doesn't apply, but keep as-is.
            if view_mode == "Tables":
                st.dataframe(df, use_container_width=True, hide_index=True, height=320)
            elif view_mode == "Bar Charts":
                _plotly_bar_keep_order(df, "Product Name", "Stock Value", height=340)
            else:
                _plotly_pie(df, "Product Name", "Stock Value", top_n=card_top)

            st.markdown("</div>", unsafe_allow_html=True)

        # Card: Supplier Purchases (NEW)
        with c6:
            state, refresh_clicked, export_clicked = _card_frame(
                "Total Purchase from Supplier",
                "Estimated from Received logs × unit price",
                "card_supplier_purchase",
                allow_export_df=supplier_purchase,
            )
            if refresh_clicked:
                st.cache_data.clear()
                st.rerun()

            card_top = int(state["top"])
            asc = state["sort"] == "Low → High"
            df = supplier_purchase.copy()
            if not df.empty:
                df = df.sort_values("Purchase Amount", ascending=asc).head(card_top)

            if export_clicked:
                st.download_button(
                    "Download CSV",
                    data=df.to_csv(index=False).encode("utf-8"),
                    file_name="supplier_purchase_card.csv",
                    mime="text/csv",
                    use_container_width=True,
                )

            if df.empty:
                st.info("📭 No supplier purchase data (missing Supplier/Price in metadata or no received logs in date range).")
            else:
                if view_mode == "Tables":
                    st.dataframe(df, use_container_width=True, hide_index=True, height=320)
                elif view_mode == "Bar Charts":
                    _plotly_bar_keep_order(df, "Supplier", "Purchase Amount", height=340)
                else:
                    _plotly_pie(df, "Supplier", "Purchase Amount", top_n=card_top)

            st.markdown("</div>", unsafe_allow_html=True)

        st.divider()

        # --- Kanban Dashboard (Streamlit-native, no true drag-drop) ---
        st.markdown('<span class="section-title">🧩 Ops Kanban (Streamlit-native)</span>', unsafe_allow_html=True)
        st.caption("Drag & drop requires a custom component; this version uses quick move controls but keeps the Kanban mental model.")

        # Build alert metrics
        kan_inv = inv_join.copy() if inv_join is not None and not inv_join.empty else pd.DataFrame(columns=["Product Name", "Closing Stock"])
        if "Product Name" not in kan_inv.columns:
            kan_inv["Product Name"] = ""
        if "Closing Stock" not in kan_inv.columns:
            kan_inv["Closing Stock"] = 0.0

        # thresholds (simple defaults; can later be driven by metadata Min Stock)
        kan_inv["Closing Stock"] = pd.to_numeric(kan_inv["Closing Stock"], errors="coerce").fillna(0.0)
        kan_inv["Min Stock"] = 0.0
        if meta_df is not None and not meta_df.empty and "Min Stock" in meta_df.columns:
            min_map = meta_df[["Product Name", "Min Stock"]].copy()
            min_map["Min Stock"] = pd.to_numeric(min_map["Min Stock"], errors="coerce").fillna(0.0)
            kan_inv = kan_inv.merge(min_map.drop_duplicates("Product Name"), on="Product Name", how="left", suffixes=("", "_m"))
            if "Min Stock_m" in kan_inv.columns:
                kan_inv["Min Stock"] = kan_inv["Min Stock_m"].fillna(kan_inv["Min Stock"])
                kan_inv = kan_inv.drop(columns=["Min Stock_m"])

        def kanban_bucket(row):
            cs = float(row.get("Closing Stock", 0) or 0)
            ms = float(row.get("Min Stock", 0) or 0)
            if ms <= 0:
                # fallback rule if Min Stock not configured
                if cs <= 0:
                    return "⚠️ Attention Needed"
                if cs < 5:
                    return "📦 Reorder Soon"
                return "✅ Healthy Stock"
            else:
                if cs <= 0:
                    return "⚠️ Attention Needed"
                if cs < ms:
                    return "📦 Reorder Soon"
                if cs > (ms * 3):
                    return "🚚 In Transit"  # placeholder bucket for now (overstock/flow); can rename later
                return "✅ Healthy Stock"

        kan_inv["Kanban"] = kan_inv.apply(kanban_bucket, axis=1)

        if "kanban_state" not in st.session_state:
            # user can move cards; we persist their chosen column
            st.session_state.kanban_state = {}

        # Apply overrides
        def effective_bucket(prod, default_bucket):
            return st.session_state.kanban_state.get(prod, default_bucket)

        kan_inv["KanbanEff"] = kan_inv.apply(lambda r: effective_bucket(str(r["Product Name"]), str(r["Kanban"])), axis=1)

        cols = ["⚠️ Attention Needed", "📦 Reorder Soon", "🚚 In Transit", "✅ Healthy Stock"]
        k1, k2, k3, k4 = st.columns(4)
        kan_cols = dict(zip(cols, [k1, k2, k3, k4]))

        def render_kanban_col(col_name, col):
            with col:
                st.markdown(f"**{col_name}**")
                items = kan_inv[kan_inv["KanbanEff"] == col_name].copy()
                items = items.sort_values("Closing Stock", ascending=True).head(12)
                if items.empty:
                    st.caption("No items")
                    return
                for _, r in items.iterrows():
                    p = str(r.get("Product Name", ""))
                    cs = float(r.get("Closing Stock", 0) or 0)
                    ms = float(r.get("Min Stock", 0) or 0)
                    badge = f"{cs:.2f}"
                    meta_line = f"Min: {ms:.2f}" if ms > 0 else "Min: —"

                    st.markdown(
                        f"""
                        <div style="
                            border: 1px solid rgba(255,255,255,0.06);
                            background: rgba(26,31,46,0.55);
                            border-radius: 14px;
                            padding: 10px;
                            margin-bottom: 8px;
                            backdrop-filter: blur(16px);
                        ">
                          <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;">
                            <div style="color:#e6edf7;font-weight:700;font-size:0.9rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:210px;">
                              {p}
                            </div>
                            <div style="color:#0f1419;background:rgba(0,217,255,0.92);padding:4px 8px;border-radius:999px;font-weight:900;font-size:0.78rem;">
                              {badge}
                            </div>
                          </div>
                          <div style="color:#93a4b8;font-size:0.74rem;margin-top:6px;">{meta_line}</div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                    move_to = st.selectbox(
                        "Move",
                        options=cols,
                        index=cols.index(col_name),
                        key=f"kan_move_{col_name}_{p}",
                        label_visibility="collapsed",
                    )
                    if move_to != col_name:
                        st.session_state.kanban_state[p] = move_to
                        st.rerun()

        for name in cols:
            render_kanban_col(name, kan_cols[name])

        st.markdown(
            '<div style="margin-top:10px;color:#7b8798;font-size:0.78rem;">Tip: Configure <b>Min Stock</b> in product metadata for smarter Kanban bucketing.</div>',
            unsafe_allow_html=True,
        )

        # Export full dashboard (kept)
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
                            "Net Flow": net_flow,
                            "Stock In Hand Qty": stock_inhand_qty,
                            "Stock In Hand Value": stock_inhand_value,
                            "Dispatch Date Basis": dispatch_date_basis,
                        }
                    ]
                ),
                "Top Ordered": top_ordered,
                "Top Dispatched": top_dispatched,
                "Top Received": top_received,
                "Top Stock Qty": top_stock_qty,
                "Top Stock Value": top_stock_val,
                "Supplier Purchases": supplier_purchase,
            }
        )
        st.download_button(
            "📥 Export Dashboard (Excel)",
            data=export_bytes,
            file_name=f"Warehouse_Dashboard_{start_date}_to_{end_date}.xlsx",
            use_container_width=True,
            key="dash_export_excel",
        )

        st.markdown(
            '<div style="margin-top:8px;color:#7b8798;font-size:0.78rem;">Note: “Supplier Purchases” is estimated from Received logs (Qty × Unit Price) using metadata Supplier + Price.</div>',
            unsafe_allow_html=True,
        )

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
