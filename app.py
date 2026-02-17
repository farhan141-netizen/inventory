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
    if df is None or df.empty: return df
    df = df.loc[:, ~df.columns.str.contains('^Unnamed', na=False)]
    df = df.dropna(axis=1, how='all')
    df = df.loc[:, ~df.columns.duplicated()]
    df.columns = [str(col).strip() for col in df.columns]
    return df

def load_from_sheet(worksheet_name, default_cols=None):
    """Safely load and clean data from Google Sheets"""
    try:
        df = conn.read(worksheet=worksheet_name, ttl="2s")
        df = clean_dataframe(df)
        if df is None or df.empty:
            return pd.DataFrame(columns=default_cols) if default_cols else pd.DataFrame()
        return df
    except Exception:
        return pd.DataFrame(columns=default_cols) if default_cols else pd.DataFrame()

def save_to_sheet(df, worksheet_name):
    """Save cleaned data to Google Sheets"""
    df = clean_dataframe(df)
    conn.update(worksheet=worksheet_name, data=df)
    st.cache_data.clear()

# --- PAGE CONFIG ---
st.set_page_config(page_title="Warehouse Pro Cloud v6.4", layout="wide")

# --- CUSTOM CSS ---
st.markdown("""
    <style>
    .main { background-color: #0e1117; }
    .stTabs [data-baseweb="tab"] { height: 50px; background-color: #1e2130; color: white; border-radius: 5px 5px 0 0; }
    .stTabs [aria-selected="true"] { background-color: #00ffcc !important; color: #000 !important; }
    .log-container { max-height: 500px; overflow-y: auto; padding: 10px; background: #161b22; border-radius: 10px; }
    .log-text { font-size: 0.85rem; line-height: 1.2; margin-bottom: 5px; }
    .log-meta { font-size: 0.7rem; color: #888; }
    .receipt-card, .action-card, .par-card { background-color: #161b22; padding: 20px; border-radius: 10px; border: 1px solid #30363d; margin-bottom: 20px; }
    </style>
    """, unsafe_allow_html=True)

# --- ENGINE ---
def recalculate_item(df, item_name):
    if item_name not in df["Product Name"].values: return df
    idx = df[df["Product Name"] == item_name].index[0]
    day_cols = [str(i) for i in range(1, 32)]
    
    for col in day_cols:
        if col not in df.columns: df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
    
    total_received = df.loc[idx, day_cols].sum()
    df.at[idx, "Total Received"] = total_received
    
    opening = pd.to_numeric(df.at[idx, "Opening Stock"], errors='coerce') or 0.0
    consumption = pd.to_numeric(df.at[idx, "Consumption"], errors='coerce') or 0.0
    closing = opening + total_received - consumption
    df.at[idx, "Closing Stock"] = closing
    
    if "Physical Count" in df.columns:
        physical_val = df.at[idx, "Physical Count"]
        if pd.notna(physical_val) and str(physical_val).strip() != "":
            physical = pd.to_numeric(physical_val, errors='coerce')
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
            if col_name not in df.columns: df[col_name] = 0.0
            current_val = pd.to_numeric(df.at[idx, col_name], errors='coerce') or 0.0
            df.at[idx, col_name] = current_val + float(qty)
        
        if not is_undo:
            new_log = pd.DataFrame([{
                "LogID": str(uuid.uuid4())[:8],
                "Timestamp": datetime.datetime.now().strftime("%H:%M:%S"),
                "Item": item_name, "Qty": qty, "Day": day_num, "Status": "Active"
            }])
            logs_df = load_from_sheet("activity_logs", ["LogID", "Timestamp", "Item", "Qty", "Day", "Status"])
            save_to_sheet(pd.concat([logs_df, new_log], ignore_index=True), "activity_logs")
        
        df = recalculate_item(df, item_name)
        st.session_state.inventory = df
        save_to_sheet(df, "persistent_inventory")
        return True
    return False

def undo_entry(log_id):
    logs = load_from_sheet("activity_logs")
    if log_id in logs["LogID"].values:
        idx = logs[logs["LogID"] == log_id].index[0]
        if logs.at[idx, "Status"] == "Undone": return
        item, qty, day = logs.at[idx, "Item"], logs.at[idx, "Qty"], logs.at[idx, "Day"]
        if apply_transaction(item, day, -qty, is_undo=True):
            logs.at[idx, "Status"] = "Undone"
            save_to_sheet(logs, "activity_logs")
            st.rerun()

# --- MODALS ---
@st.dialog("➕ Add New Product")
def add_item_modal():
    meta_cols = ["Product Name", "Supplier", "Min Stock"]
    meta_df = load_from_sheet("product_metadata", default_cols=meta_cols)
    col1, col2 = st.columns(2)
    with col1:
        name = st.text_input("Item Name*")
        uom = st.selectbox("Unit", ["pcs", "kg", "box", "ltr", "pkt", "can", "bot"])
    with col2:
        opening = st.number_input("Opening Stock", min_value=0.0)
        supplier = st.text_input("Supplier")
    
    if st.button("✅ Create Product", use_container_width=True, type="primary"):
        if name:
            inv = st.session_state.inventory
            new_row = {str(i): 0.0 for i in range(1, 32)}
            new_row.update({"Product Name": name, "UOM": uom, "Opening Stock": opening, 
                            "Total Received": 0.0, "Consumption": 0.0, "Closing Stock": opening,
                            "Physical Count": None, "Variance": 0.0})
            save_to_sheet(pd.concat([inv, pd.DataFrame([new_row])], ignore_index=True), "persistent_inventory")
            
            new_meta = pd.DataFrame([{"Product Name": name, "Supplier": supplier, "Min Stock": 0}])
            save_to_sheet(pd.concat([meta_df, new_meta], ignore_index=True), "product_metadata")
            st.rerun()

@st.dialog("🔒 Close Month & Rollover")
def close_month_modal():
    st.warning("Physical Counts will become new Opening Stocks.")
    month_label = st.text_input("Month Label", datetime.datetime.now().strftime("%b %Y"))
    if st.button("Confirm Monthly Close", type="primary", use_container_width=True):
        df = st.session_state.inventory.copy()
        history_df = load_from_sheet("monthly_history")
        archive_df = df.copy()
        archive_df["Month_Period"] = month_label
        save_to_sheet(pd.concat([history_df, archive_df], ignore_index=True), "monthly_history")
        
        new_df = df.copy()
        for i in range(1, 32): new_df[str(i)] = 0.0
        for idx, row in new_df.iterrows():
            phys_val = row.get("Physical Count")
            new_df.at[idx, "Opening Stock"] = pd.to_numeric(phys_val) if pd.notna(phys_val) else row["Closing Stock"]
        
        new_df["Total Received"] = 0.0; new_df["Consumption"] = 0.0
        new_df["Closing Stock"] = new_df["Opening Stock"]
        new_df["Physical Count"] = None; new_df["Variance"] = 0.0
        save_to_sheet(new_df, "persistent_inventory")
        st.rerun()

# --- INITIALIZATION ---
if 'inventory' not in st.session_state:
    st.session_state.inventory = load_from_sheet("persistent_inventory")

# --- MAIN UI ---
st.title("📦 Warehouse Pro Management v6.4")
tab_ops, tab_req, tab_sup = st.tabs(["📊 Operations", "🚚 Requisitions", "📞 Suppliers"])

with tab_ops:
    m_col, a_col = st.columns([3, 1])
    
    with m_col:
        st.markdown('<div class="receipt-card">', unsafe_allow_html=True)
        st.subheader("📥 Daily Receipt Portal")
        if not st.session_state.inventory.empty:
            item_list = sorted(st.session_state.inventory["Product Name"].unique().tolist())
            c1, c2, c3 = st.columns([2, 1, 1])
            with c1: sel_item = st.selectbox("Search Item", options=[""] + item_list)
            with c2: day_in = st.number_input("Day", 1, 31, datetime.datetime.now().day)
            with c3: qty_in = st.number_input("Qty Received", min_value=0.0)
            if st.button("✅ Confirm Receipt", type="primary", use_container_width=True):
                if sel_item and qty_in > 0: apply_transaction(sel_item, day_in, qty_in); st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    with a_col:
        st.markdown('<div class="action-card">', unsafe_allow_html=True)
        st.subheader("⚙️ Quick Actions")
        if st.button("➕ Add New Product", use_container_width=True): add_item_modal()
        if st.button("🔒 Close Month", type="primary", use_container_width=True): close_month_modal()
        st.markdown('</div>', unsafe_allow_html=True)

    # --- PAR ANALYSIS ---
    
    st.markdown('<div class="par-card">', unsafe_allow_html=True)
    st.subheader("📈 Multi-Month Weekly Par Analysis")
    df_current = st.session_state.inventory.copy()
    df_history = load_from_sheet("monthly_history")
    if not df_history.empty and not df_current.empty:
        df_history["Consumption"] = pd.to_numeric(df_history["Consumption"], errors='coerce').fillna(0)
        avg_cons = df_history.groupby("Product Name")["Consumption"].mean().reset_index()
        avg_cons.rename(columns={"Consumption": "Avg_Monthly"}, inplace=True)
        df_par = pd.merge(df_current[["Product Name", "UOM", "Closing Stock"]], avg_cons, on="Product Name", how="left")
        df_par["Avg_Monthly"] = df_par["Avg_Monthly"].fillna(0)
        df_par["Weekly Usage"] = (df_par["Avg_Monthly"] / 4.33).round(2)
        df_par["Min (50%)"] = (df_par["Weekly Usage"] * 0.5).round(2)
        df_par["Historical Par"] = df_par["Weekly Usage"]
        df_par["Max (150%)"] = (df_par["Weekly Usage"] * 1.5).round(2)
        st.dataframe(df_par, use_container_width=True, hide_index=True)
    else:
        st.info("Archive empty. Par analysis activates after first 'Close Month'.")
    st.markdown('</div>', unsafe_allow_html=True)

    st.divider()
    
    # --- LIVE STOCK STATUS ---
    st.subheader("📊 Live Stock Status")
    if not st.session_state.inventory.empty:
        df_status = st.session_state.inventory.copy()
        disp_cols = ["Product Name", "UOM", "Opening Stock", "Total Received", "Consumption", "Closing Stock", "Physical Count", "Variance"]
        for col in disp_cols:
            if col not in df_status.columns: df_status[col] = 0.0
        
        edited_df = st.data_editor(df_status[disp_cols], use_container_width=True, disabled=["Product Name", "UOM", "Total Received", "Closing Stock", "Variance"], hide_index=True)
        
        if st.button("💾 Save & Update Inventory", type="primary"):
            df_status.update(edited_df)
            for item in df_status["Product Name"]: df_status = recalculate_item(df_status, item)
            save_to_sheet(df_status, "persistent_inventory")
            st.rerun()
    else:
        st.warning("Inventory is empty. Please use 'Bulk Upload' in the sidebar or 'Add New Product'.")

# --- REQUISITIONS & SUPPLIERS ---
with tab_req:
    st.subheader("🚚 Create Requisition")
    meta_df = load_from_sheet("product_metadata")
    col_it, col_qt = st.columns([3, 1])
    with col_it: req_item = st.selectbox("Select Product", options=[""] + sorted(meta_df["Product Name"].tolist()) if not meta_df.empty else [""])
    with col_qt: req_qty = st.number_input("Order Qty", min_value=0.0)
    if st.button("➕ Add to List"):
        if req_item and req_qty > 0:
            orders = load_from_sheet("orders_db", ["Product Name", "Qty", "Supplier", "Status"])
            sup = meta_df[meta_df["Product Name"] == req_item]["Supplier"].values[0] if not meta_df.empty else "N/A"
            save_to_sheet(pd.concat([orders, pd.DataFrame([{"Product Name": req_item, "Qty": req_qty, "Supplier": sup, "Status": "Pending"}])], ignore_index=True), "orders_db")
            st.rerun()
    st.dataframe(load_from_sheet("orders_db"), use_container_width=True)

with tab_sup:
    st.subheader("📞 Supplier Directory")
    meta = load_from_sheet("product_metadata")
    edited_meta = st.data_editor(meta, num_rows="dynamic", use_container_width=True)
    if st.button("💾 Save Directory"): save_to_sheet(edited_meta, "product_metadata"); st.rerun()

# --- SIDEBAR: BULK UPLOAD RESTORED ---
with st.sidebar:
    st.header("Cloud Data Control")
    st.subheader("1. Bulk Inventory Upload")
    inv_file = st.file_uploader("Upload Inventory Master", type=["xlsx", "csv"])
    if inv_file and st.button("🚀 Push Inventory"):
        raw = pd.read_excel(inv_file) if inv_file.name.endswith('.xlsx') else pd.read_csv(inv_file)
        # Ensure it matches the schema
        required = ["Product Name", "UOM", "Opening Stock"]
        if all(col in raw.columns for col in required):
            for i in range(1, 32): raw[str(i)] = 0.0
            raw["Total Received"] = 0.0; raw["Consumption"] = 0.0; raw["Closing Stock"] = raw["Opening Stock"]
            save_to_sheet(raw, "persistent_inventory"); st.rerun()
        else:
            st.error("File missing required columns: Product Name, UOM, Opening Stock.")
    
    st.subheader("2. Bulk Metadata Sync")
    meta_file = st.file_uploader("Upload Product Metadata", type=["xlsx", "csv"])
    if meta_file and st.button("🚀 Push Metadata"):
        raw_m = pd.read_excel(meta_file) if meta_file.name.endswith('.xlsx') else pd.read_csv(meta_file)
        save_to_sheet(raw_m, "product_metadata"); st.rerun()
    
    if st.button("🗑️ Reset Cache"): st.cache_data.clear(); st.rerun()
