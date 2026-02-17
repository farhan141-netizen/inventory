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
st.set_page_config(page_title="Warehouse Pro Cloud v4.2", layout="wide")

# --- CUSTOM CSS ---
st.markdown("""
    <style>
    .main { background-color: #0e1117; }
    .stTabs [data-baseweb="tab"] { height: 50px; background-color: #1e2130; color: white; border-radius: 5px 5px 0 0; }
    .stTabs [aria-selected="true"] { background-color: #00ffcc !important; color: #000 !important; }
    .log-container { max-height: 400px; overflow-y: auto; padding: 10px; background: #161b22; border-radius: 10px; }
    .log-text { font-size: 0.85rem; line-height: 1.2; margin-bottom: 5px; }
    .log-meta { font-size: 0.7rem; color: #888; }
    .receipt-card, .action-card, .archive-card { background-color: #161b22; padding: 20px; border-radius: 10px; border: 1px solid #30363d; margin-bottom: 20px; }
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
                "Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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
    col1, col2 = st.columns(2)
    with col1:
        name = st.text_input("Item Name*")
        uom = st.selectbox("Unit", ["pcs", "kg", "box", "ltr", "pkt", "can", "bot"])
    with col2:
        opening = st.number_input("Opening Stock", min_value=0.0)
    
    if st.button("✅ Create Product", use_container_width=True):
        if name:
            new_row = {str(i): 0.0 for i in range(1, 32)}
            new_row.update({"Product Name": name, "UOM": uom, "Opening Stock": opening, 
                            "Total Received": 0.0, "Consumption": 0.0, "Closing Stock": opening,
                            "Physical Count": None, "Variance": 0.0})
            st.session_state.inventory = pd.concat([st.session_state.inventory, pd.DataFrame([new_row])], ignore_index=True)
            save_to_sheet(st.session_state.inventory, "persistent_inventory")
            st.rerun()

@st.dialog("🔒 Close Month & Rollover")
def close_month_modal():
    st.warning("Physical Counts (including 0) will become new Opening Stocks.")
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
            if pd.notna(phys_val) and str(phys_val).strip() != "":
                new_df.at[idx, "Opening Stock"] = pd.to_numeric(phys_val)
            else:
                new_df.at[idx, "Opening Stock"] = row["Closing Stock"]
        
        new_df["Total Received"] = 0.0
        new_df["Consumption"] = 0.0
        new_df["Closing Stock"] = new_df["Opening Stock"]
        new_df["Physical Count"] = None 
        new_df["Variance"] = 0.0
        save_to_sheet(new_df, "persistent_inventory")
        st.rerun()

# --- INITIALIZATION ---
if 'inventory' not in st.session_state:
    st.session_state.inventory = load_from_sheet("persistent_inventory")

# --- MAIN UI ---
st.title("📦 Warehouse Pro Management v4.2")
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
            with c3: qty_in = st.number_input("Qty", min_value=0.0)
            if st.button("✅ Confirm Receipt", type="primary", use_container_width=True):
                if sel_item and qty_in > 0:
                    apply_transaction(sel_item, day_in, qty_in); st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    with a_col:
        st.markdown('<div class="action-card">', unsafe_allow_html=True)
        st.subheader("⚙️ Quick Actions")
        if st.button("➕ Add New Product", use_container_width=True): add_item_modal()
        if st.button("🔒 Close Month", type="primary", use_container_width=True): close_month_modal()
        st.markdown('</div>', unsafe_allow_html=True)

    # --- ARCHIVE EXPORTER ---
    st.markdown('<div class="archive-card">', unsafe_allow_html=True)
    st.subheader("📂 Archive Explorer & Month Exporter")
    hist_df = load_from_sheet("monthly_history")
    if not hist_df.empty and "Month_Period" in hist_df.columns:
        available_months = sorted(hist_df["Month_Period"].unique().tolist(), reverse=True)
        ex_col1, ex_col2 = st.columns([2, 1])
        with ex_col1:
            selected_month = st.selectbox("Select Month to Export", options=available_months)
        with ex_col2:
            st.write("##")
            month_data = hist_df[hist_df["Month_Period"] == selected_month].drop(columns=["Month_Period"])
            buf_month = io.BytesIO()
            with pd.ExcelWriter(buf_month, engine='xlsxwriter') as writer:
                month_data.to_excel(writer, index=False, sheet_name="Archive_"+selected_month[:20])
            st.download_button(f"📥 Download {selected_month}", data=buf_month.getvalue(), 
                               file_name=f"Inventory_{selected_month}.xlsx", use_container_width=True)
    else:
        st.info("No historical data found. Archive your first month to use the explorer.")
    st.markdown('</div>', unsafe_allow_html=True)

    st.divider()
    
    log_col, stat_col = st.columns([1, 2.5])
    with log_col:
        st.subheader("📜 Activity Log")
        logs = load_from_sheet("activity_logs")
        if not logs.empty:
            st.markdown('<div class="log-container">', unsafe_allow_html=True)
            for _, row in logs.iloc[::-1].head(10).iterrows():
                is_undone = row['Status'] == "Undone"
                st.markdown(f"<div class='log-text'><b>{row['Item']}</b>: {row['Qty']} (Day {row['Day']})<br><span class='log-meta'>{row['Timestamp']} {' - REVERSED' if is_undone else ''}</span></div>", unsafe_allow_html=True)
                if not is_undone:
                    if st.button("Undo", key=f"undo_{row['LogID']}"): undo_entry(row['LogID'])
                st.markdown("---")
            st.markdown('</div>', unsafe_allow_html=True)

    with stat_col:
        st.subheader("📊 Live Inventory & Variance")
        df_status = st.session_state.inventory.copy()
        if "Physical Count" not in df_status.columns: df_status["Physical Count"] = None
        if "Variance" not in df_status.columns: df_status["Variance"] = 0.0
        
        disp_cols = ["Product Name", "UOM", "Opening Stock", "Total Received", "Closing Stock", "Consumption", "Physical Count", "Variance"]
        edited_df = st.data_editor(df_status[disp_cols], use_container_width=True, 
                                   disabled=["Product Name", "UOM", "Total Received", "Closing Stock", "Variance"], hide_index=True)
        
        btn1, btn2, btn3 = st.columns(3)
        with btn1:
            if st.button("💾 Save & Update Variance", use_container_width=True):
                df_status.update(edited_df)
                for item in df_status["Product Name"]: df_status = recalculate_item(df_status, item)
                st.session_state.inventory = df_status
                save_to_sheet(df_status, "persistent_inventory")
                st.rerun()
        with btn2:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine='xlsxwriter') as writer:
                df_status[disp_cols].to_excel(writer, index=False, sheet_name='Summary')
            st.download_button("📥 Download Live Summary", data=buf.getvalue(), file_name="Live_Summary.xlsx", use_container_width=True)
        with btn3:
            day_cols = [str(i) for i in range(1, 32)]
            full_export_cols = ["Product Name", "UOM", "Opening Stock"] + day_cols + ["Total Received", "Consumption", "Closing Stock", "Physical Count", "Variance"]
            buf_full = io.BytesIO()
            with pd.ExcelWriter(buf_full, engine='xlsxwriter') as writer:
                df_status[full_export_cols].to_excel(writer, index=False, sheet_name='Full_Details')
            st.download_button("📂 Download Full (1-31)", data=buf_full.getvalue(), file_name="Live_Full_Report.xlsx", use_container_width=True)

# --- TAB 2: REQUISITIONS ---
with tab_req:
    st.subheader("🚚 Requisition System")
    meta_df = load_from_sheet("product_metadata")
    col_it, col_qt, col_ad = st.columns([3, 1, 1])
    with col_it: req_item = st.selectbox("Select Product", options=[""] + sorted(meta_df["Product Name"].tolist()) if not meta_df.empty else [""])
    with col_qt: req_qty = st.number_input("Order Qty", min_value=0.0, key="reqqty")
    with col_ad:
        st.write("##")
        if st.button("➕ Add to Requisition", use_container_width=True):
            if req_item and req_qty > 0:
                # Find supplier if exists
                supplier = meta_df[meta_df["Product Name"] == req_item]["Supplier"].values[0] if req_item in meta_df["Product Name"].values else "Unknown"
                orders_df = load_from_sheet("orders_db", ["Product Name", "Qty", "Supplier", "Status"])
                new_order = pd.DataFrame([{"Product Name": req_item, "Qty": req_qty, "Supplier": supplier, "Status": "Pending"}])
                save_to_sheet(pd.concat([orders_df, new_order], ignore_index=True), "orders_db")
                st.rerun()
    
    orders = load_from_sheet("orders_db")
    if not orders.empty:
        st.dataframe(orders, use_container_width=True)
        if st.button("🗑️ Clear Requisitions"):
            save_to_sheet(pd.DataFrame(columns=["Product Name", "Qty", "Supplier", "Status"]), "orders_db")
            st.rerun()

# --- TAB 3: SUPPLIERS ---
with tab_sup:
    st.subheader("📞 Supplier Directory")
    meta_df = load_from_sheet("product_metadata")
    search = st.text_input("🔍 Search Suppliers/Products").lower()
    filtered = meta_df[meta_df["Product Name"].str.lower().str.contains(search, na=False) | meta_df["Supplier"].str.lower().str.contains(search, na=False)] if search else meta_df
    edited_meta = st.data_editor(filtered, num_rows="dynamic", use_container_width=True)
    if st.button("💾 Save Directory Changes"):
        save_to_sheet(edited_meta, "product_metadata")
        st.rerun()

# --- SIDEBAR ---
with st.sidebar:
    st.header("Cloud Data Control")
    st.subheader("1. Bulk Inventory Sync")
    inv_file = st.file_uploader("Upload Inventory Master", type=["csv", "xlsx"])
    if inv_file:
        try:
            raw_df = pd.read_excel(inv_file, skiprows=4, header=None) if inv_file.name.endswith('.xlsx') else pd.read_csv(inv_file, skiprows=4, header=None)
            new_df = pd.DataFrame()
            new_df["Product Name"] = raw_df[1]; new_df["UOM"] = raw_df[2]
            new_df["Opening Stock"] = pd.to_numeric(raw_df[3], errors='coerce').fillna(0.0)
            for i in range(1, 32): new_df[str(i)] = 0.0
            new_df["Total Received"] = 0.0; new_df["Consumption"] = 0.0; new_df["Closing Stock"] = new_df["Opening Stock"]
            if st.button("🚀 Push Inventory to Cloud"):
                save_to_sheet(new_df.dropna(subset=["Product Name"]), "persistent_inventory"); st.rerun()
        except Exception as e: st.error(f"Error: {e}")

    st.divider()
    st.subheader("2. Bulk Supplier Sync")
    meta_upload = st.file_uploader("Upload Product Metadata", type=["csv", "xlsx"])
    if meta_upload:
        try:
            new_meta = pd.read_excel(meta_upload) if meta_upload.name.endswith('.xlsx') else pd.read_csv(meta_upload)
            if st.button("🚀 Push Metadata to Cloud"):
                save_to_sheet(new_meta, "product_metadata"); st.rerun()
        except Exception as e: st.error(f"Error: {e}")
    if st.button("🗑️ Reset Cache"):
        st.cache_data.clear(); st.rerun()
