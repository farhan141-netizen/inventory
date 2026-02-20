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
    """Save cleaned data to Google Sheets and clear cache"""
    df = clean_dataframe(df)
    conn.update(worksheet=worksheet_name, data=df)
    st.cache_data.clear()

# --- PAGE CONFIG ---
st.set_page_config(page_title="Warehouse Pro Cloud v8.5", layout="wide", initial_sidebar_state="expanded")

# --- MODERN SOPHISTICATED CSS ---
st.markdown("""
    <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    .main { background: linear-gradient(135deg, #0f1419 0%, #1a1f2e 50%, #0f1419 100%); padding: 20px; }
    .stTabs [data-baseweb="tab-list"] { gap: 24px; background: linear-gradient(90deg, #1a1f2e 0%, #252d3d 100%); padding: 10px 20px; border-radius: 12px; margin-bottom: 20px; border: 1px solid #2d3748; }
    .stTabs [data-baseweb="tab"] { padding: 8px 20px; font-weight: 600; color: #8892b0; border-radius: 8px; transition: all 0.3s ease; }
    .stTabs [aria-selected="true"] { color: #00d9ff; background: linear-gradient(90deg, #00d9ff20 0%, #0095ff20 100%); border: 1px solid #00d9ff; }
    .header-card { background: linear-gradient(135deg, #00d9ff 0%, #0095ff 100%); border-radius: 16px; padding: 32px; color: white; margin-bottom: 24px; box-shadow: 0 8px 32px rgba(0, 217, 255, 0.3); text-align: center; }
    .header-card h1 { font-size: 2.5em; margin-bottom: 8px; font-weight: 800; letter-spacing: -1px; }
    
    /* Compact Scrollable Log Styling */
    .log-container {
        max-height: 480px;
        overflow-y: auto;
        overflow-x: auto;
        padding-right: 5px;
        margin-bottom: 10px;
        border: 1px solid #2d3748;
        border-radius: 10px;
        background: rgba(26, 31, 46, 0.4);
    }
    
    .log-row { 
        display: flex; 
        justify-content: space-between; 
        align-items: center; 
        background: linear-gradient(90deg, #1a1f2e 0%, #252d3d 100%); 
        padding: 8px 12px; 
        border-radius: 8px; 
        margin-bottom: 6px; 
        border: 1px solid #2d3748; 
        border-left: 3px solid #00d9ff; 
        min-width: 300px;
    }
    .log-row-undone { border-left: 3px solid #ff6b6b; opacity: 0.6; background: linear-gradient(90deg, #1a1f2e80 0%, #252d3d80 100%); }
    .log-info { font-size: 0.82rem; color: #e0e7ff; line-height: 1.2; }
    .log-time { font-size: 0.75rem; color: #8892b0; margin-left: 8px; font-style: italic; }
    
    .section-title { color: #00d9ff; font-size: 1.4em; font-weight: 700; margin-bottom: 16px; padding-bottom: 8px; border-bottom: 2px solid #00d9ff; display: inline-block; }
    .sidebar-title { color: #00d9ff; font-weight: 700; font-size: 1.2em; margin-bottom: 12px; }
    .stButton>button { border-radius: 10px; font-weight: 600; padding: 8px 20px; transition: all 0.3s ease; }
    hr { border: none; height: 1px; background: linear-gradient(90deg, transparent, #2d3748, transparent); margin: 20px 0; }
    
    /* Custom Scrollbar */
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: #1a1f2e; }
    ::-webkit-scrollbar-thumb { background: #2d3748; border-radius: 10px; }
    ::-webkit-scrollbar-thumb:hover { background: #00d9ff; }
    </style>
    """, unsafe_allow_html=True)

# --- CORE CALCULATION ENGINE ---
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
    col1, col2 = st.columns(2)
    with col1:
        name = st.text_input("📦 Item Name")
        uom = st.selectbox("📏 Unit", ["pcs", "kg", "box", "ltr", "pkt", "can", "bot"])
    with col2:
        opening = st.number_input("📊 Opening Stock", min_value=0.0, value=0.0)
    if st.button("✅ Create Product", use_container_width=True, type="primary"):
        if name:
            new_row = {str(i): 0.0 for i in range(1, 32)}
            new_row.update({"Product Name": name, "UOM": uom, "Opening Stock": opening, "Total Received": 0.0, "Consumption": 0.0, "Closing Stock": opening, "Physical Count": None, "Variance": 0.0})
            st.session_state.inventory = pd.concat([st.session_state.inventory, pd.DataFrame([new_row])], ignore_index=True)
            save_to_sheet(st.session_state.inventory, "persistent_inventory")
            st.rerun()

@st.dialog("📂 Archive Explorer")
def archive_explorer_modal():
    hist_df = load_from_sheet("monthly_history")
    if not hist_df.empty and "Month_Period" in hist_df.columns:
        selected_month = st.selectbox("📅 Select Month Period", options=sorted(hist_df["Month_Period"].unique().tolist(), reverse=True))
        month_data = hist_df[hist_df["Month_Period"] == selected_month].drop(columns=["Month_Period"])
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine='xlsxwriter') as writer:
            month_data.to_excel(writer, index=False, sheet_name="Archive")
        st.download_button(label=f"📥 Download {selected_month}", data=buf.getvalue(), file_name=f"Inventory_{selected_month}.xlsx", use_container_width=True, type="primary")
    else: st.info("📭 No records found.")

@st.dialog("🔒 Close Month & Rollover")
def close_month_modal():
    st.warning("⚠️ Physical Counts will become new Opening Stocks.")
    month_label = st.text_input("📅 Month Label", value=datetime.datetime.now().strftime("%b %Y"))
    if st.button("✅ Confirm Monthly Close", type="primary", use_container_width=True):
        df = st.session_state.inventory.copy()
        hist_df = load_from_sheet("monthly_history")
        archive_df = df.copy(); archive_df["Month_Period"] = month_label
        save_to_sheet(pd.concat([hist_df, archive_df], ignore_index=True), "monthly_history")
        new_df = df.copy()
        for i in range(1, 32): new_df[str(i)] = 0.0
        for idx, row in new_df.iterrows():
            phys = row.get("Physical Count")
            new_df.at[idx, "Opening Stock"] = pd.to_numeric(phys) if pd.notna(phys) and str(phys).strip() != "" else row["Closing Stock"]
        new_df["Total Received"] = 0.0; new_df["Consumption"] = 0.0; new_df["Closing Stock"] = new_df["Opening Stock"]; new_df["Physical Count"] = None; new_df["Variance"] = 0.0
        save_to_sheet(new_df, "persistent_inventory")
        st.rerun()

# --- INITIALIZATION ---
if 'inventory' not in st.session_state:
    st.session_state.inventory = load_from_sheet("persistent_inventory")

if 'log_page' not in st.session_state:
    st.session_state.log_page = 0

# --- MAIN UI ---
st.markdown('<div class="header-card"><h1>📦 Warehouse Pro Management</h1><p>v8.5 • Cloud-Powered Inventory System</p></div>', unsafe_allow_html=True)

tab_ops, tab_req, tab_sup = st.tabs(["📊 Operations", "🚚 Requisitions", "📞 Suppliers"])

with tab_ops:
    # --- RECEIPT ---
    st.markdown('<h2 class="section-title">📥 Daily Receipt Portal</h2>', unsafe_allow_html=True)
    if not st.session_state.inventory.empty:
        col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
        with col1: sel_item = st.selectbox("🔍 Select Item", options=[""] + sorted(st.session_state.inventory["Product Name"].unique().tolist()), key="receipt_item")
        with col2: day_in = st.number_input("📅 Day", 1, 31, datetime.datetime.now().day, key="receipt_day")
        with col3: qty_in = st.number_input("📊 Qty", min_value=0.0, key="receipt_qty")
        with col4:
            if st.button("✅ Confirm", use_container_width=True, type="primary"):
                if sel_item and qty_in > 0:
                    apply_transaction(sel_item, day_in, qty_in)
                    st.success("✅ Added!"); st.rerun()
    
    st.markdown('<hr>', unsafe_allow_html=True)
    
    # --- QUICK ACTIONS ---
    st.markdown('<h2 class="section-title">⚙️ Quick Actions</h2>', unsafe_allow_html=True)
    q1, q2, q3 = st.columns(3)
    with q1:
        if st.button("➕ Add New Product", use_container_width=True): add_item_modal()
    with q2:
        if st.button("📂 Archive Explorer", use_container_width=True): archive_explorer_modal()
    with q3:
        if st.button("🔒 Close Month", use_container_width=True, type="primary"): close_month_modal()

    st.markdown('<hr>', unsafe_allow_html=True)

    # --- PAR ANALYSIS ---
    with st.expander("📈 Weekly Par Analysis (Historical Consumption)", expanded=False):
        df_hist = load_from_sheet("monthly_history")
        if not df_hist.empty and not st.session_state.inventory.empty:
            df_hist["Consumption"] = pd.to_numeric(df_hist["Consumption"], errors='coerce').fillna(0)
            avg_cons = df_hist.groupby("Product Name")["Consumption"].mean().reset_index()
            df_par = pd.merge(st.session_state.inventory[["Product Name", "UOM", "Closing Stock"]], avg_cons, on="Product Name", how="left").fillna(0)
            df_par["Weekly Usage"] = (df_par["Consumption"] / 4.33).round(2)
            df_par["Min (50%)"] = (df_par["Weekly Usage"] * 0.5).round(2)
            df_par["Max (150%)"] = (df_par["Weekly Usage"] * 1.5).round(2)
            st.dataframe(df_par, use_container_width=True, hide_index=True)
        else: st.info("📊 historical data required for Par Analysis.")

    st.markdown('<hr>', unsafe_allow_html=True)

    # --- LOGS & STATUS ---
    log_col, stat_col = st.columns([1.5, 2.5])
    with log_col:
        st.markdown('<h2 class="section-title">📜 Recent Activity</h2>', unsafe_allow_html=True)
        logs = load_from_sheet("activity_logs")
        if not logs.empty:
            # Reverse for latest first
            full_logs = logs.iloc[::-1]
            
            # Pagination logic
            items_per_page = 10
            total_pages = (len(full_logs) - 1) // items_per_page + 1
            
            start_idx = st.session_state.log_page * items_per_page
            end_idx = start_idx + items_per_page
            current_logs = full_logs.iloc[start_idx:end_idx]
            
            # Scrollable Container
            st.markdown('<div class="log-container">', unsafe_allow_html=True)
            for _, row in current_logs.iterrows():
                is_undone = row['Status'] == "Undone"
                row_class = "log-row-undone" if is_undone else ""
                
                col_row = st.container()
                c_txt, c_undo = col_row.columns([3.5, 1.2])
                with c_txt:
                    h_item, h_qty, h_day, h_time = row['Item'], row['Qty'], row['Day'], row['Timestamp']
                    l_html = f'<div class="log-row {row_class}"><div class="log-info"><b>📦 {h_item}</b><br>Qty: {h_qty} | Day: {h_day} <span class="log-time">[{h_time}]</span></div></div>'
                    st.markdown(l_html, unsafe_allow_html=True)
                with c_undo:
                    if not is_undone:
                        if st.button("↩️", key=f"rev_{row['LogID']}", use_container_width=True, help="Undo this entry"):
                            undo_entry(row['LogID'])
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Pagination Controls
            p_prev, p_info, p_next = st.columns([1, 2, 1])
            with p_prev:
                if st.button("◀", disabled=st.session_state.log_page == 0, use_container_width=True):
                    st.session_state.log_page -= 1
                    st.rerun()
            with p_info:
                st.markdown(f"<p style='text-align:center; color:#8892b0; font-size:0.85em; margin-top:5px;'>Page {st.session_state.log_page + 1} of {total_pages}</p>", unsafe_allow_html=True)
            with p_next:
                if st.button("▶", disabled=st.session_state.log_page >= total_pages - 1, use_container_width=True):
                    st.session_state.log_page += 1
                    st.rerun()
        else: st.caption("📭 No activity logs.")

    with stat_col:
        st.markdown('<h2 class="section-title">📊 Live Stock Status</h2>', unsafe_allow_html=True)
        df_status = st.session_state.inventory.copy()
        disp_cols = ["Product Name", "UOM", "Opening Stock", "Total Received", "Closing Stock", "Consumption", "Physical Count", "Variance"]
        for col in disp_cols: 
            if col not in df_status.columns: df_status[col] = 0.0
        edited_df = st.data_editor(df_status[disp_cols], height=480, use_container_width=True, disabled=["Product Name", "UOM", "Total Received", "Closing Stock", "Variance"], hide_index=True)
        
        st.markdown('<hr>', unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("💾 Save & Update", use_container_width=True, type="primary"):
                df_status.update(edited_df)
                for item in df_status["Product Name"]: df_status = recalculate_item(df_status, item)
                save_to_sheet(df_status, "persistent_inventory"); st.success("✅ Saved!"); st.rerun()
        with c2:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine='xlsxwriter') as writer:
                df_status[disp_cols].to_excel(writer, index=False, sheet_name='Summary')
            st.download_button("📥 Summary XLSX", data=buf.getvalue(), file_name="Live_Summary.xlsx", use_container_width=True)
        with c3:
            day_cols = [str(i) for i in range(1, 32)]
            full_cols = ["Product Name", "UOM", "Opening Stock"] + day_cols + ["Total Received", "Consumption", "Closing Stock", "Physical Count", "Variance"]
            buf_f = io.BytesIO()
            with pd.ExcelWriter(buf_f, engine='xlsxwriter') as writer:
                df_status[full_cols].to_excel(writer, index=False, sheet_name='Details')
            st.download_button("📂 Full XLSX", data=buf_f.getvalue(), file_name="Monthly_Report.xlsx", use_container_width=True)

with tab_req:
    st.markdown('<h2 class="section-title">🚚 Requisition System</h2>', unsafe_allow_html=True)
    meta_df = load_from_sheet("product_metadata")
    r1, r2, r3 = st.columns([2, 1, 1])
    with r1: r_item = st.selectbox("🔍 Product", options=[""] + sorted(meta_df["Product Name"].tolist()) if not meta_df.empty else [""])
    with r2: r_qty = st.number_input("📊 Order Qty", min_value=0.0)
    with r3:
        if st.button("➕ Add to List", use_container_width=True, type="primary"):
            if r_item and r_qty > 0:
                orders = load_from_sheet("orders_db", ["Product Name", "Qty", "Supplier", "Status"])
                sup = meta_df[meta_df["Product Name"] == r_item]["Supplier"].values[0] if r_item in meta_df["Product Name"].values else "Unknown"
                save_to_sheet(pd.concat([orders, pd.DataFrame([{"Product Name": r_item, "Qty": r_qty, "Supplier": sup, "Status": "Pending"}])], ignore_index=True), "orders_db")
                st.rerun()
    st.dataframe(load_from_sheet("orders_db"), use_container_width=True, hide_index=True)

with tab_sup:
    st.markdown('<h2 class="section-title">📞 Supplier Directory</h2>', unsafe_allow_html=True)
    meta = load_from_sheet("product_metadata")
    search = st.text_input("🔍 Search Database", placeholder="Product or Supplier...")
    filtered = meta if not search else meta[meta["Product Name"].str.lower().str.contains(search.lower(), na=False) | meta["Supplier"].str.lower().str.contains(search.lower(), na=False)]
    edited_meta = st.data_editor(filtered, num_rows="dynamic", use_container_width=True, hide_index=True)
    if st.button("💾 Save Directory", use_container_width=True, type="primary"):
        save_to_sheet(edited_meta, "product_metadata"); st.rerun()

with st.sidebar:
    st.markdown('<h2 class="sidebar-title">☁️ Cloud Data Control</h2>', unsafe_allow_html=True)
    
    st.markdown('<p class="sidebar-title">1️⃣ Bulk Inventory Sync</p>', unsafe_allow_html=True)
    inv_file = st.file_uploader("📂 Upload Inventory Master", type=["csv", "xlsx"])
    if inv_file:
        try:
            raw = pd.read_excel(inv_file, skiprows=4, header=None) if inv_file.name.endswith('.xlsx') else pd.read_csv(inv_file, skiprows=4, header=None)
            new_inv = pd.DataFrame()
            new_inv["Product Name"] = raw[1]; new_inv["UOM"] = raw[2]; new_inv["Opening Stock"] = pd.to_numeric(raw[3], errors='coerce').fillna(0.0)
            for i in range(1, 32): new_inv[str(i)] = 0.0
            new_inv["Total Received"] = 0.0; new_inv["Consumption"] = 0.0; new_inv["Closing Stock"] = new_inv["Opening Stock"]
            if st.button("🚀 Push Inventory", type="primary", use_container_width=True):
                save_to_sheet(new_inv.dropna(subset=["Product Name"]), "persistent_inventory"); st.rerun()
        except Exception as e: st.error(f"Error: {e}")

    st.markdown('<hr>', unsafe_allow_html=True)
    
    st.markdown('<p class="sidebar-title">2️⃣ Bulk Supplier Sync</p>', unsafe_allow_html=True)
    meta_file = st.file_uploader("📂 Upload Product Metadata", type=["csv", "xlsx"])
    if meta_file:
        try:
            new_meta = pd.read_excel(meta_file) if meta_file.name.endswith('.xlsx') else pd.read_csv(meta_file)
            if st.button("🚀 Push Metadata", type="primary", use_container_width=True):
                save_to_sheet(new_meta, "product_metadata"); st.rerun()
        except Exception as e: st.error(f"Error: {e}")

    st.markdown('<hr>', unsafe_allow_html=True)
    if st.button("🗑️ Reset Cache", use_container_width=True): st.cache_data.clear(); st.rerun()
