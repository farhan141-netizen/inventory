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
    * {
        margin: 0;
        padding: 0;
        box-sizing: border-box;
    }
    
    .main {
        background: linear-gradient(135deg, #0f1419 0%, #1a1f2e 50%, #0f1419 100%);
        padding: 20px;
    }
    
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
        background: linear-gradient(90deg, #1a1f2e 0%, #252d3d 100%);
        padding: 10px 20px;
        border-radius: 12px;
        margin-bottom: 20px;
        border: 1px solid #2d3748;
    }
    
    .stTabs [data-baseweb="tab"] {
        padding: 8px 20px;
        font-weight: 600;
        color: #8892b0;
        border-radius: 8px;
        transition: all 0.3s ease;
    }
    
    .stTabs [aria-selected="true"] {
        color: #00d9ff;
        background: linear-gradient(90deg, #00d9ff20 0%, #0095ff20 100%);
        border: 1px solid #00d9ff;
    }
    
    .premium-card {
        background: linear-gradient(135deg, #1a1f2e 0%, #252d3d 100%);
        border: 1px solid #2d3748;
        border-radius: 16px;
        padding: 24px;
        margin-bottom: 20px;
        box-shadow: 0 4px 20px rgba(0, 217, 255, 0.1);
        transition: all 0.3s ease;
    }
    
    .premium-card:hover {
        border-color: #00d9ff;
        box-shadow: 0 8px 32px rgba(0, 217, 255, 0.15);
        transform: translateY(-2px);
    }
    
    .header-card {
        background: linear-gradient(135deg, #00d9ff 0%, #0095ff 100%);
        border-radius: 16px;
        padding: 32px;
        color: white;
        margin-bottom: 24px;
        box-shadow: 0 8px 32px rgba(0, 217, 255, 0.3);
        text-align: center;
    }
    
    .header-card h1 {
        font-size: 2.5em;
        margin-bottom: 8px;
        font-weight: 800;
        letter-spacing: -1px;
    }
    
    .header-card p {
        font-size: 0.95em;
        opacity: 0.95;
        font-weight: 500;
    }
    
    .action-button {
        background: linear-gradient(135deg, #00d9ff 0%, #0095ff 100%);
        border: none;
        border-radius: 10px;
        padding: 12px 20px;
        color: white;
        font-weight: 600;
        cursor: pointer;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(0, 217, 255, 0.3);
    }
    
    .action-button:hover {
        box-shadow: 0 8px 25px rgba(0, 217, 255, 0.4);
        transform: translateY(-2px);
    }
    
    .log-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        background: linear-gradient(90deg, #1a1f2e 0%, #252d3d 100%);
        padding: 14px 16px;
        border-radius: 10px;
        margin-bottom: 10px;
        border-left: 4px solid #00d9ff;
        border: 1px solid #2d3748;
        border-left: 4px solid #00d9ff;
        transition: all 0.3s ease;
    }
    
    .log-row:hover {
        border-color: #00d9ff;
        box-shadow: 0 4px 15px rgba(0, 217, 255, 0.1);
    }
    
    .log-row-undone {
        border-left: 4px solid #ff6b6b;
        opacity: 0.6;
        background: linear-gradient(90deg, #1a1f2e80 0%, #252d3d80 100%);
    }
    
    .log-info {
        font-size: 0.9rem;
        color: #e0e7ff;
        font-weight: 500;
    }
    
    .log-time {
        font-size: 0.8rem;
        color: #8892b0;
        margin-left: 12px;
        display: inline-block;
    }
    
    .stat-item {
        background: linear-gradient(135deg, #1a1f2e 0%, #252d3d 100%);
        border: 1px solid #2d3748;
        border-radius: 12px;
        padding: 16px;
        text-align: center;
        transition: all 0.3s ease;
    }
    
    .stat-item:hover {
        border-color: #00d9ff;
        box-shadow: 0 4px 15px rgba(0, 217, 255, 0.1);
        transform: translateY(-2px);
    }
    
    .stat-label {
        color: #8892b0;
        font-size: 0.85rem;
        font-weight: 600;
        margin-bottom: 8px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .stat-value {
        color: #00d9ff;
        font-size: 1.8rem;
        font-weight: 700;
    }
    
    .section-title {
        color: #e0e7ff;
        font-size: 1.4em;
        font-weight: 700;
        margin-bottom: 16px;
        padding-bottom: 12px;
        border-bottom: 2px solid #00d9ff;
        display: inline-block;
    }
    
    .input-group {
        display: flex;
        gap: 12px;
        margin-bottom: 16px;
    }
    
    .stSelectbox, .stNumberInput, .stTextInput {
        border-radius: 10px;
    }
    
    .stButton>button {
        border-radius: 10px;
        font-weight: 600;
        padding: 10px 24px;
        transition: all 0.3s ease;
    }
    
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 15px rgba(0, 217, 255, 0.3);
    }
    
    .data-table {
        background: linear-gradient(135deg, #1a1f2e 0%, #252d3d 100%);
        border-radius: 12px;
        border: 1px solid #2d3748;
        padding: 16px;
    }
    
    .expander-header {
        color: #00d9ff;
        font-weight: 700;
        font-size: 1.1em;
    }
    
    .sidebar-title {
        color: #00d9ff;
        font-weight: 700;
        font-size: 1.2em;
        margin-top: 20px;
        margin-bottom: 12px;
    }
    
    .quick-stat {
        background: linear-gradient(135deg, #00d9ff20 0%, #0095ff20 100%);
        border: 1px solid #00d9ff;
        border-radius: 10px;
        padding: 12px 16px;
        margin-bottom: 8px;
        color: #00d9ff;
        font-weight: 600;
    }
    
    hr {
        border: none;
        height: 1px;
        background: linear-gradient(90deg, transparent, #2d3748, transparent);
        margin: 20px 0;
    }
    </style>
    """, unsafe_allow_html=True)

# --- CORE CALCULATION ENGINE ---
def recalculate_item(df, item_name):
    """Full stock calculation logic restored from v8.txt"""
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
    """Applies transaction updates and maintains activity log"""
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
    """Reverses a specific log entry and updates inventory"""
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
    st.markdown('<div class="premium-card">', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        name = st.text_input("📦 Item Name", placeholder="Enter product name")
        uom = st.selectbox("📏 Unit of Measurement", ["pcs", "kg", "box", "ltr", "pkt", "can", "bot"])
    with col2:
        opening = st.number_input("📊 Opening Stock", min_value=0.0, value=0.0)
    
    if st.button("✅ Create Product", use_container_width=True, type="primary"):
        if name:
            new_row = {str(i): 0.0 for i in range(1, 32)}
            new_row.update({"Product Name": name, "UOM": uom, "Opening Stock": opening, 
                            "Total Received": 0.0, "Consumption": 0.0, "Closing Stock": opening,
                            "Physical Count": None, "Variance": 0.0})
            st.session_state.inventory = pd.concat([st.session_state.inventory, pd.DataFrame([new_row])], ignore_index=True)
            save_to_sheet(st.session_state.inventory, "persistent_inventory")
            st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

@st.dialog("📂 Archive Explorer")
def archive_explorer_modal():
    st.markdown('<div class="premium-card">', unsafe_allow_html=True)
    hist_df = load_from_sheet("monthly_history")
    if not hist_df.empty and "Month_Period" in hist_df.columns:
        available_months = sorted(hist_df["Month_Period"].unique().tolist(), reverse=True)
        selected_month = st.selectbox("📅 Select Month Period", options=available_months)
        month_data = hist_df[hist_df["Month_Period"] == selected_month].drop(columns=["Month_Period"])
        buf_month = io.BytesIO()
        with pd.ExcelWriter(buf_month, engine='xlsxwriter') as writer:
            month_data.to_excel(writer, index=False, sheet_name="Archive")
        st.download_button(label=f"📥 Download {selected_month}", data=buf_month.getvalue(), file_name=f"Inventory_{selected_month}.xlsx", use_container_width=True, type="primary")
    else:
        st.info("📭 No historical records found.")
    st.markdown('</div>', unsafe_allow_html=True)

@st.dialog("🔒 Close Month & Rollover")
def close_month_modal():
    st.markdown('<div class="premium-card">', unsafe_allow_html=True)
    st.warning("⚠️ Physical Counts will become new Opening Stocks.", icon="⚠️")
    month_label = st.text_input("📅 Month Label", value=datetime.datetime.now().strftime("%b %Y"))
    if st.button("✅ Confirm Monthly Close", type="primary", use_container_width=True):
        df = st.session_state.inventory.copy()
        history_df = load_from_sheet("monthly_history")
        archive_df = df.copy()
        archive_df["Month_Period"] = month_label
        save_to_sheet(pd.concat([history_df, archive_df], ignore_index=True), "monthly_history")
        
        new_df = df.copy()
        for i in range(1, 32): new_df[str(i)] = 0.0
        for idx, row in new_df.iterrows():
            phys_val = row.get("Physical Count")
            new_df.at[idx, "Opening Stock"] = pd.to_numeric(phys_val) if pd.notna(phys_val) and str(phys_val).strip() != "" else row["Closing Stock"]
        new_df["Total Received"] = 0.0; new_df["Consumption"] = 0.0; new_df["Closing Stock"] = new_df["Opening Stock"]
        new_df["Physical Count"] = None; new_df["Variance"] = 0.0
        save_to_sheet(new_df, "persistent_inventory")
        st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

# --- INITIALIZATION ---
if 'inventory' not in st.session_state:
    st.session_state.inventory = load_from_sheet("persistent_inventory")

# --- MAIN UI ---
st.markdown("""
    <div class="header-card">
        <h1>📦 Warehouse Pro Management</h1>
        <p>v8.5 • Cloud-Powered Inventory System</p>
    </div>
    """, unsafe_allow_html=True)

tab_ops, tab_req, tab_sup = st.tabs(["📊 Operations", "🚚 Requisitions", "📞 Suppliers"])

with tab_ops:
    # --- DAILY RECEIPT SECTION ---
    st.markdown('<div class="premium-card">', unsafe_allow_html=True)
    st.markdown('<h2 class="section-title">📥 Daily Receipt Portal</h2>', unsafe_allow_html=True)
    
    if not st.session_state.inventory.empty:
        item_list = sorted(st.session_state.inventory["Product Name"].unique().tolist())
        col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
        
        with col1:
            sel_item = st.selectbox("🔍 Search & Select Item", options=[""] + item_list, key="receipt_item")
        with col2:
            day_in = st.number_input("📅 Day", 1, 31, datetime.datetime.now().day, key="receipt_day")
        with col3:
            qty_in = st.number_input("📊 Quantity", min_value=0.0, key="receipt_qty")
        with col4:
            if st.button("✅ Confirm", use_container_width=True, type="primary", key="receipt_btn"):
                if sel_item and qty_in > 0:
                    apply_transaction(sel_item, day_in, qty_in)
                    st.success(f"✅ Added {qty_in} units of {sel_item}")
                    st.rerun()
    else:
        st.info("📭 No products available. Please add products first.")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown('<hr>', unsafe_allow_html=True)
    
    # --- QUICK ACTIONS ---
    st.markdown('<div class="premium-card">', unsafe_allow_html=True)
    st.markdown('<h2 class="section-title">⚙️ Quick Actions</h2>', unsafe_allow_html=True)
    
    action_col1, action_col2, action_col3 = st.columns(3)
    with action_col1:
        if st.button("➕ Add New Product", use_container_width=True, key="add_product_btn"):
            add_item_modal()
    with action_col2:
        if st.button("📂 Archive Explorer", use_container_width=True, key="archive_btn"):
            archive_explorer_modal()
    with action_col3:
        if st.button("🔒 Close Month", use_container_width=True, type="primary", key="close_month_btn"):
            close_month_modal()
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # --- PAR ANALYSIS ---
    with st.expander("📈 Multi-Month Weekly Par Analysis", expanded=False):
        st.markdown('<div class="data-table">', unsafe_allow_html=True)
        df_history = load_from_sheet("monthly_history")
        if not df_history.empty and not st.session_state.inventory.empty:
            df_history["Consumption"] = pd.to_numeric(df_history["Consumption"], errors='coerce').fillna(0)
            avg_cons = df_history.groupby("Product Name")["Consumption"].mean().reset_index()
            avg_cons.rename(columns={"Consumption": "Avg_Monthly"}, inplace=True)
            df_par = pd.merge(st.session_state.inventory[["Product Name", "UOM", "Closing Stock"]], avg_cons, on="Product Name", how="left").fillna(0)
            df_par["Weekly Usage"] = (df_par["Avg_Monthly"] / 4.33).round(2)
            df_par["Min (50%)"] = (df_par["Weekly Usage"] * 0.5).round(2)
            df_par["Max (150%)"] = (df_par["Weekly Usage"] * 1.5).round(2)
            st.dataframe(df_par, use_container_width=True, hide_index=True)
        else:
            st.info("📊 Historical data required for Par Analysis.")
        st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown('<hr>', unsafe_allow_html=True)
    
    # --- ACTIVITY LOG & LIVE STATUS ---
    log_col, stat_col = st.columns([1.5, 2.5])
    
    with log_col:
        st.markdown('<div class="premium-card">', unsafe_allow_html=True)
        st.markdown('<h2 class="section-title">📜 Recent Activity</h2>', unsafe_allow_html=True)
        
        logs = load_from_sheet("activity_logs")
        if not logs.empty:
            for _, row in logs.iloc[::-1].head(10).iterrows():
                is_undone = row['Status'] == "Undone"
                row_class = "log-row-undone" if is_undone else ""
                
                col_txt, col_undo = st.columns([4, 1.5])
                with col_txt:
                    st.markdown(f"""
                    <div class="log-row {row_class}">
                        <div class="log-info">
                            <b>📦 {row['Item']}</b><br>
                            Qty: {row['Qty']} | Day: {row['Day']}
                            <span class="log-time">[{row['Timestamp']}]</span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                with col_undo:
                    if not is_undone:
                        if st.button("↩️ Undo", key=f"rev_{row['LogID']}", use_container_width=True):
                            undo_entry(row['LogID'])
        else:
            st.caption("📭 No activity logs available.")
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    with stat_col:
        st.markdown('<div class="premium-card">', unsafe_allow_html=True)
        st.markdown('<h2 class="section-title">📊 Live Stock Status</h2>', unsafe_allow_html=True)
        
        df_status = st.session_state.inventory.copy()
        disp_cols = ["Product Name", "UOM", "Opening Stock", "Total Received", "Closing Stock", "Consumption", "Physical Count", "Variance"]
        for col in disp_cols: 
            if col not in df_status.columns: df_status[col] = 0.0
        
        st.markdown('<div class="data-table">', unsafe_allow_html=True)
        edited_df = st.data_editor(df_status[disp_cols], use_container_width=True, disabled=["Product Name", "UOM", "Total Received", "Closing Stock", "Variance"], hide_index=True)
        st.markdown('</div>', unsafe_allow_html=True)
        
        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("💾 Save & Update", use_container_width=True, type="primary", key="save_status_btn"):
                df_status.update(edited_df)
                for item in df_status["Product Name"]: df_status = recalculate_item(df_status, item)
                save_to_sheet(df_status, "persistent_inventory")
                st.success("✅ Updates saved!")
                st.rerun()
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
            st.download_button("📂 Full XLSX", data=buf_f.getvalue(), file_name="Full_Monthly_Report.xlsx", use_container_width=True)
        
        st.markdown('</div>', unsafe_allow_html=True)

# --- REQUISITIONS TAB ---
with tab_req:
    st.markdown('<div class="premium-card">', unsafe_allow_html=True)
    st.markdown('<h2 class="section-title">🚚 Requisition System</h2>', unsafe_allow_html=True)
    
    meta_df = load_from_sheet("product_metadata")
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        r_item = st.selectbox("🔍 Select Product", options=[""] + sorted(meta_df["Product Name"].tolist()) if not meta_df.empty else [""], key="req_item")
    with col2:
        r_qty = st.number_input("📊 Order Quantity", min_value=0.0, key="req_qty")
    with col3:
        if st.button("➕ Add to List", use_container_width=True, type="primary", key="add_req_btn"):
            if r_item and r_qty > 0:
                orders = load_from_sheet("orders_db", ["Product Name", "Qty", "Supplier", "Status"])
                sup = meta_df[meta_df["Product Name"] == r_item]["Supplier"].values[0] if r_item in meta_df["Product Name"].values else "Unknown"
                save_to_sheet(pd.concat([orders, pd.DataFrame([{"Product Name": r_item, "Qty": r_qty, "Supplier": sup, "Status": "Pending"}])], ignore_index=True), "orders_db")
                st.success(f"✅ Added {r_item} to requisition list")
                st.rerun()
    
    st.markdown('<div class="data-table">', unsafe_allow_html=True)
    st.dataframe(load_from_sheet("orders_db"), use_container_width=True, hide_index=True)
    st.markdown('</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

# --- SUPPLIERS TAB ---
with tab_sup:
    st.markdown('<div class="premium-card">', unsafe_allow_html=True)
    st.markdown('<h2 class="section-title">📞 Supplier Directory</h2>', unsafe_allow_html=True)
    
    meta = load_from_sheet("product_metadata")
    search = st.text_input("🔍 Search Database", placeholder="Search by product name or supplier...")
    
    if search:
        search_lower = search.lower()
        filtered = meta[meta["Product Name"].str.lower().str.contains(search_lower, na=False) | meta["Supplier"].str.lower().str.contains(search_lower, na=False)]
    else:
        filtered = meta
    
    st.markdown('<div class="data-table">', unsafe_allow_html=True)
    edited_meta = st.data_editor(filtered, num_rows="dynamic", use_container_width=True, hide_index=True)
    
    if st.button("💾 Save Directory", use_container_width=True, type="primary", key="save_meta_btn"):
        save_to_sheet(edited_meta, "product_metadata")
        st.success("✅ Directory saved!")
        st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

# --- SIDEBAR (Bulk Operations) ---
with st.sidebar:
    st.markdown('<h2 class="sidebar-title">☁️ Cloud Data Control</h2>', unsafe_allow_html=True)
    
    st.markdown('<div class="premium-card">', unsafe_allow_html=True)
    st.markdown('<p class="section-title">1️⃣ Bulk Inventory Sync</p>', unsafe_allow_html=True)
    
    inv_file = st.file_uploader("📂 Upload Inventory Master", type=["csv", "xlsx"], key="inv_upload")
    if inv_file:
        try:
            raw_df = pd.read_excel(inv_file, skiprows=4, header=None) if inv_file.name.endswith('.xlsx') else pd.read_csv(inv_file, skiprows=4, header=None)
            new_df = pd.DataFrame()
            new_df["Product Name"] = raw_df[1]; new_df["UOM"] = raw_df[2]
            new_df["Opening Stock"] = pd.to_numeric(raw_df[3], errors='coerce').fillna(0.0)
            for i in range(1, 32): new_df[str(i)] = 0.0
            new_df["Total Received"] = 0.0; new_df["Consumption"] = 0.0; new_df["Closing Stock"] = new_df["Opening Stock"]
            if st.button("🚀 Push Inventory", use_container_width=True, type="primary", key="push_inv_btn"):
                save_to_sheet(new_df.dropna(subset=["Product Name"]), "persistent_inventory")
                st.success("✅ Inventory pushed successfully!")
                st.rerun()
        except Exception as e:
            st.error(f"❌ Error: {e}")
    st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown('<hr>', unsafe_allow_html=True)
    
    st.markdown('<div class="premium-card">', unsafe_allow_html=True)
    st.markdown('<p class="section-title">2️⃣ Bulk Supplier Sync</p>', unsafe_allow_html=True)
    
    meta_upload = st.file_uploader("📂 Upload Product Metadata", type=["csv", "xlsx"], key="meta_upload")
    if meta_upload:
        try:
            new_meta = pd.read_excel(meta_upload) if meta_upload.name.endswith('.xlsx') else pd.read_csv(meta_upload)
            if st.button("🚀 Push Metadata", use_container_width=True, type="primary", key="push_meta_btn"):
                save_to_sheet(new_meta, "product_metadata")
                st.success("✅ Metadata pushed successfully!")
                st.rerun()
        except Exception as e:
            st.error(f"❌ Error: {e}")
    st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown('<hr>', unsafe_allow_html=True)
    
    st.markdown('<div class="premium-card">', unsafe_allow_html=True)
    if st.button("🗑️ Reset Cache", use_container_width=True, key="reset_cache_btn"):
        st.cache_data.clear()
        st.success("✅ Cache cleared!")
        st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)
