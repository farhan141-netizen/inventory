import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
import datetime
import uuid
import io

# --- CLOUD CONNECTION ---
conn = st.connection("gsheets", type=GSheetsConnection)

def clean_dataframe(df):
    if df is None or df.empty: return df
    df = df.loc[:, ~df.columns.str.contains('^Unnamed', na=False)]
    df = df.dropna(axis=1, how='all')
    df = df.loc[:, ~df.columns.duplicated()]
    df.columns = [str(col).strip() for col in df.columns]
    return df

def load_from_sheet(worksheet_name, default_cols=None):
    try:
        df = conn.read(worksheet=worksheet_name, ttl="2s")
        df = clean_dataframe(df)
        if df is None or df.empty:
            return pd.DataFrame(columns=default_cols) if default_cols else pd.DataFrame()
        return df
    except Exception:
        return pd.DataFrame(columns=default_cols) if default_cols else pd.DataFrame()

def save_to_sheet(df, worksheet_name):
    df = clean_dataframe(df)
    conn.update(worksheet=worksheet_name, data=df)
    st.cache_data.clear()

# --- PAGE CONFIG ---
st.set_page_config(page_title="Warehouse Pro Cloud v4", layout="wide")

# --- ENGINE ---
def recalculate_item(df, item_name):
    if item_name not in df["Product Name"].values: return df
    idx = df[df["Product Name"] == item_name].index[0]
    
    # Calculate Total Received (Days 1-31)
    day_cols = [str(i) for i in range(1, 32)]
    for col in day_cols:
        if col not in df.columns: df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
    
    total_received = df.loc[idx, day_cols].sum()
    df.at[idx, "Total Received"] = total_received
    
    # Calculate Closing Stock
    opening = pd.to_numeric(df.at[idx, "Opening Stock"], errors='coerce') or 0.0
    consumption = pd.to_numeric(df.at[idx, "Consumption"], errors='coerce') or 0.0
    closing = opening + total_received - consumption
    df.at[idx, "Closing Stock"] = closing
    
    # Update Variance immediately
    if "Physical Count" in df.columns:
        physical = pd.to_numeric(df.at[idx, "Physical Count"], errors='coerce')
        if not pd.isna(physical):
            df.at[idx, "Variance"] = physical - closing
    return df

def apply_transaction(item_name, day_num, qty):
    df = st.session_state.inventory
    if item_name in df["Product Name"].values:
        idx = df[df["Product Name"] == item_name].index[0]
        col_name = str(int(day_num))
        if col_name != "0":
            current_val = pd.to_numeric(df.at[idx, col_name], errors='coerce') or 0.0
            df.at[idx, col_name] = current_val + float(qty)
        
        # Log entry
        new_log = pd.DataFrame([{
            "LogID": str(uuid.uuid4())[:8],
            "Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Item": item_name, "Qty": qty, "Day": day_num, "Status": "Active", "Type": "Addition"
        }])
        logs_df = load_from_sheet("activity_logs", ["LogID", "Timestamp", "Item", "Qty", "Day", "Status", "Type"])
        save_to_sheet(pd.concat([logs_df, new_log], ignore_index=True), "activity_logs")
        
        df = recalculate_item(df, item_name)
        st.session_state.inventory = df
        save_to_sheet(df, "persistent_inventory")
        return True
    return False

# --- MODALS ---
@st.dialog("➕ Add New Product")
def add_item_modal():
    meta_cols = ["Product Name", "Category", "Supplier", "Sales Person", "Contact 1", "Email", "Min Stock"]
    meta_df = load_from_sheet("product_metadata", default_cols=meta_cols)
    
    col1, col2 = st.columns(2)
    with col1:
        name = st.text_input("Item Name*")
        supplier = st.text_input("Supplier Name")
    with col2:
        uom = st.selectbox("Unit", ["pcs", "kg", "box", "ltr", "pkt"])
        opening = st.number_input("Opening Stock", min_value=0.0)

    if st.button("✅ Create", use_container_width=True):
        if name:
            new_row = {str(i): 0.0 for i in range(1, 32)}
            new_row.update({"Product Name": name, "UOM": uom, "Opening Stock": opening, 
                            "Total Received": 0.0, "Consumption": 0.0, "Closing Stock": opening,
                            "Physical Count": 0.0, "Variance": 0.0})
            st.session_state.inventory = pd.concat([st.session_state.inventory, pd.DataFrame([new_row])], ignore_index=True)
            save_to_sheet(st.session_state.inventory, "persistent_inventory")
            st.rerun()

@st.dialog("🔒 Close Month & Rollover")
def close_month_modal():
    st.warning("This will archive current data and set Physical Counts as the new Opening Stock.")
    month_label = st.text_input("Archive Name (e.g., Jan 2024)", datetime.datetime.now().strftime("%b %Y"))
    
    if st.button("Confirm Monthly Close", type="primary", use_container_width=True):
        df = st.session_state.inventory.copy()
        
        # 1. Archive to history (Including Physical and Variance)
        history_df = load_from_sheet("monthly_history")
        archive_df = df.copy()
        archive_df["Month_Period"] = month_label
        updated_history = pd.concat([history_df, archive_df], ignore_index=True)
        save_to_sheet(updated_history, "monthly_history")
        
        # 2. Reset for New Month
        new_df = df.copy()
        for i in range(1, 32): new_df[str(i)] = 0.0
        
        # CRITICAL: Opening Stock = Physical Count (if provided), else Closing Stock
        for idx, row in new_df.iterrows():
            phys = pd.to_numeric(row.get("Physical Count"), errors='coerce')
            if not pd.isna(phys) and phys > 0:
                new_df.at[idx, "Opening Stock"] = phys
            else:
                new_df.at[idx, "Opening Stock"] = row["Closing Stock"]
        
        new_df["Total Received"] = 0.0
        new_df["Consumption"] = 0.0
        new_df["Closing Stock"] = new_df["Opening Stock"]
        new_df["Physical Count"] = 0.0
        new_df["Variance"] = 0.0
        
        save_to_sheet(new_df, "persistent_inventory")
        st.success(f"Archived {month_label}. New month started!")
        st.rerun()

# --- INITIALIZATION ---
if 'inventory' not in st.session_state:
    st.session_state.inventory = load_from_sheet("persistent_inventory")

# --- MAIN UI ---
st.title("📦 Warehouse Pro Management v4")
tab_ops, tab_req, tab_sup = st.tabs(["📊 Operations", "🚚 Requisitions", "📞 Suppliers"])

# --- TAB 1: OPERATIONS ---
with tab_ops:
    main_col, action_col = st.columns([3, 1])
    with main_col:
        st.markdown('<div class="receipt-card">', unsafe_allow_html=True)
        st.subheader("📥 Daily Receipt Portal")
        if not st.session_state.inventory.empty:
            item_list = sorted(st.session_state.inventory["Product Name"].unique().tolist())
            rc1, rc2, rc3 = st.columns([2, 1, 1])
            with rc1: selected_item = st.selectbox("🔍 Search Item", options=[""] + item_list)
            with rc2: day_input = st.number_input("Day", 1, 31, datetime.datetime.now().day)
            with rc3: qty_input = st.number_input("Qty", min_value=0.0, step=0.1)
            if st.button("✅ Confirm Receipt", use_container_width=True, type="primary"):
                if selected_item and qty_input > 0:
                    apply_transaction(selected_item, day_input, qty_input); st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    with action_col:
        st.markdown('<div class="action-card">', unsafe_allow_html=True)
        st.subheader("⚙️ Actions")
        if st.button("➕ ADD NEW PRODUCT", use_container_width=True): 
            add_item_modal()
        if st.button("🔒 CLOSE MONTH", type="primary", use_container_width=True):
            close_month_modal()
        st.markdown('</div>', unsafe_allow_html=True)

    st.divider()
    col_history, col_status = st.columns([1, 2.2])
    
    with col_history:
        st.subheader("📜 Recent Activity")
        logs = load_from_sheet("activity_logs")
        if not logs.empty:
            log_days = sorted(logs["Day"].unique().tolist())
            filter_choice = st.selectbox("📅 Filter by Day", options=["All Days"] + log_days)
            filtered_logs = logs.iloc[::-1]
            if filter_choice != "All Days":
                filtered_logs = filtered_logs[filtered_logs["Day"] == filter_choice]
            
            items_per_page = 20
            total_pages = max(1, (len(filtered_logs) // items_per_page) + 1)
            page_num = st.selectbox(f"Page (Total {total_pages})", range(1, total_pages + 1)) if total_pages > 1 else 1
            start_idx = (page_num - 1) * items_per_page
            page_logs = filtered_logs.iloc[start_idx : start_idx + items_per_page]
            
            st.markdown('<div class="log-container">', unsafe_allow_html=True)
            for _, row in page_logs.iterrows():
                is_undone = str(row.get('Status', 'Active')) == "Undone"
                status_text = " (REVERSED)" if is_undone else ""
                l_col, r_col = st.columns([3, 1])
                with l_col:
                    st.markdown(f"<div class='log-text'><b>{row['Item']}</b>: {row['Qty']}{status_text}<br><span class='log-meta'>Day {row['Day']} | {row['Timestamp']}</span></div>", unsafe_allow_html=True)
                with r_col:
                    if not is_undone and st.button("Undo", key=f"undo_{row['LogID']}", use_container_width=True):
                        undo_entry(row['LogID'])
                st.markdown("<hr style='margin:5px 0; border:0.1px solid #333'>", unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)

    with col_status:
        st.subheader("📊 Live Stock Status")
        if not st.session_state.inventory.empty:
            df_status = clean_dataframe(st.session_state.inventory)
            if "Physical Count" not in df_status.columns: df_status["Physical Count"] = 0.0
            if "Variance" not in df_status.columns: df_status["Variance"] = 0.0
            
            short_cols = ["Product Name", "UOM", "Opening Stock", "Total Received", "Closing Stock", "Consumption", "Physical Count", "Variance"]
            for c in short_cols:
                if c not in df_status.columns: df_status[c] = 0.0
            
            edited_df = st.data_editor(df_status[short_cols], use_container_width=True, 
                                       disabled=["Product Name", "UOM", "Total Received", "Closing Stock", "Variance"],
                                       hide_index=True)
            
            # --- EXCEL EXPORT LOGIC ---
            day_cols = [str(i) for i in range(1, 32)]
            detailed_df = df_status[["Product Name", "UOM", "Opening Stock"] + day_cols + ["Total Received", "Consumption", "Closing Stock"]]

            save_col, dl_short, dl_full = st.columns([1, 1, 1])
            with save_col:
                if st.button("💾 Save Changes & Update Variance", use_container_width=True):
                    df_status.update(edited_df)
                    for item in df_status["Product Name"]: df_status = recalculate_item(df_status, item)
                    save_to_sheet(df_status, "persistent_inventory"); st.rerun()
            with dl_short:
                buf_short = io.BytesIO()
                with pd.ExcelWriter(buf_short, engine='xlsxwriter') as writer:
                    df_status[short_cols].to_excel(writer, index=False, sheet_name='Summary')
                st.download_button("📥 Summary Excel", data=buf_short.getvalue(), file_name="Stock_Summary.xlsx", use_container_width=True)
            with dl_full:
                buf_full = io.BytesIO()
                with pd.ExcelWriter(buf_full, engine='xlsxwriter') as writer:
                    detailed_df.to_excel(writer, index=False, sheet_name='Detailed_1_31')
                st.download_button("📂 Full Report (1-31)", data=buf_full.getvalue(), file_name="Detailed_Stock_Report.xlsx", use_container_width=True)

# --- TAB 2: REQUISITIONS ---
with tab_req:
    st.subheader("🚚 Create Requisition")
    meta_df = load_from_sheet("product_metadata")
    if not meta_df.empty:
        col_it, col_qt, col_ad = st.columns([3, 1, 1])
        with col_it: req_item = st.selectbox("Select Product", options=[""] + sorted(meta_df["Product Name"].tolist()))
        with col_qt: req_qty = st.number_input("Order Qty", min_value=0.0)
        with col_ad:
            st.write("##")
            if st.button("➕ Add to List", use_container_width=True):
                if req_item and req_qty > 0:
                    orders_df = load_from_sheet("orders_db", default_cols=["Product Name", "Qty", "Supplier", "Contact", "Status"])
                    sup_info = meta_df[meta_df["Product Name"] == req_item].iloc[0]
                    new_order = pd.DataFrame([{
                        "Product Name": req_item, "Qty": req_qty, 
                        "Supplier": sup_info.get("Supplier", "N/A"), 
                        "Contact": sup_info.get("Contact 1", "N/A"),
                        "Status": "Pending"
                    }])
                    save_to_sheet(pd.concat([orders_df, new_order], ignore_index=True), "orders_db")
                    st.success(f"Added {req_item}"); st.rerun()

    st.divider()
    st.subheader("📋 Pending Requisitions")
    orders_df = load_from_sheet("orders_db")
    if not orders_df.empty:
        st.dataframe(orders_df, use_container_width=True)
        if st.button("🗑️ Clear All Orders"):
            save_to_sheet(pd.DataFrame(columns=orders_df.columns), "orders_db"); st.rerun()

# --- TAB 3: SUPPLIER DIRECTORY ---
with tab_sup:
    st.subheader("📞 Supplier Directory")
    meta_df = load_from_sheet("product_metadata")
    search = st.text_input("🔍 Search Directory").lower()
    filtered = meta_df[meta_df["Product Name"].str.lower().str.contains(search, na=False) | meta_df["Supplier"].str.lower().str.contains(search, na=False)] if search else meta_df
    edited_meta = st.data_editor(filtered, num_rows="dynamic", use_container_width=True)
    if st.button("💾 Save Directory Changes"):
        save_to_sheet(edited_meta if not search else meta_df.update(edited_meta) or meta_df, "product_metadata")
        st.rerun()


# --- SIDEBAR ---
with st.sidebar:
    st.header("Cloud Data Control")
    st.subheader("1. Master Inventory Sync")
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
                save_to_sheet(new_meta, "product_metadata"); st.success("Metadata uploaded!"); st.rerun()
        except Exception as e: st.error(f"Error: {e}")

    st.divider()
    if st.button("🗑️ Reset Cache"):
        st.cache_data.clear(); st.rerun()



