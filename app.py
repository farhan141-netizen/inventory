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

with tab_ops:
    m_col, a_col = st.columns([3, 1])
    with m_col:
        st.subheader("📥 Daily Receipt")
        if not st.session_state.inventory.empty:
            item_list = sorted(st.session_state.inventory["Product Name"].unique().tolist())
            c1, c2, c3 = st.columns([2, 1, 1])
            with c1: sel_item = st.selectbox("Item", options=[""] + item_list)
            with c2: day_in = st.number_input("Day", 1, 31, datetime.datetime.now().day)
            with c3: qty_in = st.number_input("Qty", min_value=0.0)
            if st.button("Confirm Receipt", type="primary"):
                if sel_item and qty_in > 0:
                    apply_transaction(sel_item, day_in, qty_in); st.rerun()

    with a_col:
        st.subheader("Actions")
        if st.button("➕ Add Item", use_container_width=True): add_item_modal()
        if st.button("🔒 Close Month", type="primary", use_container_width=True): close_month_modal()

    st.divider()
    
    st.subheader("📊 Live Stock Status & Variance")
    df_status = st.session_state.inventory.copy()
    
    # Ensure Variance columns exist
    if "Physical Count" not in df_status.columns: df_status["Physical Count"] = 0.0
    if "Variance" not in df_status.columns: df_status["Variance"] = 0.0
    
    disp_cols = ["Product Name", "UOM", "Opening Stock", "Total Received", "Closing Stock", "Consumption", "Physical Count", "Variance"]
    
    # Data Editor for Physical Count and Consumption
    edited_df = st.data_editor(df_status[disp_cols], use_container_width=True, 
                               disabled=["Product Name", "UOM", "Total Received", "Closing Stock", "Variance"],
                               hide_index=True)
    
    if st.button("💾 Save Calculations & Update Variance", use_container_width=True):
        df_status.update(edited_df)
        # Force recalculate every row to update Variance
        for item in df_status["Product Name"]:
            df_status = recalculate_item(df_status, item)
        st.session_state.inventory = df_status
        save_to_sheet(df_status, "persistent_inventory")
        st.success("Variance and Stocks Updated!")
        st.rerun()

# --- TAB 2 & 3 (Omitted for brevity, but same as previous v4) ---

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




