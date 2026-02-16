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
st.set_page_config(page_title="Warehouse Pro Cloud", layout="wide")

# --- CUSTOM CSS ---
st.markdown("""
    <style>
    .main { background-color: #0e1117; }
    .stMetric { background-color: #1e2130; padding: 15px; border-radius: 10px; border: 1px solid #333; }
    .stTabs [data-baseweb="tab"] { height: 50px; background-color: #1e2130; color: white; border-radius: 5px 5px 0 0; }
    .stTabs [aria-selected="true"] { background-color: #00ffcc !important; color: #000 !important; }
    .log-entry { border-left: 3px solid #00ffcc; padding: 10px; margin-bottom: 8px; background: #1e2130; border-radius: 0 5px 5px 0; }
    .pagination-info { font-size: 0.85rem; color: #00ffcc; margin-bottom: 10px; }
    /* Align buttons to the bottom of the row */
    div[data-testid="stVerticalBlock"] > div:has(button) {
        justify-content: flex-end;
    }
    </style>
    """, unsafe_allow_html=True)

# --- ENGINE ---
def recalculate_item(df, item_name):
    if item_name not in df["Product Name"].values: return df
    df = clean_dataframe(df)
    idx = df[df["Product Name"] == item_name].index[0]
    day_cols = [str(i) for i in range(1, 32)]
    
    for col in day_cols:
        if col not in df.columns: df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
    
    total_received = df.loc[idx, day_cols].sum()
    df.at[idx, "Total Received"] = total_received
    
    opening = pd.to_numeric(df.at[idx, "Opening Stock"], errors='coerce') or 0.0
    consumption = pd.to_numeric(df.at[idx, "Consumption"], errors='coerce') or 0.0
    df.at[idx, "Closing Stock"] = opening + total_received - consumption
    return df

def apply_transaction(item_name, day_num, qty, is_undo=False, log_type="Addition"):
    df = st.session_state.inventory
    df = clean_dataframe(df)
    
    if item_name in df["Product Name"].values:
        idx = df[df["Product Name"] == item_name].index[0]
        col_name = str(int(day_num))
        
        if col_name != "0":
            if col_name not in df.columns: df[col_name] = 0.0
            current_val = pd.to_numeric(df.at[idx, col_name], errors='coerce')
            df.at[idx, col_name] = (0.0 if pd.isna(current_val) else current_val) + float(qty)
        
        if not is_undo:
            new_log = pd.DataFrame([{
                "LogID": str(uuid.uuid4())[:8],
                "Timestamp": datetime.datetime.now().strftime("%H:%M:%S"),
                "Item": item_name, "Qty": qty, "Day": day_num, "Status": "Active", "Type": log_type
            }])
            logs_df = load_from_sheet("activity_logs", default_cols=["LogID", "Timestamp", "Item", "Qty", "Day", "Status", "Type"])
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
        if logs.at[idx, "Status"] == "Undone":
            st.warning("Already undone.")
            return

        item, qty, day = logs.at[idx, "Item"], logs.at[idx, "Qty"], logs.at[idx, "Day"]
        if apply_transaction(item, day, -qty, is_undo=True):
            logs.at[idx, "Status"] = "Undone"
            save_to_sheet(logs, "activity_logs")
            st.success(f"Reversed {qty} for {item}")
            st.rerun()

# --- MODAL: ADD NEW ITEM ---
@st.dialog("➕ Add New Product")
def add_item_modal():
    meta_cols = ["Product Name", "Category", "Supplier", "Sales Person", "Contact 1", "Contact 2", "Email", "Min Stock"]
    meta_df = load_from_sheet("product_metadata", default_cols=meta_cols)
    unique_suppliers = sorted(meta_df["Supplier"].dropna().unique().tolist())
    
    col1, col2 = st.columns(2)
    with col1:
        name = st.text_input("Item Name*")
        supplier_choice = st.selectbox("Company Name (Supplier)", options=["New Supplier"] + unique_suppliers)
        
        if supplier_choice == "New Supplier":
            supplier_name = st.text_input("Enter Company Name")
            sales_person = st.text_input("Sales Person Name")
            c1 = st.text_input("Contact 1")
            c2 = st.text_input("Contact 2")
            email_addr = st.text_input("Email")
            default_cat = "Ingredients"
        else:
            supplier_name = supplier_choice
            sup_data = meta_df[meta_df["Supplier"] == supplier_choice].iloc[-1]
            sales_person = st.text_input("Sales Person Name", value=sup_data.get("Sales Person", ""))
            c1 = st.text_input("Contact 1", value=sup_data.get("Contact 1", ""))
            c2 = st.text_input("Contact 2", value=sup_data.get("Contact 2", ""))
            email_addr = st.text_input("Email", value=sup_data.get("Email", ""))
            default_cat = sup_data.get("Category", "Ingredients")

    with col2:
        cat_list = ["Packaging", "Ingredients", "Equipment", "Cleaning", "Other"]
        category = st.selectbox("Category", options=cat_list, index=cat_list.index(default_cat) if default_cat in cat_list else 1)
        uom = st.selectbox("Unit (UOM)*", ["pcs", "kg", "box", "ltr", "pkt", "can", "bot", "g", "ml", "roll", "set", "bag"])
        opening_bal = st.number_input("Opening Stock", min_value=0.0)
        min_stock = st.number_input("Min Stock Alert", min_value=0.0)

    if st.button("✅ Create Product", use_container_width=True, type="primary"):
        if name and supplier_name:
            current_inv = clean_dataframe(st.session_state.inventory)
            new_row_inv = {str(i): 0.0 for i in range(1, 32)}
            new_row_inv.update({
                "Product Name": name, "UOM": uom, "Opening Stock": float(opening_bal), 
                "Total Received": 0.0, "Consumption": 0.0, "Closing Stock": float(opening_bal)
            })
            st.session_state.inventory = pd.concat([current_inv, pd.DataFrame([new_row_inv])], ignore_index=True)
            
            new_row_meta = {
                "Product Name": name, "Category": category, "Supplier": supplier_name, 
                "Sales Person": sales_person, "Contact 1": c1, "Contact 2": c2, "Email": email_addr, 
                "Min Stock": float(min_stock)
            }
            updated_meta = pd.concat([meta_df, pd.DataFrame([new_row_meta])], ignore_index=True)
            
            save_to_sheet(st.session_state.inventory, "persistent_inventory")
            save_to_sheet(updated_meta, "product_metadata")
            apply_transaction(name, 0, opening_bal, is_undo=False, log_type="New Item Added")
            st.success(f"Added {name}")
            st.rerun()

# --- INITIALIZATION ---
if 'inventory' not in st.session_state:
    st.session_state.inventory = load_from_sheet("persistent_inventory")

st.title("📦 Warehouse Pro Management (Cloud)")
tab_ops, tab_req, tab_sup = st.tabs(["📊 Inventory Operations", "🚚 Requisitions", "📞 Supplier Directory"])

# --- TAB 1: OPERATIONS ---
with tab_ops:
    main_col, action_col = st.columns([3, 1])

    with main_col:
        st.subheader("📥 Daily Receipt Portal")
        if not st.session_state.inventory.empty:
            item_list = sorted(st.session_state.inventory["Product Name"].unique().tolist())
            rc1, rc2, rc3, rc4 = st.columns([2, 1, 1, 1.2])
            with rc1: selected_item = st.selectbox("🔍 Search Item", options=[""] + item_list)
            with rc2: day_input = st.number_input("Day (1-31)", 1, 31, datetime.datetime.now().day)
            with rc3: qty_input = st.number_input("Qty Received", min_value=0.0, step=0.1)
            with rc4: 
                st.write("##")
                if st.button("✅ Confirm Receipt", use_container_width=True, type="primary"):
                    if selected_item and qty_input > 0:
                        if apply_transaction(selected_item, day_input, qty_input): st.rerun()

    with action_col:
        st.subheader("⚙️ Actions")
        st.write("##")
        if st.button("➕ ADD NEW PRODUCT", type="secondary", use_container_width=True): 
            add_item_modal()

    st.divider()
    col_history, col_status = st.columns([1, 2])
    
    with col_history:
        st.subheader("📜 Recent Activity")
        logs = load_from_sheet("activity_logs")
        if not logs.empty:
            log_days = sorted(logs["Day"].unique().tolist())
            filter_choice = st.selectbox("📅 Filter by Day", options=["All Days"] + log_days)
            filtered_logs = logs.iloc[::-1]
            if filter_choice != "All Days":
                filtered_logs = filtered_logs[filtered_logs["Day"] == filter_choice]
            
            items_per_page = 15
            total_items = len(filtered_logs)
            total_pages = max(1, (total_items // items_per_page) + (1 if total_items % items_per_page > 0 else 0))
            page_num = st.selectbox(f"Page selection (Total {total_pages})", range(1, total_pages + 1)) if total_pages > 1 else 1
            
            start_idx = (page_num - 1) * items_per_page
            page_logs = filtered_logs.iloc[start_idx : start_idx + items_per_page]
            
            st.markdown(f"<div class='pagination-info'>Showing {len(page_logs)} of {total_items} entries</div>", unsafe_allow_html=True)

            for _, row in page_logs.iterrows():
                is_undone = str(row.get('Status', 'Active')) == "Undone"
                status_text = " (REVERSED)" if is_undone else ""
                with st.container():
                    st.markdown(f"<div class='log-entry'><b>{row['Item']}</b>: {row['Qty']} {status_text}<br><small>Day {row['Day']} | {row['Timestamp']}</small></div>", unsafe_allow_html=True)
                    if not is_undone:
                        if st.button(f"Undo {row['LogID']}", key=f"undo_{row['LogID']}"): undo_entry(row['LogID'])
        else:
            st.info("No activity found.")

    with col_status:
        st.subheader("📊 Live Stock Status")
        if not st.session_state.inventory.empty:
            df_status = clean_dataframe(st.session_state.inventory)
            short_cols = ["Product Name", "UOM", "Opening Stock", "Total Received", "Closing Stock", "Consumption"]
            for c in short_cols:
                if c not in df_status.columns: df_status[c] = 0.0
            
            edited_df = st.data_editor(df_status[short_cols], use_container_width=True, disabled=["Product Name", "UOM", "Total Received", "Closing Stock"])
            
            day_cols = [str(i) for i in range(1, 32)]
            for d in day_cols:
                if d not in df_status.columns: df_status[d] = 0.0
            detailed_df = df_status[["Product Name", "UOM", "Opening Stock"] + day_cols + ["Total Received", "Consumption", "Closing Stock"]]

            save_col, dl_short, dl_full = st.columns([1, 1, 1])
            with save_col:
                if st.button("💾 Save Changes", use_container_width=True):
                    df_status.update(edited_df)
                    for item in df_status["Product Name"]: df_status = recalculate_item(df_status, item)
                    save_to_sheet(df_status, "persistent_inventory")
                    st.rerun()
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
                    st.success(f"Added {req_item}")
                    st.rerun()

    st.divider()
    st.subheader("📋 Pending Requisitions")
    orders_df = load_from_sheet("orders_db")
    if not orders_df.empty:
        st.dataframe(orders_df, use_container_width=True)
        if st.button("🗑️ Clear All Orders"):
            save_to_sheet(pd.DataFrame(columns=orders_df.columns), "orders_db")
            st.rerun()

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
            new_df = new_df.dropna(subset=["Product Name"])
            if st.button("🚀 Push Inventory to Cloud"):
                save_to_sheet(new_df, "persistent_inventory")
                st.rerun()
        except Exception as e: st.error(f"Error: {e}")

    st.divider()
    st.subheader("2. Bulk Supplier Directory Sync")
    meta_upload = st.file_uploader("Upload Product Metadata", type=["csv", "xlsx"])
    if meta_upload:
        try:
            new_meta = pd.read_excel(meta_upload) if meta_upload.name.endswith('.xlsx') else pd.read_csv(meta_upload)
            if st.button("🚀 Push Metadata to Cloud"):
                save_to_sheet(new_meta, "product_metadata")
                st.success("Metadata uploaded!")
                st.rerun()
        except Exception as e: st.error(f"Error: {e}")

    st.divider()
    if st.button("🗑️ Reset Cache"):
        st.cache_data.clear()
        st.rerun()
