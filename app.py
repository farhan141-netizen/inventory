import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
import datetime
import uuid

# --- CLOUD CONNECTION ---
conn = st.connection("gsheets", type=GSheetsConnection)

def load_from_sheet(worksheet_name, default_cols=None):
    """Safely load data and ensure default columns exist if sheet is new"""
    try:
        df = conn.read(worksheet=worksheet_name, ttl="2s")
        if df.empty and default_cols:
            return pd.DataFrame(columns=default_cols)
        return df
    except Exception:
        if default_cols:
            return pd.DataFrame(columns=default_cols)
        return pd.DataFrame()

def save_to_sheet(df, worksheet_name):
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
    </style>
    """, unsafe_allow_html=True)

# --- ENGINE ---
def recalculate_item(df, item_name):
    if item_name not in df["Product Name"].values: return df
    df.columns = [str(col) for col in df.columns]
    idx = df[df["Product Name"] == item_name].index[0]
    
    day_cols = [str(i) for i in range(1, 32)]
    existing_day_cols = [col for col in day_cols if col in df.columns]
    
    for col in existing_day_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    total_received = df.loc[idx, existing_day_cols].sum()
    df.at[idx, "Total Received"] = total_received
    
    opening = pd.to_numeric(df.at[idx, "Opening Stock"], errors='coerce') or 0
    consumption = pd.to_numeric(df.at[idx, "Consumption"], errors='coerce') or 0
    df.at[idx, "Closing Stock"] = opening + total_received - consumption
    return df

def apply_transaction(item_name, day_num, qty, is_undo=False, log_type="Addition"):
    df = st.session_state.inventory
    df.columns = [str(col) for col in df.columns]
    
    if item_name in df["Product Name"].values:
        idx = df[df["Product Name"] == item_name].index[0]
        col_name = str(int(day_num))
        
        if col_name not in df.columns: df[col_name] = 0
            
        current_val = pd.to_numeric(df.at[idx, col_name], errors='coerce')
        df.at[idx, col_name] = (0 if pd.isna(current_val) else current_val) + qty
        
        # Log Logic
        if not is_undo:
            new_log = pd.DataFrame([{
                "LogID": str(uuid.uuid4())[:8],
                "Timestamp": datetime.datetime.now().strftime("%H:%M:%S"),
                "Item": item_name,
                "Qty": qty,
                "Day": day_num,
                "Status": "Active",
                "Type": log_type
            }])
            log_cols = ["LogID", "Timestamp", "Item", "Qty", "Day", "Status", "Type"]
            logs_df = load_from_sheet("activity_logs", default_cols=log_cols)
            updated_logs = pd.concat([logs_df, new_log], ignore_index=True)
            save_to_sheet(updated_logs, "activity_logs")
        
        # Update Inventory
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
            st.warning("This item is already undone.")
            return

        item = logs.at[idx, "Item"]
        qty = logs.at[idx, "Qty"]
        day = logs.at[idx, "Day"]
        
        if apply_transaction(item, day, -qty, is_undo=True):
            logs.at[idx, "Status"] = "Undone"
            save_to_sheet(logs, "activity_logs")
            st.success(f"Successfully reversed {qty} for {item}")
            st.rerun()

# --- MODAL: ADD NEW ITEM (FROM LOCAL APP.PY) ---
@st.dialog("➕ Add New Product")
def add_item_modal():
    meta_cols = ["Product Name", "Category", "Supplier", "Contact", "Email", "Min Stock", "Price"]
    meta_df = load_from_sheet("product_metadata", default_cols=meta_cols)
    unique_suppliers = sorted(meta_df["Supplier"].dropna().unique().tolist())
    st.write("Register a new item. Selecting an existing supplier will auto-fill contact details.")
    
    col1, col2 = st.columns(2)
    with col1:
        name = st.text_input("Item Name*")
        supplier_choice = st.selectbox("Supplier Name", options=["New Supplier"] + unique_suppliers)
        
        default_cat, default_contact, default_email = "Ingredients", "", ""
        if supplier_choice != "New Supplier":
            sup_data = meta_df[meta_df["Supplier"] == supplier_choice].iloc[-1]
            supplier_name = supplier_choice
            default_cat = sup_data.get("Category", "Ingredients")
            default_contact = sup_data.get("Contact", "")
            default_email = sup_data.get("Email", "")
        else:
            supplier_name = st.text_input("Enter New Supplier Name")

    with col2:
        cat_list = ["Packaging", "Ingredients", "Equipment", "Cleaning", "Other"]
        cat_idx = cat_list.index(default_cat) if default_cat in cat_list else 1
        category = st.selectbox("Category", options=cat_list, index=cat_idx)
        uom = st.selectbox("Unit (UOM)*", ["pcs", "kg", "box", "ltr", "pkt", "can", "bot", "g", "ml", "roll", "set", "bag"])
        contact = st.text_input("Contact Person / Phone", value=default_contact)
        email = st.text_input("Email Address", value=default_email)

    st.divider()
    c3, c4 = st.columns(2)
    with c3: opening_bal = st.number_input("Opening Stock", min_value=0.0)
    with c4: min_stock = st.number_input("Min Stock Alert", min_value=0.0)

    if st.button("✅ Create Product", use_container_width=True, type="primary"):
        if name and (supplier_name or supplier_choice != "New Supplier"):
            # 1. Update Inventory Sheet
            new_row_inv = {str(i): 0 for i in range(1, 32)}
            new_row_inv.update({
                "Product Name": name, "UOM": uom, "Opening Stock": opening_bal, 
                "Total Received": 0, "Consumption": 0, "Closing Stock": opening_bal
            })
            updated_inv = pd.concat([st.session_state.inventory, pd.DataFrame([new_row_inv])], ignore_index=True)
            save_to_sheet(updated_inv, "persistent_inventory")
            st.session_state.inventory = updated_inv
            
            # 2. Update Metadata Sheet
            new_row_meta = {
                "Product Name": name, "Category": category, "Supplier": supplier_name, 
                "Contact": contact, "Email": email, "Min Stock": min_stock, "Price": 0
            }
            updated_meta = pd.concat([meta_df, pd.DataFrame([new_row_meta])], ignore_index=True)
            save_to_sheet(updated_meta, "product_metadata")
            
            # 3. Log the creation
            apply_transaction(name, 0, opening_bal, is_undo=False, log_type="New Item Added")
            
            st.success(f"Added {name}")
            st.rerun()

# --- DATA INITIALIZATION ---
if 'inventory' not in st.session_state:
    st.session_state.inventory = load_from_sheet("persistent_inventory")

st.title("📦 Warehouse Pro Management (Cloud)")
tab_ops, tab_req, tab_sup = st.tabs(["📊 Inventory Operations", "🚚 Requisitions", "📞 Supplier Directory"])

# --- TAB 1: OPERATIONS ---
with tab_ops:
    st.subheader("📥 Daily Receipt Portal")
    if not st.session_state.inventory.empty:
        item_list = sorted(st.session_state.inventory["Product Name"].unique().tolist())
        c1, c2, c3 = st.columns([2, 1, 1])
        with c1: selected_item = st.selectbox("🔍 Search Item", options=[""] + item_list)
        with c2: day_input = st.number_input("Day (1-31)", 1, 31, datetime.datetime.now().day)
        with c3: qty_input = st.number_input("Qty Received", min_value=0.0, step=0.1)
        if st.button("✅ Confirm Receipt", use_container_width=True, type="primary"):
            if selected_item and qty_input > 0:
                if apply_transaction(selected_item, day_input, qty_input): st.rerun()

    st.divider()
    # "ADD NEW ITEM" Button integrated here
    if st.button("➕ ADD NEW PRODUCT", type="secondary", use_container_width=True): 
        add_item_modal()

    st.divider()
    col_history, col_status = st.columns([1, 2])
    
    with col_history:
        st.subheader("📜 Recent Activity (Undo)")
        logs = load_from_sheet("activity_logs")
        if not logs.empty:
            for _, row in logs.iloc[::-1].head(10).iterrows():
                is_undone = row['Status'] == "Undone"
                status_text = " (REVERSED)" if is_undone else ""
                with st.container():
                    st.markdown(f"""<div class='log-entry'>
                        <b>{row['Item']}</b>: {'+' if row['Qty'] > 0 else ''}{row['Qty']} {status_text}<br>
                        <small>Day {row['Day']} | {row['Timestamp']} | {row.get('Type', 'Addition')}</small>
                        </div>""", unsafe_allow_html=True)
                    if not is_undone:
                        if st.button(f"Undo Entry {row['LogID']}", key=f"btn_{row['LogID']}"):
                            undo_entry(row['LogID'])
        else: st.info("No logs found.")

    with col_status:
        st.subheader("📊 Live Stock Status")
        if not st.session_state.inventory.empty:
            df = st.session_state.inventory
            summary_cols = ["Product Name", "UOM", "Opening Stock", "Total Received", "Closing Stock", "Consumption"]
            edited_df = st.data_editor(df[summary_cols], use_container_width=True, disabled=["Product Name", "UOM", "Total Received", "Closing Stock"])
            if st.button("💾 Save Table Changes"):
                df.update(edited_df)
                for item in df["Product Name"]: df = recalculate_item(df, item)
                save_to_sheet(df, "persistent_inventory")
                st.success("Cloud Updated!")
                st.rerun()

# --- TAB 2: REQUISITIONS ---
with tab_req:
    st.subheader("🚚 Pending Store Requisitions")
    orders_df = load_from_sheet("orders_db")
    if not orders_df.empty:
        st.dataframe(orders_df, use_container_width=True)
        if st.button("🗑️ Clear All Orders"):
            save_to_sheet(pd.DataFrame(columns=orders_df.columns), "orders_db")
            st.rerun()
    else: st.info("No pending requests.")

# --- TAB 3: SUPPLIER DIRECTORY ---
with tab_sup:
    st.subheader("📞 Supplier & Product Directory")
    meta_cols = ["Product Name", "Category", "Supplier", "Contact", "Email", "Min Stock", "Price"]
    meta_df = load_from_sheet("product_metadata", default_cols=meta_cols)
    
    search = st.text_input("🔍 Search Directory (Item or Supplier)").lower()
    if search:
        filtered = meta_df[
            meta_df["Product Name"].str.lower().str.contains(search, na=False) | 
            meta_df["Supplier"].str.lower().str.contains(search, na=False)
        ]
    else:
        filtered = meta_df

    edited_meta = st.data_editor(filtered, num_rows="dynamic", use_container_width=True)
    
    if st.button("💾 Save Directory Changes"):
        if search:
            meta_df.update(edited_meta)
            save_to_sheet(meta_df, "product_metadata")
        else:
            save_to_sheet(edited_meta, "product_metadata")
        st.success("Directory Updated!")
        st.rerun()

# --- SIDEBAR ---
with st.sidebar:
    st.header("Cloud Data Control")
    st.subheader("1. Master Inventory Sync")
    inv_file = st.file_uploader("Upload Inventory Master", type=["csv", "xlsx"], key="inv_up")
    if inv_file:
        try:
            raw_df = pd.read_excel(inv_file, skiprows=4, header=None) if inv_file.name.endswith('.xlsx') else pd.read_csv(inv_file, skiprows=4, header=None)
            new_df = pd.DataFrame()
            new_df["Product Name"] = raw_df[1]; new_df["UOM"] = raw_df[2]
            new_df["Opening Stock"] = pd.to_numeric(raw_df[3], errors='coerce').fillna(0)
            for i in range(1, 32): new_df[str(i)] = 0
            new_df["Total Received"] = 0; new_df["Consumption"] = 0
            new_df["Closing Stock"] = new_df["Opening Stock"]
            new_df = new_df.dropna(subset=["Product Name"])
            if st.button("🚀 Push Inventory to Cloud"):
                save_to_sheet(new_df, "persistent_inventory")
                st.success("Master Inventory Overwritten!")
                st.rerun()
        except Exception as e: st.error(f"Error: {e}")

    st.divider()
    st.subheader("2. Bulk Supplier Directory Sync")
    meta_file = st.file_uploader("Upload Product Metadata", type=["csv", "xlsx"], key="meta_up")
    if meta_file:
        try:
            new_meta = pd.read_excel(meta_file) if meta_file.name.endswith('.xlsx') else pd.read_csv(meta_file)
            if st.button("🚀 Push Directory to Cloud"):
                required_cols = ["Product Name", "Category", "Supplier", "Contact", "Email", "Min Stock", "Price"]
                for col in required_cols:
                    if col not in new_meta.columns: new_meta[col] = ""
                save_to_sheet(new_meta[required_cols], "product_metadata")
                st.success("Directory Overwritten!")
                st.rerun()
        except Exception as e: st.error(f"Error: {e}")

    st.divider()
    if st.button("🗑️ Reset System Cache"):
        st.cache_data.clear()
        st.rerun()
