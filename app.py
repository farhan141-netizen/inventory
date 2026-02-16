import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
import datetime
import uuid

# --- CLOUD CONNECTION ---
# This automatically uses the Secrets you configured in the Streamlit Dashboard
conn = st.connection("gsheets", type=GSheetsConnection)

def load_from_sheet(worksheet_name, default_cols=None):
    """Safely load data from a specific tab in Google Sheets"""
    try:
        return conn.read(worksheet=worksheet_name, ttl="10s")
    except:
        if default_cols:
            return pd.DataFrame(columns=default_cols)
        return pd.DataFrame()

def save_to_sheet(df, worksheet_name):
    """Write dataframe to Google Sheets and clear cache"""
    conn.update(worksheet=worksheet_name, data=df)
    st.cache_data.clear()

# --- PAGE CONFIG ---
st.set_page_config(page_title="Warehouse Pro Cloud", layout="wide")

# --- CUSTOM CSS (Kept exactly as your original) ---
st.markdown("""
    <style>
    .main { background-color: #0e1117; }
    .stMetric { background-color: #1e2130; padding: 15px; border-radius: 10px; border: 1px solid #333; }
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] { 
        height: 50px; white-space: pre-wrap; background-color: #1e2130; 
        border-radius: 5px 5px 0 0; padding: 10px 20px; color: white;
    }
    .stTabs [aria-selected="true"] { background-color: #00ffcc !important; color: #000 !important; }
    .log-entry { border-left: 3px solid #00ffcc; padding: 10px; margin-bottom: 10px; background: #1e2130; border-radius: 0 5px 5px 0; }
    .batch-container { 
        border: 1px solid #333; background-color: #1e2130; padding: 20px; border-radius: 10px; margin-bottom: 20px; 
    }
    .followup-alert { color: #ff4b4b; font-weight: bold; border: 1px solid #ff4b4b; padding: 2px 5px; border-radius: 3px; }
    </style>
    """, unsafe_allow_html=True)

# --- UPDATED ENGINE ---
def recalculate_item(df, item_name):
    if item_name not in df["Product Name"].values: return df
    idx = df[df["Product Name"] == item_name].index[0]
    day_cols = [str(i) for i in range(1, 32)]
    for col in day_cols: 
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    total_received = df.iloc[idx][day_cols].sum()
    df.at[idx, "Total Received"] = total_received
    df.at[idx, "Closing Stock"] = df.at[idx, "Opening Stock"] + total_received - df.at[idx, "Consumption"]
    return df

def apply_transaction(item_name, day_num, qty, log_type="Addition"):
    df = st.session_state.inventory
    if item_name in df["Product Name"].values:
        idx = df[df["Product Name"] == item_name].index[0]
        col_name = str(int(day_num))
        if log_type != "New Item Added" and col_name in df.columns:
            current_val = pd.to_numeric(df.at[idx, col_name], errors='coerce')
            df.at[idx, col_name] = (0 if pd.isna(current_val) else current_val) + qty
        df = recalculate_item(df, item_name)
        st.session_state.inventory = df
        save_to_sheet(df, "persistent_inventory")
        append_log(item_name, day_num, qty, log_type)
        return True
    return False

def append_log(item, day, qty, log_type):
    logs = load_from_sheet("activity_logs", ["id", "timestamp", "item", "day", "qty", "type", "undone"])
    new_log = pd.DataFrame([{
        "id": str(uuid.uuid4())[:8],
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "item": item,
        "day": int(day),
        "qty": float(qty),
        "type": log_type,
        "undone": False
    }])
    updated_logs = pd.concat([new_log, logs], ignore_index=True)
    save_to_sheet(updated_logs, "activity_logs")

def undo_transaction(log_id):
    logs = load_from_sheet("activity_logs")
    if log_id in logs["id"].values:
        idx = logs[logs["id"] == log_id].index[0]
        if logs.at[idx, "undone"]: return
        item, day, qty = logs.at[idx, "item"], logs.at[idx, "day"], logs.at[idx, "qty"]
        if apply_transaction(item, day, -qty, log_type=f"Undo ({log_id})"):
            logs.at[idx, "undone"] = True
            save_to_sheet(logs, "activity_logs")
            st.toast(f"Undone: {item}")
            st.rerun()

# --- MODAL: ADD NEW ITEM ---
@st.dialog("➕ Add New Product")
def add_item_modal():
    meta_df = load_from_sheet("product_metadata", ["Product Name", "Category", "Supplier", "Contact", "Email", "Min Stock", "Price"])
    unique_suppliers = sorted(meta_df["Supplier"].dropna().unique().tolist())
    st.write("Register a new item.")
    
    col1, col2 = st.columns(2)
    with col1:
        name = st.text_input("Item Name*")
        supplier_choice = st.selectbox("Supplier Name", options=["New Supplier"] + unique_suppliers)
        if supplier_choice != "New Supplier":
            sup_data = meta_df[meta_df["Supplier"] == supplier_choice].iloc[-1]
            supplier_name = supplier_choice
            default_cat = sup_data.get("Category", "Ingredients")
            default_contact = sup_data.get("Contact", "")
            default_email = sup_data.get("Email", "")
        else:
            supplier_name = st.text_input("Enter New Supplier Name")
            default_cat, default_contact, default_email = "Ingredients", "", ""

    with col2:
        cat_list = ["Packaging", "Ingredients", "Equipment", "Cleaning", "Other"]
        cat_idx = cat_list.index(default_cat) if default_cat in cat_list else 1
        category = st.selectbox("Category", options=cat_list, index=cat_idx)
        uom = st.selectbox("Unit (UOM)*", ["pcs", "kg", "box", "ltr", "pkt", "can", "bot", "g", "ml", "roll", "set", "bag"])
        contact = st.text_input("Contact Person / Phone", value=default_contact)
        email = st.text_input("Email Address", value=default_email)

    c3, c4 = st.columns(2)
    with c3: opening_bal = st.number_input("Opening Stock", min_value=0.0)
    with c4: min_stock = st.number_input("Min Stock Alert", min_value=0.0)

    if st.button("✅ Create Product", use_container_width=True, type="primary"):
        if name:
            new_row_inv = {str(i): 0 for i in range(1, 32)}
            new_row_inv.update({"Product Name": name, "UOM": uom, "Opening Stock": opening_bal, "Total Received": 0, "Consumption": 0, "Closing Stock": opening_bal})
            st.session_state.inventory = pd.concat([st.session_state.inventory, pd.DataFrame([new_row_inv])], ignore_index=True)
            
            new_row_meta = {"Product Name": name, "Category": category, "Supplier": supplier_name, "Contact": contact, "Email": email, "Min Stock": min_stock, "Price": 0}
            meta_df = pd.concat([meta_df, pd.DataFrame([new_row_meta])], ignore_index=True)
            
            save_to_sheet(st.session_state.inventory, "persistent_inventory")
            save_to_sheet(meta_df, "product_metadata")
            append_log(name, 0, opening_bal, "New Item Added")
            st.rerun()

# --- APP START ---
if 'inventory' not in st.session_state:
    st.session_state.inventory = load_from_sheet("persistent_inventory")

st.title("📦 Warehouse Pro Management (Cloud)")
tab_ops, tab_req, tab_sup = st.tabs(["📊 Inventory Operations", "🚚 Requisitions", "📞 Supplier Directory"])

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
    if st.button("➕ ADD NEW ITEM"): add_item_modal()

    col_log, col_table = st.columns([1, 2])
    with col_log:
        st.subheader("📜 Activity History")
        all_logs = load_from_sheet("activity_logs")
        if not all_logs.empty:
            for i, row in all_logs.head(10).iterrows():
                with st.container():
                    st.write(f"**{row['qty']}** to {row['item']} (D{row['day']})")
                    if not row['undone'] and st.button("Undo", key=f"un_{row['id']}"): undo_transaction(row['id'])
                st.divider()

    with col_table:
        st.subheader("📊 Live Stock Status")
        if not st.session_state.inventory.empty:
            df = st.session_state.inventory
            summary_cols = ["Product Name", "UOM", "Opening Stock", "Total Received", "Closing Stock", "Consumption"]
            edited_df = st.data_editor(df[summary_cols], use_container_width=True, disabled=["Product Name", "UOM", "Total Received", "Closing Stock"])
            if st.button("💾 Save Changes"):
                df.update(edited_df)
                for item in df["Product Name"]: df = recalculate_item(df, item)
                save_to_sheet(df, "persistent_inventory")
                st.success("Cloud Updated!")
                st.rerun()

with tab_req:
    st.subheader("🚚 Pending Requisitions")
    orders_df = load_from_sheet("orders_db", ["OrderID", "Date", "From", "Item", "Qty", "Status", "FollowUp"])
    if not orders_df.empty:
        pending = orders_df[orders_df["Status"] == "Pending"]
        for idx, row in pending.iterrows():
            with st.container():
                c1, c2, c3 = st.columns([2, 1, 1])
                c1.write(f"**{row['Item']}** for {row['From']}")
                send_qty = c2.number_input("Qty to Send", value=float(row['Qty']), key=f"send_{idx}")
                if c3.button("📦 Dispatch", key=f"proc_{idx}"):
                    # Update Warehouse Inventory
                    wh_inv = st.session_state.inventory
                    if row['Item'] in wh_inv["Product Name"].values:
                        it_idx = wh_inv[wh_inv["Product Name"] == row['Item']].index[0]
                        wh_inv.at[it_idx, "Consumption"] += send_qty
                        recalculate_item(wh_inv, row['Item'])
                        save_to_sheet(wh_inv, "persistent_inventory")
                    
                    # Update Order Status
                    orders_df.at[idx, "Status"] = "In Transit"
                    save_to_sheet(orders_df, "orders_db")
                    st.rerun()

with tab_sup:
    st.subheader("📞 Supplier Directory")
    meta_df = load_from_sheet("product_metadata")
    if not meta_df.empty:
        edited_meta = st.data_editor(meta_df, use_container_width=True, num_rows="dynamic")
        if st.button("💾 Save Directory"):
            save_to_sheet(edited_meta, "product_metadata")
            st.rerun()

with st.sidebar:
    st.header("Cloud Data Control")
    inv_file = st.file_uploader("Upload New Master File", type=["csv", "xlsx"])
    if inv_file:
        # Code to process and upload to Google Sheets
        if inv_file.name.endswith('.xlsx'): raw_df = pd.read_excel(inv_file, skiprows=4, header=None)
        else: raw_df = pd.read_csv(inv_file, skiprows=4, header=None)
        # (Processing logic same as your original...)
        new_df = pd.DataFrame()
        new_df["Product Name"] = raw_df[1]; new_df["UOM"] = raw_df[2]
        new_df["Opening Stock"] = pd.to_numeric(raw_df[3], errors='coerce').fillna(0)
        for i in range(1, 32): new_df[str(i)] = 0
        new_df["Consumption"] = 0
        for item in new_df["Product Name"]: new_df = recalculate_item(new_df, item)
        save_to_sheet(new_df, "persistent_inventory")
        st.success("Sheet Synced!")
