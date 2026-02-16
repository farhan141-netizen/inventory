import streamlit as st
import pandas as pd
import datetime
import os
import uuid

# --- FILE SETTINGS ---
DB_FILE = "persistent_inventory.csv"
LOG_FILE = "activity_logs.csv"
META_FILE = "product_metadata.csv"
ORDERS_FILE = "orders_db.csv"  # Bridge file for Restaurant requests

st.set_page_config(page_title="Warehouse Pro", layout="wide")

# --- CUSTOM CSS ---
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
        border: 1px solid #333; 
        background-color: #1e2130; 
        padding: 20px; 
        border-radius: 10px; 
        margin-bottom: 20px; 
    }
    .followup-alert { color: #ff4b4b; font-weight: bold; border: 1px solid #ff4b4b; padding: 2px 5px; border-radius: 3px; }
    </style>
    """, unsafe_allow_html=True)

# --- DATA PERSISTENCE ---
def load_data():
    if os.path.exists(DB_FILE): return pd.read_csv(DB_FILE)
    return pd.DataFrame()

def load_metadata():
    if os.path.exists(META_FILE): return pd.read_csv(META_FILE)
    return pd.DataFrame(columns=["Product Name", "Category", "Supplier", "Contact", "Email", "Min Stock", "Price"])

def load_logs():
    if os.path.exists(LOG_FILE): 
        logs = pd.read_csv(LOG_FILE)
        logs['undone'] = logs['undone'].astype(bool)
        return logs
    return pd.DataFrame(columns=["id", "timestamp", "item", "day", "qty", "type", "undone"])

def load_orders():
    if os.path.exists(ORDERS_FILE):
        df = pd.read_csv(ORDERS_FILE)
        # Ensure 'FollowUp' column exists
        if "FollowUp" not in df.columns: df["FollowUp"] = False
        return df
    return pd.DataFrame(columns=["OrderID", "Date", "From", "Item", "Qty", "Status", "FollowUp"])

def save_all(inv_df=None, meta_df=None, orders_df=None):
    if inv_df is not None: inv_df.to_csv(DB_FILE, index=False)
    if meta_df is not None: meta_df.to_csv(META_FILE, index=False)
    if orders_df is not None: orders_df.to_csv(ORDERS_FILE, index=False)

def append_log(item, day, qty, log_type):
    logs = load_logs()
    new_log = pd.DataFrame([{
        "id": str(uuid.uuid4())[:8],
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "item": item,
        "day": int(day),
        "qty": float(qty),
        "type": log_type,
        "undone": False
    }])
    pd.concat([new_log, logs], ignore_index=True).to_csv(LOG_FILE, index=False)

# --- ENGINE ---
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
        save_all(inv_df=df)
        append_log(item_name, day_num, qty, log_type)
        return True
    return False

def undo_transaction(log_id):
    logs = load_logs()
    if log_id in logs["id"].values:
        idx = logs[logs["id"] == log_id].index[0]
        if logs.at[idx, "undone"]: return
        item, day, qty = logs.at[idx, "item"], logs.at[idx, "day"], logs.at[idx, "qty"]
        if apply_transaction(item, day, -qty, log_type=f"Undo ({log_id})"):
            logs.at[idx, "undone"] = True
            logs.to_csv(LOG_FILE, index=False)
            st.toast(f"Undone: {item}")
            st.rerun()

# --- MODAL: ADD NEW ITEM ---
@st.dialog("➕ Add New Product")
def add_item_modal():
    meta_df = load_metadata()
    unique_suppliers = sorted(meta_df["Supplier"].dropna().unique().tolist())
    st.write("Register a new item. Selecting an existing supplier will auto-fill contact details.")
    
    col1, col2 = st.columns(2)
    with col1:
        name = st.text_input("Item Name*")
        supplier_choice = st.selectbox("Supplier Name (Search or Select)", options=["New Supplier"] + unique_suppliers)
        
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
            new_row_inv = {str(i): 0 for i in range(1, 32)}
            new_row_inv.update({"Product Name": name, "UOM": uom, "Opening Stock": opening_bal, "Total Received": 0, "Consumption": 0, "Closing Stock": opening_bal})
            st.session_state.inventory = pd.concat([st.session_state.inventory, pd.DataFrame([new_row_inv])], ignore_index=True)
            new_row_meta = {"Product Name": name, "Category": category, "Supplier": supplier_name, "Contact": contact, "Email": email, "Min Stock": min_stock, "Price": 0}
            meta_df = pd.concat([meta_df, pd.DataFrame([new_row_meta])], ignore_index=True)
            save_all(inv_df=st.session_state.inventory, meta_df=meta_df)
            append_log(name, 0, opening_bal, "New Item Added")
            st.success(f"Added {name}")
            st.rerun()

# --- APP START ---
if 'inventory' not in st.session_state:
    st.session_state.inventory = load_data()

st.title("📦 Warehouse Pro Management")
tab_ops, tab_req, tab_sup = st.tabs(["📊 Inventory Operations", "🚚 Requisitions", "📞 Supplier Directory"])

# --- TAB 1: OPERATIONS ---
with tab_ops:
    with st.container():
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
        else: st.info("Sync Master File to begin.")

    st.divider()
    if st.button("➕ ADD NEW ITEM", type="secondary"): add_item_modal()

    col_log, col_table = st.columns([1, 2])
    with col_log:
        st.subheader("📜 Activity History")
        all_logs = load_logs()
        filter_day = st.selectbox("Filter History by Day", ["All Days"] + [str(i) for i in range(1, 32)])
        display_logs = all_logs if filter_day == "All Days" else all_logs[all_logs["day"] == int(filter_day)]
        
        if display_logs.empty: st.write("No entries.")
        else:
            for i, row in display_logs.head(15).iterrows():
                with st.container():
                    strike = "~~" if row['undone'] else ""
                    status = "🔄" if "Undo" in str(row['type']) else "✅"
                    l1, l2 = st.columns([4, 1])
                    l1.markdown(f"{status} {strike}{row['qty']} to **{row['item']}** (D{row['day']}){strike}")
                    l1.caption(f"{row['timestamp']} | ID: {row['id']}")
                    if not row['undone'] and "Undo" not in str(row['type']):
                        if l2.button("Undo", key=f"un_{row['id']}"): undo_transaction(row['id'])
                st.markdown("---")

    with col_table:
        st.subheader("📊 Live Stock Status")
        df = st.session_state.inventory
        if not df.empty:
            summary_cols = ["Product Name", "UOM", "Opening Stock", "Total Received", "Closing Stock", "Consumption"]
            edited_df = st.data_editor(df[summary_cols], use_container_width=True, disabled=["Product Name", "UOM", "Total Received", "Closing Stock"])
            
            if st.button("💾 Save Changes"):
                df.update(edited_df)
                for item in df["Product Name"]: df = recalculate_item(df, item)
                save_all(inv_df=df)
                append_log("Batch Edit", 0, 0, "Table Update")
                st.success("Consumption Saved!")
                st.rerun()

            st.divider()
            day_cols = [str(i) for i in range(1, 32)]
            export_df = df[["Product Name", "UOM", "Opening Stock"] + day_cols + ["Total Received", "Closing Stock", "Consumption"]]
            st.download_button("🚀 Download Full CSV", export_df.to_csv(index=False), "Full_Report.csv")

# --- TAB 2: REQUISITIONS (PARTIAL FULFILLMENT UPDATED) ---
with tab_req:
    st.subheader("🚚 Pending Requisitions (Partial Fulfillment Enabled)")
    orders_df = load_orders()
    
    if not orders_df.empty:
        pending_reqs = orders_df[orders_df["Status"] == "Pending"]
        
        if pending_reqs.empty:
            st.info("No pending requisitions.")
        else:
            batches = pending_reqs.groupby(["OrderID", "From", "Date"])
            
            for (batch_id, outlet, date), group in batches:
                st.markdown(f'<div class="batch-container">', unsafe_allow_html=True)
                
                c_head, c_btn = st.columns([3, 1])
                with c_head:
                    st.markdown(f"### 📍 {outlet}")
                    st.caption(f"Batch: {batch_id} | Date: {date}")
                
                # We use a form-like approach to allow editing quantities per item
                to_dispatch_items = []
                
                for idx, row in group.iterrows():
                    r1, r2, r3, r4 = st.columns([2, 1, 1, 1])
                    r1.write(f"**{row['Item']}**")
                    if row.get("FollowUp", False):
                        r1.markdown('<span class="followup-alert">🚩 Follow-up Requested</span>', unsafe_allow_html=True)
                    
                    r2.write(f"Ordered: {row['Qty']}")
                    
                    # Manual Quantity Input
                    send_qty = r3.number_input(f"Qty to Send", min_value=0.0, max_value=float(row['Qty']), value=float(row['Qty']), key=f"send_{idx}")
                    to_dispatch_items.append({"idx": idx, "row": row, "send_qty": send_qty})
                    st.divider()

                if c_btn.button("📦 Process & Dispatch", key=f"proc_{batch_id}", type="primary", use_container_width=True):
                    wh_inv = st.session_state.inventory
                    current_day = str(datetime.datetime.now().day)
                    
                    for item_data in to_dispatch_items:
                        orig_idx = item_data["idx"]
                        orig_row = item_data["row"]
                        send_qty = item_data["send_qty"]
                        
                        if send_qty > 0:
                            # 1. Update Warehouse Stock
                            if orig_row['Item'] in wh_inv["Product Name"].values:
                                item_idx = wh_inv[wh_inv["Product Name"] == orig_row['Item']].index[0]
                                wh_inv.at[item_idx, "Consumption"] += send_qty
                                recalculate_item(wh_inv, orig_row['Item'])
                                append_log(orig_row['Item'], current_day, send_qty, f"Partial Dispatch to {outlet}")
                            
                            # 2. Manage the Orders DB
                            if send_qty < orig_row['Qty']:
                                # Create Backorder (The remaining amount)
                                remaining = orig_row['Qty'] - send_qty
                                backorder_row = orig_row.copy()
                                backorder_row["Qty"] = remaining
                                backorder_row["Status"] = "Pending"
                                # We add it to the dataframe
                                orders_df = pd.concat([orders_df, pd.DataFrame([backorder_row])], ignore_index=True)
                                
                                # Update current row to what was actually sent
                                orders_df.at[orig_idx, "Qty"] = send_qty
                                orders_df.at[orig_idx, "Status"] = "In Transit"
                            else:
                                # Fully sent
                                orders_df.at[orig_idx, "Status"] = "In Transit"
                        else:
                            # If 0 was sent, keep it as pending (do nothing)
                            pass
                            
                    st.session_state.inventory = wh_inv
                    save_all(inv_df=wh_inv, orders_df=orders_df)
                    st.success(f"Dispatched processed items for {outlet}")
                    st.rerun()
                
                st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.info("No requisitions found.")

# --- TAB 3: SUPPLIER DIRECTORY ---
with tab_sup:
    st.subheader("📞 Supplier & Contact Database")
    meta_df = load_metadata()
    if not meta_df.empty:
        search = st.text_input("🔍 Search Directory").lower()
        filtered = meta_df[meta_df["Product Name"].str.lower().str.contains(search) | meta_df["Supplier"].str.lower().str.contains(search)] if search else meta_df
        edited_meta = st.data_editor(filtered, use_container_width=True, num_rows="dynamic", key="dir_edit_tab")
        if st.button("💾 Save Directory Changes"):
            save_all(meta_df=edited_meta)
            st.success("Directory Updated!")
            st.rerun()
    else: st.info("Directory empty.")

# --- SIDEBAR ---
with st.sidebar:
    st.header("Data Control")
    st.subheader("1. Master Inventory Sync")
    inv_file = st.file_uploader("Upload Inventory", type=["csv", "xlsx"])
    if inv_file:
        try:
            if inv_file.name.endswith('.xlsx'): raw_df = pd.read_excel(inv_file, skiprows=4, header=None)
            else: raw_df = pd.read_csv(inv_file, skiprows=4, header=None, encoding='utf-8')
            new_df = pd.DataFrame()
            new_df["Product Name"] = raw_df[1]; new_df["UOM"] = raw_df[2]; new_df["Opening Stock"] = pd.to_numeric(raw_df[3], errors='coerce').fillna(0)
            for i in range(1, 32): new_df[str(i)] = pd.to_numeric(raw_df[i+3], errors='coerce').fillna(0)
            new_df["Consumption"] = 0
            st.session_state.inventory = new_df.dropna(subset=["Product Name"])
            for item in st.session_state.inventory["Product Name"]: st.session_state.inventory = recalculate_item(st.session_state.inventory, item)
            save_all(inv_df=st.session_state.inventory)
            st.success("Inventory Synced!")
            st.rerun()
        except Exception as e: st.error(e)

    if st.button("🗑️ Reset All"):
        for f in [DB_FILE, LOG_FILE, META_FILE, ORDERS_FILE]:
            if os.path.exists(f): os.remove(f)
        st.rerun()