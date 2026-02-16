import streamlit as st
import pandas as pd
import datetime
import os
import uuid

# --- FILE SETTINGS ---
DB_FILE = "rest_01_inventory.csv"
LOG_FILE = "rest_01_logs.csv"
ORDERS_FILE = "orders_db.csv"  # The shared bridge to the Warehouse

st.set_page_config(page_title="Restaurant 01 Pro", layout="wide")

# --- CUSTOM UI ---
st.markdown("""
    <style>
    .main { background-color: #0e1117; }
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] { 
        height: 50px; background-color: #1e2130; 
        border-radius: 5px 5px 0 0; padding: 10px 20px; color: white;
    }
    .stTabs [aria-selected="true"] { background-color: #ffaa00 !important; color: #000 !important; }
    .cart-container { 
        padding: 20px; 
        background-color: #1e2130; 
        border-radius: 10px; 
        border: 1px solid #ffaa00;
        margin-top: 20px;
    }
    .pending-box {
        border-left: 4px solid #ff4b4b;
        padding: 10px;
        background: #1e2130;
        margin: 5px 0;
    }
    </style>
    """, unsafe_allow_html=True)

# --- DATA PERSISTENCE ---
def load_data():
    if os.path.exists(DB_FILE):
        try:
            return pd.read_csv(DB_FILE, sep=';', encoding='utf-8-sig')
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def load_logs():
    if os.path.exists(LOG_FILE):
        try:
            logs = pd.read_csv(LOG_FILE, sep=';', encoding='utf-8-sig')
            logs['undone'] = logs['undone'].astype(bool)
            return logs
        except:
            return pd.DataFrame(columns=["id", "timestamp", "item", "day", "qty", "type", "undone"])
    return pd.DataFrame(columns=["id", "timestamp", "item", "day", "qty", "type", "undone"])

def load_orders():
    if os.path.exists(ORDERS_FILE):
        df = pd.read_csv(ORDERS_FILE)
        if "FollowUp" not in df.columns: df["FollowUp"] = False
        return df
    return pd.DataFrame(columns=["OrderID", "Date", "From", "Item", "Qty", "Status", "FollowUp"])

def save_all(df):
    df.to_csv(DB_FILE, index=False, sep=';', encoding='utf-8-sig')

def append_log(item, day, qty, log_type):
    logs = load_logs()
    new_log = pd.DataFrame([{
        "id": str(uuid.uuid4())[:8],
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "item": item, "day": int(day), "qty": float(qty), "type": log_type, "undone": False
    }])
    pd.concat([new_log, logs], ignore_index=True).to_csv(LOG_FILE, index=False, sep=';', encoding='utf-8-sig')

# --- ENGINE ---
def recalculate_item(df, item_name):
    if item_name not in df["Product Name"].values: return df
    idx = df[df["Product Name"] == item_name].index[0]
    
    for col in ["Total Received", "Consumption", "Closing Stock"]:
        if col not in df.columns: df[col] = 0
    
    day_cols = [str(i) for i in range(1, 32)]
    for col in day_cols:
        if col not in df.columns: df[col] = 0
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
    total_received = df.loc[idx, day_cols].sum()
    df.at[idx, "Total Received"] = total_received
    df.at[idx, "Closing Stock"] = df.at[idx, "Opening Stock"] + total_received - df.at[idx, "Consumption"]
    return df

# --- MODAL: MANUAL ADD ---
@st.dialog("➕ Add Item to Restaurant Inventory")
def add_item_modal():
    st.write("Register an item and its current opening balance.")
    col1, col2 = st.columns(2)
    with col1:
        name = st.text_input("Item Name*")
        uom = st.selectbox("Unit", ["pcs", "kg", "box", "ltr", "pkt", "can", "g", "ml", "bot"])
    with col2:
        category = st.selectbox("Category", ["Food", "Beverage", "Cleaning", "Packaging"])
        opening_bal = st.number_input("Opening Balance", min_value=0.0)
    
    if st.button("✅ Register Item", use_container_width=True):
        if name:
            new_row = {str(i): 0 for i in range(1, 32)}
            new_row.update({"Product Name": name, "UOM": uom, "Category": category, 
                            "Opening Stock": opening_bal, "Total Received": 0, 
                            "Consumption": 0, "Closing Stock": opening_bal})
            st.session_state.rest_inv = pd.concat([st.session_state.rest_inv, pd.DataFrame([new_row])], ignore_index=True)
            save_all(st.session_state.rest_inv)
            append_log(name, 0, opening_bal, "Initial Stock Set")
            st.rerun()

# --- INITIAL LOAD ---
if 'rest_inv' not in st.session_state:
    st.session_state.rest_inv = load_data()

if 'requisition_cart' not in st.session_state:
    st.session_state.requisition_cart = []

st.title("🍴 Restaurant 01: Inventory Control")

tab_inv, tab_req, tab_receive = st.tabs(["📊 Stock & Consumption", "📝 Internal Requisition", "📥 Receive Shipment"])

# --- TAB 1: CONSUMPTION TRACKING ---
with tab_inv:
    col_a, col_b = st.columns([2, 1])
    with col_a:
        st.subheader("Live Inventory Status")
        if not st.session_state.rest_inv.empty:
            summary_cols = ["Product Name", "UOM", "Opening Stock", "Total Received", "Closing Stock", "Consumption"]
            edited_df = st.data_editor(st.session_state.rest_inv[summary_cols], use_container_width=True, 
                                     disabled=["Product Name", "UOM", "Total Received", "Closing Stock"])
            
            if st.button("💾 Save Changes"):
                st.session_state.rest_inv.update(edited_df)
                for item in st.session_state.rest_inv["Product Name"]:
                    st.session_state.rest_inv = recalculate_item(st.session_state.rest_inv, item)
                save_all(st.session_state.rest_inv)
                st.success("Data Updated!")
                st.rerun()
        else:
            st.info("Your inventory is currently empty.")

    with col_b:
        st.subheader("Actions")
        if st.button("➕ ADD NEW ITEM", use_container_width=True): add_item_modal()
        st.divider()
        st.write("📜 Recent Activity")
        logs = load_logs()
        for i, row in logs.head(8).iterrows():
            st.caption(f"{row['timestamp']} - {row['item']} ({row['type']})")

# --- TAB 2: REQUISITION ---
with tab_req:
    st.subheader("Build Requisition Request")
    if not st.session_state.rest_inv.empty:
        item_list = sorted(st.session_state.rest_inv["Product Name"].unique().tolist())
        
        with st.container():
            c_sel, c_qty, c_btn = st.columns([2, 1, 1])
            selected_item = c_sel.selectbox("Select Item", options=item_list)
            qty_needed = c_qty.number_input("Qty", min_value=0.1, step=0.1, value=1.0)
            
            if c_btn.button("➕ Add to List", use_container_width=True):
                st.session_state.requisition_cart.append({"Item": selected_item, "Qty": qty_needed})
                st.toast(f"Added {selected_item}")

        if st.session_state.requisition_cart:
            st.markdown('<div class="cart-container">', unsafe_allow_html=True)
            cart_df = pd.DataFrame(st.session_state.requisition_cart)
            st.table(cart_df)
            
            c_clear, c_send = st.columns(2)
            if c_clear.button("🗑️ Clear List", use_container_width=True):
                st.session_state.requisition_cart = []
                st.rerun()
                
            if c_send.button("🚀 Send All to Warehouse", use_container_width=True, type="primary"):
                existing = load_orders()
                batch_id = f"REQ-{uuid.uuid4().hex[:4].upper()}"
                new_orders = []
                for entry in st.session_state.requisition_cart:
                    new_orders.append({
                        "OrderID": batch_id, "Date": datetime.date.today(), "From": "Restaurant 01",
                        "Item": entry["Item"], "Qty": entry["Qty"], "Status": "Pending", "FollowUp": False
                    })
                pd.concat([existing, pd.DataFrame(new_orders)], ignore_index=True).to_csv(ORDERS_FILE, index=False)
                st.session_state.requisition_cart = []
                st.success("Sent to Warehouse!")
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)
    else: st.warning("Please initialize inventory.")

# --- TAB 3: RECEIVE SHIPMENT (PARTIAL DELIVERY LOGIC) ---
with tab_receive:
    st.subheader("Shipment & Backorder Status")
    orders_df = load_orders()
    
    # 1. Show items currently in Transit (Available to Accept)
    transit_items = orders_df[(orders_df["Status"] == "In Transit") & (orders_df["From"] == "Restaurant 01")]
    
    # 2. Show items currently Pending (Backorders/Owed items)
    pending_items = orders_df[(orders_df["Status"] == "Pending") & (orders_df["From"] == "Restaurant 01")]

    st.markdown("### 🚚 Incoming Now")
    if transit_items.empty:
        st.info("No shipments in transit.")
    else:
        for idx, row in transit_items.iterrows():
            with st.container():
                c1, c2, c3 = st.columns([3, 1, 1])
                c1.write(f"📦 **{row['Item']}**")
                c2.write(f"Warehouse Sent: **{row['Qty']}**")
                
                if c3.button("✅ Accept", key=f"acc_{idx}"):
                    rest_df = st.session_state.rest_inv
                    if row['Item'] in rest_df["Product Name"].values:
                        item_idx = rest_df[rest_df["Product Name"] == row['Item']].index[0]
                        day_col = str(datetime.datetime.now().day)
                        rest_df.at[item_idx, day_col] = pd.to_numeric(rest_df.at[item_idx, day_col]) + row['Qty']
                        st.session_state.rest_inv = recalculate_item(rest_df, row['Item'])
                        save_all(st.session_state.rest_inv)
                        
                        orders_df.at[idx, "Status"] = "Completed"
                        orders_df.to_csv(ORDERS_FILE, index=False)
                        append_log(row['Item'], day_col, row['Qty'], "Stock Accepted")
                        st.rerun()
            st.divider()

    st.markdown("### ⏳ Pending / Backorders")
    if pending_items.empty:
        st.write("No pending items.")
    else:
        for idx, row in pending_items.iterrows():
            st.markdown(f'<div class="pending-box">', unsafe_allow_html=True)
            p1, p2, p3 = st.columns([3, 1, 1])
            p1.write(f"**{row['Item']}** (Status: Owed by Warehouse)")
            p2.write(f"Remaining: {row['Qty']}")
            
            follow_text = "🔔 Following Up..." if row.get("FollowUp", False) else "🚩 Click to Follow Up"
            if p3.button(follow_text, key=f"fup_{idx}", disabled=row.get("FollowUp", False)):
                orders_df.at[idx, "FollowUp"] = True
                orders_df.to_csv(ORDERS_FILE, index=False)
                st.toast("Warehouse notified to follow up on this item!")
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

# --- SIDEBAR ---
with st.sidebar:
    st.header("Settings")
    inv_file = st.file_uploader("Upload Master Template", type=["csv", "xlsx"])
    if inv_file:
        try:
            if inv_file.name.endswith('.xlsx'): raw_df = pd.read_excel(inv_file, skiprows=4, header=None)
            else: raw_df = pd.read_csv(inv_file, skiprows=4, header=None, encoding='utf-8')
            
            new_df = pd.DataFrame()
            new_df["Product Name"] = raw_df[1]; new_df["UOM"] = raw_df[2]; new_df["Opening Stock"] = pd.to_numeric(raw_df[3], errors='coerce').fillna(0)
            for i in range(1, 32): new_df[str(i)] = 0
            new_df["Total Received"] = 0; new_df["Consumption"] = 0; new_df["Closing Stock"] = 0; new_df["Category"] = "General"
            st.session_state.rest_inv = new_df.dropna(subset=["Product Name"])
            save_all(st.session_state.rest_inv)
            st.success("Synced!")
            st.rerun()
        except Exception as e: st.error(e)

    if st.button("🗑️ Reset Local Data"):
        for f in [DB_FILE, LOG_FILE]:
            if os.path.exists(f): os.remove(f)
        st.rerun()