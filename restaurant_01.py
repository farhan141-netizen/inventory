import streamlit as st
import pandas as pd
import datetime
import uuid
import io
from streamlit_gsheets import GSheetsConnection

# --- CONNECTION ---
@st.cache_resource
def get_connection():
    return st.connection("gsheets", type=GSheetsConnection)

def load_from_sheet(worksheet_name, default_cols=None):
    """Load from Google Sheets"""
    try:
        conn = get_connection()
        df = conn.read(worksheet=worksheet_name, ttl="5m")
        if df is None or df.empty:
            return pd.DataFrame(columns=default_cols) if default_cols else pd.DataFrame()
        return df
    except:
        return pd.DataFrame(columns=default_cols) if default_cols else pd.DataFrame()

def save_to_sheet(df, worksheet_name):
    """Save to Google Sheets"""
    try:
        conn = get_connection()
        conn.update(worksheet=worksheet_name, data=df)
        st.cache_data.clear()
        return True
    except:
        return False

# --- PAGE CONFIG ---
st.set_page_config(page_title="Restaurant 01 Pro", layout="wide")

# --- CUSTOM CSS ---
st.markdown("""
    <style>
    .main { background-color: #0e1117; }
    .header { background: linear-gradient(90deg, #ff6b35 0%, #f7931e 100%); padding: 20px; border-radius: 10px; color: white; margin-bottom: 20px; }
    .header h1 { margin: 0; font-size: 2em; }
    
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] { 
        height: 50px; background-color: #1e2130; 
        border-radius: 5px 5px 0 0; padding: 10px 20px; color: white; font-weight: 600;
    }
    .stTabs [aria-selected="true"] { background-color: #ff6b35 !important; color: white !important; }
    
    .cart-container { 
        padding: 20px; background-color: #1e2130; 
        border-radius: 10px; border: 2px solid #ff6b35; margin-top: 20px;
    }
    .cart-item {
        background: #262730; padding: 10px; margin-bottom: 8px; 
        border-left: 4px solid #ff6b35; border-radius: 4px;
    }
    
    .pending-box {
        border-left: 5px solid #ff6b35; background: #262730;
        padding: 15px; margin-bottom: 12px; border-radius: 4px;
    }
    .pending-pending { border-left: 5px solid #ffaa00; background: #3a2f1a; }
    .pending-dispatched { border-left: 5px solid #00d9ff; background: #1a2f3f; }
    .pending-completed { border-left: 5px solid #00ff00; background: #1a3a1a; }
    
    .section-title { color: #ff6b35; font-size: 1.2em; font-weight: 700; margin-bottom: 15px; }
    .stButton>button { border-radius: 6px; font-weight: 600; }
    </style>
    """, unsafe_allow_html=True)

# --- INITIALIZATION ---
if 'inventory' not in st.session_state:
    st.session_state.inventory = load_from_sheet("rest_01_inventory")

if 'cart' not in st.session_state:
    st.session_state.cart = []

# --- HEADER ---
st.markdown("""
    <div class="header">
        <h1>🍴 Restaurant 01 | Operations Portal</h1>
        <p>Inventory Management & Warehouse Requisitions</p>
    </div>
""", unsafe_allow_html=True)

# --- TABS ---
tab_inv, tab_req, tab_pending, tab_received = st.tabs(["📋 Inventory Count", "🛒 Send Requisition", "🚚 Pending Orders", "📦 Received Items"])

# ===================== INVENTORY TAB =====================
with tab_inv:
    st.markdown('<div class="section-title">📊 Daily Stock Take</div>', unsafe_allow_html=True)
    if not st.session_state.inventory.empty:
        if "Category" not in st.session_state.inventory.columns:
            st.session_state.inventory["Category"] = "General"
            
        cats = ["All"] + sorted(st.session_state.inventory["Category"].unique().tolist())
        sel_cat = st.selectbox("Filter Category", cats, key="inv_cat")
        
        display_df = st.session_state.inventory.copy()
        if sel_cat != "All":
            display_df = display_df[display_df["Category"] == sel_cat]
        
        master_cols = ["Product Name", "UOM", "Opening Stock", "Total Received", "Physical Count", "Consumption", "Closing Stock"]
        cols_to_show = [c for c in master_cols if c in display_df.columns]
        
        edited_inv = st.data_editor(
            display_df[cols_to_show],
            use_container_width=True,
            disabled=["Product Name", "UOM", "Opening Stock", "Total Received", "Consumption", "Closing Stock"],
            hide_index=True,
            key="inv_editor"
        )
        
        if st.button("💾 Save Daily Count", type="primary", use_container_width=True, key="save_inv"):
            st.session_state.inventory.update(edited_inv)
            save_to_sheet(st.session_state.inventory, "rest_01_inventory")
            st.success("✅ Inventory saved!")
            st.rerun()
    else:
        st.warning("No inventory found. Please use sidebar to upload template.")

# ===================== REQUISITION TAB =====================
with tab_req:
    col_l, col_r = st.columns([2, 1])
    
    with col_l:
        st.markdown('<div class="section-title">🛒 Add Items to Requisition</div>', unsafe_allow_html=True)
        if not st.session_state.inventory.empty:
            search_item = st.text_input("🔍 Search Product", key="search_req").lower()
            items = st.session_state.inventory[st.session_state.inventory["Product Name"].str.lower().str.contains(search_item, na=False)]
            
            for _, row in items.head(20).iterrows():
                c1, c2, c3 = st.columns([3, 1, 1])
                c1.write(f"**{row['Product Name']}** ({row['UOM']})")
                qty = c2.number_input("Qty", min_value=0.0, key=f"req_{row['Product Name']}", label_visibility="collapsed")
                if c3.button("Add ➕", key=f"btn_{row['Product Name']}", use_container_width=True):
                    if qty > 0:
                        st.session_state.cart.append({'name': row['Product Name'], 'qty': qty, 'uom': row['UOM']})
                        st.toast(f"✅ Added {row['Product Name']}")

    with col_r:
        st.markdown('<div class="cart-container">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">🛒 Cart</div>', unsafe_allow_html=True)
        if st.session_state.cart:
            for i, item in enumerate(st.session_state.cart):
                st.markdown(f"""
                <div class="cart-item">
                    {item['name']}: {item['qty']} {item['uom']}
                </div>
                """, unsafe_allow_html=True)
                if st.button(f"Remove {item['name']}", key=f"rm_{i}", use_container_width=True):
                    st.session_state.cart.pop(i)
                    st.rerun()
            
            st.divider()
            if st.button("🗑️ Clear Cart", use_container_width=True, key="clear_cart"):
                st.session_state.cart = []
                st.rerun()
                
            if st.button("🚀 Submit to Warehouse", type="primary", use_container_width=True, key="submit_req"):
                all_reqs = load_from_sheet("restaurant_requisitions", ["ReqID", "Restaurant", "Item", "Qty", "Status", "DispatchQty", "Timestamp"])
                
                for item in st.session_state.cart:
                    new_req = pd.DataFrame([{
                        "ReqID": str(uuid.uuid4())[:8],
                        "Restaurant": "Restaurant 01",
                        "Item": item['name'],
                        "Qty": item['qty'],
                        "Status": "Pending",
                        "DispatchQty": 0,
                        "Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }])
                    all_reqs = pd.concat([all_reqs, new_req], ignore_index=True)
                
                if save_to_sheet(all_reqs, "restaurant_requisitions"):
                    st.session_state.cart = []
                    st.success("✅ Requisition sent to Warehouse!")
                    st.rerun()
        else:
            st.write("🛒 Cart is empty")
        st.markdown('</div>', unsafe_allow_html=True)

# ===================== PENDING ORDERS TAB =====================
with tab_pending:
    st.markdown('<div class="section-title">🚚 Pending Orders Status</div>', unsafe_allow_html=True)
    all_reqs = load_from_sheet("restaurant_requisitions")
    
    if not all_reqs.empty:
        my_pending = all_reqs[(all_reqs["Restaurant"] == "Restaurant 01") & (all_reqs["Status"] == "Pending")]
        
        if not my_pending.empty:
            for idx, row in my_pending.iterrows():
                item_name = row["Item"]
                req_qty = row["Qty"]
                
                st.markdown(f"""
                <div class="pending-box pending-pending">
                    <b>🟡 {item_name}</b> | Requested: {req_qty}
                </div>
                """, unsafe_allow_html=True)
                st.write("⏳ Waiting for warehouse to process...")
                st.divider()
        else:
            st.info("✅ No pending orders")
    else:
        st.info("📭 No orders found")

# ===================== RECEIVED ITEMS TAB =====================
with tab_received:
    st.markdown('<div class="section-title">📦 Received Items</div>', unsafe_allow_html=True)
    all_reqs = load_from_sheet("restaurant_requisitions")
    
    if not all_reqs.empty:
        my_dispatched = all_reqs[(all_reqs["Restaurant"] == "Restaurant 01") & (all_reqs["Status"] == "Dispatched")]
        
        if not my_dispatched.empty:
            for idx, row in my_dispatched.iterrows():
                item_name = row["Item"]
                dispatch_qty = row["DispatchQty"]
                req_id = row["ReqID"]
                
                st.markdown(f"""
                <div class="pending-box pending-dispatched">
                    <b>🟢 {item_name}</b> | Dispatched: {dispatch_qty}
                </div>
                """, unsafe_allow_html=True)
                
                c1, c2 = st.columns([1, 1])
                with c1:
                    if st.button(f"✅ Accept {item_name}", key=f"accept_{req_id}", use_container_width=True):
                        # Update requisition status
                        all_reqs.at[idx, "Status"] = "Completed"
                        save_to_sheet(all_reqs, "restaurant_requisitions")
                        
                        # Add to restaurant inventory
                        inv_idx = st.session_state.inventory[st.session_state.inventory["Product Name"] == item_name].index
                        if len(inv_idx) > 0:
                            idx_val = inv_idx[0]
                            current_closing = st.session_state.inventory.at[idx_val, "Closing Stock"]
                            st.session_state.inventory.at[idx_val, "Closing Stock"] = float(current_closing) + float(dispatch_qty)
                            st.session_state.inventory.at[idx_val, "Total Received"] = float(st.session_state.inventory.at[idx_val, "Total Received"]) + float(dispatch_qty)
                            save_to_sheet(st.session_state.inventory, "rest_01_inventory")
                        
                        st.success(f"✅ {item_name} received and added to inventory!")
                        st.rerun()
                
                with c2:
                    if st.button(f"❌ Reject {item_name}", key=f"reject_{req_id}", use_container_width=True):
                        all_reqs.at[idx, "Status"] = "Pending"
                        save_to_sheet(all_reqs, "restaurant_requisitions")
                        st.warning(f"❌ Returned to pending")
                        st.rerun()
                
                st.divider()
        else:
            st.info("📭 No dispatched items")
    else:
        st.info("📭 No orders found")

# ===================== SIDEBAR =====================
with st.sidebar:
    st.header("⚙️ Settings")
    st.subheader("Upload Inventory Template")
    
    inv_file = st.file_uploader("Upload Excel/CSV", type=["csv", "xlsx"], key="upload_inv")
    
    if inv_file:
        try:
            if inv_file.name.endswith('.xlsx'):
                raw_df = pd.read_excel(inv_file, skiprows=4, header=None)
            else:
                raw_df = pd.read_csv(inv_file, skiprows=4, header=None)

            new_df = pd.DataFrame()
            new_df["Product Name"] = raw_df[1]
            new_df["UOM"] = raw_df[2]
            new_df["Opening Stock"] = pd.to_numeric(raw_df[3], errors='coerce').fillna(0)

            required_logic = {
                "Total Received": 0.0, "Physical Count": None,
                "Consumption": 0.0, "Closing Stock": 0.0, "Category": "General"
            }
            for col, val in required_logic.items():
                if col not in new_df.columns:
                    new_df[col] = val

            new_df = new_df.dropna(subset=["Product Name"])

            if st.button("🚀 Push Inventory", type="primary", use_container_width=True, key="push_inv_rest"):
                if save_to_sheet(new_df, "rest_01_inventory"):
                    st.success(f"✅ Inventory created with {len(new_df)} items!")
                    st.rerun()

        except Exception as e:
            st.error(f"❌ Error: {e}")
            
    st.divider()
    if st.button("🗑️ Clear Cache", use_container_width=True, key="clear_cache_rest"):
        st.cache_data.clear()
        st.rerun()
