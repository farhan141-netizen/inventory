import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
import datetime
import uuid
import io

# --- CLOUD CONNECTION ---
conn = st.connection("gsheets", type=GSheetsConnection)

def clean_dataframe(df):
    """Removes ghost columns and ensures unique headers"""
    if df is None or df.empty: return df
    df = df.loc[:, ~df.columns.str.contains('^Unnamed', na=False)]
    df = df.dropna(axis=1, how='all')
    df = df.loc[:, ~df.columns.duplicated()]
    df.columns = [str(col).strip() for col in df.columns]
    return df

def load_from_sheet(worksheet_name, default_cols=None):
    """Safely loads data with a fallback to empty DataFrame if sheet/cols missing"""
    try:
        df = conn.read(worksheet=worksheet_name, ttl="2s")
        df = clean_dataframe(df)
        if df is None or df.empty:
            return pd.DataFrame(columns=default_cols) if default_cols else pd.DataFrame()
        return df
    except Exception:
        return pd.DataFrame(columns=default_cols) if default_cols else pd.DataFrame()

def save_to_sheet(df, worksheet_name):
    """Saves data and clears Streamlit cache for immediate update"""
    df = clean_dataframe(df)
    conn.update(worksheet=worksheet_name, data=df)
    st.cache_data.clear()

# --- PAGE CONFIG ---
st.set_page_config(page_title="Restaurant 01 Pro", layout="wide")

# --- CUSTOM CSS ---
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
        padding: 20px; background-color: #1e2130; 
        border-radius: 10px; border: 1px solid #ffaa00; margin-top: 20px;
    }
    .pending-box {
        border-left: 5px solid #ffaa00; background: #262730;
        padding: 10px; margin-bottom: 10px; border-radius: 4px;
    }
    </style>
    """, unsafe_allow_html=True)

# --- INITIALIZATION ---
if 'inventory' not in st.session_state:
    st.session_state.inventory = load_from_sheet("rest_01_inventory")

if 'cart' not in st.session_state:
    st.session_state.cart = []

# --- CALCULATION ENGINE ---
def recalculate_rest_item(df, item_name):
    if item_name not in df["Product Name"].values: return df
    idx = df[df["Product Name"] == item_name].index[0]
    
    opening = pd.to_numeric(df.at[idx, "Opening Stock"], errors='coerce') or 0
    received = pd.to_numeric(df.at[idx, "Total Received"], errors='coerce') or 0
    physical = pd.to_numeric(df.at[idx, "Physical Count"], errors='coerce')
    
    if pd.notna(physical):
        df.at[idx, "Consumption"] = (opening + received) - physical
        df.at[idx, "Closing Stock"] = physical
    else:
        df.at[idx, "Closing Stock"] = opening + received
    return df

# --- MAIN APP ---
st.title("🍴 Restaurant 01 | Operations Portal")

tab_inv, tab_req, tab_pending = st.tabs(["📋 Inventory Count", "🛒 New Requisition", "🚚 Pending Orders"])

with tab_inv:
    st.subheader("Daily Stock Take")
    if not st.session_state.inventory.empty:
        # Category Filter (Safe check)
        if "Category" not in st.session_state.inventory.columns:
            st.session_state.inventory["Category"] = "General"
            
        cats = ["All"] + sorted(st.session_state.inventory["Category"].unique().tolist())
        sel_cat = st.selectbox("Filter Category", cats)
        
        display_df = st.session_state.inventory.copy()
        if sel_cat != "All":
            display_df = display_df[display_df["Category"] == sel_cat]
        
        # SAFETY CHECK: Only show columns that actually exist in the dataframe
        master_cols = ["Product Name", "UOM", "Opening Stock", "Total Received", "Physical Count", "Consumption", "Closing Stock"]
        cols_to_show = [c for c in master_cols if c in display_df.columns]
        
        edited_inv = st.data_editor(
            display_df[cols_to_show],
            use_container_width=True,
            disabled=["Product Name", "UOM", "Opening Stock", "Total Received", "Consumption", "Closing Stock"],
            hide_index=True,
            key="inv_editor"
        )
        
        if st.button("💾 Save Daily Count", type="primary"):
            st.session_state.inventory.update(edited_inv)
            for item in st.session_state.inventory["Product Name"]:
                st.session_state.inventory = recalculate_rest_item(st.session_state.inventory, item)
            save_to_sheet(st.session_state.inventory, "rest_01_inventory")
            st.success("Cloud Sync Complete!")
            st.rerun()
    else:
        st.warning("No inventory found. Please use the Sidebar to upload your template.")

with tab_req:
    col_l, col_r = st.columns([2, 1])
    
    with col_l:
        st.subheader("Add Items to Requisition")
        if not st.session_state.inventory.empty:
            search_item = st.text_input("🔍 Search Product").lower()
            items = st.session_state.inventory[st.session_state.inventory["Product Name"].str.lower().str.contains(search_item, na=False)]
            
            for _, row in items.head(20).iterrows(): # Show top 20 for performance
                c1, c2, c3 = st.columns([3, 1, 1])
                c1.write(f"**{row['Product Name']}** ({row['UOM']})")
                qty = c2.number_input("Qty", min_value=0.0, key=f"req_{row['Product Name']}")
                if c3.button("Add ➕", key=f"btn_{row['Product Name']}"):
                    if qty > 0:
                        st.session_state.cart.append({'name': row['Product Name'], 'qty': qty, 'uom': row['UOM']})
                        st.toast(f"Added {row['Product Name']}")

    with col_r:
        st.markdown('<div class="cart-container">', unsafe_allow_html=True)
        st.subheader("🛒 Current Cart")
        if st.session_state.cart:
            for i, item in enumerate(st.session_state.cart):
                st.write(f"- {item['name']}: {item['qty']} {item['uom']}")
            
            if st.button("🗑️ Clear Cart"):
                st.session_state.cart = []
                st.rerun()
                
            if st.button("🚀 Submit Order to Warehouse", type="primary", use_container_width=True):
                orders_df = load_from_sheet("orders_db", ["Product Name", "Qty", "Supplier", "Status", "FollowUp"])
                new_entries = []
                for item in st.session_state.cart:
                    new_entries.append({
                        "Product Name": item['name'], "Qty": item['qty'],
                        "Supplier": "Main Warehouse", "Status": "Pending", "FollowUp": False
                    })
                updated_orders = pd.concat([orders_df, pd.DataFrame(new_entries)], ignore_index=True)
                save_to_sheet(updated_orders, "orders_db")
                st.session_state.cart = []
                st.success("Order pushed to Cloud!")
                st.rerun()
        else:
            st.write("Cart is empty.")
        st.markdown('</div>', unsafe_allow_html=True)

with tab_pending:
    st.subheader("Your Requisitions at Warehouse")
    orders_df = load_from_sheet("orders_db")
    
    # SAFETY 1: Ensure columns exist
    for col in ["Supplier", "Status", "FollowUp", "Product Name", "Qty"]:
        if col not in orders_df.columns:
            orders_df[col] = False if col == "FollowUp" else "Unknown"

    if not orders_df.empty:
        # Filter for Main Warehouse orders
        my_orders = orders_df[orders_df["Supplier"] == "Main Warehouse"]
        
        if my_orders.empty:
            st.info("No active requisitions found for this restaurant.")
        
        for idx, row in my_orders.iterrows():
            with st.container():
                # Get values safely with fallbacks
                item_name = row.get('Product Name', 'Unknown Item')
                item_qty = row.get('Qty', 0)
                item_status = row.get('Status', 'Pending')
                
                # SAFETY 2: Force FollowUp to be True or False (Fixes the TypeError)
                fup_val = row.get("FollowUp")
                fup_needed = bool(fup_val) if pd.notna(fup_val) else False
                
                st.markdown(f"""
                <div class="pending-box">
                    <b>{item_name}</b> | Qty: {item_qty} | Status: <span style="color:#ffaa00">{item_status}</span>
                </div>
                """, unsafe_allow_html=True)
                
                c1, c2 = st.columns(2)
                
                # Only show actions if still Pending
                if item_status == "Pending":
                    if c1.button("Mark Received ✅", key=f"recv_{idx}"):
                        orders_df.at[idx, "Status"] = "Received"
                        save_to_sheet(orders_df, "orders_db")
                        st.rerun()
                    
                    fup_label = "⚠️ Follow Up Sent" if fup_needed else "🚩 Request Follow Up"
                    # The 'disabled' parameter now receives a guaranteed boolean
                    if c2.button(fup_label, key=f"fup_{idx}", disabled=fup_needed):
                        orders_df.at[idx, "FollowUp"] = True
                        save_to_sheet(orders_df, "orders_db")
                        st.toast("Warehouse notified!")
                        st.rerun()
    else:
        st.info("No orders found in the cloud.")

# --- SIDEBAR (DYNAMIC IMPORT) ---
with st.sidebar:
    st.header("Admin Settings")
    st.subheader("1. Dynamic Template Import")
    inv_file = st.file_uploader("Upload Inventory Excel", type=["csv", "xlsx"])
    
    if inv_file:
        try:
            # Load raw data based on your specific template (skiprows=4)
            if inv_file.name.endswith('.xlsx'):
                raw_df = pd.read_excel(inv_file, skiprows=4, header=None)
            else:
                raw_df = pd.read_csv(inv_file, skiprows=4, header=None)

            # Map based on standard column positions (B=Name, C=UOM, D=Opening)
            new_df = pd.DataFrame()
            new_df["Product Name"] = raw_df[1]
            new_df["UOM"] = raw_df[2]
            new_df["Opening Stock"] = pd.to_numeric(raw_df[3], errors='coerce').fillna(0)

            # DYNAMIC COLUMN ADDITION: Ensure required logic columns exist
            required_logic = {
                "Total Received": 0.0, "Physical Count": None,
                "Consumption": 0.0, "Closing Stock": 0.0, "Category": "General"
            }
            for col, val in required_logic.items():
                if col not in new_df.columns:
                    new_df[col] = val

            # Clean up empty rows
            new_df = new_df.dropna(subset=["Product Name"])

            if st.button("🚀 Push to Restaurant Cloud"):
                save_to_sheet(new_df, "rest_01_inventory")
                st.success(f"Sheet Created with {len(new_df)} items!")
                st.rerun()

        except Exception as e:
            st.error(f"Mapping Error: {e}. Check if column B is Product Name and C is UOM.")
            
    st.divider()
    if st.button("🗑️ Reset Application Cache"):
        st.cache_data.clear()
        st.rerun()

