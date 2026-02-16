import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
import datetime
import uuid

# --- CLOUD CONNECTION ---
conn = st.connection("gsheets", type=GSheetsConnection)

def load_from_sheet(worksheet_name, default_cols=None):
    """Safely load data from a specific tab in Google Sheets"""
    try:
        return conn.read(worksheet=worksheet_name, ttl="5s")
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

# --- CUSTOM CSS ---
st.markdown("""
    <style>
    .main { background-color: #0e1117; }
    .stMetric { background-color: #1e2130; padding: 15px; border-radius: 10px; border: 1px solid #333; }
    .stTabs [data-baseweb="tab"] { height: 50px; background-color: #1e2130; color: white; border-radius: 5px 5px 0 0; }
    .stTabs [aria-selected="true"] { background-color: #00ffcc !important; color: #000 !important; }
    .log-entry { border-left: 3px solid #00ffcc; padding: 10px; margin-bottom: 5px; background: #1e2130; }
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

def apply_transaction(item_name, day_num, qty):
    df = st.session_state.inventory
    df.columns = [str(col) for col in df.columns]
    
    if item_name in df["Product Name"].values:
        idx = df[df["Product Name"] == item_name].index[0]
        col_name = str(int(day_num))
        
        if col_name not in df.columns: df[col_name] = 0
            
        current_val = pd.to_numeric(df.at[idx, col_name], errors='coerce')
        df.at[idx, col_name] = (0 if pd.isna(current_val) else current_val) + qty
        
        # Log the activity
        new_log = pd.DataFrame([{
            "Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
            "Item": item_name,
            "Type": "Addition",
            "Qty": qty,
            "Day": day_num
        }])
        logs_df = pd.concat([load_from_sheet("activity_logs"), new_log], ignore_index=True)
        save_to_sheet(logs_df, "activity_logs")
        
        # Update Inventory
        df = recalculate_item(df, item_name)
        st.session_state.inventory = df
        save_to_sheet(df, "persistent_inventory")
        return True
    return False

# --- DATA INITIALIZATION ---
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
    col_history, col_status = st.columns([1, 2])
    
    with col_history:
        st.subheader("📜 Recent Activity")
        logs = load_from_sheet("activity_logs")
        if not logs.empty:
            for _, row in logs.tail(8).iterrows():
                st.markdown(f"""<div class='log-entry'><b>{row['Item']}</b>: +{row['Qty']} (Day {row['Day']})<br><small>{row['Timestamp']}</small></div>""", unsafe_allow_html=True)
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

with tab_req:
    st.subheader("🚚 Pending Store Requisitions")
    orders_df = load_from_sheet("orders_db")
    if not orders_df.empty:
        st.dataframe(orders_df, use_container_width=True)
        if st.button("🗑️ Clear All Orders"):
            save_to_sheet(pd.DataFrame(columns=orders_df.columns), "orders_db")
            st.rerun()
    else: st.info("No pending requests.")

with tab_sup:
    st.subheader("📞 Supplier & Product Directory")
    meta_df = load_from_sheet("product_metadata", default_cols=["Product Name", "Supplier", "Contact", "Lead Time"])
    edited_meta = st.data_editor(meta_df, num_rows="dynamic", use_container_width=True)
    if st.button("💾 Save Directory Changes"):
        save_to_sheet(edited_meta, "product_metadata")
        st.success("Directory Updated!")

# --- SIDEBAR ---
with st.sidebar:
    st.header("Cloud Data Control")
    st.subheader("1. Master Inventory Sync")
    inv_file = st.file_uploader("Upload Master File (Row 5 start)", type=["csv", "xlsx"])
    
    if inv_file:
        try:
            raw_df = pd.read_excel(inv_file, skiprows=4, header=None) if inv_file.name.endswith('.xlsx') else pd.read_csv(inv_file, skiprows=4, header=None)
            new_df = pd.DataFrame()
            new_df["Product Name"] = raw_df[1]
            new_df["UOM"] = raw_df[2]
            new_df["Opening Stock"] = pd.to_numeric(raw_df[3], errors='coerce').fillna(0)
            for i in range(1, 32): new_df[str(i)] = 0
            new_df["Total Received"] = 0
            new_df["Consumption"] = 0
            new_df["Closing Stock"] = new_df["Opening Stock"]
            new_df = new_df.dropna(subset=["Product Name"])
            
            st.write(f"Items found: {len(new_df)}")
            if st.button("🚀 Push to Cloud"):
                save_to_sheet(new_df, "persistent_inventory")
                st.success("Master Inventory Overwritten!")
                st.rerun()
        except Exception as e: st.error(f"Error: {e}")

    if st.button("🗑️ Reset System Cache"):
        st.cache_data.clear()
        st.rerun()
