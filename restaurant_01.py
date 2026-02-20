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
    """Safely loads data with a fallback to empty DataFrame"""
    try:
        df = conn.read(worksheet=worksheet_name, ttl="2s")
        df = clean_dataframe(df)
        if df is None or df.empty:
            return pd.DataFrame(columns=default_cols) if default_cols else pd.DataFrame()
        return df
    except Exception:
        return pd.DataFrame(columns=default_cols) if default_cols else pd.DataFrame()

def save_to_sheet(df, worksheet_name):
    """Saves data and clears Streamlit cache"""
    df = clean_dataframe(df)
    conn.update(worksheet=worksheet_name, data=df)
    st.cache_data.clear()

# --- PAGE CONFIG ---
st.set_page_config(page_title="Restaurant Cloud v1.2", layout="wide", initial_sidebar_state="expanded")

# --- UI STYLING ---
st.markdown("""
    <style>
    .block-container { padding-top: 1rem; }
    .header-bar { 
        background: linear-gradient(90deg, #ff4b2b 0%, #ff416c 100%); 
        border-radius: 12px; padding: 15px 25px; color: white; margin-bottom: 20px;
        display: flex; justify-content: space-between; align-items: center;
        box-shadow: 0 4px 15px rgba(255, 75, 43, 0.3);
    }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; background: #1e1e26; padding: 8px; border-radius: 12px; }
    .stTabs [data-baseweb="tab"] { padding: 6px 16px; border-radius: 8px; color: #a0a0a0; }
    .stTabs [aria-selected="true"] { background: #ff4b2b22; color: #ff4b2b; border: 1px solid #ff4b2b44; }
    .section-title { color: #ff4b2b; font-size: 1.1em; font-weight: 700; margin-bottom: 12px; display: block; }
    .log-container { max-height: 400px; overflow-y: auto; background: #1a1a1a; border-radius: 8px; padding: 10px; }
    .log-entry { border-left: 3px solid #ff4b2b; background: #262626; padding: 8px; margin-bottom: 6px; border-radius: 4px; font-size: 0.85em; }
    </style>
""", unsafe_allow_html=True)

# --- APP LOGIC ---
def recalculate_item(df, item_name):
    if item_name not in df["Product Name"].values: return df
    idx = df[df["Product Name"] == item_name].index[0]
    opening = pd.to_numeric(df.at[idx, "Opening Stock"], errors='coerce') or 0.0
    received = pd.to_numeric(df.at[idx, "Total Received"], errors='coerce') or 0.0
    consumption = pd.to_numeric(df.at[idx, "Consumption"], errors='coerce') or 0.0
    
    closing = opening + received - consumption
    df.at[idx, "Closing Stock"] = closing
    return df

# --- UI HEADER ---
st.markdown('<div class="header-bar"><div><h2 style="margin:0;">🍴 Restaurant Cloud</h2><p style="margin:0; opacity:0.8;">Outlet Inventory & Requisition System</p></div></div>', unsafe_allow_html=True)

# --- DATA LOAD ---
if 'inventory' not in st.session_state:
    st.session_state.inventory = load_from_sheet("rest_01_inventory")

tab_inv, tab_req, tab_set = st.tabs(["📋 Inventory Dashboard", "📦 Requisitions", "⚙️ Setup"])

with tab_inv:
    col_main, col_logs = st.columns([3, 1])
    
    with col_main:
        st.markdown('<span class="section-title">📊 Live Stock & Consumption</span>', unsafe_allow_html=True)
        df_inv = st.session_state.inventory.copy()
        
        # Display selection
        categories = ["All"] + sorted(df_inv["Category"].unique().tolist()) if "Category" in df_inv.columns else ["All"]
        sel_cat = st.selectbox("Filter Category", categories, label_visibility="collapsed")
        
        if sel_cat != "All":
            df_inv = df_inv[df_inv["Category"] == sel_cat]
            
        cols = ["Category", "Product Name", "UOM", "Opening Stock", "Total Received", "Consumption", "Closing Stock"]
        edited_df = st.data_editor(df_inv[cols], use_container_width=True, hide_index=True, height=500, disabled=["Category", "Product Name", "UOM", "Closing Stock"])
        
        if st.button("💾 Save Inventory Updates", type="primary", use_container_width=True):
            st.session_state.inventory.update(edited_df)
            for item in st.session_state.inventory["Product Name"]:
                st.session_state.inventory = recalculate_item(st.session_state.inventory, item)
            save_to_sheet(st.session_state.inventory, "rest_01_inventory")
            st.rerun()

    with col_logs:
        st.markdown('<span class="section-title">🕒 Activity Log</span>', unsafe_allow_html=True)
        logs = load_from_sheet("rest_01_logs")
        if not logs.empty:
            st.markdown('<div class="log-container">', unsafe_allow_html=True)
            for _, row in logs.iloc[::-1].head(20).iterrows():
                st.markdown(f'<div class="log-entry"><b>{row["Item"]}</b><br>{row["Qty"]} {row["Action"]} @ {row["Timestamp"]}</div>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.info("No recent activity.")

with tab_req:
    st.markdown('<span class="section-title">🚚 New Requisition Request</span>', unsafe_allow_html=True)
    
    # Compact Requisition Entry Bar
    r1, r2, r3 = st.columns([3, 1, 1])
    with r1:
        req_item = st.selectbox("Select Item", options=[""] + sorted(st.session_state.inventory["Product Name"].tolist()), key="req_item_sel", label_visibility="collapsed")
    with r2:
        req_qty = st.number_input("Qty", min_value=0.0, step=1.0, key="req_qty_val", label_visibility="collapsed")
    with r3:
        if st.button("➕ Create Order", use_container_width=True, type="primary"):
            if req_item and req_qty > 0:
                orders = load_from_sheet("rest_01_orders", ["Date", "Product Name", "Qty", "Status", "OrderID"])
                new_id = str(uuid.uuid4())[:8]
                new_row = pd.DataFrame([{
                    "Date": datetime.datetime.now().strftime("%Y-%m-%d"),
                    "Product Name": req_item,
                    "Qty": req_qty,
                    "Status": "Pending",
                    "OrderID": new_id
                }])
                save_to_sheet(pd.concat([orders, new_row], ignore_index=True), "rest_01_orders")
                st.rerun()

    st.markdown('<hr>', unsafe_allow_html=True)
    st.markdown('<span class="section-title">⏳ Pending & Historical Orders</span>', unsafe_allow_html=True)
    
    orders_df = load_from_sheet("rest_01_orders")
    if not orders_df.empty:
        # Sort by date (newest first)
        if "Date" in orders_df.columns:
            orders_df = orders_df.sort_values(by="Date", ascending=False)
        
        updated_orders = st.data_editor(
            orders_df,
            use_container_width=True,
            hide_index=True,
            height=400,
            column_config={
                "Status": st.column_config.SelectboxColumn("Status", options=["Pending", "Ordered", "Received", "Cancelled"], required=True),
                "OrderID": st.column_config.TextColumn("ID", disabled=True),
                "Date": st.column_config.TextColumn("Date", disabled=True)
            }
        )
        
        if st.button("💾 Save Order Changes", type="primary"):
            save_to_sheet(updated_orders, "rest_01_orders")
            st.rerun()
    else:
        st.info("No orders on record.")

with tab_set:
    st.markdown('<span class="section-title">📂 Bulk Import & Reset</span>', unsafe_allow_html=True)
    
    with st.expander("📥 Import Master Inventory (XLSX/CSV)"):
        st.caption("Expects columns: B=Name, C=UOM, D=Opening Stock")
        up_file = st.file_uploader("Choose File", type=["xlsx", "csv"])
        if up_file:
            try:
                raw = pd.read_excel(up_file, skiprows=4, header=None) if up_file.name.endswith('.xlsx') else pd.read_csv(up_file, skiprows=4, header=None)
                new_df = pd.DataFrame()
                new_df["Product Name"] = raw[1]
                new_df["UOM"] = raw[2]
                new_df["Opening Stock"] = pd.to_numeric(raw[3], errors='coerce').fillna(0)
                new_df["Category"] = "General"
                new_df["Total Received"] = 0.0
                new_df["Consumption"] = 0.0
                new_df["Closing Stock"] = new_df["Opening Stock"]
                
                if st.button("🚀 Overwrite Cloud Inventory"):
                    save_to_sheet(new_df.dropna(subset=["Product Name"]), "rest_01_inventory")
                    st.rerun()
            except Exception as e:
                st.error(f"Error parsing file: {e}")

    if st.button("🧹 Clear All Sheets (Danger)", type="secondary"):
        save_to_sheet(pd.DataFrame(columns=["Product Name", "UOM", "Opening Stock", "Category", "Total Received", "Consumption", "Closing Stock"]), "rest_01_inventory")
        save_to_sheet(pd.DataFrame(columns=["Date", "Product Name", "Qty", "Status", "OrderID"]), "rest_01_orders")
        st.rerun()

with st.sidebar:
    st.markdown("### 🛠 Support")
    st.info("This instance is connected to **rest_01** worksheets.")
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
