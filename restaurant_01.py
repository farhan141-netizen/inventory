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
st.set_page_config(page_title="Restaurant Cloud Pro", layout="wide", initial_sidebar_state="expanded")

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
    .stTabs [data-baseweb="tab"] { padding: 6px 16px; border-radius: 8px; color: #a0a0a0; font-weight: 600; }
    .stTabs [aria-selected="true"] { background: #ff4b2b22; color: #ff4b2b; border: 1px solid #ff4b2b44; }
    
    .section-title { color: #ff4b2b; font-size: 1.15em; font-weight: 700; margin-bottom: 12px; display: block; border-bottom: 1px solid #ff4b2b30; padding-bottom: 5px; }
    
    .log-container { max-height: 450px; overflow-y: auto; background: #1a1a1a; border-radius: 8px; padding: 10px; }
    .log-entry { border-left: 3px solid #ff4b2b; background: #262626; padding: 8px; margin-bottom: 6px; border-radius: 4px; font-size: 0.85em; color: #e0e0e0; }
    
    .premium-card { background: #1a1f2e; border: 1px solid #2d3748; padding: 15px; border-radius: 12px; margin-bottom: 15px; }
    </style>
""", unsafe_allow_html=True)

# --- APP LOGIC ---
def recalculate_item(df, item_name):
    """Restaurant Logic: Opening + Received - Consumption = Closing"""
    if item_name not in df["Product Name"].values: return df
    idx = df[df["Product Name"] == item_name].index[0]
    
    opening = pd.to_numeric(df.at[idx, "Opening Stock"], errors='coerce') or 0.0
    received = pd.to_numeric(df.at[idx, "Total Received"], errors='coerce') or 0.0
    consumption = pd.to_numeric(df.at[idx, "Consumption"], errors='coerce') or 0.0
    
    closing = opening + received - consumption
    df.at[idx, "Closing Stock"] = closing
    
    # Calculate Variance if Physical Count exists
    if "Physical Count" in df.columns:
        phys = df.at[idx, "Physical Count"]
        if pd.notna(phys) and str(phys).strip() != "":
            df.at[idx, "Variance"] = pd.to_numeric(phys) - closing
    return df

# --- UI HEADER ---
st.markdown('<div class="header-bar"><div><h2 style="margin:0;">🍴 Restaurant Cloud Pro</h2><p style="margin:0; opacity:0.8;">Centralized Outlet Inventory & Requisitions</p></div></div>', unsafe_allow_html=True)

# --- SESSION STATE & INITIAL LOAD ---
if 'inventory' not in st.session_state:
    st.session_state.inventory = load_from_sheet("rest_01_inventory")

# --- TABS ---
tab_inv, tab_req, tab_set = st.tabs(["📋 Dashboard", "📦 Requisitions", "⚙️ Control Panel"])

with tab_inv:
    col_main, col_logs = st.columns([3, 1])
    
    with col_main:
        st.markdown('<span class="section-title">📊 Inventory & Consumption Tracker</span>', unsafe_allow_html=True)
        df_inv = st.session_state.inventory.copy()
        
        # Category Filter
        cats = ["All"] + sorted(df_inv["Category"].unique().tolist()) if "Category" in df_inv.columns else ["All"]
        c1, c2 = st.columns([1, 2])
        with c1:
            sel_cat = st.selectbox("Filter Category", cats, label_visibility="collapsed")
        
        if sel_cat != "All":
            df_inv = df_inv[df_inv["Category"] == sel_cat]
            
        # Define display columns
        disp_cols = ["Category", "Product Name", "UOM", "Opening Stock", "Total Received", "Consumption", "Closing Stock"]
        if "Physical Count" in df_inv.columns: disp_cols.append("Physical Count")
        if "Variance" in df_inv.columns: disp_cols.append("Variance")
            
        edited_df = st.data_editor(
            df_inv[disp_cols], 
            use_container_width=True, 
            hide_index=True, 
            height=450, 
            disabled=["Category", "Product Name", "UOM", "Closing Stock", "Variance"]
        )
        
        if st.button("💾 Sync & Save Changes", type="primary", use_container_width=True):
            st.session_state.inventory.update(edited_df)
            # Recalculate logic for all items
            for item in st.session_state.inventory["Product Name"]:
                st.session_state.inventory = recalculate_item(st.session_state.inventory, item)
            save_to_sheet(st.session_state.inventory, "rest_01_inventory")
            st.rerun()

    with col_logs:
        st.markdown('<span class="section-title">🕒 Activity</span>', unsafe_allow_html=True)
        logs = load_from_sheet("rest_01_logs")
        if not logs.empty:
            st.markdown('<div class="log-container">', unsafe_allow_html=True)
            for _, row in logs.iloc[::-1].head(30).iterrows():
                # Handling flexible log columns
                act = row.get("Action", "Update")
                st.markdown(f'<div class="log-entry"><b>{row["Item"]}</b><br>{row["Qty"]} {act} @ {row["Timestamp"]}</div>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.info("No logs recorded yet.")

with tab_req:
    # --- COMPACT REQUISITION PORTAL ---
    st.markdown('<span class="section-title">🚚 Create New Requisition</span>', unsafe_allow_html=True)
    
    # Quick Entry Bar (Single Line)
    r1, r2, r3 = st.columns([3, 1, 1])
    with r1:
        req_item = st.selectbox("Select Product", options=[""] + sorted(st.session_state.inventory["Product Name"].tolist()), key="req_item_sel", label_visibility="collapsed")
    with r2:
        req_qty = st.number_input("Order Qty", min_value=0.0, step=1.0, key="req_qty_val", label_visibility="collapsed")
    with r3:
        if st.button("➕ Create Order", use_container_width=True, type="primary"):
            if req_item and req_qty > 0:
                orders = load_from_sheet("rest_01_orders", ["Date", "Product Name", "Qty", "Status", "OrderID"])
                new_row = pd.DataFrame([{
                    "Date": datetime.datetime.now().strftime("%Y-%m-%d"),
                    "Product Name": req_item,
                    "Qty": req_qty,
                    "Status": "Pending",
                    "OrderID": str(uuid.uuid4())[:8]
                }])
                save_to_sheet(pd.concat([orders, new_row], ignore_index=True), "rest_01_orders")
                st.rerun()

    st.markdown('<hr>', unsafe_allow_html=True)
    st.markdown('<span class="section-title">⏳ Pending Orders (Date Sorted)</span>', unsafe_allow_html=True)
    
    orders_df = load_from_sheet("rest_01_orders")
    if not orders_df.empty:
        # Sort logic: Ensure date exists, then sort newest first
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
                "Date": st.column_config.TextColumn("Date", disabled=True),
                "Product Name": st.column_config.TextColumn("Product", disabled=True)
            }
        )
        
        if st.button("💾 Save Order Updates", type="primary"):
            save_to_sheet(updated_orders, "rest_01_orders")
            st.rerun()
    else:
        st.info("No requisition history found.")

with tab_set:
    st.markdown('<span class="section-title">⚙️ Advanced Tools</span>', unsafe_allow_html=True)
    
    col_l, col_r = st.columns(2)
    
    with col_l:
        st.markdown('<div class="premium-card">', unsafe_allow_html=True)
        st.subheader("📥 Master Import")
        st.caption("Standard layout: B=Name, C=UOM, D=Opening Stock")
        up_file = st.file_uploader("Upload Master File", type=["xlsx", "csv"])
        if up_file:
            try:
                # Use res v1 logic for skipping headers
                raw = pd.read_excel(up_file, skiprows=4, header=None) if up_file.name.endswith('.xlsx') else pd.read_csv(up_file, skiprows=4, header=None)
                new_df = pd.DataFrame()
                new_df["Product Name"] = raw[1]
                new_df["UOM"] = raw[2]
                new_df["Opening Stock"] = pd.to_numeric(raw[3], errors='coerce').fillna(0)
                new_df["Category"] = "General"
                new_df["Total Received"] = 0.0
                new_df["Consumption"] = 0.0
                new_df["Closing Stock"] = new_df["Opening Stock"]
                new_df["Physical Count"] = None
                new_df["Variance"] = 0.0
                
                if st.button("🚀 Push to Restaurant Cloud", type="primary"):
                    save_to_sheet(new_df.dropna(subset=["Product Name"]), "rest_01_inventory")
                    st.success("Master Inventory Initialized!")
                    st.rerun()
            except Exception as e:
                st.error(f"Import Error: {e}")
        st.markdown('</div>', unsafe_allow_html=True)

    with col_r:
        st.markdown('<div class="premium-card">', unsafe_allow_html=True)
        st.subheader("🧹 System Reset")
        st.warning("This will permanently clear all cloud data.")
        if st.button("Clear All Sheets", type="secondary", use_container_width=True):
            save_to_sheet(pd.DataFrame(columns=["Category", "Product Name", "UOM", "Opening Stock", "Total Received", "Consumption", "Closing Stock", "Physical Count", "Variance"]), "rest_01_inventory")
            save_to_sheet(pd.DataFrame(columns=["Date", "Product Name", "Qty", "Status", "OrderID"]), "rest_01_orders")
            save_to_sheet(pd.DataFrame(columns=["Timestamp", "Item", "Qty", "Action"]), "rest_01_logs")
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

with st.sidebar:
    st.markdown("### 🏬 Outlet Info")
    st.info("Instance: **Restaurant Outlet 01**")
    st.markdown("---")
    if st.button("🔄 Full System Refresh"):
        st.cache_data.clear()
        st.rerun()
    
    st.markdown("### 📊 Quick Stats")
    if not st.session_state.inventory.empty:
        low_stock = len(st.session_state.inventory[st.session_state.inventory["Closing Stock"] < 5])
        st.metric("Low Stock Items", low_stock)
        st.metric("Total SKU Count", len(st.session_state.inventory))
