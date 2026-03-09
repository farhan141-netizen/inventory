import streamlit as st
import pandas as pd
import datetime
import uuid
import io
import numpy as np
from st_supabase_connection import SupabaseConnection
conn = st.connection("supabase", type=SupabaseConnection)

# Column name remap: Supabase returns lowercase, app expects Title Case
_COL_REMAP = {
    "product name":   "Product Name",
    "category":       "Category",
    "uom":            "UOM",
    "opening stock":  "Opening Stock",
    "total received": "Total Received",
    "consumption":    "Consumption",
    "closing stock":  "Closing Stock",
    "physical count": "Physical Count",
    "variance":       "Variance",
    "reqid":          "ReqID",
    "restaurant":     "Restaurant",
    "item":           "Item",
    "qty":            "Qty",
    "status":         "Status",
    "dispatchqty":    "DispatchQty",
    "acceptedqty":    "AcceptedQty",
    "timestamp":      "Timestamp",
    "requesteddate":  "RequestedDate",
    "followupsent":   "FollowupSent",
}

def _remap_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename lowercased Supabase columns back to expected Title Case."""
    return df.rename(columns={k: v for k, v in _COL_REMAP.items() if k in df.columns})

def load_from_sheet(table_name, default_cols=None):
    """Load from Supabase table."""
    try:
        response = conn.table(table_name).select("*").execute()
        data = response.data
        if not data:
            return pd.DataFrame(columns=default_cols) if default_cols else pd.DataFrame()
        df = pd.DataFrame(data)
        df = _remap_columns(df)
        df = df.replace({None: np.nan})
        return df
    except Exception as e:
        st.warning(f"Table '{table_name}' not found or empty: {e}")
        return pd.DataFrame(columns=default_cols) if default_cols else pd.DataFrame()

def _clean_for_supabase(df: pd.DataFrame) -> pd.DataFrame:
    """Cast types correctly to avoid Supabase bigint/float errors."""
    df = df.copy()
    df = df.replace({np.nan: None})

    # Float columns
    float_cols = ["Qty", "DispatchQty", "AcceptedQty", "Opening Stock",
                  "Total Received", "Consumption", "Closing Stock", "Variance"]
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            # Cast to int if all non-null values are whole numbers (avoids bigint error)
            non_null = df[col].dropna()
            if len(non_null) == 0 or (non_null % 1 == 0).all():
                df[col] = df[col].apply(lambda x: int(x) if pd.notna(x) else None)

    # Day columns 1–31 — keep as float
    for day in range(1, 32):
        col = str(day)
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    return df

# Map: table_name → primary key column
_TABLE_PK = {
    "rest_01_inventory":        "Product Name",
    "restaurant_requisitions":  "ReqID",
}

def save_to_sheet(df, table_name):
    """Upsert DataFrame rows into a Supabase table."""
    try:
        if df is None or df.empty:
            st.error(f"Cannot save empty dataframe to {table_name}")
            return False

        df = _clean_for_supabase(df)
        records = df.to_dict(orient="records")

        pk = _TABLE_PK.get(table_name)
        response = conn.table(table_name).upsert(records, on_conflict=pk).execute()

        if response.data is not None:
            return True
        else:
            st.error(f"❌ Save error ({table_name}): no response data")
            return False

    except Exception as e:
        st.error(f"❌ Database Save Error ({table_name}): {e}")
        return False

def create_standard_inventory(df):
    """Convert uploaded inventory to standard format with all required columns"""
    standard_df = pd.DataFrame()
    
    # Map columns or use defaults
    standard_df["Product Name"] = df[1] if 1 in df.columns else ""
    standard_df["Category"] = "General"
    standard_df["UOM"] = df[2] if 2 in df.columns else "pcs"
    standard_df["Opening Stock"] = pd.to_numeric(df[3] if 3 in df.columns else 0, errors='coerce').fillna(0)
    
    # Add day columns (1-31)
    for day in range(1, 32):
        standard_df[str(day)] = 0.0
    
    # Add calculation columns
    standard_df["Total Received"] = 0.0
    standard_df["Consumption"] = 0.0
    standard_df["Closing Stock"] = standard_df["Opening Stock"]
    standard_df["Physical Count"] = None
    standard_df["Variance"] = 0.0
    
    # Remove empty rows
    standard_df = standard_df.dropna(subset=["Product Name"])
    standard_df["Product Name"] = standard_df["Product Name"].astype(str).str.strip()
    standard_df = standard_df[standard_df["Product Name"] != ""]
    
    return standard_df

def recalculate_inventory(df):
    """Recalculate totals and closing stock"""
    day_cols = [str(i) for i in range(1, 32)]
    
    # Ensure all day columns are numeric
    for col in day_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
    
    # Ensure Opening Stock and Consumption are numeric
    df["Opening Stock"] = pd.to_numeric(df["Opening Stock"], errors='coerce').fillna(0.0)
    df["Consumption"] = pd.to_numeric(df["Consumption"], errors='coerce').fillna(0.0)
    
    for idx, row in df.iterrows():
        # Calculate total received from day columns
        total_received = 0.0
        for col in day_cols:
            if col in df.columns:
                total_received += float(df.at[idx, col]) if pd.notna(df.at[idx, col]) else 0.0
        
        df.at[idx, "Total Received"] = total_received
        
        # Calculate closing stock: Opening + Received - Consumption
        opening = float(df.at[idx, "Opening Stock"]) if pd.notna(df.at[idx, "Opening Stock"]) else 0.0
        consumption = float(df.at[idx, "Consumption"]) if pd.notna(df.at[idx, "Consumption"]) else 0.0
        df.at[idx, "Closing Stock"] = opening + total_received - consumption
        
        # Calculate variance
        physical = df.at[idx, "Physical Count"]
        if pd.notna(physical) and str(physical).strip() != "":
            try:
                physical_val = float(physical)
                df.at[idx, "Variance"] = physical_val - df.at[idx, "Closing Stock"]
            except:
                df.at[idx, "Variance"] = 0.0
        else:
            df.at[idx, "Variance"] = 0.0
    
    return df

# --- PAGE CONFIG ---
st.set_page_config(page_title="Restaurant 01 Pro", layout="wide")

# --- CUSTOM CSS - COMPACT & OPTIMIZED ---
st.markdown("""
    <style>
    .main { background-color: #0e1117; }
    .header { background: linear-gradient(90deg, #ff6b35 0%, #f7931e 100%); padding: 10px 15px; border-radius: 10px; color: white; margin-bottom: 10px; }
    .header h1 { margin: 0; font-size: 1.4em; }
    .header p { margin: 2px 0 0 0; font-size: 0.8em; }
    
    .stTabs [data-baseweb="tab-list"] { gap: 6px; }
    .stTabs [data-baseweb="tab"] { height: 40px; background-color: #1e2130; border-radius: 5px 5px 0 0; padding: 6px 10px; color: white; font-weight: 600; font-size: 0.85em; }
    .stTabs [aria-selected="true"] { background-color: #ff6b35 !important; color: white !important; }
    
    .section-title { color: #ff6b35; font-size: 0.95em; font-weight: 700; margin-bottom: 6px; margin-top: 2px; }
    .stButton>button { border-radius: 4px; font-weight: 600; font-size: 0.8em; padding: 3px 6px; }
    
    /* Cart Styles */
    .cart-compact { background: #1e2130; padding: 8px; border-radius: 8px; border: 2px solid #ff6b35; }
    .cart-item-row { display: flex; justify-content: space-between; align-items: center; background: #262730; padding: 4px 6px; margin-bottom: 3px; border-left: 3px solid #ff6b35; border-radius: 3px; font-size: 0.85em; }
    .cart-item-name { flex: 1; }
    .cart-item-btn { margin-left: 4px; }
    
    /* Status Colors */
    .status-pending { border-left: 4px solid #ffaa00; background: #3a2f1a; }
    .status-dispatched { border-left: 4px solid #00d9ff; background: #1a2f3f; }
    .status-completed { border-left: 4px solid #00ff00; background: #1a3a1a; }
    
    .req-item { padding: 6px 8px; margin: 3px 0; border-radius: 4px; font-size: 0.85em; line-height: 1.3; display: flex; justify-content: space-between; align-items: center; }
    .req-item-content { flex: 1; }
    .req-item-buttons { display: flex; gap: 4px; margin-left: 8px; }
    
    hr { margin: 4px 0; opacity: 0.1; }
    </style>
    """, unsafe_allow_html=True)

# --- INITIALIZATION ---
if 'inventory' not in st.session_state:
    st.session_state.inventory = load_from_sheet("rest_01_inventory")
    if not st.session_state.inventory.empty:
        st.session_state.inventory = recalculate_inventory(st.session_state.inventory)

if 'cart' not in st.session_state:
    st.session_state.cart = []

# --- HEADER ---
st.markdown("""
    <div class="header">
        <h1>🍴 Restaurant 01 | Operations Portal</h1>
        <p>Inventory Management & Warehouse Requisitions</p>
    </div>
""", unsafe_allow_html=True)

# --- REFRESH BUTTON ---
col_refresh, col_empty = st.columns([1, 5])
with col_refresh:
    if st.button("🔄 Refresh Data", use_container_width=True, key="refresh_all"):
        for key in ["inventory"]:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()
# --- TABS ---
tab_inv, tab_req, tab_pending, tab_received, tab_history = st.tabs(["📋 Inventory Count", "🛒 Send Requisition", "🚚 Pending Orders", "📦 Received Items", "📊 History"])

# ===================== INVENTORY TAB =====================
with tab_inv:
    st.markdown('<div class="section-title">📊 Daily Stock Take</div>', unsafe_allow_html=True)
    
    if not st.session_state.inventory.empty:
        # Ensure standard columns exist
        standard_cols = ["Product Name", "Category", "UOM", "Opening Stock"] + [str(i) for i in range(1, 32)] + ["Total Received", "Consumption", "Closing Stock", "Physical Count", "Variance"]
        for col in standard_cols:
            if col not in st.session_state.inventory.columns:
                if col == "Category":
                    st.session_state.inventory[col] = "General"
                elif col == "UOM":
                    st.session_state.inventory[col] = "pcs"
                else:
                    st.session_state.inventory[col] = 0.0 if col not in ["Physical Count"] else None
        
        # Category filter
        cats = ["All"] + sorted(st.session_state.inventory["Category"].unique().tolist())
        sel_cat = st.selectbox("Filter Category", cats, key="inv_cat", label_visibility="collapsed")
        
        display_df = st.session_state.inventory.copy()
        if sel_cat != "All":
            display_df = display_df[display_df["Category"] == sel_cat]
        
        # Display columns
        day_cols = [str(i) for i in range(1, 32)]
        display_cols = ["Product Name", "Category", "UOM", "Opening Stock"] + day_cols + ["Total Received", "Consumption", "Closing Stock", "Physical Count", "Variance"]
        display_cols_filtered = [c for c in display_cols if c in display_df.columns]
        
        edited_inv = st.data_editor(
            display_df[display_cols_filtered],
            use_container_width=True,
            disabled=["Product Name", "Category", "UOM", "Opening Stock", "Total Received", "Closing Stock", "Variance"],
            hide_index=True,
            key="inv_editor",
            height=300
        )
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("💾 Save Daily Count", type="primary", use_container_width=True, key="save_inv"):
                # Get the edited values and update the main inventory
                for col in edited_inv.columns:
                    if col not in ["Product Name", "Category", "UOM", "Opening Stock", "Total Received", "Closing Stock", "Variance"]:
                        # Only update editable columns
                        for edited_idx in edited_inv.index:
                            if edited_idx in st.session_state.inventory.index:
                                st.session_state.inventory.at[edited_idx, col] = edited_inv.at[edited_idx, col]
                
                st.session_state.inventory = recalculate_inventory(st.session_state.inventory)
                if save_to_sheet(st.session_state.inventory, "rest_01_inventory"):
                    st.success("✅ Inventory saved!")
                    st.rerun()
        
        with col2:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine='xlsxwriter') as writer:
                st.session_state.inventory[display_cols_filtered].to_excel(writer, index=False, sheet_name='Inventory')
            st.download_button("📥 Download Inventory", data=buf.getvalue(), file_name="Inventory_Count.xlsx", use_container_width=True, key="dl_inv")
        
        with col3:
            st.info(f"📊 Total Items: {len(st.session_state.inventory)}")
    else:
        st.warning("⚠️ No inventory found. Please upload template from Settings.")

# ===================== REQUISITION TAB =====================
with tab_req:
    col_l, col_r = st.columns([2.5, 1])
    
    with col_l:
        st.markdown('<div class="section-title">🛒 Add Items to Requisition</div>', unsafe_allow_html=True)
        if not st.session_state.inventory.empty:
            search_item = st.text_input("🔍 Search Product", key="search_req", placeholder="Type product name...").lower()
            
            if search_item:
                items = st.session_state.inventory[st.session_state.inventory["Product Name"].str.lower().str.contains(search_item, na=False)]
            else:
                items = st.session_state.inventory
            
            for item_idx, (_, row) in enumerate(items.head(12).iterrows()):
                c1, c2, c3 = st.columns([3, 0.7, 0.7])
                product_name = row['Product Name']
                uom = row['UOM']
                closing_stock = row.get('Closing Stock', 0)
                
                c1.write(f"**{product_name}** ({uom}) | Stock: {closing_stock}")
                
                qty = c2.number_input(
                    "Qty", 
                    min_value=0.0, 
                    key=f"req_qty_{item_idx}_{search_item}",
                    label_visibility="collapsed"
                )
                
                if c3.button("➕", key=f"btn_add_{item_idx}_{search_item}", use_container_width=True):
                    if qty > 0:
                        st.session_state.cart.append({
                            'name': product_name, 
                            'qty': qty, 
                            'uom': uom
                        })
                        st.toast(f"✅ Added {product_name}")
                        st.rerun()

    with col_r:
        st.markdown('<div class="section-title">🛒 Cart</div>', unsafe_allow_html=True)
        
        if st.session_state.cart:
            cart_total = sum([item['qty'] for item in st.session_state.cart])
            st.markdown(f'<div style="text-align:center; color:#ff6b35;"><b>{len(st.session_state.cart)}</b> items | <b>{cart_total}</b> qty</div>', unsafe_allow_html=True)
            
            st.markdown('<div class="cart-compact">', unsafe_allow_html=True)
            
            for i, item in enumerate(st.session_state.cart):
                col1, col2 = st.columns([4, 1])
                with col1:
                    st.markdown(f'<div class="cart-item-row"><span class="cart-item-name">{item["name"]}: {item["qty"]} {item["uom"]}</span></div>', unsafe_allow_html=True)
                with col2:
                    if st.button("❌", key=f"rm_{i}", use_container_width=True):
                        st.session_state.cart.pop(i)
                        st.rerun()
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            col1, col2 = st.columns(2)
            
            if col1.button("🗑️ Clear", use_container_width=True, key="clear_cart"):
                st.session_state.cart = []
                st.rerun()
                
            if col2.button("🚀 Submit", type="primary", use_container_width=True, key="submit_req"):
                try:
                    all_reqs = load_from_sheet("restaurant_requisitions", ["ReqID", "Restaurant", "Item", "Qty", "Status", "DispatchQty", "AcceptedQty", "Timestamp", "RequestedDate", "FollowupSent"])
                    
                    st.info(f"📤 Sending {len(st.session_state.cart)} items...")
                    
                    for item in st.session_state.cart:
                        new_req = pd.DataFrame([{
                            "ReqID": str(uuid.uuid4())[:8],
                            "Restaurant": "Restaurant 01",
                            "Item": item['name'],
                            "Qty": float(item['qty']),
                            "Status": "Pending",
                            "DispatchQty": 0.0,
                            "AcceptedQty": 0.0,
                            "Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "RequestedDate": datetime.datetime.now().strftime("%Y-%m-%d"),
                            "FollowupSent": False
                        }])
                        all_reqs = pd.concat([all_reqs, new_req], ignore_index=True)
                    
                    st.write(f"✅ Total records to save: {len(all_reqs)}")
                    
                    all_reqs["Qty"] = pd.to_numeric(all_reqs["Qty"], errors='coerce')
                    all_reqs["DispatchQty"] = pd.to_numeric(all_reqs["DispatchQty"], errors='coerce')
                    all_reqs = all_reqs.reset_index(drop=True)
                    
                    if save_to_sheet(all_reqs, "restaurant_requisitions"):
                        st.success("✅ Requisition sent to Warehouse successfully!")
                        st.balloons()
                        st.session_state.cart = []
                        st.rerun()
                    else:
                        st.error("❌ Failed to send requisition. Please check your Supabase connection and table permissions.")
                
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")
                    st.write("Please ensure your Supabase connection has proper permissions and try again.")
        else:
            st.write("🛒 Cart is empty")

# ===================== PENDING ORDERS TAB =====================
with tab_pending:
    st.markdown('<div class="section-title">🚚 Pending Orders Status (All Remaining Items)</div>', unsafe_allow_html=True)
    all_reqs = load_from_sheet("restaurant_requisitions")
    
    if not all_reqs.empty:
        # Ensure FollowupSent column exists
        if "FollowupSent" not in all_reqs.columns:
            all_reqs["FollowupSent"] = False
        
        # Calculate remaining qty for each row
        all_reqs["Remaining"] = all_reqs["Qty"] - all_reqs["DispatchQty"]
        
        # Show ALL items where remaining qty > 0 (regardless of status)
        my_pending = all_reqs[(all_reqs["Restaurant"] == "Restaurant 01") & (all_reqs["Remaining"] > 0)]
        
        if not my_pending.empty:
            # Convert RequestedDate safely
            my_pending = my_pending.copy()
            my_pending["RequestedDate"] = pd.to_datetime(my_pending["RequestedDate"], errors='coerce')
            my_pending = my_pending[my_pending["RequestedDate"].notna()]
            
            if not my_pending.empty:
                my_pending = my_pending.sort_values("RequestedDate", ascending=False)
                unique_dates = sorted(my_pending["RequestedDate"].unique(), reverse=True)
                
                st.metric("Total Items Pending", len(my_pending))
                
                for req_date in unique_dates:
                    try:
                        date_str = pd.Timestamp(req_date).strftime("%d/%m/%Y")
                    except:
                        date_str = "Unknown Date"
                    
                    date_reqs = my_pending[my_pending["RequestedDate"] == req_date]
                    
                    with st.expander(f"📅 {date_str} ({len(date_reqs)} items)", expanded=False):
                        for idx, row in date_reqs.iterrows():
                            item_name = row["Item"]
                            req_qty = float(row["Qty"])
                            dispatch_qty = float(row["DispatchQty"])
                            remaining_qty = float(row["Remaining"])
                            status = row["Status"]
                            req_id = row["ReqID"]
                            followup_sent = row.get("FollowupSent", False)
                            
                            # Show status indicator
                            if status == "Pending":
                                status_indicator = "🟡"
                                status_text = "Pending"
                                bg_class = "status-pending"
                            elif status == "Dispatched":
                                status_indicator = "🟠"
                                status_text = "Partial Delivery"
                                bg_class = "status-dispatched"
                            else:
                                status_indicator = "🟢"
                                status_text = "Completed"
                                bg_class = "status-completed"
                            
                            followup_indicator = "⚠️ Follow-up Sent" if followup_sent else "⏳ No Follow-up"
                            
                            # Create item box with buttons inside
                            col_item, col_fup, col_complete = st.columns([2, 1, 1])
                            
                            with col_item:
                                st.markdown(f"""
                                <div class="req-item {bg_class}">
                                    <div class="req-item-content">
                                        <b>{status_indicator} {item_name}</b><br>
                                        Req:{req_qty} | Got:{dispatch_qty} | Rem:{remaining_qty}<br>
                                        <small>{status_text} | {followup_indicator}</small>
                                    </div>
                                </div>
                                """, unsafe_allow_html=True)
                            
                            with col_fup:
                                if st.button(f"🚩", key=f"followup_{idx}_{req_id}", use_container_width=True, help="Request Follow-up"):
                                    try:
                                        all_reqs.at[idx, "FollowupSent"] = True
                                        all_reqs.at[idx, "Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                        save_to_sheet(all_reqs, "restaurant_requisitions")
                                        st.success(f"✅ Follow-up sent!")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"❌ Error: {str(e)}")
                            
                            with col_complete:
                                if st.button(f"✅", key=f"complete_{idx}_{req_id}", use_container_width=True, help="Mark Complete"):
                                    try:
                                        # Only if all items are received (remaining = 0)
                                        if remaining_qty <= 0:
                                            all_reqs.at[idx, "Status"] = "Completed"
                                            save_to_sheet(all_reqs, "restaurant_requisitions")
                                            st.success(f"✅ Marked complete!")
                                            st.rerun()
                                        else:
                                            st.warning(f"⚠️ Still {remaining_qty} units pending.")
                                    except Exception as e:
                                        st.error(f"❌ Error: {str(e)}")
            else:
                st.success("✅ All items received! No pending orders.")
        else:
            st.success("✅ All items received! No pending orders.")
    else:
        st.info("📭 No orders found. Submit your first requisition!")

# ===================== RECEIVED ITEMS TAB =====================
with tab_received:
    st.markdown('<div class="section-title">📦 Received Items - Accept Dispatches</div>', unsafe_allow_html=True)
    all_reqs = load_from_sheet("restaurant_requisitions")
    
    if not all_reqs.empty:
        all_reqs["Remaining"] = all_reqs["Qty"] - all_reqs["DispatchQty"]
        # Add AcceptedQty column if missing
        if "AcceptedQty" not in all_reqs.columns:
            all_reqs["AcceptedQty"] = 0.0
        all_reqs["AcceptedQty"] = pd.to_numeric(all_reqs["AcceptedQty"], errors='coerce').fillna(0.0)
        my_dispatched = all_reqs[
            (all_reqs["Restaurant"] == "Restaurant 01") &
            (all_reqs["Status"] == "Dispatched") &
            (all_reqs["AcceptedQty"] < all_reqs["DispatchQty"])
        ]
        
        if not my_dispatched.empty:
            # Convert RequestedDate safely
            my_dispatched = my_dispatched.copy()
            my_dispatched["RequestedDate"] = pd.to_datetime(my_dispatched["RequestedDate"], errors='coerce')
            my_dispatched = my_dispatched[my_dispatched["RequestedDate"].notna()]
            
            if not my_dispatched.empty:
                my_dispatched = my_dispatched.sort_values("RequestedDate", ascending=False)
                unique_dates = sorted(my_dispatched["RequestedDate"].unique(), reverse=True)
                
                st.metric("Total Dispatched Items", len(my_dispatched))
                
                for req_date in unique_dates:
                    try:
                        date_str = pd.Timestamp(req_date).strftime("%d/%m/%Y")
                    except:
                        date_str = "Unknown Date"
                    
                    date_reqs = my_dispatched[my_dispatched["RequestedDate"] == req_date]
                    
                    with st.expander(f"📅 {date_str} ({len(date_reqs)} items)", expanded=False):
                        for recv_idx, (original_idx, row) in enumerate(date_reqs.iterrows()):
                            item_name = row["Item"]
                            dispatch_qty = float(row["DispatchQty"])
                            req_qty = float(row["Qty"])
                            req_id = row["ReqID"]
                            remaining_qty = req_qty - dispatch_qty
                            accepted_qty = float(row.get("AcceptedQty", 0)) if pd.notna(row.get("AcceptedQty", 0)) else 0.0
                            accept_amount = dispatch_qty - accepted_qty
                            
                            # Color based on whether all items are received
                            status_indicator = "🟢" if accept_amount <= 0 else "🟡"
                            
                            # Create item box with buttons inside
                            col_item, col_accept, col_reject = st.columns([2, 1, 1])
                            
                            with col_item:
                                st.markdown(f"""
                                <div class="req-item status-dispatched">
                                    <div class="req-item-content">
                                        <b>{status_indicator} {item_name}</b><br>
                                        Req:{req_qty} | Dispatched:{dispatch_qty} | Accepted:{accepted_qty} | To Accept:{accept_amount}
                                    </div>
                                </div>
                                """, unsafe_allow_html=True)
                            
                            with col_accept:
                                if accept_amount <= 0:
                                    st.caption("✅ Accepted")
                                else:
                                    if st.button(f"✅", key=f"accept_{recv_idx}_{req_id}", use_container_width=True, help="Accept & Add to Inventory"):
                                        try:
                                            # Mark this dispatch as accepted
                                            all_reqs.at[original_idx, "AcceptedQty"] = dispatch_qty
                                            
                                            # If fully dispatched AND accepted, mark complete
                                            if remaining_qty <= 0:
                                                all_reqs.at[original_idx, "Status"] = "Completed"
                                            
                                            # Add to restaurant inventory - add to today's column
                                            today = datetime.datetime.now().day
                                            day_col = str(today)
                                            
                                            # Normalize item name for matching
                                            item_name_clean = item_name.strip().lower()
                                            inv_match = st.session_state.inventory[
                                                st.session_state.inventory["Product Name"].str.strip().str.lower() == item_name_clean
                                            ]
                                            
                                            if not inv_match.empty:
                                                idx_val = inv_match.index[0]
                                                
                                                # Ensure day column is numeric
                                                if day_col in st.session_state.inventory.columns:
                                                    st.session_state.inventory[day_col] = pd.to_numeric(
                                                        st.session_state.inventory[day_col], errors='coerce'
                                                    ).fillna(0.0)
                                                
                                                # Add only unaccepted amount to today's day column
                                                current_day_qty = float(st.session_state.inventory.at[idx_val, day_col]) if pd.notna(st.session_state.inventory.at[idx_val, day_col]) else 0.0
                                                st.session_state.inventory.at[idx_val, day_col] = current_day_qty + accept_amount
                                                
                                                # Recalculate all totals for this item
                                                st.session_state.inventory = recalculate_inventory(st.session_state.inventory)
                                            else:
                                                st.warning(f"⚠️ Item '{item_name}' not found in inventory. Cannot update stock.")
                                            
                                            # Save both
                                            save_to_sheet(all_reqs, "restaurant_requisitions")
                                            save_to_sheet(st.session_state.inventory, "rest_01_inventory")
                                            
                                            st.success(f"✅ Accepted {accept_amount} units!")
                                            st.rerun()
                                        except Exception as e:
                                            st.error(f"❌ Error: {str(e)}")
                            
                            with col_reject:
                                if st.button(f"❌", key=f"reject_{recv_idx}_{req_id}", use_container_width=True, help="Reject"):
                                    try:
                                        all_reqs.at[original_idx, "Status"] = "Pending"
                                        all_reqs.at[original_idx, "DispatchQty"] = 0
                                        all_reqs.at[original_idx, "AcceptedQty"] = 0.0
                                        save_to_sheet(all_reqs, "restaurant_requisitions")
                                        st.warning(f"❌ Returned to pending")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"❌ Error: {str(e)}")
            else:
                st.info("📭 No dispatched items")
        else:
            st.info("📭 No dispatched items")
    else:
        st.info("📭 No orders found")

# ===================== HISTORY TAB =====================
with tab_history:
    st.markdown('<div class="section-title">📊 Requisition History</div>', unsafe_allow_html=True)
    
    all_reqs = load_from_sheet("restaurant_requisitions")
    
    if not all_reqs.empty:
        my_history = all_reqs[all_reqs["Restaurant"] == "Restaurant 01"]
        
        if not my_history.empty:
            # Filters
            col1, col2, col3 = st.columns(3)
            
            with col1:
                filter_status = st.multiselect("Filter by Status", ["Pending", "Dispatched", "Completed"], default=["Pending", "Dispatched", "Completed"], key="hist_status")
            
            with col2:
                filter_item = st.text_input("Filter by Item", placeholder="Type item name...", key="hist_item").lower()
            
            with col3:
                sort_by = st.selectbox("Sort by", ["Latest First", "Oldest First", "Item Name"], key="hist_sort")
            
            # Apply filters
            filtered_history = my_history[my_history["Status"].isin(filter_status)]
            
            if filter_item:
                filtered_history = filtered_history[filtered_history["Item"].str.lower().str.contains(filter_item, na=False)]
            
            # Convert RequestedDate safely
            filtered_history = filtered_history.copy()
            filtered_history["RequestedDate"] = pd.to_datetime(filtered_history["RequestedDate"], errors='coerce')
            filtered_history = filtered_history[filtered_history["RequestedDate"].notna()]
            
            if not filtered_history.empty:
                # Sort
                if sort_by == "Latest First":
                    filtered_history = filtered_history.sort_values("RequestedDate", ascending=False)
                elif sort_by == "Oldest First":
                    filtered_history = filtered_history.sort_values("RequestedDate", ascending=True)
                else:
                    filtered_history = filtered_history.sort_values("Item", ascending=True)
                
                st.metric("Total Records", len(filtered_history))
                
                # Group by date
                unique_dates = sorted(filtered_history["RequestedDate"].unique(), reverse=True)
                
                for req_date in unique_dates:
                    try:
                        date_str = pd.Timestamp(req_date).strftime("%d/%m/%Y")
                    except:
                        date_str = "Unknown Date"
                    
                    date_hist = filtered_history[filtered_history["RequestedDate"] == req_date]
                    
                    with st.expander(f"📅 {date_str} ({len(date_hist)} items)", expanded=False):
                        for hist_idx, (_, row) in enumerate(date_hist.iterrows()):
                            item_name = row["Item"]
                            req_qty = float(row["Qty"])
                            dispatch_qty = float(row["DispatchQty"])
                            status = row["Status"]
                            timestamp = row.get("Timestamp", "N/A")
                            remaining = req_qty - dispatch_qty
                            followup = row.get("FollowupSent", False)
                            
                            # Color based on status
                            if status == "Pending":
                                status_color = "🟡"
                                box_class = "status-pending"
                            elif status == "Dispatched":
                                status_color = "🟠"
                                box_class = "status-dispatched"
                            else:  # Completed
                                status_color = "🟢"
                                box_class = "status-completed"
                            
                            followup_text = "⚠️ Follow-up Sent" if followup else ""
                            
                            st.markdown(f"""
                            <div class="req-item {box_class}">
                                <div class="req-item-content">
                                    <b>{status_color} {item_name}</b><br>
                                    Req:{req_qty} | Got:{dispatch_qty} | Rem:{remaining}<br>
                                    <small>Status: {status} | {timestamp} | {followup_text}</small>
                                </div>
                            </div>
                            """, unsafe_allow_html=True)
            else:
                st.info("📭 No records match your filters")
        else:
            st.info("📭 No history found")
    else:
        st.info("📭 No orders yet")

# ===================== SIDEBAR =====================
with st.sidebar:
    st.header("⚙️ Settings")
    
    # REFRESH BUTTON IN SIDEBAR
    if st.button("🔄 Refresh All Data", use_container_width=True, key="refresh_sidebar"):
        if "inventory" in st.session_state:
            del st.session_state["inventory"]
        st.rerun()
    
    st.divider()
    st.subheader("📋 Create Standard Inventory")
    
    st.info("""
    📌 **Expected Format:**
    - Row 5: Headers (ignored)
    - Column B: Product Name
    - Column C: UOM (Unit of Measure)
    - Column D: Opening Stock
    
    ✅ The system will automatically create:
    - Days 1-31 columns
    - Total Received, Consumption
    - Closing Stock, Physical Count, Variance
    """)
    
    inv_file = st.file_uploader("📁 Upload Excel/CSV", type=["csv", "xlsx"], key="upload_inv")
    
    if inv_file:
        try:
            if inv_file.name.endswith('.xlsx'):
                raw_df = pd.read_excel(inv_file, skiprows=4, header=None)
            else:
                raw_df = pd.read_csv(inv_file, skiprows=4, header=None)

            # Create standard format
            standard_df = create_standard_inventory(raw_df)
            standard_df = recalculate_inventory(standard_df)

            st.success(f"✅ Template validated: {len(standard_df)} items found")
            
            # Preview
            preview_cols = ["Product Name", "Category", "UOM", "Opening Stock", "Closing Stock"]
            preview_cols_filtered = [c for c in preview_cols if c in standard_df.columns]
            
            st.write("📊 Preview (first 5 items):")
            st.dataframe(standard_df[preview_cols_filtered].head(), use_container_width=True)

            if st.button("🚀 Create Inventory", type="primary", use_container_width=True, key="push_inv_rest"):
                try:
                    if save_to_sheet(standard_df, "rest_01_inventory"):
                        st.session_state.inventory = standard_df
                        st.success(f"✅ Inventory created with {len(standard_df)} items!")
                        st.balloons()
                        st.rerun()
                except Exception as e:
                    st.error(f"❌ Error saving inventory: {str(e)}")

        except Exception as e:
            st.error(f"❌ Error processing file: {e}")
    
    st.divider()
    st.subheader("📊 Quick Info")
    st.write("""
    **Inventory Features:**
    - Days 1-31 tracking
    - Auto-calculation of totals
    - Daily stock updates
    
    **Requisition Tracking:**
    - Request & receive tracking
    - Pending items with follow-up
    - Complete requisition history
    """)
    
    st.divider()
    if st.button("🗑️ Clear Cache", use_container_width=True, key="clear_cache_rest"):
        if "inventory" in st.session_state:
            del st.session_state["inventory"]
        st.rerun()
