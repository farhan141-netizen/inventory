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
    except Exception as e:
        st.warning(f"Sheet '{worksheet_name}' not found or empty. Creating with default columns...")
        return pd.DataFrame(columns=default_cols) if default_cols else pd.DataFrame()

def save_to_sheet(df, worksheet_name):
    """Save to Google Sheets with automatic sheet creation"""
    try:
        if df is None or df.empty:
            st.error(f"Cannot save empty dataframe to {worksheet_name}")
            return False
        
        conn = get_connection()
        
        # Try to update existing sheet
        try:
            conn.update(worksheet=worksheet_name, data=df)
            st.cache_data.clear()
            return True
        except Exception as update_error:
            # If sheet doesn't exist, create it
            st.info(f"📝 Creating new sheet: {worksheet_name}")
            try:
                conn.create(worksheet=worksheet_name, data=df)
                st.cache_data.clear()
                st.success(f"✅ Sheet '{worksheet_name}' created successfully!")
                return True
            except Exception as create_error:
                st.error(f"❌ Could not create sheet: {str(create_error)}")
                return False
                
    except Exception as e:
        st.error(f"❌ Error saving to {worksheet_name}: {str(e)}")
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
    
    for idx, row in df.iterrows():
        # Calculate total received from day columns
        total_received = 0.0
        for col in day_cols:
            if col in df.columns:
                try:
                    total_received += float(df.at[idx, col]) if pd.notna(df.at[idx, col]) else 0.0
                except:
                    pass
        
        df.at[idx, "Total Received"] = total_received
        
        # Calculate closing stock
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
    .pending-followup { border-left: 5px solid #ff6b35; background: #3a2f1a; }
    
    .section-title { color: #ff6b35; font-size: 1.2em; font-weight: 700; margin-bottom: 15px; }
    .stButton>button { border-radius: 6px; font-weight: 600; }
    
    .history-box { background: #1a2f3f; border-left: 4px solid #00d9ff; padding: 12px; margin-bottom: 10px; border-radius: 6px; }
    .history-pending { border-left: 4px solid #ffaa00; background: #3a2f1a; }
    .history-received { border-left: 4px solid #00ff00; background: #1a3a1a; }
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

# --- REFRESH BUTTON IN HEADER ---
col_refresh, col_empty = st.columns([1, 5])
with col_refresh:
    if st.button("🔄 Refresh Data", use_container_width=True, key="refresh_all"):
        st.cache_data.clear()
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
        sel_cat = st.selectbox("Filter Category", cats, key="inv_cat")
        
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
            height=500
        )
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("💾 Save Daily Count", type="primary", use_container_width=True, key="save_inv"):
                st.session_state.inventory.update(edited_inv)
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
    col_l, col_r = st.columns([2, 1])
    
    with col_l:
        st.markdown('<div class="section-title">🛒 Add Items to Requisition</div>', unsafe_allow_html=True)
        if not st.session_state.inventory.empty:
            search_item = st.text_input("🔍 Search Product", key="search_req", placeholder="Type product name...").lower()
            
            if search_item:
                items = st.session_state.inventory[st.session_state.inventory["Product Name"].str.lower().str.contains(search_item, na=False)]
            else:
                items = st.session_state.inventory
            
            for item_idx, (_, row) in enumerate(items.head(20).iterrows()):
                c1, c2, c3 = st.columns([3, 1, 1])
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
                
                if c3.button("Add ➕", key=f"btn_add_{item_idx}_{search_item}", use_container_width=True):
                    if qty > 0:
                        st.session_state.cart.append({
                            'name': product_name, 
                            'qty': qty, 
                            'uom': uom
                        })
                        st.toast(f"✅ Added {product_name}")
                        st.rerun()

    with col_r:
        st.markdown('<div class="cart-container">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">🛒 Cart</div>', unsafe_allow_html=True)
        
        if st.session_state.cart:
            cart_total = sum([item['qty'] for item in st.session_state.cart])
            st.metric("Total Items", len(st.session_state.cart))
            st.metric("Total Qty", f"{cart_total}")
            
            for i, item in enumerate(st.session_state.cart):
                st.markdown(f"""
                <div class="cart-item">
                    {item['name']}: {item['qty']} {item['uom']}
                </div>
                """, unsafe_allow_html=True)
                
                if st.button(f"Remove", key=f"rm_{i}", use_container_width=True):
                    st.session_state.cart.pop(i)
                    st.rerun()
            
            st.divider()
            
            if st.button("🗑️ Clear Cart", use_container_width=True, key="clear_cart"):
                st.session_state.cart = []
                st.rerun()
                
            if st.button("🚀 Submit to Warehouse", type="primary", use_container_width=True, key="submit_req"):
                try:
                    # Load existing requisitions
                    all_reqs = load_from_sheet("restaurant_requisitions", ["ReqID", "Restaurant", "Item", "Qty", "Status", "DispatchQty", "Timestamp", "RequestedDate", "FollowupSent"])
                    
                    st.info(f"📤 Sending {len(st.session_state.cart)} items to warehouse...")
                    
                    # Add each item from cart
                    for item in st.session_state.cart:
                        new_req = pd.DataFrame([{
                            "ReqID": str(uuid.uuid4())[:8],
                            "Restaurant": "Restaurant 01",
                            "Item": item['name'],
                            "Qty": float(item['qty']),
                            "Status": "Pending",
                            "DispatchQty": 0.0,
                            "Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "RequestedDate": datetime.datetime.now().strftime("%Y-%m-%d"),
                            "FollowupSent": False
                        }])
                        all_reqs = pd.concat([all_reqs, new_req], ignore_index=True)
                    
                    st.write(f"✅ Total records to save: {len(all_reqs)}")
                    
                    # Ensure correct data types
                    all_reqs["Qty"] = pd.to_numeric(all_reqs["Qty"], errors='coerce')
                    all_reqs["DispatchQty"] = pd.to_numeric(all_reqs["DispatchQty"], errors='coerce')
                    all_reqs = all_reqs.reset_index(drop=True)
                    
                    # Save to sheet
                    if save_to_sheet(all_reqs, "restaurant_requisitions"):
                        st.success("✅ Requisition sent to Warehouse successfully!")
                        st.balloons()
                        st.session_state.cart = []
                        st.rerun()
                    else:
                        st.error("❌ Failed to send requisition. Please check your Google Sheets permissions.")
                
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")
                    st.write("Please ensure your Google Sheet has proper permissions and try again.")
        else:
            st.write("🛒 Cart is empty")
        
        st.markdown('</div>', unsafe_allow_html=True)

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
            st.metric("Total Items Pending", len(my_pending))
            st.divider()
            
            for pending_idx, (idx, row) in enumerate(my_pending.iterrows()):
                item_name = row["Item"]
                req_qty = float(row["Qty"])
                dispatch_qty = float(row["DispatchQty"])
                remaining_qty = float(row["Remaining"])
                req_date = row.get("RequestedDate", "N/A")
                status = row["Status"]
                req_id = row["ReqID"]
                followup_sent = row.get("FollowupSent", False)
                
                # Show status indicator
                if status == "Pending":
                    status_indicator = "🟡"
                    status_text = "Pending"
                elif status == "Dispatched":
                    status_indicator = "🟠"
                    status_text = "Partial Delivery"
                else:
                    status_indicator = "🟢"
                    status_text = "Completed"
                
                followup_indicator = "⚠️ Follow-up Sent" if followup_sent else "⏳ No Follow-up"
                
                st.markdown(f"""
                <div class="pending-box pending-pending">
                    <b>{status_indicator} {item_name}</b><br>
                    Requested: {req_qty} | Received: {dispatch_qty} | Remaining: {remaining_qty} | Date: {req_date}<br>
                    <small>Status: {status_text} | {followup_indicator}</small>
                </div>
                """, unsafe_allow_html=True)
                
                c1, c2 = st.columns([1, 1])
                
                with c1:
                    if st.button(f"🚩 Request Follow-up", key=f"followup_{pending_idx}_{req_id}", use_container_width=True):
                        try:
                            all_reqs.at[idx, "FollowupSent"] = True
                            all_reqs.at[idx, "Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            save_to_sheet(all_reqs, "restaurant_requisitions")
                            st.success(f"✅ Follow-up notification sent to Warehouse!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error: {str(e)}")
                
                with c2:
                    if st.button(f"✅ Mark Complete", key=f"complete_{pending_idx}_{req_id}", use_container_width=True):
                        try:
                            # Only if all items are received (remaining = 0)
                            if remaining_qty <= 0:
                                all_reqs.at[idx, "Status"] = "Completed"
                                save_to_sheet(all_reqs, "restaurant_requisitions")
                                st.success(f"✅ {item_name} marked as complete!")
                                st.rerun()
                            else:
                                st.warning(f"⚠️ Still {remaining_qty} units pending. Cannot mark as complete.")
                        except Exception as e:
                            st.error(f"❌ Error: {str(e)}")
                
                st.divider()
        else:
            st.success("✅ All items received! No pending orders.")
    else:
        st.info("📭 No orders found. Submit your first requisition!")

# ===================== RECEIVED ITEMS TAB =====================
with tab_received:
    st.markdown('<div class="section-title">📦 Received Items - Accept Dispatches</div>', unsafe_allow_html=True)
    all_reqs = load_from_sheet("restaurant_requisitions")
    
    if not all_reqs.empty:
        my_dispatched = all_reqs[(all_reqs["Restaurant"] == "Restaurant 01") & (all_reqs["Status"] == "Dispatched")]
        
        if not my_dispatched.empty:
            st.metric("Total Dispatched Items", len(my_dispatched))
            st.divider()
            
            for recv_idx, (original_idx, row) in enumerate(my_dispatched.iterrows()):
                item_name = row["Item"]
                dispatch_qty = float(row["DispatchQty"])
                req_qty = float(row["Qty"])
                req_id = row["ReqID"]
                remaining_qty = req_qty - dispatch_qty
                
                # Color based on whether all items are received
                status_indicator = "🟢" if remaining_qty == 0 else "🟡"
                
                st.markdown(f"""
                <div class="pending-box pending-dispatched">
                    <b>{status_indicator} {item_name}</b> | Requested: {req_qty} | Dispatched: {dispatch_qty} | Remaining: {remaining_qty}
                </div>
                """, unsafe_allow_html=True)
                
                c1, c2 = st.columns([1, 1])
                
                with c1:
                    if st.button(f"✅ Accept & Add to Inventory", key=f"accept_{recv_idx}_{req_id}", use_container_width=True):
                        try:
                            # Keep status as Dispatched if partial, Completed if all received
                            if remaining_qty <= 0:
                                all_reqs.at[original_idx, "Status"] = "Completed"
                            else:
                                all_reqs.at[original_idx, "Status"] = "Dispatched"  # Keep as dispatched for tracking
                            
                            # Add to restaurant inventory - add to today's column
                            today = datetime.datetime.now().day
                            day_col = str(today)
                            
                            inv_idx = st.session_state.inventory[st.session_state.inventory["Product Name"] == item_name].index
                            if len(inv_idx) > 0:
                                idx_val = inv_idx[0]
                                
                                # Add to today's day column
                                current_day_qty = st.session_state.inventory.at[idx_val, day_col]
                                st.session_state.inventory.at[idx_val, day_col] = float(current_day_qty if pd.notna(current_day_qty) else 0) + dispatch_qty
                                
                                # Update Total Received and Closing Stock
                                current_total = st.session_state.inventory.at[idx_val, "Total Received"]
                                st.session_state.inventory.at[idx_val, "Total Received"] = float(current_total) + dispatch_qty
                                
                                # Recalculate closing stock
                                opening = float(st.session_state.inventory.at[idx_val, "Opening Stock"])
                                total_received = float(st.session_state.inventory.at[idx_val, "Total Received"])
                                consumption = float(st.session_state.inventory.at[idx_val, "Consumption"])
                                st.session_state.inventory.at[idx_val, "Closing Stock"] = opening + total_received - consumption
                            
                            # Save both
                            save_to_sheet(all_reqs, "restaurant_requisitions")
                            save_to_sheet(st.session_state.inventory, "rest_01_inventory")
                            
                            st.success(f"✅ {item_name} ({dispatch_qty} units) received and added to inventory on Day {today}!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error: {str(e)}")
                
                with c2:
                    if st.button(f"❌ Reject", key=f"reject_{recv_idx}_{req_id}", use_container_width=True):
                        try:
                            all_reqs.at[original_idx, "Status"] = "Pending"
                            all_reqs.at[original_idx, "DispatchQty"] = 0
                            save_to_sheet(all_reqs, "restaurant_requisitions")
                            st.warning(f"❌ Returned to pending")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error: {str(e)}")
                
                st.divider()
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
            
            # Apply sorting
            if sort_by == "Latest First":
                filtered_history = filtered_history.sort_values("Timestamp", ascending=False)
            elif sort_by == "Oldest First":
                filtered_history = filtered_history.sort_values("Timestamp", ascending=True)
            else:
                filtered_history = filtered_history.sort_values("Item", ascending=True)
            
            if not filtered_history.empty:
                st.metric("Total Records", len(filtered_history))
                st.divider()
                
                # Display history
                for hist_idx, (_, row) in enumerate(filtered_history.iterrows()):
                    item_name = row["Item"]
                    req_qty = float(row["Qty"])
                    dispatch_qty = float(row["DispatchQty"])
                    status = row["Status"]
                    req_date = row.get("RequestedDate", "N/A")
                    timestamp = row.get("Timestamp", "N/A")
                    remaining = req_qty - dispatch_qty
                    followup = row.get("FollowupSent", False)
                    
                    # Color based on status
                    if status == "Pending":
                        status_color = "🟡"
                        box_class = "history-pending"
                    elif status == "Dispatched":
                        status_color = "🟠"
                        box_class = "pending-dispatched"
                    else:  # Completed
                        status_color = "🟢"
                        box_class = "history-received"
                    
                    followup_text = "⚠️ Follow-up Sent" if followup else ""
                    
                    st.markdown(f"""
                    <div class="history-box {box_class}">
                        <b>{status_color} {item_name}</b><br>
                        Requested: {req_qty} | Received: {dispatch_qty} | Remaining: {remaining}<br>
                        <small>Status: {status} | Requested: {req_date} | Updated: {timestamp} | {followup_text}</small>
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
        st.cache_data.clear()
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
        st.cache_data.clear()
        st.rerun()
