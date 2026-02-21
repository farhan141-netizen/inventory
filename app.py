import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
import datetime
import uuid
import io

# --- CLOUD CONNECTION ---
conn = st.connection("gsheets", type=GSheetsConnection)

def clean_dataframe(df):
    """Ensures unique columns and removes ghost columns from Google Sheets"""
    if df is None or df.empty: return df
    df = df.loc[:, ~df.columns.str.contains('^Unnamed', na=False)]
    df = df.dropna(axis=1, how='all')
    df = df.loc[:, ~df.columns.duplicated()]
    df.columns = [str(col).strip() for col in df.columns]
    return df

@st.cache_data(ttl=60)
def load_from_sheet(worksheet_name, default_cols=None):
    """Safely load and clean data from Google Sheets with caching"""
    try:
        df = conn.read(worksheet=worksheet_name, ttl="1m")
        df = clean_dataframe(df)
        if df is None or df.empty:
            return pd.DataFrame(columns=default_cols) if default_cols else pd.DataFrame()
        return df
    except Exception:
        return pd.DataFrame(columns=default_cols) if default_cols else pd.DataFrame()

def save_to_sheet(df, worksheet_name):
    """Save cleaned data to Google Sheets and clear cache"""
    if df is None or df.empty:
        return False
    
    df = clean_dataframe(df)
    try:
        conn.update(worksheet=worksheet_name, data=df)
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Error saving to {worksheet_name}: {str(e)}")
        return False

# --- PAGE CONFIG ---
st.set_page_config(page_title="Warehouse Pro Cloud v8.6", layout="wide", initial_sidebar_state="expanded")

# --- COMPACT SOPHISTICATED CSS ---
st.markdown("""
    <style>
    .block-container { padding-top: 0.8rem; padding-bottom: 0.8rem; }
    
    * { margin: 0; padding: 0; box-sizing: border-box; }
    .main { background: #0f1419; }
    
    .header-bar { 
        background: linear-gradient(90deg, #00d9ff 0%, #0095ff 100%); 
        border-radius: 10px; 
        padding: 10px 20px; 
        color: white; 
        margin-bottom: 8px; 
        display: flex; 
        justify-content: space-between; 
        align-items: center;
        box-shadow: 0 4px 15px rgba(0, 217, 255, 0.2);
    }
    .header-bar h1 { font-size: 1.3em !important; margin: 0; font-weight: 800; }
    .header-bar p { font-size: 0.8em; margin: 0; opacity: 0.9; }

    .stTabs [data-baseweb="tab-list"] { gap: 8px; background: #1a1f2e; padding: 4px; border-radius: 10px; margin-bottom: 8px; border: 1px solid #2d3748; }
    .stTabs [data-baseweb="tab"] { padding: 3px 12px; font-weight: 600; color: #8892b0; border-radius: 6px; font-size: 0.85em; height: 38px; }
    .stTabs [aria-selected="true"] { color: #00d9ff; background: #00d9ff15; border: 1px solid #00d9ff30; }
    
    .log-container {
        max-height: 300px;
        overflow-y: auto;
        padding-right: 5px;
        border-radius: 10px;
        background: rgba(26, 31, 46, 0.4);
    }
    
    .log-row { 
        display: flex; 
        justify-content: space-between; 
        align-items: center; 
        background: #1a1f2e; 
        padding: 4px 8px; 
        border-radius: 6px; 
        margin-bottom: 3px; 
        border-left: 3px solid #00d9ff; 
    }
    .log-row-undone { border-left: 3px solid #ff6b6b; opacity: 0.5; }
    .log-info { font-size: 0.75rem; color: #e0e7ff; line-height: 1.1; }
    .log-time { font-size: 0.65rem; color: #8892b0; margin-left: 4px; }
    
    .section-title { 
        color: #00d9ff; 
        font-size: 1em; 
        font-weight: 700; 
        margin-bottom: 6px; 
        margin-top: 2px;
        padding-bottom: 3px; 
        border-bottom: 1px solid #00d9ff30; 
        display: block; 
    }
    .sidebar-title { color: #00d9ff; font-weight: 700; font-size: 0.95em; margin-bottom: 6px; }
    
    .stButton>button { border-radius: 6px; font-size: 0.8em; padding: 2px 8px; transition: all 0.2s ease; }
    
    .req-box { background: #1a2f3f; border-left: 3px solid #ffaa00; padding: 6px 8px; margin: 3px 0; border-radius: 4px; font-size: 0.85em; line-height: 1.3; }
    .req-compact-button { font-size: 0.75em; padding: 2px 6px; }
    
    hr { margin: 6px 0; opacity: 0.1; }
    </style>
    """, unsafe_allow_html=True)

# --- CORE CALCULATION ENGINE ---
def recalculate_item(df, item_name):
    if item_name not in df["Product Name"].values: return df
    idx = df[df["Product Name"] == item_name].index[0]
    day_cols = [str(i) for i in range(1, 32)]
    for col in day_cols:
        if col not in df.columns: df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
    
    total_received = df.loc[idx, day_cols].sum()
    df.at[idx, "Total Received"] = total_received
    opening = pd.to_numeric(df.at[idx, "Opening Stock"], errors='coerce') or 0.0
    consumption = pd.to_numeric(df.at[idx, "Consumption"], errors='coerce') or 0.0
    closing = opening + total_received - consumption
    df.at[idx, "Closing Stock"] = closing
    
    if "Physical Count" in df.columns:
        physical_val = df.at[idx, "Physical Count"]
        if pd.notna(physical_val) and str(physical_val).strip() != "":
            physical = pd.to_numeric(physical_val, errors='coerce')
            df.at[idx, "Variance"] = physical - closing
        else:
            df.at[idx, "Variance"] = 0.0
    return df

def apply_transaction(item_name, day_num, qty, is_undo=False):
    df = st.session_state.inventory
    if item_name in df["Product Name"].values:
        idx = df[df["Product Name"] == item_name].index[0]
        col_name = str(int(day_num))
        if col_name != "0":
            if col_name not in df.columns: df[col_name] = 0.0
            current_val = pd.to_numeric(df.at[idx, col_name], errors='coerce') or 0.0
            df.at[idx, col_name] = current_val + float(qty)
        
        if not is_undo:
            new_log = pd.DataFrame([{
                "LogID": str(uuid.uuid4())[:8],
                "Timestamp": datetime.datetime.now().strftime("%H:%M:%S"),
                "Item": item_name, "Qty": qty, "Day": day_num, "Status": "Active"
            }])
            logs_df = load_from_sheet("activity_logs", ["LogID", "Timestamp", "Item", "Qty", "Day", "Status"])
            save_to_sheet(pd.concat([logs_df, new_log], ignore_index=True), "activity_logs")
        
        df = recalculate_item(df, item_name)
        st.session_state.inventory = df
        save_to_sheet(df, "persistent_inventory")
        return True
    return False

def undo_entry(log_id):
    logs = load_from_sheet("activity_logs")
    if log_id in logs["LogID"].values:
        idx = logs[logs["LogID"] == log_id].index[0]
        if logs.at[idx, "Status"] == "Undone": return
        item, qty, day = logs.at[idx, "Item"], logs.at[idx, "Qty"], logs.at[idx, "Day"]
        if apply_transaction(item, day, -qty, is_undo=True):
            logs.at[idx, "Status"] = "Undone"
            save_to_sheet(logs, "activity_logs")
            st.rerun()

# --- MODALS ---
@st.dialog("🗂️ Manage Categories")
def manage_categories_modal():
    st.subheader("🗂️ Category Manager")
    
    meta_df = load_from_sheet("product_metadata")
    existing_categories = []
    if not meta_df.empty and "Category" in meta_df.columns:
        all_cats = meta_df["Category"].dropna().unique().tolist()
        existing_categories = sorted([cat for cat in all_cats if not str(cat).startswith("CATEGORY_") and cat != "Supplier_Master" and cat != "General"])
    
    tab1, tab2, tab3 = st.tabs(["➕ Add", "✏️ Modify", "🗑️ Delete"])
    
    with tab1:
        st.subheader("Add New Category")
        category_name = st.text_input("📌 Category Name", placeholder="e.g., Vegetables, Grains, Dairy", key="cat_add_name")
        description = st.text_area("📝 Description", placeholder="Brief description of this category", height=60, key="cat_add_desc")
        
        if st.button("✅ Add Category", use_container_width=True, type="primary", key="add_cat_confirm"):
            if not category_name or not category_name.strip():
                st.error("❌ Please fill in Category Name")
                return
            
            category_name = category_name.strip()
            
            if category_name in existing_categories:
                st.error(f"❌ Category '{category_name}' already exists!")
                return
            
            new_category = pd.DataFrame([{
                "Product Name": f"CATEGORY_{category_name}",
                "UOM": "",
                "Supplier": "",
                "Contact": "",
                "Email": "",
                "Category": category_name,
                "Lead Time": "",
                "Price": 0,
                "Currency": "",
            }])
            
            meta_df = pd.concat([meta_df, new_category], ignore_index=True)
            
            if save_to_sheet(meta_df, "product_metadata"):
                st.success(f"✅ Category '{category_name}' added successfully!")
                st.balloons()
                st.rerun()
            else:
                st.error("❌ Failed to save category")
    
    with tab2:
        st.subheader("Modify Category")
        if existing_categories:
            selected_cat = st.selectbox("Select Category to Modify", existing_categories, key="cat_modify_select")
            
            cat_records = meta_df[meta_df["Category"] == selected_cat]
            current_desc = ""
            if not cat_records.empty:
                current_desc = cat_records.iloc[0].get("Product Name", "").replace(f"CATEGORY_{selected_cat}", "").strip()
            
            new_name = st.text_input("📌 New Category Name", value=selected_cat, key="cat_new_name")
            new_desc = st.text_area("📝 New Description", value=current_desc, height=60, key="cat_new_desc")
            
            if st.button("✅ Update Category", use_container_width=True, type="primary", key="modify_cat_confirm"):
                if not new_name or not new_name.strip():
                    st.error("❌ Please fill in Category Name")
                    return
                
                new_name = new_name.strip()
                
                if new_name != selected_cat and new_name in existing_categories:
                    st.error(f"❌ Category '{new_name}' already exists!")
                    return
                
                meta_df.loc[meta_df["Category"] == selected_cat, "Category"] = new_name
                
                for idx in meta_df[meta_df["Category"] == new_name].index:
                    if str(meta_df.at[idx, "Product Name"]).startswith("CATEGORY_"):
                        meta_df.at[idx, "Product Name"] = f"CATEGORY_{new_name}"
                
                if save_to_sheet(meta_df, "product_metadata"):
                    st.success(f"✅ Category '{selected_cat}' renamed to '{new_name}'!")
                    st.balloons()
                    st.rerun()
                else:
                    st.error("❌ Failed to update category")
        else:
            st.info("📭 No categories to modify")
    
    with tab3:
        st.subheader("Delete Category")
        if existing_categories:
            selected_cat = st.selectbox("Select Category to Delete", existing_categories, key="cat_delete_select")
            
            cat_usage = meta_df[meta_df["Category"] == selected_cat]
            product_count = len(cat_usage[~cat_usage["Product Name"].str.startswith("CATEGORY_", na=False)])
            
            if product_count > 0:
                st.warning(f"⚠️ This category is used by {product_count} product(s). Products will be reassigned to 'General'.")
            
            if st.button("🗑️ Delete Category", use_container_width=True, type="secondary", key="delete_cat_confirm"):
                meta_df.loc[(meta_df["Category"] == selected_cat) & (~meta_df["Product Name"].str.startswith("CATEGORY_", na=False)), "Category"] = "General"
                meta_df = meta_df[~meta_df["Product Name"].str.startswith(f"CATEGORY_{selected_cat}", na=False)]
                
                if save_to_sheet(meta_df, "product_metadata"):
                    st.success(f"✅ Category '{selected_cat}' deleted successfully!")
                    st.balloons()
                    st.rerun()
                else:
                    st.error("❌ Failed to delete category")
        else:
            st.info("📭 No categories to delete")

@st.dialog("➕ Add New Product")
def add_item_modal():
    st.subheader("📦 Product Details")
    col1, col2 = st.columns(2)
    with col1:
        name = st.text_input("📦 Item Name", placeholder="e.g., Tomato, Rice", key="item_name_input")
        uom = st.selectbox("📏 Unit of Measure", ["pcs", "kg", "box", "ltr", "pkt", "can", "bot", "bag", "carton"], key="uom_select")
    with col2:
        opening = st.number_input("📊 Opening Stock", min_value=0.0, value=0.0, key="opening_input")
        
        meta_df = load_from_sheet("product_metadata")
        category_list = ["General"]
        if not meta_df.empty and "Category" in meta_df.columns:
            all_cats = meta_df["Category"].dropna().unique().tolist()
            user_cats = [cat for cat in all_cats if not str(cat).startswith("CATEGORY_") and cat != "Supplier_Master"]
            if user_cats:
                category_list = sorted(set(user_cats))
            if "General" not in category_list:
                category_list.insert(0, "General")
        
        category = st.selectbox("🗂️ Category", category_list, key="cat_select")

    col3, col4 = st.columns(2)
    with col3:
        price = st.number_input("💵 Unit Price", min_value=0.0, value=0.0, step=0.01, key="price_input")
    with col4:
        currency = st.text_input("💱 Currency", value="USD", placeholder="e.g., USD, INR", key="currency_input")

    st.divider()
    st.subheader("🏭 Supplier Details")
    
    meta_df = load_from_sheet("product_metadata")
    existing_suppliers = []
    
    if not meta_df.empty and "Supplier" in meta_df.columns:
        all_suppliers = meta_df["Supplier"].dropna().unique().tolist()
        existing_suppliers = sorted([s for s in all_suppliers if s and str(s).strip()])
    
    supplier_choice = st.radio("Supplier Option:", ["Select Existing Supplier", "Create New Supplier"], horizontal=True, key="supp_choice")
    
    supplier = None
    contact = ""
    email = ""
    lead_time = ""
    
    if supplier_choice == "Select Existing Supplier":
        if existing_suppliers:
            supplier = st.selectbox("🏪 Choose Supplier", existing_suppliers, key="supp_select")
            
            if supplier:
                supplier_rows = meta_df[meta_df["Supplier"] == supplier]
                if not supplier_rows.empty:
                    current_data = supplier_rows.iloc[0]
                    contact = current_data.get("Contact", "")
                    email = current_data.get("Email", "")
                    lead_time = current_data.get("Lead Time", "")
                    
                    st.info(f"✅ **Contact:** {contact}\n\n📧 **Email:** {email}\n\n⏱️ **Lead Time:** {lead_time}")
        else:
            st.warning("⚠️ No suppliers found. Please create a new one.")
            supplier = None
    else:
        supplier = st.text_input("🏪 New Supplier Name", placeholder="e.g., ABC Trading", key="new_supp_input")
        contact = st.text_input("📞 Contact / Phone", placeholder="e.g., +1-234-567-8900", key="contact_input")
        email = st.text_input("📧 Email", placeholder="e.g., supplier@abc.com", key="email_input")
        lead_time = st.text_input("🕐 Lead Time (days)", placeholder="e.g., 2-3", key="lead_time_input")

    if st.button("✅ Create Product", use_container_width=True, type="primary", key="create_prod_btn"):
        if not name or not name.strip():
            st.error("❌ Please fill in Product Name")
            return
        if not supplier or not supplier.strip():
            st.error("❌ Please fill in Supplier Name")
            return
        
        name = name.strip()
        supplier = supplier.strip()
        
        new_row = {str(i): 0.0 for i in range(1, 32)}
        new_row.update({
            "Product Name": name, 
            "UOM": uom, 
            "Opening Stock": opening, 
            "Total Received": 0.0, 
            "Consumption": 0.0, 
            "Closing Stock": opening, 
            "Physical Count": None, 
            "Variance": 0.0,
            "Category": category
        })
        st.session_state.inventory = pd.concat([st.session_state.inventory, pd.DataFrame([new_row])], ignore_index=True)
        save_to_sheet(st.session_state.inventory, "persistent_inventory")
        
        supplier_meta = pd.DataFrame([{
            "Product Name": name,
            "UOM": uom,
            "Supplier": supplier,
            "Contact": contact,
            "Email": email,
            "Category": category,
            "Lead Time": lead_time,
            "Price": price,
            "Currency": currency
        }])
        meta_df = load_from_sheet("product_metadata")
        meta_df = pd.concat([meta_df, supplier_meta], ignore_index=True)
        save_to_sheet(meta_df, "product_metadata")
        
        st.success(f"✅ Product '{name}' created with supplier '{supplier}' at {currency} {price}!")
        st.balloons()
        st.rerun()

@st.dialog("➕ Add New Supplier")
def add_supplier_modal():
    st.subheader("🏭 Add New Supplier")
    
    supplier_name = st.text_input("🏪 Supplier Name", placeholder="e.g., ABC Trading", key="add_supp_name")
    contact = st.text_input("📞 Contact / Phone", placeholder="e.g., +1-234-567-8900", key="add_supp_contact")
    email = st.text_input("📧 Email", placeholder="e.g., supplier@abc.com", key="add_supp_email")
    
    if st.button("✅ Add Supplier", use_container_width=True, type="primary", key="add_supp_btn"):
        if not supplier_name or not supplier_name.strip():
            st.error("❌ Please fill in Supplier Name")
            return
        
        supplier_name = supplier_name.strip()
        
        meta_df = load_from_sheet("product_metadata")
        
        if not meta_df.empty and "Supplier" in meta_df.columns:
            existing = meta_df[meta_df["Supplier"] == supplier_name]
            if not existing.empty:
                st.error(f"❌ Supplier '{supplier_name}' already exists!")
                return
        
        supplier_entry = pd.DataFrame([{
            "Product Name": f"SUPPLIER_{supplier_name}",
            "Supplier": supplier_name,
            "Contact": contact,
            "Email": email,
            "Category": "Supplier_Master",
            "UOM": "",
            "Price": 0,
            "Currency": "",
            "Lead Time": ""
        }])
        
        meta_df = pd.concat([meta_df, supplier_entry], ignore_index=True)
        
        if save_to_sheet(meta_df, "product_metadata"):
            st.success(f"✅ Supplier '{supplier_name}' added successfully!")
            st.balloons()
            st.rerun()
        else:
            st.error("❌ Failed to save supplier")

@st.dialog("✏️ Update Supplier Details")
def update_supplier_modal(supplier_name):
    st.subheader(f"Update Supplier: {supplier_name}")
    
    meta_df = load_from_sheet("product_metadata")
    supplier_data = meta_df[meta_df["Supplier"] == supplier_name]
    
    if supplier_data.empty:
        st.error("Supplier not found")
        return
    
    current_data = supplier_data.iloc[0]
    
    contact = st.text_input("📞 Contact / Phone", value=str(current_data.get("Contact", "")), placeholder="e.g., +1-234-567-8900", key="upd_contact")
    email = st.text_input("📧 Email", value=str(current_data.get("Email", "")), placeholder="e.g., supplier@abc.com", key="upd_email")
    lead_time = st.text_input("🕐 Lead Time (days)", value=str(current_data.get("Lead Time", "")), placeholder="e.g., 2-3", key="upd_lead_time")
    
    if st.button("✅ Update Supplier", use_container_width=True, type="primary", key="upd_supp_btn"):
        meta_df.loc[meta_df["Supplier"] == supplier_name, "Contact"] = contact
        meta_df.loc[meta_df["Supplier"] == supplier_name, "Email"] = email
        meta_df.loc[meta_df["Supplier"] == supplier_name, "Lead Time"] = lead_time
        
        if save_to_sheet(meta_df, "product_metadata"):
            st.success(f"✅ Supplier '{supplier_name}' updated successfully!")
            st.balloons()
            st.rerun()
        else:
            st.error("❌ Failed to update supplier")

@st.dialog("📂 Archive Explorer")
def archive_explorer_modal():
    hist_df = load_from_sheet("monthly_history")
    if not hist_df.empty and "Month_Period" in hist_df.columns:
        selected_month = st.selectbox("📅 Select Month Period", options=sorted(hist_df["Month_Period"].unique().tolist(), reverse=True), key="arch_month")
        month_data = hist_df[hist_df["Month_Period"] == selected_month].drop(columns=["Month_Period"])
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine='xlsxwriter') as writer:
            month_data.to_excel(writer, index=False, sheet_name="Archive")
        st.download_button(label=f"📥 Download {selected_month}", data=buf.getvalue(), file_name=f"Inventory_{selected_month}.xlsx", use_container_width=True, type="primary")
    else: st.info("📭 No records found.")

@st.dialog("🔒 Close Month & Rollover")
def close_month_modal():
    st.warning("⚠️ Physical Counts will become new Opening Stocks.")
    month_label = st.text_input("📅 Month Label", value=datetime.datetime.now().strftime("%b %Y"), key="month_label_input")
    if st.button("✅ Confirm Monthly Close", type="primary", use_container_width=True, key="close_month_btn"):
        df = st.session_state.inventory.copy()
        hist_df = load_from_sheet("monthly_history")
        archive_df = df.copy(); archive_df["Month_Period"] = month_label
        save_to_sheet(pd.concat([hist_df, archive_df], ignore_index=True), "monthly_history")
        new_df = df.copy()
        for i in range(1, 32): new_df[str(i)] = 0.0
        for idx, row in new_df.iterrows():
            phys = row.get("Physical Count")
            new_df.at[idx, "Opening Stock"] = pd.to_numeric(phys) if pd.notna(phys) and str(phys).strip() != "" else row["Closing Stock"]
        new_df["Total Received"] = 0.0; new_df["Consumption"] = 0.0; new_df["Closing Stock"] = new_df["Opening Stock"]; new_df["Physical Count"] = None; new_df["Variance"] = 0.0
        save_to_sheet(new_df, "persistent_inventory")
        st.rerun()

# --- INITIALIZATION ---
if 'inventory' not in st.session_state:
    st.session_state.inventory = load_from_sheet("persistent_inventory")

if 'log_page' not in st.session_state:
    st.session_state.log_page = 0

# --- MAIN UI ---
st.markdown("""
    <div class="header-bar">
        <h1>📦 Warehouse Pro Cloud</h1>
        <p>v8.6 | Multi-Restaurant Distribution Hub</p>
    </div>
""", unsafe_allow_html=True)

# --- REFRESH BUTTON IN HEADER ---
col_refresh, col_empty = st.columns([1, 5])
with col_refresh:
    if st.button("🔄 Refresh Data", use_container_width=True, key="refresh_all"):
        st.cache_data.clear()
        st.rerun()

tab_ops, tab_req, tab_sup = st.tabs(["📊 Operations", "🚚 Requisitions", "📞 Suppliers"])

with tab_ops:
    col_receipt_main, col_quick_main = st.columns([3, 1])

    with col_receipt_main:
        st.markdown('<span class="section-title">📥 Daily Receipt Portal</span>', unsafe_allow_html=True)
        if not st.session_state.inventory.empty:
            c1, c2, c3, c4 = st.columns([2, 0.8, 0.8, 1])
            with c1: sel_item = st.selectbox("🔍 Item", options=[""] + sorted(st.session_state.inventory["Product Name"].unique().tolist()), key="receipt_item", label_visibility="collapsed")
            with c2: day_in = st.number_input("Day", 1, 31, datetime.datetime.now().day, key="receipt_day", label_visibility="collapsed")
            with c3: qty_in = st.number_input("Qty", min_value=0.0, key="receipt_qty", label_visibility="collapsed")
            with c4:
                if st.button("✅ Confirm", use_container_width=True, type="primary", key="receipt_confirm"):
                    if sel_item and qty_in > 0:
                        apply_transaction(sel_item, day_in, qty_in)
                        st.rerun()
        else:
            st.info("Initialize inventory first.")

    with col_quick_main:
        st.markdown('<span class="section-title">⚙️ Actions</span>', unsafe_allow_html=True)
        ac1, ac2, ac3, ac4 = st.columns(4)
        with ac1: 
            if st.button("➕ Item", use_container_width=True, help="New Product", key="btn_add_item"): add_item_modal()
        with ac2: 
            if st.button("🗂️ Cat", use_container_width=True, help="Manage Categories", key="btn_add_cat"): manage_categories_modal()
        with ac3: 
            if st.button("📂 Exp", use_container_width=True, help="Explorer", key="btn_exp"): archive_explorer_modal()
        with ac4: 
            if st.button("🔒 Close", use_container_width=True, type="primary", help="Close Month", key="btn_close"): close_month_modal()

    st.markdown('<hr>', unsafe_allow_html=True)
    
    log_col, stat_col = st.columns([1.2, 2.8])
    
    with log_col:
        st.markdown('<span class="section-title">📜 Activity</span>', unsafe_allow_html=True)
        logs = load_from_sheet("activity_logs")
        if not logs.empty:
            full_logs = logs.iloc[::-1]
            items_per_page = 6
            total_pages = (len(full_logs) - 1) // items_per_page + 1
            start_idx = st.session_state.log_page * items_per_page
            end_idx = start_idx + items_per_page
            current_logs = full_logs.iloc[start_idx:end_idx]
            
            st.markdown('<div class="log-container">', unsafe_allow_html=True)
            for _, row in current_logs.iterrows():
                is_undone = row['Status'] == "Undone"
                row_class = "log-row-undone" if is_undone else ""
                
                c_row = st.container()
                c_txt, c_undo = c_row.columns([4, 1])
                with c_txt:
                    h_item, h_qty, h_day, h_time = row['Item'], row['Qty'], row['Day'], row['Timestamp']
                    l_html = f'<div class="log-row {row_class}"><div class="log-info"><b>{h_item}</b><br>{h_qty} | D{h_day} <span class="log-time">{h_time}</span></div></div>'
                    st.markdown(l_html, unsafe_allow_html=True)
                with c_undo:
                    if not is_undone:
                        if st.button("↩", key=f"rev_{row['LogID']}", use_container_width=True):
                            undo_entry(row['LogID'])
            st.markdown('</div>', unsafe_allow_html=True)
            
            p_prev, p_next = st.columns(2)
            with p_prev:
                if st.button("◀", disabled=st.session_state.log_page == 0, use_container_width=True, key="log_prev"):
                    st.session_state.log_page -= 1; st.rerun()
            with p_next:
                if st.button("▶", disabled=st.session_state.log_page >= total_pages - 1, use_container_width=True, key="log_next"):
                    st.session_state.log_page += 1; st.rerun()
        else: st.caption("📭 No logs.")

    with stat_col:
        st.markdown('<span class="section-title">📊 Live Stock Status</span>', unsafe_allow_html=True)
        df_status = st.session_state.inventory.copy()
        disp_cols = ["Product Name", "Category", "UOM", "Opening Stock", "Total Received", "Closing Stock", "Consumption", "Physical Count", "Variance"]
        for col in disp_cols: 
            if col not in df_status.columns: df_status[col] = 0.0
        
        edited_df = st.data_editor(df_status[disp_cols], height=300, use_container_width=True, disabled=["Product Name", "Category", "UOM", "Total Received", "Closing Stock", "Variance"], hide_index=True)
        
        sc1, sc2, sc3 = st.columns(3)
        with sc1:
            if st.button("💾 Update Stock", use_container_width=True, type="primary", key="update_stock"):
                df_status.update(edited_df)
                for item in df_status["Product Name"]: df_status = recalculate_item(df_status, item)
                save_to_sheet(df_status, "persistent_inventory"); st.rerun()
        with sc2:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine='xlsxwriter') as writer:
                df_status[disp_cols].to_excel(writer, index=False, sheet_name='Summary')
            st.download_button("📥 Summary", data=buf.getvalue(), file_name="Summary.xlsx", use_container_width=True, key="dl_summary")
        with sc3:
            day_cols = [str(i) for i in range(1, 32)]
            existing_day_cols = [col for col in day_cols if col in df_status.columns]
            full_cols = ["Product Name", "Category", "UOM", "Opening Stock"] + existing_day_cols + ["Total Received", "Consumption", "Closing Stock", "Physical Count", "Variance"]
            full_cols = [col for col in full_cols if col in df_status.columns]
            
            if full_cols:
                buf_f = io.BytesIO()
                with pd.ExcelWriter(buf_f, engine='xlsxwriter') as writer:
                    df_status[full_cols].to_excel(writer, index=False, sheet_name='Details')
                st.download_button("📂 Details", data=buf_f.getvalue(), file_name="Full_Report.xlsx", use_container_width=True, key="dl_details")
            else:
                st.warning("⚠️ No data columns available for export")

    with st.expander("📈 Weekly Par Analysis", expanded=False):
        df_hist = load_from_sheet("monthly_history")
        if not df_hist.empty and not st.session_state.inventory.empty:
            df_hist["Consumption"] = pd.to_numeric(df_hist["Consumption"], errors='coerce').fillna(0)
            avg_cons = df_hist.groupby("Product Name")["Consumption"].mean().reset_index()
            df_par = pd.merge(st.session_state.inventory[["Product Name", "UOM", "Closing Stock"]], avg_cons, on="Product Name", how="left").fillna(0)
            df_par["Weekly Usage"] = (df_par["Consumption"] / 4.33).round(2)
            df_par["Min (50%)"] = (df_par["Weekly Usage"] * 0.5).round(2)
            df_par["Max (150%)"] = (df_par["Weekly Usage"] * 1.5).round(2)
            st.dataframe(df_par, use_container_width=True, hide_index=True, height=250)
        else: st.info("Historical data required.")

with tab_req:
    st.markdown('<span class="section-title">🚚 Restaurant Requisitions</span>', unsafe_allow_html=True)
    
    # REFRESH BUTTON
    if st.button("🔄 Refresh Requisitions", use_container_width=True, key="refresh_reqs"):
        st.cache_data.clear()
        st.rerun()
    
    all_reqs = load_from_sheet("restaurant_requisitions", ["ReqID", "Restaurant", "Item", "Qty", "Status", "DispatchQty", "Timestamp", "RequestedDate", "FollowupSent"])
    
    if not all_reqs.empty:
        # Ensure FollowupSent column exists
        if "FollowupSent" not in all_reqs.columns:
            all_reqs["FollowupSent"] = False
        
        status_filter = st.selectbox("Filter by Status", ["All", "Pending", "Dispatched", "Completed"], key="req_status_filter", label_visibility="collapsed")
        
        if status_filter != "All":
            display_reqs = all_reqs[all_reqs["Status"] == status_filter]
        else:
            display_reqs = all_reqs
        
        if not display_reqs.empty:
            # Convert RequestedDate to datetime safely
            display_reqs = display_reqs.copy()
            display_reqs["RequestedDate"] = pd.to_datetime(display_reqs["RequestedDate"], errors='coerce')
            
            # Drop rows with NaT dates
            display_reqs = display_reqs[display_reqs["RequestedDate"].notna()]
            
            if not display_reqs.empty:
                # Sort by date
                display_reqs = display_reqs.sort_values("RequestedDate", ascending=False)
                
                # Get unique dates
                unique_dates = sorted(display_reqs["RequestedDate"].unique(), reverse=True)
                
                for req_date in unique_dates:
                    # Safe date formatting
                    try:
                        date_str = pd.Timestamp(req_date).strftime("%d/%m/%Y")
                    except:
                        date_str = "Unknown Date"
                    
                    date_reqs = display_reqs[display_reqs["RequestedDate"] == req_date]
                    
                    with st.expander(f"📅 {date_str} ({len(date_reqs)} items)", expanded=False):
                        restaurants = date_reqs["Restaurant"].unique()
                        
                        for restaurant in restaurants:
                            rest_reqs = date_reqs[date_reqs["Restaurant"] == restaurant]
                            st.write(f"🏪 **{restaurant}** - {len(rest_reqs)} items")
                            
                            for idx, row in rest_reqs.iterrows():
                                item_name = row["Item"]
                                req_qty = float(row["Qty"])
                                status = row["Status"]
                                dispatch_qty = float(row.get("DispatchQty", 0))
                                req_id = row["ReqID"]
                                remaining_qty = req_qty - dispatch_qty
                                followup_sent = row.get("FollowupSent", False)
                                
                                stock_info = st.session_state.inventory[st.session_state.inventory["Product Name"] == item_name]
                                available_qty = float(stock_info["Closing Stock"].values[0]) if not stock_info.empty else 0.0
                                
                                status_color = "🟡" if status == "Pending" else "🟠" if status == "Dispatched" else "🔵"
                                followup_text = " ⚠️" if followup_sent else ""
                                
                                st.markdown(f"""
                                <div class="req-box">
                                    <b>{status_color} {item_name}</b> | Req:{req_qty} | Got:{dispatch_qty} | Rem:{remaining_qty} | Avail:{available_qty}{followup_text}
                                </div>
                                """, unsafe_allow_html=True)
                                
                                if status == "Pending":
                                    c1, c2, c3 = st.columns([2, 1, 1])
                                    with c1:
                                        default_dispatch = min(req_qty, available_qty)
                                        dispatch_qty_input = st.number_input(
                                            f"Dispatch", 
                                            min_value=0.0, 
                                            max_value=available_qty, 
                                            value=default_dispatch,
                                            key=f"dispatch_{req_id}",
                                            label_visibility="collapsed"
                                        )
                                    with c2:
                                        if st.button("🚀 Dispatch", key=f"dispatch_btn_{req_id}", use_container_width=True):
                                            if dispatch_qty_input > 0:
                                                all_reqs.at[idx, "DispatchQty"] = dispatch_qty_input
                                                all_reqs.at[idx, "Status"] = "Dispatched"
                                                all_reqs.at[idx, "Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                                
                                                if save_to_sheet(all_reqs, "restaurant_requisitions"):
                                                    st.success(f"✅ Dispatched {dispatch_qty_input}")
                                                    st.cache_data.clear()
                                                    st.rerun()
                                    with c3:
                                        if st.button("❌ Cancel", key=f"cancel_btn_{req_id}", use_container_width=True):
                                            all_reqs = all_reqs.drop(idx)
                                            save_to_sheet(all_reqs, "restaurant_requisitions")
                                            st.warning(f"❌ Cancelled")
                                            st.rerun()
                                
                                elif status == "Dispatched":
                                    c1, c2 = st.columns(2)
                                    with c1:
                                        st.caption(f"✅ Dispatched: {dispatch_qty} | Rem: {remaining_qty}")
                                    with c2:
                                        if st.button(f"🚩 Follow-up", key=f"followup_{idx}", use_container_width=True):
                                            all_reqs.at[idx, "FollowupSent"] = True
                                            all_reqs.at[idx, "Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                            save_to_sheet(all_reqs, "restaurant_requisitions")
                                            st.success(f"✅ Follow-up sent!")
                                            st.rerun()
            else:
                st.info("📭 No valid dates found in requisitions")
        else:
            st.info(f"📭 No {status_filter.lower()} requisitions found")
    else:
        st.info("📭 No requisitions yet")

with tab_sup:
    st.markdown('<span class="section-title">📞 Supplier Directory</span>', unsafe_allow_html=True)
    
    col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 3])
    with col_btn1:
        if st.button("➕ Add Supplier", use_container_width=True, key="btn_add_supp"):
            add_supplier_modal()
    
    with col_btn2:
        meta_df = load_from_sheet("product_metadata")
        if not meta_df.empty and "Supplier" in meta_df.columns:
            all_suppliers = meta_df["Supplier"].dropna().unique().tolist()
            suppliers_list = sorted([s for s in all_suppliers if s and str(s).strip()])
            if suppliers_list:
                selected_supplier = st.selectbox("Select Supplier", suppliers_list, label_visibility="collapsed", key="upd_supp_select")
                if st.button("✏️ Update", use_container_width=True, key="btn_upd_supp"):
                    update_supplier_modal(selected_supplier)
    
    st.divider()
    
    meta = load_from_sheet("product_metadata")
    search = st.text_input("🔍 Filter...", placeholder="Item or Supplier...", key="sup_search")
    
    if not meta.empty:
        filtered = meta[~meta["Product Name"].str.startswith("CATEGORY_", na=False) & ~meta["Product Name"].str.startswith("SUPPLIER_", na=False)]
        if search:
            filtered = filtered[filtered["Product Name"].str.lower().str.contains(search.lower(), na=False) | filtered["Supplier"].str.lower().str.contains(search.lower(), na=False)]
    else:
        filtered = meta
    
    if not filtered.empty:
        display_cols = ["Product Name", "Category", "Supplier", "Contact", "Email", "Price", "Currency", "Lead Time", "UOM"]
        available_cols = [col for col in display_cols if col in filtered.columns]
        filtered_display = filtered[available_cols]
    else:
        filtered_display = filtered
    
    edited_meta = st.data_editor(filtered_display, num_rows="dynamic", use_container_width=True, hide_index=True, height=400, key="sup_editor")
    if st.button("💾 Save Directory", use_container_width=True, type="primary", key="save_sup_dir"):
        save_to_sheet(edited_meta, "product_metadata"); st.rerun()

with st.sidebar:
    st.markdown('<h2 class="sidebar-title">☁️ Data Management</h2>', unsafe_allow_html=True)
    
    # REFRESH BUTTON IN SIDEBAR
    if st.button("🔄 Refresh All Data", use_container_width=True, key="refresh_sidebar"):
        st.cache_data.clear()
        st.rerun()
    
    st.divider()
    
    with st.expander("📦 Inventory Master Sync"):
        inv_file = st.file_uploader("Upload XLSX/CSV", type=["csv", "xlsx"], key="inv_upload")
        if inv_file:
            try:
                raw = pd.read_excel(inv_file, skiprows=4, header=None) if inv_file.name.endswith('.xlsx') else pd.read_csv(inv_file, skiprows=4, header=None)
                new_inv = pd.DataFrame()
                new_inv["Product Name"] = raw[1]; new_inv["UOM"] = raw[2]; new_inv["Opening Stock"] = pd.to_numeric(raw[3], errors='coerce').fillna(0.0)
                for i in range(1, 32): new_inv[str(i)] = 0.0
                new_inv["Total Received"] = 0.0; new_inv["Consumption"] = 0.0; new_inv["Closing Stock"] = new_inv["Opening Stock"]
                new_inv["Category"] = "General"
                if st.button("🚀 Push Inventory", type="primary", use_container_width=True, key="push_inv"):
                    save_to_sheet(new_inv.dropna(subset=["Product Name"]), "persistent_inventory"); st.rerun()
            except Exception as e: st.error(f"Error: {e}")

    with st.expander("📞 Supplier Metadata Sync"):
        meta_file = st.file_uploader("Upload Product Data", type=["csv", "xlsx"], key="meta_upload")
        if meta_file:
            try:
                new_meta = pd.read_excel(meta_file) if meta_file.name.endswith('.xlsx') else pd.read_csv(meta_file)
                if st.button("🚀 Push Metadata", type="primary", use_container_width=True, key="push_meta"):
                    save_to_sheet(new_meta, "product_metadata"); st.rerun()
            except Exception as e: st.error(f"Error: {e}")

    st.markdown('<hr>', unsafe_allow_html=True)
    if st.button("🗑️ Clear Cache", use_container_width=True, key="clear_cache"): 
        st.cache_data.clear(); st.rerun()
