import streamlit as st
import pandas as pd
import datetime
import uuid
import io
import math
import numpy as np
import plotly.express as px
from st_supabase_connection import SupabaseConnection
from org_helpers import (
    get_user_memberships,
    validate_invite_code,
    redeem_invite_code,
    is_location_active,
)

conn = st.connection("supabase", type=SupabaseConnection)


def _rest_safe_rerun():
    for candidate in ("rerun", "experimental_rerun"):
        fn = getattr(st, candidate, None)
        if callable(fn):
            try:
                fn()
                return
            except Exception:
                pass
    try:
        if callable(getattr(st, "experimental_set_query_params", None)):
            st.experimental_set_query_params(_r=uuid.uuid4().hex)
            return
    except Exception:
        pass
    fn = getattr(st, "stop", None)
    if callable(fn):
        try:
            fn()
            return
        except Exception:
            pass


def _rest_get_current_user_id():
    uid = st.session_state.get("user_id")
    if uid:
        return uid
    try:
        auth = getattr(conn, "auth", None)
        if auth is not None:
            get_user = getattr(auth, "get_user", None)
            if callable(get_user):
                try:
                    resp = get_user()
                    if isinstance(resp, dict) and "user" in resp and resp["user"]:
                        return resp["user"].get("id") or resp["user"].get("sub")
                    if hasattr(resp, "data"):
                        ud = resp.data.get("user") if isinstance(resp.data, dict) else None
                        if ud:
                            return ud.get("id")
                except Exception:
                    pass
            user_fn = getattr(auth, "user", None)
            if callable(user_fn):
                try:
                    u = user_fn()
                    if isinstance(u, dict):
                        return u.get("id") or u.get("sub")
                    if hasattr(u, "get"):
                        return u.get("id")
                except Exception:
                    pass
    except Exception:
        pass
    return None


def _rest_after_login_set_session(user_id: str, user_email: str = ""):
    """Populate session state after login. Fetches memberships and resolves org/location."""
    if "logged_out" in st.session_state:
        del st.session_state["logged_out"]
    memberships = get_user_memberships(user_id)
    st.session_state.user_id = user_id
    st.session_state.user_email = user_email
    st.session_state.memberships = memberships or []

    # Filter to restaurant-role memberships only
    rest_memberships = [m for m in (memberships or []) if m.get("role") == "restaurant"]

    if rest_memberships:
        m = rest_memberships[0]
        st.session_state.org_id = m.get("org_id")
        st.session_state.location_id = m.get("location_id")
        st.session_state.role = m.get("role", "restaurant")
        # Fetch restaurant name from locations table
        try:
            loc_resp = conn.table("locations").select("name").eq("id", m.get("location_id")).execute()
            if loc_resp.data:
                st.session_state.restaurant_name = loc_resp.data[0].get("name", "Restaurant")
            else:
                st.session_state.restaurant_name = "Restaurant"
        except Exception:
            st.session_state.restaurant_name = "Restaurant"
    else:
        st.session_state.org_id = None
        st.session_state.location_id = None
        st.session_state.role = None
        st.session_state.restaurant_name = "Restaurant"


def _rest_logout():
    try:
        if hasattr(conn, "auth") and callable(getattr(conn.auth, "sign_out", None)):
            conn.auth.sign_out()
        elif callable(getattr(conn, "sign_out", None)):
            conn.sign_out()
    except Exception:
        pass
    keys_to_clear = [
        "user_id", "user_email", "org_id", "location_id", "memberships", "role",
        "restaurant_name", "read_only", "inventory", "cart",
    ]
    for k in keys_to_clear:
        if k in st.session_state:
            del st.session_state[k]
    try:
        if hasattr(st, "cache_data"):
            st.cache_data.clear()
    except Exception:
        pass
    st.session_state["logged_out"] = True
    _rest_safe_rerun()

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
# Map: table_name → primary key column
_TABLE_PK = {
    "rest_01_inventory":        '"Product Name",user_id',
    "restaurant_requisitions":  '"ReqID",user_id',
}

# Tables that need user_id injected on save
_USER_ID_TABLES = {"rest_01_inventory", "restaurant_requisitions"}

# Tables that have org_id column
_ORG_SCOPED_TABLES = {"restaurant_requisitions"}

# Tables that have location_id column (and what that column is named)
_LOCATION_COL = {
    "restaurant_requisitions": "to_location_id",
}

def load_from_sheet(table_name, default_cols=None):
    """Load from Supabase table with org/location filtering."""
    try:
        org_id = st.session_state.get("org_id")
        location_id = st.session_state.get("location_id")

        q = conn.table(table_name).select("*")

        # Only filter by org_id if this table has that column
        if org_id and table_name in _ORG_SCOPED_TABLES:
            q = q.eq("org_id", org_id)

        # Filter by location using the correct column name for each table
        if location_id and table_name in _LOCATION_COL:
            q = q.eq(_LOCATION_COL[table_name], location_id)

        response = q.execute()
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

# Columns that exist in session_state inventory for display/calc
# but are NOT in the rest_01_inventory DB schema — must be stripped before saving
_REST_INVENTORY_UI_ONLY_COLS = frozenset(["Price", "Amount", "Physical Count"])

def save_to_sheet(df, table_name):
    """Upsert DataFrame rows into a Supabase table with org/location isolation."""
    try:
        if df is None or df.empty:
            st.error(f"Cannot save empty dataframe to {table_name}")
            return False

        df = df.copy()
        df = _clean_for_supabase(df)

        # Strip UI-only / computed columns that don't exist in the DB schema
        if table_name == "rest_01_inventory":
            drop_cols = [c for c in _REST_INVENTORY_UI_ONLY_COLS if c in df.columns]
            if drop_cols:
                df = df.drop(columns=drop_cols)

        # Inject org_id only for tables that have that column
        org_id = st.session_state.get("org_id")
        if org_id and table_name in _ORG_SCOPED_TABLES:
            df["org_id"] = org_id

        # Inject location using the correct column name for each table
        location_id = st.session_state.get("location_id")
        if location_id and table_name in _LOCATION_COL:
            df[_LOCATION_COL[table_name]] = location_id

        # Inject user_id for tables that need it in their PK
        user_id = st.session_state.get("user_id")
        if user_id and table_name in _USER_ID_TABLES:
            df["user_id"] = user_id

        # Replace NaN with None for JSON serialisation
        df = df.where(pd.notnull(df), None)

        records = df.to_dict(orient="records")
        records = [
            {k: (None if isinstance(v, float) and not math.isfinite(v) else v)
             for k, v in rec.items()}
            for rec in records
        ]
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

# --- Chart color palette (modern light theme) ---
_R01_CHART_PALETTE = ["#7C5CFC", "#10B981", "#F59E0B", "#EF4444", "#6366F1", "#06B6D4", "#F97316", "#8B5CF6", "#14B8A6", "#EC4899"]

# --- Dashboard card state helpers ---
def _r01_make_pie(df, label_col, value_col):
    if df is None or df.empty:
        st.info("📭 No data.")
        return
    try:
        fig = px.pie(df, names=label_col, values=value_col, hole=0.45,
                     color_discrete_sequence=_R01_CHART_PALETTE)
        fig.update_traces(textposition="inside", textinfo="percent+label")
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#64748B", size=11),
            margin=dict(l=0, r=0, t=20, b=0), height=260, showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)
    except Exception:
        st.dataframe(df, use_container_width=True, hide_index=True)

def _r01_make_bar(df, label_col, value_col):
    if df is None or df.empty:
        st.info("📭 No data.")
        return
    try:
        fig = px.bar(df, x=label_col, y=value_col,
                     color_discrete_sequence=["rgba(124,92,252,0.75)"])
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#64748B", size=11),
            margin=dict(l=0, r=0, t=20, b=40), height=260,
            xaxis=dict(tickangle=-35, showgrid=False),
            yaxis=dict(showgrid=True, gridcolor="#F1F5F9"),
        )
        st.plotly_chart(fig, use_container_width=True)
    except Exception:
        st.dataframe(df, use_container_width=True, hide_index=True)

def _r01_render_card(df, label_col, value_col, chart_type):
    if chart_type == "Table":
        st.dataframe(df, use_container_width=True, hide_index=True, height=260)
    elif chart_type == "Bar Chart":
        _r01_make_bar(df, label_col, value_col)
    else:
        _r01_make_pie(df, label_col, value_col)


def _r01_card_settings(card_key: str, default_sort="High → Low", default_count=10, default_chart="Pie Chart"):
    """Render a ⋮ popover with card settings (matches app.py pattern). Returns (sort_ascending, item_count, chart_type)."""
    sort_key = f"r01_cs_sort_{card_key}"
    count_key = f"r01_cs_count_{card_key}"
    chart_key = f"r01_cs_chart_{card_key}"

    # Initialize defaults in session state
    if sort_key not in st.session_state:
        st.session_state[sort_key] = default_sort
    if count_key not in st.session_state:
        st.session_state[count_key] = default_count
    if chart_key not in st.session_state:
        st.session_state[chart_key] = default_chart

    with st.popover("⋮", use_container_width=False):
        st.caption("⚙  CARD SETTINGS")
        st.session_state[sort_key] = st.selectbox(
            "Sort order",
            options=["High → Low", "Low → High"],
            index=0 if st.session_state[sort_key] == "High → Low" else 1,
            key=f"_sel_sort_{card_key}",
        )
        st.session_state[count_key] = st.selectbox(
            "Item count",
            options=[5, 10, 15, 20],
            index=[5, 10, 15, 20].index(st.session_state[count_key]) if st.session_state[count_key] in [5, 10, 15, 20] else 1,
            key=f"_sel_count_{card_key}",
        )
        _chart_opts = ["Pie Chart", "Bar Chart", "Table"]
        _chart_idx = _chart_opts.index(st.session_state[chart_key]) if st.session_state[chart_key] in _chart_opts else 0
        st.session_state[chart_key] = st.selectbox(
            "Chart type",
            options=_chart_opts,
            index=_chart_idx,
            key=f"_sel_chart_{card_key}",
        )

    # Read back from widget keys
    sort_val = st.session_state.get(f"_sel_sort_{card_key}", st.session_state[sort_key])
    count_val = st.session_state.get(f"_sel_count_{card_key}", st.session_state[count_key])
    chart_val = st.session_state.get(f"_sel_chart_{card_key}", st.session_state[chart_key])

    st.session_state[sort_key] = sort_val
    st.session_state[count_key] = count_val
    st.session_state[chart_key] = chart_val

    ascending = (sort_val == "Low → High")
    return ascending, count_val, chart_val

@st.dialog("📊 Expanded View", width="large")
def _r01_show_fullscreen_card(title: str, df: pd.DataFrame, label_col: str, value_col: str, chart_type: str):
    st.markdown(f"### {title}")
    if df is None or df.empty:
        st.info("📭 No data available.")
        return
    if chart_type == "Table":
        st.dataframe(df, use_container_width=True, hide_index=True)
        return
    try:
        if chart_type == "Bar Chart":
            fig = px.bar(df, x=label_col, y=value_col,
                         color_discrete_sequence=["rgba(124,92,252,0.75)"])
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#64748B", size=12),
                margin=dict(l=0, r=0, t=20, b=60), height=500,
                xaxis=dict(tickangle=-35, showgrid=False),
                yaxis=dict(showgrid=True, gridcolor="#F1F5F9"),
                showlegend=False,
            )
        else:  # Pie Chart
            fig = px.pie(df, names=label_col, values=value_col, hole=0.38,
                         color_discrete_sequence=_R01_CHART_PALETTE)
            fig.update_traces(textposition="inside", textinfo="percent+label")
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#64748B", size=12),
                margin=dict(l=0, r=0, t=30, b=0), height=500,
                legend=dict(
                    visible=True,
                    orientation="v",
                    yanchor="middle", y=0.5,
                    xanchor="left", x=1.05,
                    font=dict(size=12, color="#64748B"),
                ),
            )
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Chart error: {e}")
        st.dataframe(df, use_container_width=True, hide_index=True)

@st.dialog("📊 Live Stock Status — Full View", width="large")
def _show_live_stock_fullscreen(inv_df: pd.DataFrame):
    """Show full live stock table with Price, Amount, Grand Total in a dialog."""
    if inv_df is None or inv_df.empty:
        st.info("📭 No inventory data.")
        return

    # Filter out meta rows (CATEGORY_ / SUPPLIER_ prefixes)
    df = inv_df[
        ~inv_df["Product Name"].astype(str).str.startswith("CATEGORY_") &
        ~inv_df["Product Name"].astype(str).str.startswith("SUPPLIER_")
    ].copy()

    if df.empty:
        st.info("📭 No valid inventory items.")
        return

    # Only show meaningful columns: Product Name, Category, UOM, Closing Stock, Price, Amount
    df["Closing Stock"] = pd.to_numeric(df.get("Closing Stock", 0), errors="coerce").fillna(0)
    df["Price"]         = pd.to_numeric(df.get("Price", 0),         errors="coerce").fillna(0)
    df["Amount"]        = (df["Closing Stock"] * df["Price"]).round(2)

    live_cols = ["Product Name", "Category", "UOM", "Closing Stock", "Price", "Amount"]
    live_cols = [c for c in live_cols if c in df.columns or c in ["Amount"]]
    display_df = df[live_cols].copy()

    grand_total = df["Amount"].sum()

    # Compact column config for single-page view
    col_cfg = {
        "Product Name":  st.column_config.TextColumn("Product",       width=180),
        "Category":      st.column_config.TextColumn("Category",      width=100),
        "UOM":           st.column_config.TextColumn("UOM",           width=60),
        "Closing Stock": st.column_config.NumberColumn("Closing Stk", width=90,  format="%.1f"),
        "Price":         st.column_config.NumberColumn("Price",       width=80,  format="%.2f"),
        "Amount":        st.column_config.NumberColumn("Amount",      width=90,  format="%.2f"),
    }

    # Dynamic height — show all rows without internal scroll
    row_height = 35
    header_h   = 40
    table_h    = min(650, header_h + len(display_df) * row_height)

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        height=table_h,
        column_config=col_cfg,
    )

    # Grand total row
    st.markdown(
        f"<div style='display:flex;justify-content:flex-end;margin-top:10px;'>"
        f"<div style='background:linear-gradient(135deg,#F97316,#F59E0B);color:white;"
        f"padding:10px 22px;border-radius:10px;font-size:14px;font-weight:700;"
        f"box-shadow:0 4px 14px rgba(249,115,22,0.3);'>"
        f"🧾 Grand Total: &nbsp;<span style='font-family:monospace;font-size:16px;'>"
        f"{grand_total:,.2f}</span></div></div>",
        unsafe_allow_html=True,
    )


def _show_rest_login_page():
    """Login / register page for the restaurant app (orange theme)."""
    st.markdown(
        """
        <div style="max-width:420px;margin:60px auto 0 auto;text-align:center;">
            <div style="background:linear-gradient(135deg,#F97316,#F59E0B);padding:24px;border-radius:16px;
                        color:white;margin-bottom:24px;box-shadow:0 4px 20px rgba(249,115,22,0.3);">
                <h2 style="margin:0;font-size:1.6em;">🍴 Restaurant Pro</h2>
                <p style="margin:6px 0 0;opacity:0.85;font-size:0.9em;">
                    Inventory Management & Warehouse Requisitions
                </p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    _, center_col, _ = st.columns([1, 2, 1])
    with center_col:
        mode = st.radio("", ["Sign In", "Register"], horizontal=True, key="rest_auth_mode", label_visibility="collapsed")
        email = st.text_input("📧 Email", placeholder="you@example.com", key="rest_login_email")
        password = st.text_input("🔑 Password", type="password", placeholder="••••••••", key="rest_login_password")

        if mode == "Sign In":
            if st.button("🔓 Sign In", use_container_width=True, type="primary", key="rest_signin_btn"):
                if not email.strip() or not password.strip():
                    st.error("Please enter both email and password.")
                    return
                try:
                    resp = conn.auth.sign_in_with_password({"email": email.strip(), "password": password.strip()})
                    user = None
                    if hasattr(resp, "user") and resp.user:
                        user = resp.user
                    elif isinstance(resp, dict) and resp.get("user"):
                        user = resp["user"]
                    if user:
                        uid = getattr(user, "id", None) or (user.get("id") if isinstance(user, dict) else None)
                        uemail = getattr(user, "email", None) or (user.get("email", "") if isinstance(user, dict) else "")
                        if uid:
                            if "logged_out" in st.session_state:
                                del st.session_state["logged_out"]
                            _rest_after_login_set_session(uid, uemail or email.strip())
                            st.success("✅ Signed in!")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("Sign in succeeded but user ID not found. Please try again.")
                    else:
                        st.error("❌ Invalid email or password.")
                except Exception as e:
                    st.error(f"❌ Sign in failed: {e}")
        else:
            if st.button("📝 Create Account", use_container_width=True, type="primary", key="rest_signup_btn"):
                if not email.strip() or not password.strip():
                    st.error("Please enter both email and password.")
                    return
                if len(password) < 6:
                    st.error("Password must be at least 6 characters.")
                    return
                try:
                    resp = conn.auth.sign_up({"email": email.strip(), "password": password.strip()})
                    user = None
                    if hasattr(resp, "user") and resp.user:
                        user = resp.user
                    elif isinstance(resp, dict) and resp.get("user"):
                        user = resp["user"]
                    if user:
                        uid = getattr(user, "id", None) or (user.get("id") if isinstance(user, dict) else None)
                        uemail = getattr(user, "email", None) or (user.get("email", "") if isinstance(user, dict) else "")
                        if uid:
                            if "logged_out" in st.session_state:
                                del st.session_state["logged_out"]
                            _rest_after_login_set_session(uid, uemail or email.strip())
                            st.success("✅ Account created! Welcome.")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.warning("Account created. Please check your email for a confirmation link, then sign in.")
                    else:
                        st.warning("Account may have been created. Please check your email and sign in.")
                except Exception as e:
                    st.error(f"❌ Registration failed: {e}")


# --- PAGE CONFIG ---
st.set_page_config(page_title="Restaurant Pro", layout="wide")

# --- MODERN LIGHT THEME (matching Warehouse Pro Cloud) ---
# --- THEME ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@300;400;500&display=swap');

    :root {
        --bg: #F1F5F9;
        --panel: #FFFFFF;
        --panel-2: #E8EDF4;
        --border: #CBD5E1;
        --text: #1E293B;
        --muted: #64748B;
        --muted2: #94A3B8;
        --accent: #F97316;
        --warn: #F59E0B;
        --danger: #EF4444;
        --good: #10B981;
        --shadow: 0 2px 4px rgba(0,0,0,0.08), 0 8px 32px rgba(0,0,0,0.12);
        --shadow-hover: 0 4px 12px rgba(0,0,0,0.12), 0 12px 40px rgba(0,0,0,0.18);
        --radius: 16px;
    }

    /* ===== Base ===== */
    html, body, [class*="css"], [data-testid="stAppViewContainer"] {
        font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif !important;
        color: var(--text) !important;
        background: var(--bg) !important;
    }
    .main { background: var(--bg) !important; }
    .stApp { background: var(--bg) !important; }
    [data-testid="stAppViewContainer"] { background: var(--bg) !important; }
    [data-testid="stSidebar"] { background: #FFFFFF !important; }
    footer { visibility: hidden; }

    /* ===== Header ===== */
    .header {
        background: linear-gradient(135deg, #F97316 0%, #F59E0B 100%);
        padding: 8px 18px;
        border-radius: 12px;
        color: white;
        margin-bottom: 8px;
        box-shadow: 0 3px 12px rgba(249,115,22,0.22);
        display: flex;
        align-items: center;
        justify-content: space-between;
    }
    .header-left h1 { margin: 0; font-size: 1.05em; font-weight: 700; color: white !important; }
    .header-left p  { margin: 1px 0 0 0; font-size: 0.75em; opacity: 0.82; color: white !important; }
    /* legacy selectors kept for safety */
    .header h1 { margin: 0; font-size: 1.05em; font-weight: 700; color: white !important; }
    .header p  { margin: 1px 0 0 0; font-size: 0.75em; opacity: 0.82; color: white !important; }

    /* ===== Tabs ===== */
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
        background: #FFFFFF;
        padding: 6px;
        border-radius: var(--radius);
        border: 1px solid var(--border);
        box-shadow: var(--shadow);
    }
    .stTabs [data-baseweb="tab"] {
        height: 40px;
        background: transparent;
        border-radius: 10px;
        padding: 6px 14px;
        color: var(--muted) !important;
        font-weight: 500;
        font-size: 13px;
        transition: all 160ms ease;
    }
    .stTabs [aria-selected="true"] {
        background: rgba(249,115,22,0.10) !important;
        color: #F97316 !important;
        border: 1px solid rgba(249,115,22,0.20) !important;
        font-weight: 600 !important;
    }

    /* ===== Section title ===== */
    .section-title {
        color: var(--text) !important;
        font-size: 13px;
        font-weight: 600;
        margin-bottom: 10px;
        margin-top: 4px;
        border-bottom: 2px solid rgba(249,115,22,0.18);
        padding-bottom: 8px;
        letter-spacing: 0.02em;
        display: block;
    }

    /* ===== ALL text visible ===== */
    p, span, label, div, li, td, th, h1, h2, h3, h4, h5, h6 {
        color: var(--text) !important;
    }

    /* ===== Buttons ===== */
    .stButton > button {
        border-radius: 10px !important;
        font-weight: 500 !important;
        font-size: 13px !important;
        padding: 7px 14px !important;
        border: 1px solid var(--border) !important;
        background: #FFFFFF !important;
        color: var(--text) !important;
        box-shadow: 0 1px 2px rgba(0,0,0,0.05) !important;
        transition: all 150ms ease !important;
    }
    .stButton > button:hover {
        border-color: rgba(249,115,22,0.40) !important;
        background: rgba(249,115,22,0.06) !important;
        color: #F97316 !important;
    }
    /* Primary button — orange fill */
    .stButton > button[kind="primary"],
    [data-testid="baseButton-primary"] {
        background: linear-gradient(135deg, #F97316, #F59E0B) !important;
        color: #FFFFFF !important;
        border: none !important;
        font-weight: 600 !important;
    }
    .stButton > button[kind="primary"]:hover,
    [data-testid="baseButton-primary"]:hover {
        background: linear-gradient(135deg, #EA6C0E, #E8920A) !important;
        color: #FFFFFF !important;
        box-shadow: 0 4px 12px rgba(249,115,22,0.35) !important;
    }
    /* Secondary buttons */
    [data-testid="baseButton-secondary"],
    button[kind="secondary"] {
        background: #FFFFFF !important;
        color: var(--text) !important;
        border: 1px solid var(--border) !important;
        border-radius: 10px !important;
        box-shadow: 0 1px 2px rgba(0,0,0,0.05) !important;
    }
    [data-testid="baseButton-secondary"]:hover,
    button[kind="secondary"]:hover {
        background: rgba(249,115,22,0.05) !important;
        border-color: rgba(249,115,22,0.30) !important;
        color: #F97316 !important;
    }

    /* ===== Inputs — white bg, dark text, visible border ===== */
    [data-baseweb="input"],
    [data-baseweb="base-input"],
    [data-baseweb="textarea"] {
        background: #FFFFFF !important;
        border: 1px solid var(--border) !important;
        border-radius: 10px !important;
        box-shadow: 0 1px 2px rgba(0,0,0,0.04) !important;
        color: var(--text) !important;
    }
    [data-baseweb="input"] input,
    [data-baseweb="base-input"] input,
    input[type="text"],
    input[type="email"],
    input[type="password"],
    input[type="number"],
    textarea {
        background: #FFFFFF !important;
        color: var(--text) !important;
        caret-color: #F97316 !important;
    }
    [data-baseweb="input"]:focus-within,
    [data-baseweb="base-input"]:focus-within {
        border-color: #F97316 !important;
        box-shadow: 0 0 0 3px rgba(249,115,22,0.12) !important;
    }
    input::placeholder,
    textarea::placeholder {
        color: var(--muted2) !important;
        opacity: 1 !important;
    }
    .stTextInput > div > div,
    .stNumberInput > div > div,
    .stDateInput > div > div {
        border: none !important;
        box-shadow: none !important;
        outline: none !important;
    }
    [data-testid="stDateInput"] > div,
    [data-testid="stDateInput"] > label + div {
        border: none !important;
        box-shadow: none !important;
    }

    /* ===== Selectbox / Dropdown ===== */
    [data-baseweb="select"] > div:first-child {
        background: #FFFFFF !important;
        border: 1px solid var(--border) !important;
        border-radius: 10px !important;
        color: var(--text) !important;
    }
    [data-baseweb="select"]:focus-within > div:first-child {
        border-color: #F97316 !important;
        box-shadow: 0 0 0 3px rgba(249,115,22,0.12) !important;
    }
    div[data-baseweb="select"] * { color: var(--text) !important; }
    div[data-baseweb="select"] input::placeholder { color: var(--muted2) !important; }
    /* Dropdown menu */
    div[data-baseweb="menu"],
    div[data-baseweb="popover"] > div,
    [role="listbox"] {
        background: #FFFFFF !important;
        border: 1px solid var(--border) !important;
        border-radius: 12px !important;
        box-shadow: var(--shadow-hover) !important;
    }
    div[data-baseweb="menu"] li,
    div[data-baseweb="popover"] li,
    [role="listbox"] li,
    [role="option"] {
        background: #FFFFFF !important;
        color: var(--text) !important;
    }
    [role="option"]:hover,
    div[data-baseweb="menu"] li:hover {
        background: rgba(249,115,22,0.08) !important;
        color: #F97316 !important;
    }

    /* ===== Radio buttons ===== */
    [data-testid="stRadio"] label,
    [data-testid="stRadio"] label span,
    [data-testid="stRadio"] div[role="radiogroup"] label {
        color: var(--text) !important;
    }
    [data-testid="stRadio"] [role="radiogroup"] {
        background: #FFFFFF !important;
        border-radius: 10px !important;
        padding: 4px !important;
        border: 1px solid var(--border) !important;
    }

    /* ===== Widget labels ===== */
    [data-testid="stWidgetLabel"] label,
    [data-testid="stWidgetLabel"] p,
    .stSelectbox label,
    .stTextInput label,
    .stNumberInput label,
    .stDateInput label,
    .stMultiSelect label {
        color: var(--text) !important;
        font-weight: 500 !important;
    }


    /* ===== Cards / containers ===== */
    [data-testid="stVerticalBlockBorderWrapper"] {
        background: #FFFFFF !important;
        border: 1.5px solid #FFFFFF !important;
        border-radius: 16px !important;
        box-shadow: 0 4px 6px rgba(0,0,0,0.04), 0 10px 40px rgba(0,0,0,0.10) !important;
        padding: 20px !important;
        margin-bottom: 1px !important;
        transition: box-shadow 200ms ease, border-color 200ms ease, transform 150ms ease;
        position: relative;
        overflow: hidden;
    }
    [data-testid="stVerticalBlockBorderWrapper"]:hover {
        border-color: rgba(249,115,22,0.40) !important;
        box-shadow: 0 8px 16px rgba(0,0,0,0.08), 0 20px 60px rgba(0,0,0,0.14) !important;
        transform: translateY(-2px) !important;
    }
    [data-testid="stVerticalBlockBorderWrapper"] > div {
        padding: 0 !important;
    }

    /* NESTED border wrappers inside a card — make them invisible */
    [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stVerticalBlockBorderWrapper"] {
        background: transparent !important;
        border: none !important;
        border-radius: 0 !important;
        box-shadow: none !important;
        padding: 0 !important;
        margin: 0 !important;
        transform: none !important;
        overflow: visible !important;
    }
    [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stVerticalBlockBorderWrapper"]:hover {
        border: none !important;
        box-shadow: none !important;
        transform: none !important;
    }
    [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stVerticalBlockBorderWrapper"]::before {
        display: none !important;
    }

    /* Popover body — ALWAYS white, override nested reset */
    [data-testid="stPopoverBody"] [data-testid="stVerticalBlockBorderWrapper"] {
        background: #FFFFFF !important;
        border: none !important;
        box-shadow: none !important;
        padding: 0 !important;
    }
    div[data-baseweb="popover"] > div {
        background: #FFFFFF !important;
        border: 1px solid var(--border) !important;
        border-radius: 14px !important;
        box-shadow: 0 12px 40px rgba(0,0,0,0.12), 0 2px 8px rgba(0,0,0,0.06) !important;
        overflow: visible !important;
    }
    div[data-baseweb="popover"] {
        background: transparent !important;
        border: none !important;
        box-shadow: none !important;
    }
    div[data-baseweb="popover"] > div > div {
        background: #FFFFFF !important;
        border-radius: 14px !important;
    }

    /* Orange top accent line — only on top-level cards */
    [data-testid="stVerticalBlockBorderWrapper"]::before {
        content: "";
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 3px;
        background: linear-gradient(90deg, #F97316, #F59E0B);
        border-radius: 16px 16px 0 0;
    }

    /* ===== Popover (⋮) card-settings button — compact & inline ===== */
    [data-testid="stPopover"] {
        display: inline-flex !important;
        width: auto !important;
        vertical-align: middle !important;
    }
    [data-testid="stPopover"] > div > button,
    [data-testid="stPopover"] button[data-testid="stBaseButton-secondary"] {
        padding: 2px 6px !important;
        min-height: 28px !important;
        height: 28px !important;
        width: 28px !important;
        background: transparent !important;
        border: 1px solid var(--border) !important;
        border-radius: 6px !important;
        color: var(--muted) !important;
        font-size: 16px !important;
        line-height: 1 !important;
        box-shadow: none !important;
        transition: all 150ms ease !important;
    }
    [data-testid="stPopover"] > div > button:hover,
    [data-testid="stPopover"] button[data-testid="stBaseButton-secondary"]:hover {
        background: rgba(249,115,22,0.06) !important;
        border-color: #F97316 !important;
        color: #F97316 !important;
    }

    /* Inner content padding inside white card settings panel */
    [data-testid="stPopoverBody"] > [data-testid="stVerticalBlock"],
    [data-testid="stPopoverBody"] > div > [data-testid="stVerticalBlock"] {
        padding: 5px !important;
    }

    /* Popover header caption */
    [data-testid="stPopoverBody"] [data-testid="stCaptionContainer"] p {
        font-size: 10px !important;
        font-weight: 700 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.10em !important;
        color: var(--muted) !important;
        padding-bottom: 10px !important;
        margin-bottom: 6px !important;
        border-bottom: 1px solid var(--border) !important;
    }

    /* Compact selectbox labels inside popover */
    [data-testid="stPopoverBody"] .stSelectbox label p,
    [data-testid="stPopoverBody"] [data-testid="stWidgetLabel"] label p {
        font-size: 11px !important;
        font-weight: 600 !important;
        color: #64748B !important;
        text-transform: uppercase !important;
        letter-spacing: 0.05em !important;
        margin-bottom: 2px !important;
    }

    /* Selectbox controls inside popover */
    [data-testid="stPopoverBody"] div[data-baseweb="select"] > div {
        min-height: 34px !important;
        border-radius: 8px !important;
        font-size: 13px !important;
        border: 1.5px solid var(--border) !important;
        background: var(--panel-2, #F8FAFC) !important;
        transition: border-color 150ms ease, box-shadow 150ms ease !important;
    }
    [data-testid="stPopoverBody"] div[data-baseweb="select"] > div:hover {
        border-color: rgba(249,115,22,0.50) !important;
    }
    [data-testid="stPopoverBody"] div[data-baseweb="select"] > div:focus-within {
        border-color: #F97316 !important;
        box-shadow: 0 0 0 3px rgba(249,115,22,0.10) !important;
    }

    /* Reduce vertical gaps inside popover */
    [data-testid="stPopoverBody"] [data-testid="stVerticalBlock"] > div {
        margin-bottom: 0 !important;
        padding-top: 0 !important;
        padding-bottom: 0 !important;
    }
    [data-testid="stPopoverBody"] .stSelectbox {
        margin-bottom: 4px !important;
    }

    /* ===== DataFrames ===== */
    [data-testid="stDataFrame"] {
        border-radius: var(--radius) !important;
        border: 1px solid var(--border) !important;
        overflow: hidden !important;
        box-shadow: var(--shadow) !important;
    }
    [data-testid="stDataFrame"] th,
    [data-testid="stDataFrameResizable"] th {
        background: #F1F5F9 !important;
        color: var(--text) !important;
        border-bottom: 1px solid var(--border) !important;
    }

    /* ===== Alerts ===== */
    [data-testid="stAlert"] {
        border-radius: 10px !important;
        color: var(--text) !important;
    }
    .stSuccess { background: #ECFDF5 !important; color: #065F46 !important; border-left: 4px solid #10B981 !important; border-radius: 10px !important; }
    .stError   { background: #FEF2F2 !important; color: #991B1B !important; border-left: 4px solid #EF4444 !important; border-radius: 10px !important; }
    .stWarning { background: #FFFBEB !important; color: #92400E !important; border-left: 4px solid #F59E0B !important; border-radius: 10px !important; }
    .stInfo    { background: #EFF6FF !important; color: #1E40AF !important; border-left: 4px solid #6366F1 !important; border-radius: 10px !important; }

    /* ===== Cart ===== */
    .cart-compact {
        background: #FFFFFF;
        padding: 14px;
        border-radius: var(--radius);
        border: 2px solid rgba(249,115,22,0.25);
        box-shadow: var(--shadow);
    }
    .cart-item-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        background: #FFF7ED;
        padding: 8px 10px;
        margin-bottom: 6px;
        border-left: 3px solid #F97316;
        border-radius: 8px;
        font-size: 13px;
        color: var(--text) !important;
    }
    .cart-item-name { flex: 1; }

    /* ===== Status colors ===== */
    .status-pending    { border-left: 4px solid var(--warn) !important;  background: #FFFBEB !important; color: #92400E !important; }
    .status-dispatched { border-left: 4px solid #06B6D4 !important;      background: #ECFEFF !important; color: #164E63 !important; }
    .status-completed  { border-left: 4px solid var(--good) !important;  background: #ECFDF5 !important; color: #065F46 !important; }

    .req-item {
        padding: 8px 10px;
        margin: 4px 0;
        border-radius: 10px;
        font-size: 13px;
        line-height: 1.45;
        display: flex;
        justify-content: space-between;
        align-items: center;
        border: 1px solid var(--border);
        background: #FFFFFF;
        color: var(--text) !important;
    }
    .req-item-content { flex: 1; }

    /* ===== Scrollbars ===== */
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: #F1F5F9; border-radius: 99px; }
    ::-webkit-scrollbar-thumb { background: #CBD5E1; border-radius: 99px; }
    ::-webkit-scrollbar-thumb:hover { background: #94A3B8; }

    /* ===== Dashboard KPI boxes ===== */
    .r01-dash-kpi-row { display: flex; flex-direction: row; gap: 14px; flex-wrap: nowrap; margin-bottom: 16px; }
    .r01-dash-kpi-box {
        flex: 1;
        background: #FFFFFF;
        border: 1px solid var(--border);
        border-radius: var(--radius);
        padding: 18px 14px 16px;
        min-width: 0;
        text-align: center;
        box-shadow: var(--shadow);
        transition: box-shadow 200ms ease;
    }
    .r01-dash-kpi-box:hover { box-shadow: var(--shadow-hover); }
    .r01-dash-kpi-box .kpi-icon  { font-size: 20px; margin-bottom: 6px; }
    .r01-dash-kpi-box .kpi-label { font-size: 11px; color: var(--muted) !important; font-weight: 600; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 0.06em; }
    .r01-dash-kpi-box .kpi-value { font-size: 20px; font-weight: 700; color: var(--text) !important; font-family: "JetBrains Mono", ui-monospace, monospace; }
    .r01-dash-kpi-box .kpi-value.bad  { color: var(--danger) !important; }
    .r01-dash-kpi-box .kpi-value.good { color: var(--good)   !important; }

    .r01-card-title {
        font-size: 13px; font-weight: 600; color: var(--text) !important;
        margin: 0 0 10px 0; display: flex; align-items: center; justify-content: space-between;
    }
    .r01-card-title .meta {
        font-size: 11px; color: var(--muted) !important; font-weight: 400;
        background: var(--panel-2); padding: 2px 8px; border-radius: 999px;
    }
    
    hr { margin: 8px 0; opacity: 0.15; border-color: var(--border); }

    /* ===== Top header — light theme, all buttons visible ===== */
    header[data-testid="stHeader"] {
        background: var(--bg) !important;
        border-bottom: 1px solid var(--border) !important;
        box-shadow: none !important;
    }
    [data-testid="stDecoration"],
    #stDecoration {
        display: none !important;
    }
    /* Style toolbar buttons to match light theme */
    [data-testid="stToolbar"] {
        background: var(--bg) !important;
    }
    [data-testid="stToolbar"] button,
    [data-testid="stToolbar"] a,
    header[data-testid="stHeader"] button,
    header[data-testid="stHeader"] a {
        color: var(--muted) !important;
        background: transparent !important;
        border: none !important;
        box-shadow: none !important;
    }
    [data-testid="stToolbar"] button:hover,
    header[data-testid="stHeader"] button:hover {
        color: #F97316 !important;
        background: rgba(249,115,22,0.08) !important;
        border-radius: 6px !important;
    }
    /* Sidebar toggle — always visible */
    [data-testid="stSidebarCollapsedControl"] {
        display: flex !important;
        visibility: visible !important;
        opacity: 1 !important;
        z-index: 999999 !important;
    }
    [data-testid="stSidebarCollapsedControl"] button svg,
    [data-testid="stSidebarCollapsedControl"] svg {
        color: var(--text) !important;
        fill: var(--text) !important;
    }

    /* ===== Expander headers — white background, dark text ===== */
    [data-testid="stExpander"] {
        background: #FFFFFF !important;
        border: 1px solid var(--border) !important;
        border-radius: 12px !important;
        margin-bottom: 8px !important;
    }
    [data-testid="stExpander"] summary,
    [data-testid="stExpander"] summary p,
    [data-testid="stExpander"] summary span,
    [data-testid="stExpander"] > div:first-child {
        background: #FFFFFF !important;
        color: var(--text) !important;
        border-radius: 12px !important;
    }
    [data-testid="stExpander"] summary:hover {
        background: rgba(249,115,22,0.05) !important;
    }
    [data-testid="stExpanderDetails"] {
        background: #FFFFFF !important;
        border-top: 1px solid var(--border) !important;
    }

    /* ===== Dialog / Modal (Expanded View) — white background ===== */
    [data-testid="stModal"],
    [data-testid="stDialog"],
    div[role="dialog"] {
        background: #FFFFFF !important;
        border-radius: 16px !important;
        border: 1px solid var(--border) !important;
        box-shadow: var(--shadow-hover) !important;
    }
    div[role="dialog"] > div,
    [data-testid="stModal"] > div,
    [data-testid="stDialog"] > div {
        background: #FFFFFF !important;
        color: var(--text) !important;
    }
    div[role="dialog"] h1,
    div[role="dialog"] h2,
    div[role="dialog"] h3,
    div[role="dialog"] p,
    div[role="dialog"] span,
    div[role="dialog"] label {
        color: var(--text) !important;
    }
    /* Dialog overlay backdrop */
    [data-testid="stModalOverlay"],
    div[data-baseweb="modal"] > div:first-child {
        background: rgba(15, 23, 42, 0.5) !important;
    }
    /* Dialog close button */
    div[role="dialog"] button[aria-label="Close"] {
        color: var(--muted) !important;
        background: transparent !important;
    }


    </style>
    """, unsafe_allow_html=True)

# --- AUTH GATE ---
_rest_current_uid = _rest_get_current_user_id()

if st.session_state.get("logged_out"):
    _show_rest_login_page()
    st.stop()
elif _rest_current_uid:
    if st.session_state.get("user_id") != _rest_current_uid:
        _rest_email = ""
        try:
            _auth = getattr(conn, "auth", None)
            if _auth:
                _gu = getattr(_auth, "get_user", None)
                if callable(_gu):
                    _ur = _gu()
                    if hasattr(_ur, "user") and _ur.user:
                        _rest_email = getattr(_ur.user, "email", "") or ""
                    elif hasattr(_ur, "data") and isinstance(_ur.data, dict):
                        _rest_email = (_ur.data.get("user") or {}).get("email", "")
        except Exception:
            pass
        _rest_after_login_set_session(_rest_current_uid, _rest_email)
else:
    _show_rest_login_page()
    st.stop()

# --- INVITE CODE ONBOARDING ---
_rest_uid = st.session_state.get("user_id")
_rest_memberships = st.session_state.get("memberships", [])
_rest_memberships_restaurant = [m for m in _rest_memberships if m.get("role") == "restaurant"]

if _rest_uid and not _rest_memberships_restaurant:
    st.markdown(
        """
        <div style="max-width:480px;margin:40px auto 0 auto;text-align:center;">
            <div style="background:linear-gradient(135deg,#F97316,#F59E0B);padding:20px;border-radius:16px;
                        color:white;margin-bottom:20px;box-shadow:0 4px 20px rgba(249,115,22,0.25);">
                <h2 style="margin:0;font-size:1.4em;">🍴 Join Your Restaurant</h2>
                <p style="margin:6px 0 0;opacity:0.85;font-size:0.88em;">
                    Enter the invite code provided by your warehouse manager.
                </p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    _, cc, _ = st.columns([1, 2, 1])
    with cc:
        with st.form("invite_code_form"):
            invite_input = st.text_input("🔑 Invite Code", placeholder="e.g. 483920", max_chars=10)
            submitted = st.form_submit_button("✅ Join Restaurant", type="primary", use_container_width=True)
        if submitted:
            if not invite_input.strip():
                st.error("Please enter an invite code.")
            else:
                _valid = validate_invite_code(invite_input.strip())
                if not _valid:
                    st.error("❌ Invalid, expired, or inactive invite code. Please check with your manager.")
                else:
                    _mem = redeem_invite_code(
                        invite_input.strip(),
                        _rest_uid,
                        st.session_state.get("user_email", ""),
                    )
                    if _mem:
                        st.success(f"✅ Joined **{_valid.get('_location_name', 'Restaurant')}** successfully!")
                        _rest_after_login_set_session(_rest_uid, st.session_state.get("user_email", ""))
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("❌ Failed to redeem invite code. Please try again or contact your manager.")
        st.divider()
        if st.button("🚪 Logout", use_container_width=True, key="rest_logout_invite"):
            _rest_logout()
    st.stop()

# --- INITIALIZATION ---
# --- INITIALIZATION ---
def _build_inventory_from_catalogue() -> pd.DataFrame:
    """
    Build restaurant inventory from the org-wide product_metadata catalogue.
    Merges with any existing rest_01_inventory rows so saved day/consumption
    data is preserved. New products from the catalogue are added automatically.
    """
    org_id = st.session_state.get("org_id")
    if not org_id:
        return pd.DataFrame()

    # 1. Load org product catalogue
    try:
        cat_resp = conn.table("product_metadata").select(
            '"Product Name", "UOM", "Price", "Category"'
        ).eq("org_id", org_id).execute()
        cat_rows = cat_resp.data or []
    except Exception:
        cat_rows = []

    if not cat_rows:
        return pd.DataFrame()

    # Normalise catalogue column names (Supabase returns lowercase)
    cat_df = pd.DataFrame(cat_rows).rename(columns={
        "product name": "Product Name",
        "uom":          "UOM",
        "price":        "Price",
        "category":     "Category",
    })

    # FIX: Strip meta/system rows (CATEGORY_ / SUPPLIER_ header rows added by
    # the warehouse app). These are not real products and should never appear
    # in the restaurant inventory or daily stock count.
    cat_df = cat_df[
        ~cat_df["Product Name"].astype(str).str.startswith("CATEGORY_") &
        ~cat_df["Product Name"].astype(str).str.startswith("SUPPLIER_")
    ].reset_index(drop=True)

    if cat_df.empty:
        return pd.DataFrame()

    # 2. Load existing restaurant inventory (has day columns + consumption etc.)
    existing = load_from_sheet("rest_01_inventory")

    # 3. Build base inventory from catalogue
    day_cols = [str(i) for i in range(1, 32)]
    base = pd.DataFrame()
    base["Product Name"] = cat_df["Product Name"]
    base["Category"]     = cat_df.get("Category", pd.Series(["General"] * len(cat_df)))
    base["UOM"]          = cat_df.get("UOM",      pd.Series(["pcs"]     * len(cat_df)))
    base["Price"]        = pd.to_numeric(cat_df.get("Price", pd.Series([0.0] * len(cat_df))), errors="coerce").fillna(0.0)
    for d in day_cols:
        base[d] = 0.0
    base["Opening Stock"]  = 0.0
    base["Total Received"] = 0.0
    base["Consumption"]    = 0.0
    base["Closing Stock"]  = 0.0
    base["Physical Count"] = None
    base["Variance"]       = 0.0

    # 4. Merge: overlay existing saved data onto the base
    if not existing.empty and "Product Name" in existing.columns:
        existing = existing.set_index("Product Name")
        for i, row in base.iterrows():
            pname = row["Product Name"]
            if pname in existing.index:
                ex = existing.loc[pname]
                # Restore saved numeric columns
                for col in day_cols + ["Opening Stock", "Total Received", "Consumption",
                                       "Closing Stock", "Physical Count", "Variance"]:
                    if col in existing.columns:
                        base.at[i, col] = ex[col]

    base = base.fillna({d: 0.0 for d in day_cols})
    base = base.reset_index(drop=True)
    return recalculate_inventory(base)


if "inventory" not in st.session_state:
    st.session_state.inventory = _build_inventory_from_catalogue()

if "cart" not in st.session_state:
    st.session_state.cart = []

# --- READ-ONLY MODE CHECK ---
_rest_location_id = st.session_state.get("location_id")
if _rest_location_id:
    if not is_location_active(_rest_location_id):
        st.session_state.read_only = True
    else:
        st.session_state.read_only = False
else:
    st.session_state.read_only = False

_rest_read_only = st.session_state.get("read_only", False)

if _rest_read_only:
    st.warning("⚠️ This restaurant has been deactivated by the warehouse owner. Read-only mode.")

# --- COMPACT HEADER with inline Refresh button ---
_rest_name = st.session_state.get("restaurant_name", "Restaurant")
_hcol_left, _hcol_right = st.columns([5, 1])
with _hcol_left:
    st.markdown(f"""
        <div class="header">
            <div class="header-left">
                <h1>🍴 {_rest_name} | Operations Portal</h1>
                <p>Inventory Management &amp; Warehouse Requisitions</p>
            </div>
        </div>
    """, unsafe_allow_html=True)
with _hcol_right:
    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)
    if st.button("🔄 Refresh", key="refresh_all", use_container_width=True):
        for key in ["inventory"]:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()
# --- TABS ---
tab_inv, tab_req, tab_pending, tab_received, tab_history, tab_dash = st.tabs(["📋 Inventory Count", "🛒 Send Requisition", "🚚 Pending Orders", "📦 Received Items", "📊 History", "📊 Dashboard"])

# ===================== INVENTORY TAB =====================
with tab_inv:

    # ── Daily Receipt Portal (compact) ────────────────────────────────────────
    _today_day = datetime.datetime.now().day
    _today_str = datetime.datetime.now().strftime("%d %b %Y")
    _all_reqs_inv = load_from_sheet("restaurant_requisitions")
    _rest_name_inv = st.session_state.get("restaurant_name", "")

    # Compute today receipts
    _recv_today_qty = 0.0
    _recv_today_amt = 0.0
    if not _all_reqs_inv.empty:
        _r = _all_reqs_inv[
            (_all_reqs_inv.get("Restaurant", pd.Series()) == _rest_name_inv) &
            (_all_reqs_inv["Status"].isin(["Dispatched", "Completed"]))
        ].copy() if "Restaurant" in _all_reqs_inv.columns else pd.DataFrame()
        if not _r.empty and "RequestedDate" in _r.columns:
            _r["_rd"] = pd.to_datetime(_r["RequestedDate"], errors="coerce").dt.day
            _r_today = _r[_r["_rd"] == _today_day]
            _recv_today_qty = pd.to_numeric(_r_today.get("DispatchQty", pd.Series()), errors="coerce").sum()

    # Compute month receipts
    _recv_month_qty = 0.0
    if not _all_reqs_inv.empty and "Restaurant" in _all_reqs_inv.columns:
        _rm = _all_reqs_inv[
            (_all_reqs_inv["Restaurant"] == _rest_name_inv) &
            (_all_reqs_inv["Status"].isin(["Dispatched", "Completed"]))
        ].copy()
        _recv_month_qty = pd.to_numeric(_rm.get("DispatchQty", pd.Series()), errors="coerce").sum()

    _dr_title, _dr_expand, _dr_close = st.columns([5, 1, 1])
    with _dr_title:
        st.markdown(
            f"<div style='font-size:12px;font-weight:700;color:#F97316;letter-spacing:0.04em;"
            f"padding:4px 0 2px;border-bottom:2px solid rgba(249,115,22,0.15);'>"
            f"📦 DAILY RECEIPT PORTAL &nbsp;·&nbsp; "
            f"<span style='color:#64748B;font-weight:500;'>{_today_str}</span>"
            f"&nbsp;&nbsp;|&nbsp;&nbsp;"
            f"Today Received: <b style='color:#1E293B;'>{_recv_today_qty:.0f} units</b>"
            f"&nbsp;&nbsp;|&nbsp;&nbsp;"
            f"Month Total: <b style='color:#1E293B;'>{_recv_month_qty:.0f} units</b>"
            f"</div>",
            unsafe_allow_html=True,
        )
    with _dr_expand:
        if st.button("⛶ Explorer", key="receipt_portal_expand", use_container_width=True):
            @st.dialog("📦 Daily Receipt Explorer", width="large")
            def _show_receipt_explorer():
                if _all_reqs_inv.empty or "Restaurant" not in _all_reqs_inv.columns:
                    st.info("No receipt data."); return
                _re = _all_reqs_inv[
                    (_all_reqs_inv["Restaurant"] == _rest_name_inv) &
                    (_all_reqs_inv["Status"].isin(["Dispatched", "Completed"]))
                ].copy()
                if _re.empty:
                    st.info("No receipts yet."); return
                _re["RequestedDate"] = pd.to_datetime(_re["RequestedDate"], errors="coerce")
                _re = _re.dropna(subset=["RequestedDate"])
                _re["Month"] = _re["RequestedDate"].dt.strftime("%Y-%m")
                months = sorted(_re["Month"].unique(), reverse=True)
                sel_month = st.selectbox("Select Month", months, key="re_month_sel")
                _re_m = _re[_re["Month"] == sel_month]
                _re_m = _re_m[["Item", "DispatchQty", "RequestedDate", "Status"]].copy()
                _re_m["RequestedDate"] = _re_m["RequestedDate"].dt.strftime("%d/%m/%Y")
                _re_m = _re_m.rename(columns={"DispatchQty": "Qty Received", "RequestedDate": "Date"})
                st.dataframe(_re_m.sort_values("Date", ascending=False), use_container_width=True, hide_index=True)
                st.caption(f"Total items: {len(_re_m)} | Total qty: {pd.to_numeric(_re_m['Qty Received'], errors='coerce').sum():.0f}")
            _show_receipt_explorer()
    with _dr_close:
        if st.button("📅 Month Close", key="receipt_portal_close", use_container_width=True):
            @st.dialog("📅 Month Close", width="small")
            def _show_month_close_confirm():
                st.warning("This will reset Opening Stock to current Closing Stock and clear all day columns.")
                if st.button("✅ Confirm Month Close", type="primary", use_container_width=True, key="mc_confirm"):
                    inv = st.session_state.inventory.copy()
                    inv["Opening Stock"] = pd.to_numeric(inv.get("Closing Stock", 0), errors="coerce").fillna(0)
                    for _d in [str(i) for i in range(1, 32)]:
                        if _d in inv.columns:
                            inv[_d] = 0.0
                    inv["Total Received"] = 0.0
                    inv["Consumption"]    = 0.0
                    inv = recalculate_inventory(inv)
                    st.session_state.inventory = inv
                    save_to_sheet(inv, "rest_01_inventory")
                    st.success("✅ Month closed! Opening Stock updated.")
                    st.rerun()
                if st.button("✖ Cancel", use_container_width=True, key="mc_cancel"):
                    st.rerun()
            _show_month_close_confirm()

    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

    # ── Daily Stock Take ──────────────────────────────────────────────────────
    st.markdown('<div class="section-title">📊 Daily Stock Take</div>', unsafe_allow_html=True)

    if not st.session_state.inventory.empty:
        # Ensure standard columns exist
        standard_cols = (
            ["Product Name", "Category", "UOM", "Opening Stock"]
            + [str(i) for i in range(1, 32)]
            + ["Total Received", "Consumption", "Closing Stock", "Physical Count", "Variance"]
        )
        for col in standard_cols:
            if col not in st.session_state.inventory.columns:
                if col == "Category":
                    st.session_state.inventory[col] = "General"
                elif col == "UOM":
                    st.session_state.inventory[col] = "pcs"
                else:
                    st.session_state.inventory[col] = 0.0 if col != "Physical Count" else None

        # ── Filter out meta/system rows (CATEGORY_ / SUPPLIER_ prefixes) ──────
        _inv_clean = st.session_state.inventory[
            ~st.session_state.inventory["Product Name"].astype(str).str.startswith("CATEGORY_") &
            ~st.session_state.inventory["Product Name"].astype(str).str.startswith("SUPPLIER_")
        ].copy()

        # ── Filter row: compact selectbox + Expand button side by side ─────────
        _filt_col, _expand_col = st.columns([4, 1])
        with _filt_col:
            _raw_cats = sorted(_inv_clean["Category"].dropna().unique().tolist())
            _cats = ["All"] + _raw_cats
            sel_cat = st.selectbox(
                "Category",
                _cats,
                key="inv_cat",
                label_visibility="collapsed",
            )
        with _expand_col:
            if st.button("⛶ Expand", key="expand_live_stock", use_container_width=True):
                _filtered_for_expand = (
                    _inv_clean if sel_cat == "All"
                    else _inv_clean[_inv_clean["Category"] == sel_cat]
                )
                _show_live_stock_fullscreen(_filtered_for_expand)

        # Apply category filter
        display_df = _inv_clean.copy()
        if sel_cat != "All":
            display_df = display_df[display_df["Category"] == sel_cat]

        # Display columns for the editable daily count table
        day_cols = [str(i) for i in range(1, 32)]
        display_cols = (
            ["Product Name", "Category", "UOM", "Opening Stock"]
            + day_cols
            + ["Total Received", "Consumption", "Closing Stock", "Physical Count", "Variance"]
        )
        display_cols_filtered = [c for c in display_cols if c in display_df.columns]

        edited_inv = st.data_editor(
            display_df[display_cols_filtered],
            use_container_width=True,
            disabled=["Product Name", "Category", "UOM", "Opening Stock", "Total Received", "Closing Stock", "Variance"],
            hide_index=True,
            key="inv_editor",
            height=300,
        )

        # ── Live Stock Status summary (Price + Amount + Grand Total) ───────────
        with st.expander("💰 Live Stock Status (Price × Closing Stock)", expanded=False):
            _live_df = _inv_clean.copy()
            if sel_cat != "All":
                _live_df = _live_df[_live_df["Category"] == sel_cat]
            _live_df["Closing Stock"] = pd.to_numeric(_live_df.get("Closing Stock", 0), errors="coerce").fillna(0)
            if "Price" not in _live_df.columns:
                _live_df["Price"] = 0.0
            _live_df["Price"]  = pd.to_numeric(_live_df["Price"],  errors="coerce").fillna(0)
            _live_df["Amount"] = (_live_df["Closing Stock"] * _live_df["Price"]).round(2)
            _grand_total = _live_df["Amount"].sum()

            _live_display = _live_df[["Product Name", "Category", "UOM", "Closing Stock", "Price", "Amount"]].copy()
            st.dataframe(
                _live_display,
                use_container_width=True,
                hide_index=True,
                height=min(400, 40 + len(_live_display) * 35),
                column_config={
                    "Product Name":  st.column_config.TextColumn("Product",       width=200),
                    "Category":      st.column_config.TextColumn("Category",      width=100),
                    "UOM":           st.column_config.TextColumn("UOM",           width=60),
                    "Closing Stock": st.column_config.NumberColumn("Closing Stk", width=100, format="%.1f"),
                    "Price":         st.column_config.NumberColumn("Price",       width=80,  format="%.2f"),
                    "Amount":        st.column_config.NumberColumn("Amount",      width=100, format="%.2f"),
                },
            )
            st.markdown(
                f"<div style='display:flex;justify-content:flex-end;margin-top:8px;'>"
                f"<div style='background:linear-gradient(135deg,#F97316,#F59E0B);color:white;"
                f"padding:8px 20px;border-radius:10px;font-size:13px;font-weight:700;"
                f"box-shadow:0 4px 14px rgba(249,115,22,0.25);'>"
                f"🧾 Grand Total: &nbsp;<span style='font-family:monospace;font-size:15px;'>"
                f"{_grand_total:,.2f}</span></div></div>",
                unsafe_allow_html=True,
            )

        # ── Action buttons ─────────────────────────────────────────────────────
        col1, col2, col3 = st.columns(3)

        with col1:
            if _rest_read_only:
                st.warning("🔒 Read-only mode — saving is disabled.")
            elif st.button("💾 Save Daily Count", type="primary", use_container_width=True, key="save_inv"):
                for col in edited_inv.columns:
                    if col not in ["Product Name", "Category", "UOM", "Opening Stock", "Total Received", "Closing Stock", "Variance"]:
                        for edited_idx in edited_inv.index:
                            # Map back to main inventory via Product Name
                            pname = edited_inv.at[edited_idx, "Product Name"] if "Product Name" in edited_inv.columns else None
                            if pname is not None:
                                main_idx = st.session_state.inventory[st.session_state.inventory["Product Name"] == pname].index
                                if len(main_idx) > 0:
                                    st.session_state.inventory.at[main_idx[0], col] = edited_inv.at[edited_idx, col]

                st.session_state.inventory = recalculate_inventory(st.session_state.inventory)
                if save_to_sheet(st.session_state.inventory, "rest_01_inventory"):
                    st.success("✅ Inventory saved!")
                    st.rerun()

        with col2:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine='xlsxwriter') as writer:
                st.session_state.inventory[display_cols_filtered].to_excel(writer, index=False, sheet_name='Inventory')
            st.download_button(
                "📥 Download Inventory",
                data=buf.getvalue(),
                file_name="Inventory_Count.xlsx",
                use_container_width=True,
                key="dl_inv",
            )

        with col3:
            st.info(f"📊 {len(display_df)} item(s) shown")

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
            
            # Filter out CATEGORY_/SUPPLIER_ meta rows from requisition list
            items = items[
                ~items["Product Name"].astype(str).str.startswith("CATEGORY_") &
                ~items["Product Name"].astype(str).str.startswith("SUPPLIER_")
            ]

            for item_idx, (_, row) in enumerate(items.iterrows()):
                product_name  = row["Product Name"]
                uom           = row["UOM"]
                closing_stock = row.get("Closing Stock", 0)

                # Compact single-line layout: Name | − qty + | ➕
                rc1, rc2, rc3, rc4, rc5 = st.columns([4, 0.5, 1.2, 0.5, 0.7])
                rc1.markdown(
                    f"<div style='font-size:13px;padding:6px 0;'>"
                    f"<b>{product_name}</b> "
                    f"<span style='color:#94A3B8;font-size:11px;'>({uom}) Stock: {float(closing_stock):.1f}</span>"
                    f"</div>",
                    unsafe_allow_html=True,
                )
                with rc2:
                    if st.button("−", key=f"req_dec_{item_idx}_{search_item}", use_container_width=True):
                        _k = f"req_qty_val_{item_idx}_{search_item}"
                        st.session_state[_k] = max(0.0, st.session_state.get(_k, 0.0) - 1.0)
                        st.rerun()
                with rc3:
                    _qty_key = f"req_qty_val_{item_idx}_{search_item}"
                    qty = st.number_input(
                        "q", min_value=0.0, step=1.0,
                        value=st.session_state.get(_qty_key, 0.0),
                        key=f"req_qty_{item_idx}_{search_item}",
                        label_visibility="collapsed",
                    )
                    st.session_state[_qty_key] = qty
                with rc4:
                    if st.button("+", key=f"req_inc_{item_idx}_{search_item}", use_container_width=True):
                        _k = f"req_qty_val_{item_idx}_{search_item}"
                        st.session_state[_k] = st.session_state.get(_k, 0.0) + 1.0
                        st.rerun()
                with rc5:
                    if st.button("➕", key=f"btn_add_{item_idx}_{search_item}", use_container_width=True):
                        if qty > 0:
                            st.session_state.cart.append({"name": product_name, "qty": qty, "uom": uom})
                            st.session_state[f"req_qty_val_{item_idx}_{search_item}"] = 0.0
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

            if _rest_read_only:
                col2.warning("🔒 Read-only — cannot submit.")
            elif col2.button("🚀 Submit", type="primary", use_container_width=True, key="submit_req"):
                try:
                    all_reqs = load_from_sheet("restaurant_requisitions", ["ReqID", "Restaurant", "Item", "Qty", "Status", "DispatchQty", "AcceptedQty", "Timestamp", "RequestedDate", "FollowupSent"])
                    
                    st.info(f"📤 Sending {len(st.session_state.cart)} items...")
                    
                    for item in st.session_state.cart:
                        new_req = pd.DataFrame([{
                            "ReqID": str(uuid.uuid4())[:8],
                            "Restaurant": st.session_state.get("restaurant_name", "Unknown Restaurant"),
                            "Item": item['name'],
                            "Qty": float(item['qty']),
                            "Status": "Pending",
                            "DispatchQty": 0.0,
                            "AcceptedQty": 0.0,
                            "Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "RequestedDate": datetime.datetime.now().strftime("%Y-%m-%d"),
                            "FollowupSent": False,
                            "submitted_by": st.session_state.get("user_id"),
                            "submitted_by_email": st.session_state.get("user_email", ""),
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
    st.markdown('<div class="section-title">🚚 Pending Orders</div>', unsafe_allow_html=True)
    all_reqs = load_from_sheet("restaurant_requisitions")

    if not all_reqs.empty:
        if "FollowupSent" not in all_reqs.columns:
            all_reqs["FollowupSent"] = False
        all_reqs["Remaining"] = (
            pd.to_numeric(all_reqs["Qty"], errors="coerce").fillna(0) -
            pd.to_numeric(all_reqs["DispatchQty"], errors="coerce").fillna(0)
        )
        my_pending = all_reqs[
            (all_reqs["Restaurant"] == st.session_state.get("restaurant_name", "")) &
            (all_reqs["Remaining"] > 0)
        ].copy()

        if not my_pending.empty:
            my_pending["RequestedDate"] = pd.to_datetime(my_pending["RequestedDate"], errors="coerce")
            my_pending = my_pending.dropna(subset=["RequestedDate"])
            my_pending = my_pending.sort_values("RequestedDate", ascending=False)

            # Group by MONTH (not day) to avoid 30+ expanders
            my_pending["_month"] = my_pending["RequestedDate"].dt.strftime("%B %Y")
            my_pending["_month_sort"] = my_pending["RequestedDate"].dt.strftime("%Y-%m")
            unique_months = my_pending.sort_values("_month_sort", ascending=False)["_month"].unique()

            _p_col1, _p_col2 = st.columns([3, 1])
            _p_col1.metric("Total Items Pending", len(my_pending))
            with _p_col2:
                _pend_view = st.selectbox("View by", ["Month", "Week", "Day"], key="pend_view_by", label_visibility="collapsed")

            if _pend_view == "Month":
                group_fmt   = "%B %Y"
                sort_fmt    = "%Y-%m"
            elif _pend_view == "Week":
                group_fmt   = "Week %W · %Y"
                sort_fmt    = "%Y-W%W"
            else:
                group_fmt   = "%d/%m/%Y"
                sort_fmt    = "%Y-%m-%d"

            my_pending["_grp"]      = my_pending["RequestedDate"].dt.strftime(group_fmt)
            my_pending["_grp_sort"] = my_pending["RequestedDate"].dt.strftime(sort_fmt)
            unique_grps = my_pending.sort_values("_grp_sort", ascending=False)["_grp"].unique()

            for grp in unique_grps:
                grp_reqs = my_pending[my_pending["_grp"] == grp]
                with st.expander(f"📅 {grp}  ·  {len(grp_reqs)} item(s)", expanded=False):
                    for idx, row in grp_reqs.iterrows():
                        item_name     = row["Item"]
                        req_qty       = float(row["Qty"])
                        dispatch_qty  = float(row["DispatchQty"])
                        remaining_qty = float(row["Remaining"])
                        status        = row["Status"]
                        req_id        = row["ReqID"]
                        followup_sent = row.get("FollowupSent", False)
                        date_str      = pd.Timestamp(row["RequestedDate"]).strftime("%d/%m")

                        if status == "Pending":
                            si, sc, bc = "🟡", "Pending",          "status-pending"
                        elif status == "Dispatched":
                            si, sc, bc = "🟠", "Partial",          "status-dispatched"
                        else:
                            si, sc, bc = "🟢", "Completed",        "status-completed"

                        # Compact single row: status badge | item info | 🚩 | ✅
                        pc1, pc2, pc3, pc4 = st.columns([0.5, 4, 0.6, 0.6])
                        with pc1:
                            st.markdown(f"<div style='font-size:18px;padding:4px 0;text-align:center;'>{si}</div>", unsafe_allow_html=True)
                        with pc2:
                            _fup_badge = " ⚠️" if followup_sent else ""
                            st.markdown(
                                f"<div style='font-size:12px;padding:2px 0;line-height:1.4;'>"
                                f"<b>{item_name}</b>{_fup_badge} "
                                f"<span style='color:#94A3B8;'>{date_str} · Req:{req_qty:.0f} Got:{dispatch_qty:.0f} Rem:{remaining_qty:.0f} · {sc}</span>"
                                f"</div>",
                                unsafe_allow_html=True,
                            )
                        with pc3:
                            if st.button("🚩", key=f"followup_{idx}_{req_id}", use_container_width=True, help="Follow-up"):
                                try:
                                    all_reqs.at[idx, "FollowupSent"] = True
                                    all_reqs.at[idx, "Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                    save_to_sheet(all_reqs, "restaurant_requisitions")
                                    st.rerun()
                                except Exception as e:
                                    st.error(str(e))
                        with pc4:
                            if st.button("✅", key=f"complete_{idx}_{req_id}", use_container_width=True, help="Mark Complete"):
                                try:
                                    if remaining_qty <= 0:
                                        all_reqs.at[idx, "Status"] = "Completed"
                                        save_to_sheet(all_reqs, "restaurant_requisitions")
                                        st.rerun()
                                    else:
                                        st.warning(f"⚠️ {remaining_qty:.0f} units still pending.")
                                except Exception as e:
                                    st.error(str(e))
        else:
            st.success("✅ No pending orders!")
    else:
        st.info("📭 No orders found.")

# ===================== RECEIVED ITEMS TAB =====================
with tab_received:
    st.markdown('<div class="section-title">📦 Received Items - Accept Dispatches</div>', unsafe_allow_html=True)
    all_reqs = load_from_sheet("restaurant_requisitions")

    if not all_reqs.empty:
        # Ensure AcceptedQty column exists and is numeric
        if "AcceptedQty" not in all_reqs.columns:
            all_reqs["AcceptedQty"] = 0.0
        all_reqs["AcceptedQty"]  = pd.to_numeric(all_reqs["AcceptedQty"],  errors="coerce").fillna(0.0)
        all_reqs["DispatchQty"]  = pd.to_numeric(all_reqs["DispatchQty"],  errors="coerce").fillna(0.0)
        all_reqs["Qty"]          = pd.to_numeric(all_reqs["Qty"],          errors="coerce").fillna(0.0)

        # FIX: Show ALL dispatched rows where DispatchQty > AcceptedQty
        # This catches BOTH full dispatches AND partial second-round dispatches
        my_dispatched = all_reqs[
            (all_reqs["Restaurant"] == st.session_state.get("restaurant_name", "")) &
            (all_reqs["DispatchQty"] > 0) &
            (all_reqs["AcceptedQty"] < all_reqs["DispatchQty"])
        ].copy()

        if not my_dispatched.empty:
            my_dispatched["RequestedDate"] = pd.to_datetime(my_dispatched["RequestedDate"], errors="coerce")
            my_dispatched = my_dispatched[my_dispatched["RequestedDate"].notna()]

        if not my_dispatched.empty:
            my_dispatched = my_dispatched.sort_values("RequestedDate", ascending=False)
            unique_dates  = sorted(my_dispatched["RequestedDate"].unique(), reverse=True)

            st.metric("Total Dispatched Items", len(my_dispatched))

            # Session state key to remember which date expanders are open
            if "_recv_open_dates" not in st.session_state:
                st.session_state["_recv_open_dates"] = set()

            for req_date in unique_dates:
                try:
                    date_str = pd.Timestamp(req_date).strftime("%d/%m/%Y")
                except Exception:
                    date_str = "Unknown Date"

                date_reqs    = my_dispatched[my_dispatched["RequestedDate"] == req_date]
                _exp_key     = f"recv_exp_{date_str}"
                # Keep expander open if it was previously opened or has pending items
                _is_expanded = (date_str in st.session_state["_recv_open_dates"])

                with st.expander(f"📅 {date_str} ({len(date_reqs)} items)", expanded=_is_expanded):
                    # Mark this expander as open while we render items inside it
                    st.session_state["_recv_open_dates"].add(date_str)

                    for recv_idx, (original_idx, row) in enumerate(date_reqs.iterrows()):
                        item_name    = row["Item"]
                        dispatch_qty = float(row["DispatchQty"])
                        req_qty      = float(row["Qty"])
                        req_id       = row["ReqID"]
                        accepted_qty = float(row.get("AcceptedQty", 0)) if pd.notna(row.get("AcceptedQty", 0)) else 0.0
                        accept_amount = dispatch_qty - accepted_qty   # qty still to accept this round
                        # remaining = original qty not yet dispatched at all
                        remaining_qty = req_qty - dispatch_qty

                        status_indicator = "🟢" if accept_amount <= 0 else "🟡"

                        col_item, col_accept, col_reject = st.columns([2, 1, 1])

                        with col_item:
                            st.markdown(f"""
                            <div class="req-item status-dispatched">
                                <div class="req-item-content">
                                    <b>{status_indicator} {item_name}</b><br>
                                    Req:{req_qty:.0f} | Dispatched:{dispatch_qty:.0f} | Accepted:{accepted_qty:.0f} | To Accept:{accept_amount:.0f}
                                </div>
                            </div>
                            """, unsafe_allow_html=True)

                        with col_accept:
                            if accept_amount <= 0:
                                st.caption("✅ Accepted")
                            elif _rest_read_only:
                                st.caption("🔒 Read-only")
                            else:
                                if st.button("✅", key=f"accept_{recv_idx}_{req_id}", use_container_width=True, help="Accept & Add to Inventory"):
                                    try:
                                        # Mark accepted qty = full dispatch qty for this row
                                        all_reqs.at[original_idx, "AcceptedQty"] = dispatch_qty

                                        # Only mark Completed if NOTHING more is remaining to dispatch
                                        if remaining_qty <= 0:
                                            all_reqs.at[original_idx, "Status"] = "Completed"

                                        # Add to restaurant inventory → today's day column
                                        today   = datetime.datetime.now().day
                                        day_col = str(today)
                                        item_name_clean = item_name.strip().lower()
                                        inv_match = st.session_state.inventory[
                                            st.session_state.inventory["Product Name"].str.strip().str.lower() == item_name_clean
                                        ]
                                        if not inv_match.empty:
                                            idx_val = inv_match.index[0]
                                            if day_col in st.session_state.inventory.columns:
                                                st.session_state.inventory[day_col] = pd.to_numeric(
                                                    st.session_state.inventory[day_col], errors="coerce"
                                                ).fillna(0.0)
                                            current_day_qty = float(
                                                st.session_state.inventory.at[idx_val, day_col]
                                            ) if pd.notna(st.session_state.inventory.at[idx_val, day_col]) else 0.0
                                            st.session_state.inventory.at[idx_val, day_col] = current_day_qty + accept_amount
                                            st.session_state.inventory = recalculate_inventory(st.session_state.inventory)
                                        else:
                                            st.warning(f"⚠️ '{item_name}' not found in inventory.")

                                        save_to_sheet(all_reqs, "restaurant_requisitions")
                                        save_to_sheet(st.session_state.inventory, "rest_01_inventory")
                                        st.success(f"✅ Accepted {accept_amount:.0f} units of {item_name}!")
                                        # Do NOT rerun — keep the expander open
                                        # Just reload data on next interaction

                                    except Exception as e:
                                        st.error(f"❌ Error: {str(e)}")

                        with col_reject:
                            if _rest_read_only:
                                st.caption("🔒")
                            elif st.button("❌", key=f"reject_{recv_idx}_{req_id}", use_container_width=True, help="Reject"):
                                try:
                                    all_reqs.at[original_idx, "Status"]      = "Pending"
                                    all_reqs.at[original_idx, "DispatchQty"] = 0
                                    all_reqs.at[original_idx, "AcceptedQty"] = 0.0
                                    save_to_sheet(all_reqs, "restaurant_requisitions")
                                    st.warning("❌ Returned to pending")
                                except Exception as e:
                                    st.error(f"❌ Error: {str(e)}")
        else:
            # Clear open-dates state when nothing to show
            st.session_state["_recv_open_dates"] = set()
            st.info("📭 No dispatched items awaiting acceptance.")
    else:
        st.info("📭 No orders found")

# ===================== HISTORY TAB =====================
with tab_history:
    st.markdown('<div class="section-title">📊 Requisition History</div>', unsafe_allow_html=True)
    all_reqs = load_from_sheet("restaurant_requisitions")

    if not all_reqs.empty:
        my_history = all_reqs[all_reqs["Restaurant"] == st.session_state.get("restaurant_name", "")].copy()

        if not my_history.empty:
            # Compact filter row
            hf1, hf2, hf3, hf4 = st.columns([1.8, 1.8, 1.2, 1.2])
            with hf1:
                filter_status = st.multiselect("Status", ["Pending", "Dispatched", "Completed"],
                                               default=["Pending", "Dispatched", "Completed"],
                                               key="hist_status", label_visibility="collapsed")
            with hf2:
                filter_item = st.text_input("Item", placeholder="Search item…", key="hist_item", label_visibility="collapsed").lower()
            with hf3:
                sort_by = st.selectbox("Sort", ["Latest First", "Oldest First", "Item Name"],
                                       key="hist_sort", label_visibility="collapsed")
            with hf4:
                hist_view = st.selectbox("Group", ["Month", "Week", "Day"],
                                         key="hist_view_by", label_visibility="collapsed")

            filtered_history = my_history[my_history["Status"].isin(filter_status)]
            if filter_item:
                filtered_history = filtered_history[filtered_history["Item"].str.lower().str.contains(filter_item, na=False)]
            filtered_history = filtered_history.copy()
            filtered_history["RequestedDate"] = pd.to_datetime(filtered_history["RequestedDate"], errors="coerce")
            filtered_history = filtered_history.dropna(subset=["RequestedDate"])

            if not filtered_history.empty:
                if sort_by == "Latest First":
                    filtered_history = filtered_history.sort_values("RequestedDate", ascending=False)
                elif sort_by == "Oldest First":
                    filtered_history = filtered_history.sort_values("RequestedDate", ascending=True)
                else:
                    filtered_history = filtered_history.sort_values("Item", ascending=True)

                # Group format
                if hist_view == "Month":
                    grp_fmt  = "%B %Y";  sort_fmt = "%Y-%m"
                elif hist_view == "Week":
                    grp_fmt  = "Week %W · %Y"; sort_fmt = "%Y-W%W"
                else:
                    grp_fmt  = "%d/%m/%Y"; sort_fmt = "%Y-%m-%d"

                filtered_history["_grp"]      = filtered_history["RequestedDate"].dt.strftime(grp_fmt)
                filtered_history["_grp_sort"] = filtered_history["RequestedDate"].dt.strftime(sort_fmt)
                unique_grps = filtered_history.sort_values("_grp_sort", ascending=False)["_grp"].unique()

                st.caption(f"{len(filtered_history)} record(s) across {len(unique_grps)} {hist_view.lower()}(s)")

                for grp in unique_grps:
                    grp_hist = filtered_history[filtered_history["_grp"] == grp]
                    with st.expander(f"📅 {grp}  ·  {len(grp_hist)} item(s)", expanded=False):
                        for _, row in grp_hist.iterrows():
                            item_name    = row["Item"]
                            req_qty      = float(row["Qty"])
                            dispatch_qty = float(row["DispatchQty"])
                            status       = row["Status"]
                            remaining    = req_qty - dispatch_qty
                            followup     = row.get("FollowupSent", False)
                            date_str     = pd.Timestamp(row["RequestedDate"]).strftime("%d/%m")

                            if status == "Pending":
                                si = "🟡"
                            elif status == "Dispatched":
                                si = "🟠"
                            else:
                                si = "🟢"

                            _fup = " ⚠️" if followup else ""
                            st.markdown(
                                f"<div style='font-size:12px;padding:3px 0;border-bottom:1px solid #F1F5F9;'>"
                                f"{si} <b>{item_name}</b>{_fup} "
                                f"<span style='color:#94A3B8;'>{date_str} · Req:{req_qty:.0f} Got:{dispatch_qty:.0f} Rem:{remaining:.0f} · {status}</span>"
                                f"</div>",
                                unsafe_allow_html=True,
                            )
            else:
                st.info("📭 No records match your filters")
        else:
            st.info("📭 No history found")
    else:
        st.info("📭 No orders yet")

# ===================== DASHBOARD TAB =====================
with tab_dash:
    st.markdown(f'<div class="section-title">📊 {st.session_state.get("restaurant_name", "Restaurant")} Dashboard</div>', unsafe_allow_html=True)

    # --- Date range controls ---
    today = datetime.date.today()
    _QUICK_OPTIONS = ["Last 7 days", "Last 14 days", "Last 30 days", "Custom"]
    _QUICK_DAYS    = {"Last 7 days": 7, "Last 14 days": 14, "Last 30 days": 30}
    d_col1, d_col2, d_col3, d_col4 = st.columns([1.2, 1.2, 1.2, 1])
    with d_col1:
        quick = st.selectbox("Quick range", _QUICK_OPTIONS, key="r01_dash_quick")
    if quick == "Custom":
        with d_col2:
            start_date = st.date_input("From", today - datetime.timedelta(days=30), key="r01_dash_from")
        with d_col3:
            end_date = st.date_input("To", today, key="r01_dash_to")
    else:
        start_date = today - datetime.timedelta(days=_QUICK_DAYS[quick])
        end_date   = today

    # --- Load data ---
    all_reqs_dash = load_from_sheet("restaurant_requisitions")
    inv_dash = st.session_state.inventory.copy() if not st.session_state.inventory.empty else pd.DataFrame()

    # Filter requisitions by date range
    req_filtered = pd.DataFrame()
    if not all_reqs_dash.empty and "RequestedDate" in all_reqs_dash.columns:
        all_reqs_dash["RequestedDate"] = pd.to_datetime(all_reqs_dash["RequestedDate"], errors="coerce")
        all_reqs_dash = all_reqs_dash[all_reqs_dash["Restaurant"] == st.session_state.get("restaurant_name", "")]
        req_filtered = all_reqs_dash[
            (all_reqs_dash["RequestedDate"] >= pd.Timestamp(start_date)) &
            (all_reqs_dash["RequestedDate"] <= pd.Timestamp(end_date))
        ]

    # ── Prepare price lookup from inventory (Product Name → Price) ─────────────
    _price_map = {}
    if not inv_dash.empty and "Product Name" in inv_dash.columns and "Price" in inv_dash.columns:
        for _, _pr in inv_dash[["Product Name", "Price"]].dropna().iterrows():
            _price_map[str(_pr["Product Name"]).strip()] = float(pd.to_numeric(_pr["Price"], errors="coerce") or 0)

    # ── KPI calculations ─────────────────────────────────────────────────────
    # Total Received (amount) = sum(DispatchQty * Price) for Dispatched/Completed
    _recv_df = pd.DataFrame()
    if not req_filtered.empty and "DispatchQty" in req_filtered.columns:
        _recv_df = req_filtered[req_filtered["Status"].isin(["Dispatched", "Completed"])].copy()
        _recv_df["_price"] = _recv_df["Item"].apply(lambda x: _price_map.get(str(x).strip(), 0))
        _recv_df["_amount"] = pd.to_numeric(_recv_df["DispatchQty"], errors="coerce").fillna(0) * _recv_df["_price"]
    _kpi_total_received_amt = _recv_df["_amount"].sum() if not _recv_df.empty and "_amount" in _recv_df.columns else 0.0

    # Sold (amount) = Consumption × Price (from inventory)
    _kpi_sold_amt = 0.0
    if not inv_dash.empty and "Consumption" in inv_dash.columns:
        _inv_sold = inv_dash[["Product Name", "Consumption"]].copy()
        _inv_sold["_price"] = _inv_sold["Product Name"].apply(lambda x: _price_map.get(str(x).strip(), 0))
        _inv_sold["_amount"] = pd.to_numeric(_inv_sold["Consumption"], errors="coerce").fillna(0) * _inv_sold["_price"]
        _kpi_sold_amt = _inv_sold["_amount"].sum()

    # P&L = Sales (sold) − Purchase (received)
    _kpi_pnl = _kpi_sold_amt - _kpi_total_received_amt
    _pnl_color = "#10B981" if _kpi_pnl >= 0 else "#EF4444"
    _pnl_sign  = "+" if _kpi_pnl >= 0 else ""

    # Stock In Hand (value) = Closing Stock × Price
    _kpi_stock_value = 0.0
    if not inv_dash.empty and "Closing Stock" in inv_dash.columns:
        _inv_sih = inv_dash[["Product Name", "Closing Stock"]].copy()
        _inv_sih["_price"] = _inv_sih["Product Name"].apply(lambda x: _price_map.get(str(x).strip(), 0))
        _inv_sih["_val"]   = pd.to_numeric(_inv_sih["Closing Stock"], errors="coerce").fillna(0) * _inv_sih["_price"]
        _kpi_stock_value   = _inv_sih["_val"].sum()

    # Pending: fully pending + partial (dispatched but remainder still unfulfilled)
    _pending_full_df = pd.DataFrame()
    _pending_part_df = pd.DataFrame()
    if not req_filtered.empty and "Status" in req_filtered.columns:
        _pending_full_df = req_filtered[req_filtered["Status"] == "Pending"].copy()
        _disp = req_filtered[req_filtered["Status"] == "Dispatched"].copy()
        if not _disp.empty:
            _disp["_remaining"] = (
                pd.to_numeric(_disp["Qty"], errors="coerce").fillna(0) -
                pd.to_numeric(_disp["DispatchQty"], errors="coerce").fillna(0)
            )
            _pending_part_df = _disp[_disp["_remaining"] > 0].copy()
    _kpi_pending_count = len(_pending_full_df) + len(_pending_part_df)

    # ── KPI row — 4 static boxes + 1 clickable pending button ────────────────
    kpi_c1, kpi_c2, kpi_c3, kpi_c4, kpi_c5 = st.columns(5)

    with kpi_c1:
        st.markdown(f"""
        <div class="r01-dash-kpi-box">
            <div class="kpi-icon">📥</div>
            <div class="kpi-label">Total Received</div>
            <div class="kpi-value">{_kpi_total_received_amt:,.0f}</div>
        </div>""", unsafe_allow_html=True)

    with kpi_c2:
        st.markdown(f"""
        <div class="r01-dash-kpi-box">
            <div class="kpi-icon">💸</div>
            <div class="kpi-label">Sold (Amount)</div>
            <div class="kpi-value">{_kpi_sold_amt:,.0f}</div>
        </div>""", unsafe_allow_html=True)

    with kpi_c3:
        st.markdown(f"""
        <div class="r01-dash-kpi-box">
            <div class="kpi-icon">📈</div>
            <div class="kpi-label">P&amp;L</div>
            <div class="kpi-value" style="color:{_pnl_color};">{_pnl_sign}{_kpi_pnl:,.0f}</div>
        </div>""", unsafe_allow_html=True)

    with kpi_c4:
        st.markdown(f"""
        <div class="r01-dash-kpi-box">
            <div class="kpi-icon">🏦</div>
            <div class="kpi-label">Stock In Hand</div>
            <div class="kpi-value">{_kpi_stock_value:,.0f}</div>
        </div>""", unsafe_allow_html=True)

    with kpi_c5:
        # Pending Orders as a styled clickable button matching KPI box height
        _pend_color = "#EF4444" if _kpi_pending_count > 0 else "#10B981"
        st.markdown(f"""
        <div class="r01-dash-kpi-box" style="padding:0;">
            <div style="padding:18px 14px 6px;text-align:center;">
                <div class="kpi-icon">⏳</div>
                <div class="kpi-label">Pending Orders</div>
                <div class="kpi-value" style="color:{_pend_color};">{_kpi_pending_count}</div>
            </div>
        </div>""", unsafe_allow_html=True)
        if st.button(
            "📋 View Details",
            key="dash_view_pending",
            use_container_width=True,
            disabled=(_kpi_pending_count == 0),
        ):
            @st.dialog("⏳ Pending & Partially Fulfilled Orders", width="large")
            def _show_pending_popup():
                if _pending_full_df.empty and _pending_part_df.empty:
                    st.info("✅ No pending orders.")
                    return

                if not _pending_full_df.empty:
                    st.markdown("### 🟡 Fully Pending (not dispatched yet)")
                    _p1 = _pending_full_df[["Item", "Qty", "RequestedDate"]].copy()
                    _p1["RequestedDate"] = pd.to_datetime(_p1["RequestedDate"], errors="coerce").dt.strftime("%d/%m/%Y")
                    _p1 = _p1.rename(columns={"RequestedDate": "Order Date", "Qty": "Qty Ordered"})
                    st.dataframe(_p1, use_container_width=True, hide_index=True,
                                 column_config={
                                     "Item":        st.column_config.TextColumn("Item",       width=220),
                                     "Qty Ordered": st.column_config.NumberColumn("Qty",      width=80, format="%.0f"),
                                     "Order Date":  st.column_config.TextColumn("Order Date", width=110),
                                 })

                if not _pending_part_df.empty:
                    st.markdown("### 🟠 Partially Dispatched (balance still pending)")
                    _p2 = _pending_part_df[["Item", "Qty", "DispatchQty", "_remaining", "RequestedDate"]].copy()
                    _p2["RequestedDate"] = pd.to_datetime(_p2["RequestedDate"], errors="coerce").dt.strftime("%d/%m/%Y")
                    _p2 = _p2.rename(columns={
                        "RequestedDate": "Order Date",
                        "Qty":           "Qty Ordered",
                        "DispatchQty":   "Dispatched",
                        "_remaining":    "Balance Pending",
                    })
                    st.dataframe(_p2, use_container_width=True, hide_index=True,
                                 column_config={
                                     "Item":            st.column_config.TextColumn("Item",            width=200),
                                     "Qty Ordered":     st.column_config.NumberColumn("Qty Ordered",   width=90, format="%.0f"),
                                     "Dispatched":      st.column_config.NumberColumn("Dispatched",    width=90, format="%.0f"),
                                     "Balance Pending": st.column_config.NumberColumn("Balance Pending", width=110, format="%.0f"),
                                     "Order Date":      st.column_config.TextColumn("Order Date",      width=110),
                                 })
                st.caption(f"Date range: {start_date} → {end_date}")
            _show_pending_popup()

    st.markdown("---")

    # ── Row 1: Top Purchased QTY | Top Selling QTY ───────────────────────────
    row1_l, row1_r = st.columns(2, gap="small")

    with row1_l:
        with st.container(border=True):
            st.markdown('<div class="r01-card-title">📥 Top Purchased <span class="meta">by qty</span></div>', unsafe_allow_html=True)
            asc1, topn1, chart1 = _r01_card_settings("top_purch_qty")
            top_purch_qty = pd.DataFrame(columns=["Item", "Purchased Qty"])
            if not req_filtered.empty and "Item" in req_filtered.columns and "DispatchQty" in req_filtered.columns:
                _pq = req_filtered[req_filtered["Status"].isin(["Dispatched", "Completed"])].copy()
                if not _pq.empty:
                    top_purch_qty = (
                        _pq.groupby("Item", as_index=False)["DispatchQty"]
                        .sum()
                        .rename(columns={"DispatchQty": "Purchased Qty"})
                        .sort_values("Purchased Qty", ascending=asc1)
                        .head(topn1)
                    )
            _r01_render_card(top_purch_qty, "Item", "Purchased Qty", chart1)
            if st.button("⛶ Expand", key="expand_top_purch_qty", use_container_width=True):
                _r01_show_fullscreen_card("Top Purchased Items (Qty)", top_purch_qty, "Item", "Purchased Qty", chart1)

    with row1_r:
        with st.container(border=True):
            st.markdown('<div class="r01-card-title">💸 Top Selling <span class="meta">by qty (consumption)</span></div>', unsafe_allow_html=True)
            asc2, topn2, chart2 = _r01_card_settings("top_sell_qty")
            top_sell_qty = pd.DataFrame(columns=["Product Name", "Consumption"])
            if not inv_dash.empty and "Consumption" in inv_dash.columns:
                top_sell_qty = (
                    inv_dash[["Product Name", "Consumption"]]
                    .copy()
                    .assign(Consumption=lambda d: pd.to_numeric(d["Consumption"], errors="coerce").fillna(0))
                    .query("Consumption > 0")
                    .sort_values("Consumption", ascending=asc2)
                    .head(topn2)
                )
            _r01_render_card(top_sell_qty, "Product Name", "Consumption", chart2)
            if st.button("⛶ Expand", key="expand_top_sell_qty", use_container_width=True):
                _r01_show_fullscreen_card("Top Selling Items (Qty)", top_sell_qty, "Product Name", "Consumption", chart2)

    # ── Row 2: Top Purchased Amount | Top Selling Amount ─────────────────────
    row2_l, row2_r = st.columns(2, gap="small")

    with row2_l:
        with st.container(border=True):
            st.markdown('<div class="r01-card-title">💰 Top Purchased <span class="meta">by amount</span></div>', unsafe_allow_html=True)
            asc3, topn3, chart3 = _r01_card_settings("top_purch_amt")
            top_purch_amt = pd.DataFrame(columns=["Item", "Purchase Amount"])
            if not req_filtered.empty and "Item" in req_filtered.columns and "DispatchQty" in req_filtered.columns:
                _pa = req_filtered[req_filtered["Status"].isin(["Dispatched", "Completed"])].copy()
                if not _pa.empty:
                    _pa["_price"]  = _pa["Item"].apply(lambda x: _price_map.get(str(x).strip(), 0))
                    _pa["_amount"] = pd.to_numeric(_pa["DispatchQty"], errors="coerce").fillna(0) * _pa["_price"]
                    top_purch_amt = (
                        _pa.groupby("Item", as_index=False)["_amount"]
                        .sum()
                        .rename(columns={"_amount": "Purchase Amount"})
                        .sort_values("Purchase Amount", ascending=asc3)
                        .head(topn3)
                    )
            _r01_render_card(top_purch_amt, "Item", "Purchase Amount", chart3)
            if st.button("⛶ Expand", key="expand_top_purch_amt", use_container_width=True):
                _r01_show_fullscreen_card("Top Purchased Items (Amount)", top_purch_amt, "Item", "Purchase Amount", chart3)

    with row2_r:
        with st.container(border=True):
            st.markdown('<div class="r01-card-title">💵 Top Selling <span class="meta">by amount</span></div>', unsafe_allow_html=True)
            asc4, topn4, chart4 = _r01_card_settings("top_sell_amt")
            top_sell_amt = pd.DataFrame(columns=["Product Name", "Sales Amount"])
            if not inv_dash.empty and "Consumption" in inv_dash.columns:
                _sa = inv_dash[["Product Name", "Consumption"]].copy()
                _sa["_price"]  = _sa["Product Name"].apply(lambda x: _price_map.get(str(x).strip(), 0))
                _sa["_cons"]   = pd.to_numeric(_sa["Consumption"], errors="coerce").fillna(0)
                _sa["Sales Amount"] = _sa["_cons"] * _sa["_price"]
                top_sell_amt = (
                    _sa[_sa["Sales Amount"] > 0][["Product Name", "Sales Amount"]]
                    .sort_values("Sales Amount", ascending=asc4)
                    .head(topn4)
                )
            _r01_render_card(top_sell_amt, "Product Name", "Sales Amount", chart4)
            if st.button("⛶ Expand", key="expand_top_sell_amt", use_container_width=True):
                _r01_show_fullscreen_card("Top Selling Items (Amount)", top_sell_amt, "Product Name", "Sales Amount", chart4)

    # ── Row 3: Total Purchase From Supplier | Low / Zero Stock ───────────────
    row3_l, row3_r = st.columns(2, gap="small")

    with row3_l:
        with st.container(border=True):
            st.markdown('<div class="r01-card-title">🏭 Total Purchase From Supplier <span class="meta">by amount</span></div>', unsafe_allow_html=True)
            asc5, topn5, chart5 = _r01_card_settings("supplier_purchase")
            supplier_purchase = pd.DataFrame(columns=["Item", "Total Amount"])
            if not req_filtered.empty and "Item" in req_filtered.columns and "DispatchQty" in req_filtered.columns:
                _sp = req_filtered[req_filtered["Status"].isin(["Dispatched", "Completed"])].copy()
                if not _sp.empty:
                    _sp["_price"]  = _sp["Item"].apply(lambda x: _price_map.get(str(x).strip(), 0))
                    _sp["_amount"] = pd.to_numeric(_sp["DispatchQty"], errors="coerce").fillna(0) * _sp["_price"]
                    supplier_purchase = (
                        _sp.groupby("Item", as_index=False)["_amount"]
                        .sum()
                        .rename(columns={"_amount": "Total Amount"})
                        .sort_values("Total Amount", ascending=asc5)
                        .head(topn5)
                    )
            _r01_render_card(supplier_purchase, "Item", "Total Amount", chart5)
            if st.button("⛶ Expand", key="expand_supplier_purchase", use_container_width=True):
                _r01_show_fullscreen_card("Total Purchase From Supplier", supplier_purchase, "Item", "Total Amount", chart5)

    with row3_r:
        with st.container(border=True):
            st.markdown('<div class="r01-card-title">⚠️ Low / Zero Stock Items <span class="meta">needs reorder</span></div>', unsafe_allow_html=True)
            _inv_clean_dash = inv_dash[
                ~inv_dash["Product Name"].astype(str).str.startswith("CATEGORY_") &
                ~inv_dash["Product Name"].astype(str).str.startswith("SUPPLIER_")
            ] if not inv_dash.empty else inv_dash
            low_stock = pd.DataFrame(columns=["Product Name", "Closing Stock"])
            if not _inv_clean_dash.empty and "Closing Stock" in _inv_clean_dash.columns:
                low_stock = (
                    _inv_clean_dash[["Product Name", "Closing Stock"]]
                    .copy()
                    .assign(**{"Closing Stock": lambda d: pd.to_numeric(d["Closing Stock"], errors="coerce").fillna(0)})
                    .query("`Closing Stock` <= 5")
                    .sort_values("Closing Stock")
                )
            if low_stock.empty:
                st.success("✅ All items have sufficient stock!")
            else:
                st.warning(f"⚠️ {len(low_stock)} item(s) with low/zero stock")
                st.dataframe(
                    low_stock,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Product Name":  st.column_config.TextColumn("Product",      width=200),
                        "Closing Stock": st.column_config.NumberColumn("Stock Left", width=100, format="%.1f"),
                    },
                )

    # --- Row 4: Daily Requisition Trend | Category Stock Distribution ---
    row4_l, row4_r = st.columns(2, gap="small")

    with row4_l:
        with st.container(border=True):
            st.markdown('<div class="r01-card-title">📅 Daily Requisition Trend <span class="meta">purchase amount over time</span></div>', unsafe_allow_html=True)
            _asc7, _topn7, chart7 = _r01_card_settings("trend", default_chart="Bar Chart")
            trend_df = pd.DataFrame(columns=["Date", "Total Amount"])
            if not req_filtered.empty and "RequestedDate" in req_filtered.columns and "DispatchQty" in req_filtered.columns:
                _trend_src = req_filtered[req_filtered["Status"].isin(["Dispatched", "Completed"])].copy()
                if not _trend_src.empty:
                    _trend_src["_price"]  = _trend_src["Item"].apply(lambda x: _price_map.get(str(x).strip(), 0))
                    _trend_src["_amount"] = pd.to_numeric(_trend_src["DispatchQty"], errors="coerce").fillna(0) * _trend_src["_price"]
                    _trend_src["_date"]   = pd.to_datetime(_trend_src["RequestedDate"], errors="coerce").dt.date
                    _trend_src = _trend_src.dropna(subset=["_date"])
                    trend_df = (
                        _trend_src.groupby("_date", as_index=False)["_amount"]
                        .sum()
                        .rename(columns={"_date": "Date", "_amount": "Total Amount"})
                        .sort_values("Date", ascending=not _asc7)
                        .head(_topn7)
                        .sort_values("Date")
                    )
            if not trend_df.empty:
                if chart7 == "Table":
                    st.dataframe(trend_df, use_container_width=True, hide_index=True, height=260,
                                 column_config={"Total Amount": st.column_config.NumberColumn("Total Amount", format="%.2f")})
                elif chart7 == "Pie Chart":
                    _r01_make_pie(trend_df, "Date", "Total Amount")
                else:
                    try:
                        fig_trend = px.bar(
                            trend_df, x="Date", y="Total Amount",
                            color_discrete_sequence=["rgba(249,115,22,0.75)"],
                        )
                        fig_trend.update_layout(
                            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                            font=dict(color="#64748B", size=11),
                            margin=dict(l=0, r=0, t=20, b=40), height=260,
                            xaxis=dict(tickangle=-35, showgrid=False),
                            yaxis=dict(showgrid=True, gridcolor="#F1F5F9"),
                        )
                        st.plotly_chart(fig_trend, use_container_width=True)
                    except Exception:
                        st.dataframe(trend_df, use_container_width=True, hide_index=True)
            else:
                st.info("📭 No trend data in selected range.")
            if st.button("⛶ Expand", key="expand_r01_trend", use_container_width=True):
                _r01_show_fullscreen_card("Daily Requisition Trend", trend_df, "Date", "Total Amount", chart7)

    with row4_r:
        with st.container(border=True):
            st.markdown('<div class="r01-card-title">🗂️ Stock by Category <span class="meta">closing stock</span></div>', unsafe_allow_html=True)
            asc8, topn8, chart8 = _r01_card_settings("cat_stock")
            cat_stock = pd.DataFrame(columns=["Category", "Total Stock"])
            if not inv_dash.empty and "Category" in inv_dash.columns and "Closing Stock" in inv_dash.columns:
                cat_stock = (
                    inv_dash.assign(**{"Closing Stock": lambda d: pd.to_numeric(d["Closing Stock"], errors="coerce").fillna(0)})
                    .groupby("Category", as_index=False)["Closing Stock"]
                    .sum()
                    .rename(columns={"Closing Stock": "Total Stock"})
                    .sort_values("Total Stock", ascending=asc8)
                    .head(topn8)
                )
            _r01_render_card(cat_stock, "Category", "Total Stock", chart8)
            if st.button("⛶ Expand", key="expand_r01_cat_stock", use_container_width=True):
                _r01_show_fullscreen_card("Stock by Category", cat_stock, "Category", "Total Stock", chart8)

    # --- Export ---
    st.markdown("---")
    if st.button("📤 Export Dashboard to Excel", use_container_width=True, key="r01_dash_export", type="primary"):
        try:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
                if not most_req.empty:      most_req.to_excel(writer, index=False, sheet_name="Most Requested")
                if not most_recv.empty:     most_recv.to_excel(writer, index=False, sheet_name="Most Received")
                if not stock_bal.empty:     stock_bal.to_excel(writer, index=False, sheet_name="Stock Balance")
                if not low_stock.empty:     low_stock.to_excel(writer, index=False, sheet_name="Low Stock")
                if not status_breakdown.empty: status_breakdown.to_excel(writer, index=False, sheet_name="Req Status")
                if not pending_items.empty: pending_items.to_excel(writer, index=False, sheet_name="Pending Items")
                if not trend_df.empty:      trend_df.to_excel(writer, index=False, sheet_name="Daily Trend")
                if not cat_stock.empty:     cat_stock.to_excel(writer, index=False, sheet_name="Category Stock")
            st.download_button(
                "📥 Download Excel",
                data=buf.getvalue(),
                file_name=f"R01_Dashboard_{start_date}_to_{end_date}.xlsx",
                use_container_width=True,
                key="r01_dash_dl",
            )
        except Exception as e:
            st.error(f"Export error: {e}")

# ===================== SIDEBAR =====================
with st.sidebar:
    # User info & logout
    _rest_user_email = st.session_state.get("user_email", "")
    _rest_sid_name = st.session_state.get("restaurant_name", "Restaurant")
    st.markdown(
        f"""
        <div style="background:linear-gradient(135deg,#F97316,#F59E0B);padding:12px 14px;border-radius:12px;
                    color:white;margin-bottom:12px;">
            <div style="font-size:13px;font-weight:600;">🍴 {_rest_sid_name}</div>
            <div style="font-size:11px;opacity:0.85;margin-top:2px;">{_rest_user_email}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if st.button("🚪 Logout", use_container_width=True, key="rest_sidebar_logout"):
        _rest_logout()

    st.header("⚙️ Settings")
    
    # REFRESH BUTTON IN SIDEBAR
    if st.button("🔄 Refresh All Data", use_container_width=True, key="refresh_sidebar"):
        if "inventory" in st.session_state:
            del st.session_state["inventory"]
        st.rerun()
    
    st.divider()
    st.subheader("📦 Product Catalogue")
    st.info(
        "Products are loaded automatically from your organisation's warehouse catalogue. "
        "When the warehouse manager adds or updates a product, it will appear here on the next refresh.",
        icon="ℹ️",
    )

    _sidebar_org_id = st.session_state.get("org_id")
    if _sidebar_org_id:
        try:
            _cat_resp = conn.table("product_metadata").select(
                '"Product Name", "Category", "UOM", "Price"'
            ).eq("org_id", _sidebar_org_id).execute()
            _cat_count = len(_cat_resp.data or [])
            st.metric("Products in Catalogue", _cat_count)
        except Exception:
            st.caption("Could not load catalogue count.")

    st.divider()
    st.subheader("📊 Quick Info")
    st.write("""
    **Inventory Features:**
    - Auto-synced from warehouse catalogue
    - Days 1-31 daily count tracking
    - Auto-calculation of totals

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
