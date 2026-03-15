import streamlit as st
from st_supabase_connection import SupabaseConnection
import pandas as pd
import datetime
import uuid
import io
import math
import numpy as np
from typing import Optional
from org_helpers import create_organization, create_location, add_membership
from org_helpers import get_user_memberships
from org_helpers import (
    create_restaurant_with_invite,
    get_org_restaurants,
    get_invite_codes_for_location,
    deactivate_restaurant,
    reactivate_restaurant,
    regenerate_invite_code,
    get_location_members,
)

# --- 1. CLOUD CONNECTION ---
conn = st.connection("supabase", type=SupabaseConnection)


def safe_rerun():
    """
    Try multiple ways to programmatically rerun the Streamlit app.
    1) st.rerun()
    2) st.experimental_rerun()
    3) st.experimental_set_query_params(...) (causes a rerun)
    4) st.stop() (last resort)
    """
    for candidate in ("rerun", "experimental_rerun"):
        fn = getattr(st, candidate, None)
        if callable(fn):
            try:
                fn()
                return
            except Exception:
                # try next
                pass

    # Try to change query params (this causes a rerun in most builds)
    try:
        if callable(getattr(st, "experimental_set_query_params", None)):
            st.experimental_set_query_params(_r=uuid.uuid4().hex)
            return
    except Exception:
        pass

    # Last resort: stop execution (Streamlit will re-run when user interacts)
    fn = getattr(st, "stop", None)
    if callable(fn):
        try:
            fn()
            return
        except Exception:
            pass

    # If nothing worked, throw a clear error
    raise RuntimeError("safe_rerun failed: cannot programmatically rerun or stop Streamlit.")

def get_current_user_id():
    """
    Return the current logged-in user id (uuid string) if available.
    Tries common places: st.session_state['user_id'], Supabase client auth.get_user(), auth.user(), etc.
    Returns None if not found.
    """
    # 1) If your login flow sets st.session_state['user_id']
    uid = st.session_state.get("user_id")
    if uid:
        return uid

    # 2) Common supabase client patterns (try both get_user and user)
    try:
        auth = getattr(conn, "auth", None)
        if auth is not None:
            # supabase-py modern: auth.get_user()
            get_user = getattr(auth, "get_user", None)
            if callable(get_user):
                try:
                    resp = get_user()
                    # resp may be dict or object; try common shapes
                    if isinstance(resp, dict) and "user" in resp and resp["user"]:
                        return resp["user"].get("id") or resp["user"].get("sub")
                    # some clients return an object with 'data' attr
                    if hasattr(resp, "data"):
                        ud = resp.data.get("user") if isinstance(resp.data, dict) else None
                        if ud:
                            return ud.get("id")
                except Exception:
                    pass

            # legacy: auth.user()
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

    # 3) Some wrappers expose conn.get_session() or conn.session
    try:
        get_sess = getattr(conn, "get_session", None)
        if callable(get_sess):
            sess = get_sess()
            if isinstance(sess, dict):
                user = sess.get("user")
                if user:
                    return user.get("id") or user.get("sub")
    except Exception:
        pass

    # Not found
    return None


# Module-level constant for the 31 possible day columns in inventory tables.
_DAY_COLUMNS = [str(d) for d in range(1, 32)]


def clean_dataframe(df):
    """Ensures unique columns, removes ghost columns, and formats for Supabase"""
    if df is None or df.empty:
        return df
    
    # Drop unnamed/duplicate columns
    df = df.loc[:, ~df.columns.str.contains("^Unnamed", na=False)]
    # Only drop all-None columns if there are multiple rows.
    # For single-row DataFrames (e.g., adding a new product/category/supplier),
    # dropping all-None columns removes optional fields that the DB expects.
    if len(df) > 1:
        df = df.dropna(axis=1, how="all")
    df = df.loc[:, ~df.columns.duplicated()]
    
    # Fix Column Casing & Whitespace: 
    # We map common database names back to the exact casing used in your app logic.
    col_map = {
        'product name': 'Product Name',
        'logid': 'LogID',
        'item': 'Item',
        'qty': 'Qty',
        'uom': 'UOM',
        'status': 'Status',
        'timestamp': 'Timestamp',
        'category': 'Category',
        'opening stock': 'Opening Stock',
        'consumption': 'Consumption',
        'closing stock': 'Closing Stock'
    }
    
    # Strip whitespace and apply mapping
    df.columns = [str(col).strip() for col in df.columns]
    df.rename(columns=lambda x: col_map.get(x.lower(), x), inplace=True)
    
    # CRITICAL SUPABASE FIX: Convert Pandas NaNs/Empty cells to 'None' (Null)
    df = df.replace({np.nan: None})

    # ✅ NEW: Cast known integer columns to int to avoid bigint type errors
    int_cols = ["Day"]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # ✅ NEW: Cast Qty to float then to int only if it's a whole number
    # This handles both activity_logs.Qty (bigint) and inventory day cols (float8)
    if "Qty" in df.columns:
        df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0)
        # Only cast to int if ALL values are whole numbers (safe for bigint tables)
        try:
            if (df["Qty"] % 1 == 0).all():
                df["Qty"] = df["Qty"].astype(int)
        except Exception:
            # in case of mixed types or strings, keep as numeric float
            df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0)

    # ✅ Coerce numeric DB columns so empty strings don't cause bigint/numeric errors
    # Lead Time is an integer column; empty/invalid → None (NULL in DB)
    if "Lead Time" in df.columns:
        df["Lead Time"] = pd.to_numeric(df["Lead Time"], errors="coerce")
        df["Lead Time"] = df["Lead Time"].where(df["Lead Time"].notna(), other=None)

    # Price is a numeric/float column; empty/invalid → None
    if "Price" in df.columns:
        df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
        df["Price"] = df["Price"].where(df["Price"].notna(), other=None)

    # Contact: coerce to None if empty string (DB may store as numeric/bigint)
    if "Contact" in df.columns:
        def _blank_to_none(v):
            if v is None:
                return None
            s = str(v).strip()
            return None if s in ("", "nan") else v
        df["Contact"] = df["Contact"].apply(_blank_to_none)

        # Cast numeric Contact values to int to avoid bigint parse errors (e.g., 8645.0 → 8645)
        def _contact_to_int(v):
            if v is None:
                return None
            try:
                f = float(v)
                if f == int(f):
                    return int(f)
                return v
            except (ValueError, TypeError):
                return v  # Keep strings like phone numbers as-is
        df["Contact"] = df["Contact"].apply(_contact_to_int)

    # ✅ Coerce inventory quantity columns: empty strings / invalid text → 0
    # These columns map to float8 columns in persistent_inventory / monthly_history.
    _inventory_num_cols = [
        "Opening Stock", "Total Received", "Consumption", "Closing Stock", "Variance",
    ]
    for col in _inventory_num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # ✅ Coerce day columns "1".."31": empty strings / invalid text → 0
    for col in _DAY_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df

# -------------------------
#  --- Org / Location helpers
# -------------------------
# These helpers create orgs/locations and manage user memberships in Supabase.
# Use these in your signup/onboarding logic.

# -------------------------
#  --- Onboarding & Session helpers
# -------------------------

def onboard_new_user(user_id: str, org_name: str, initial_location_name: str = "Main Warehouse"):
    """
    Run this once after a fresh sign-up to create org, location and membership.
    Sets st.session_state variables on success and returns True.
    """
    org = create_organization(owner_user_id=user_id, org_name=org_name)
    if not org:
        st.error("Could not create organization.")
        return False

    loc = create_location(org_id=org["id"], name=initial_location_name, loc_type="warehouse")
    if not loc:
        st.error("Could not create initial location.")
        return False

    mem = add_membership(user_id=user_id, org_id=org["id"], location_id=loc["id"], role="owner")
    if not mem:
        st.error("Could not add membership.")
        return False

    # set session state
    st.session_state.user_id = user_id
    st.session_state.org_id = org["id"]
    st.session_state.location_id = loc["id"]
    st.session_state.role = "owner"
    st.session_state.memberships = [mem]
    return True

def after_login_set_session(user_id: str):
    """
    Call after login to resolve memberships and set session state.
    If no memberships are present, session org_id/location_id remain None (so app can show setup wizard).
    """
    # Clear the logged_out flag so normal session bootstrap can resume
    if "logged_out" in st.session_state:
        del st.session_state["logged_out"]
    memberships = get_user_memberships(user_id)
    st.session_state.user_id = user_id
    st.session_state.memberships = memberships or []
    if memberships:
        # default to first membership; UI can allow switching later
        m = memberships[0]
        st.session_state.org_id = m.get("org_id")
        st.session_state.location_id = m.get("location_id")
        st.session_state.role = m.get("role", "member")
    else:
        st.session_state.org_id = None
        st.session_state.location_id = None
        st.session_state.role = None

# -------------------------
#  --- Location switcher UI
# -------------------------
def location_switcher_ui():
    """
    Shows a sidebar selectbox if the logged-in user has multiple memberships.
    Displays real org/location names instead of UUIDs.
    Updates st.session_state.org_id / location_id / role based on selection.
    """
    memberships = st.session_state.get("memberships", []) or []
    if not memberships:
        return

    # Collect unique org and location IDs to look up names
    org_ids = list({m.get("org_id") for m in memberships if m.get("org_id")})
    loc_ids = list({m.get("location_id") for m in memberships if m.get("location_id")})

    # Fetch org names
    org_names = {}
    if org_ids:
        try:
            resp = conn.table("organizations").select("id, name").in_("id", org_ids).execute()
            for row in (resp.data or []):
                org_names[row["id"]] = row["name"]
        except Exception:
            pass

    # Fetch location names
    loc_names = {}
    if loc_ids:
        try:
            resp = conn.table("locations").select("id, name").in_("id", loc_ids).execute()
            for row in (resp.data or []):
                loc_names[row["id"]] = row["name"]
        except Exception:
            pass

    # Build human-readable labels
    labels = []
    for m in memberships:
        oid = m.get("org_id")
        lid = m.get("location_id")
        role = m.get("role", "member")
        org_label = org_names.get(oid, oid[:8] if oid else "no-org")
        loc_label = loc_names.get(lid, lid[:8] if lid else "no-loc")
        labels.append(f"{org_label} · {loc_label} · {role}")

    if len(labels) > 1:
        pick = st.sidebar.selectbox("Select organization / location", labels, key="location_picker")
        idx = labels.index(pick)
        sel = memberships[idx]
        st.session_state.org_id = sel.get("org_id")
        st.session_state.location_id = sel.get("location_id")
        st.session_state.role = sel.get("role", "member")

# -------------------------
#  --- Org-aware load/save
# -------------------------
# Replace your old load_from_sheet and save_to_sheet with these org-aware versions.

def _current_org_id():
    return st.session_state.get("org_id")

def _current_location_id():
    return st.session_state.get("location_id")

@st.cache_data(ttl=60)
def load_from_sheet(table_name, default_cols=None, allow_global_meta=False):
    """
    Org-aware loader.
    - If st.session_state['org_id'] is set, applies .eq('org_id', ...)
    - For product_metadata: if allow_global_meta=True, returns rows where org_id IS NULL OR equals current org
    - For location-scoped tables (like persistent_inventory, activity_logs), filter by location_id if set.
    """
    org_id = _current_org_id()
    loc_id = _current_location_id()

    try:
        q = conn.table(table_name).select("*")

        # Special handling for product_metadata: include global rows if requested
        if table_name == "product_metadata" and allow_global_meta:
            resp = q.execute()
            df = pd.DataFrame(resp.data)
            if df.empty and default_cols:
                return pd.DataFrame(columns=default_cols)
            elif df.empty:
                return pd.DataFrame()
            df = clean_dataframe(df)
            # keep global rows (org_id null) and org-specific rows
            if org_id:
                df = df[df["org_id"].isnull() | (df["org_id"].astype(str) == str(org_id))]
            else:
                df = df[df["org_id"].isnull()]
            # ensure default cols
            if default_cols:
                for c in default_cols:
                    if c not in df.columns:
                        df[c] = None
            return df

        # Normal tables: apply filters server-side
        if org_id:
            q = q.eq("org_id", org_id)
        # apply location filter for location-scoped tables
        # Note: restaurant_requisitions uses from_location_id / to_location_id
        # (not location_id), so it is intentionally excluded from location filtering here.
        # It is still filtered by org_id above.
        if loc_id and table_name in ("persistent_inventory", "activity_logs", "monthly_history", "orders_db", "rest_01_inventory"):
            q = q.eq("location_id", loc_id)

        resp = q.execute()
        df = pd.DataFrame(resp.data)
        if df.empty and default_cols:
            return pd.DataFrame(columns=default_cols)
        elif df.empty:
            return pd.DataFrame()
        df = clean_dataframe(df)
        if default_cols:
            for col in default_cols:
                if col not in df.columns:
                    df[col] = None
        return df

    except Exception as e:
        st.warning(f"Load error for {table_name}: {e}")
        # return skeleton if defaults provided
        if default_cols:
            return pd.DataFrame(columns=default_cols)
        return pd.DataFrame()

# Mapping of table names to their upsert conflict-target columns.
# Values are comma-separated column names as expected by Supabase's on_conflict parameter.
# Columns that contain spaces must be wrapped in double-quotes (e.g. "Product Name").
# These must match the unique indexes defined in sql/unique_indexes.sql.
_ON_CONFLICT_BY_TABLE = {
    "product_metadata": 'org_id,"Product Name"',
    "persistent_inventory": 'org_id,location_id,"Product Name"',
    "user_memberships": "user_id,org_id,location_id",
    "activity_logs": 'org_id,location_id,"LogID"',
}

# Tables whose primary key is server-generated (uuid DEFAULT gen_random_uuid()).
# When saving to these tables, null/blank id values must be omitted from the
# upsert payload so that Postgres can apply the column default.  Sending an
# explicit null bypasses the default and causes a NOT NULL violation.
_SERVER_UUID_PK_TABLES = ("persistent_inventory", "product_metadata")
# UI-only columns that exist in local DataFrames/session state but not in the DB schema
_PERSISTENT_INVENTORY_UI_ONLY_COLS = frozenset(["Physical Count", "Price", "Total Amount"])


def save_to_sheet(df: pd.DataFrame, table_name: str, pk: str = None):
    """
    Org-aware save. Ensures org_id and (optionally) location_id are written to every record.
    pk: optional conflict-target override. When omitted, conflict target is looked up
        from _ON_CONFLICT_BY_TABLE.
    """
    if df is None:
        return False
    if isinstance(df, pd.DataFrame) and df.empty:
        return False

    org_id = _current_org_id()
    loc_id = _current_location_id()

    df = clean_dataframe(df)

    # Inject org_id to all rows if available (RLS requires this)
    if org_id:
        df["org_id"] = org_id

    # Inject location_id for location-scoped tables
    if loc_id and table_name in ("persistent_inventory", "activity_logs", "monthly_history", "orders_db", "rest_01_inventory"):
        df["location_id"] = loc_id

    # Inject user_id for tables that require it (activity_logs has NOT NULL user_id)
    user_id = st.session_state.get("user_id")
    if user_id and table_name in ("activity_logs", "product_metadata"):
        df["user_id"] = user_id

    # Convert NaN to None for JSON compatibility
    df = df.where(pd.notnull(df), None)

    records = df.to_dict(orient="records")
    # Sanitize any remaining float NaN/Inf values (e.g. from float64 columns where
    # df.where(notnull, None) coerces None back to NaN) so they serialise as JSON null.
    records = [
        {k: (None if isinstance(v, float) and not math.isfinite(v) else v)
         for k, v in rec.items()}
        for rec in records
    ]

    # Strip UI-only columns that don't exist in the DB schema for persistent_inventory
    if table_name == "persistent_inventory":
        records = [{k: v for k, v in rec.items() if k not in _PERSISTENT_INVENTORY_UI_ONLY_COLS} for rec in records]

    # IMPORTANT: for server-generated uuid PK tables, always omit 'id' from the payload.
    # Conflict resolution is via a unique index (org_id, location_id, "Product Name"),
    # not via id, so id is not needed.  Sending id=null (or any non-null stale id from
    # a local DataFrame) bypasses Postgres DEFAULT gen_random_uuid() and causes a NOT NULL
    # violation on new rows, or unexpected id-mismatches on updates.
    if table_name in _SERVER_UUID_PK_TABLES:
        records = [{k: v for k, v in rec.items() if k != "id"} for rec in records]

    conflict_target = pk if pk else _ON_CONFLICT_BY_TABLE.get(table_name)

    try:
        if conflict_target:
            conn.table(table_name).upsert(records, on_conflict=conflict_target).execute()
        else:
            conn.table(table_name).upsert(records).execute()

        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Database Save Error on '{table_name}': {e}")
        return False

# Replace your existing logout helper + dialog with this robust version

def logout_user():
    """
    Best-effort sign-out + clear session-state keys relevant to auth + trigger safe_rerun().
    - Clears common keys: user_id, org_id, location_id, memberships, role, etc.
    """
    # Attempt client sign-out (various wrappers differ)
    try:
        if hasattr(conn, "auth") and callable(getattr(conn.auth, "sign_out", None)):
            conn.auth.sign_out()
        elif callable(getattr(conn, "sign_out", None)):
            conn.sign_out()
    except Exception:
        # ignore signout failures — proceed to clear session
        pass

    # Keys to clear (extend if your app stores additional keys)
    keys_to_clear = [
        "user_id", "org_id", "location_id", "memberships", "role",
        "dash_cards", "r01_dash_cards", "inventory", "log_page",
        "bulk_upload_state", "cart", "_show_lss_fullscreen", "_lss_fmt_pending", "_lss_sort",
    ]
    for k in keys_to_clear:
        if k in st.session_state:
            del st.session_state[k]

    # clear caches if available
    try:
        if hasattr(st, "cache_data"):
            st.cache_data.clear()
    except Exception:
        pass

    # Force a rerun (so UI shows login/register)
    st.session_state["logged_out"] = True
    safe_rerun()

# Confirmation dialog (uses your @st.dialog pattern)
@st.dialog("🔒 Confirm Logout")
def _logout_confirm_dialog():
    st.subheader("Confirm Logout")
    st.write("Are you sure you want to log out? Your session will be cleared from this browser.")
    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("Yes, logout", type="primary", use_container_width=True, key="confirm_logout_btn"):
            logout_user()
    with c2:
        if st.button("Cancel", use_container_width=True, key="cancel_logout_btn"):
            st.info("Cancelled")


# --- PAGE CONFIG ---
st.set_page_config(page_title="Warehouse Pro Cloud v8.6", layout="wide", initial_sidebar_state="expanded")

# --- MODERN LIGHT THEME (Geex/Linear-inspired) ---
st.markdown(
    """
    <style>
    /* ===== Fonts ===== */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@300;400;500&display=swap');

    :root{
        --bg: #F1F5F9;
        --panel: #FFFFFF;
        --panel-2: #E8EDF4;
        --border: #CBD5E1;
        --border-2: rgba(0,0,0,0.08);
        --text: #1E293B;
        --muted: #64748B;
        --muted2: #94A3B8;
        --accent: #7C5CFC;
        --accent2: #6366F1;
        --warn: #F59E0B;
        --danger: #EF4444;
        --good: #10B981;
        --shadow: 0 2px 4px rgba(0,0,0,0.08), 0 8px 32px rgba(0,0,0,0.12);
        --shadow-hover: 0 4px 12px rgba(0,0,0,0.12), 0 12px 40px rgba(0,0,0,0.18);
        --radius: 16px;
        --radius-sm: 10px;
    }

    html, body, [class*="css"], [data-testid="stAppViewContainer"]{
        font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif !important;
        color: var(--text) !important;
    }
    code, pre, .stMarkdown code {
        font-family: "JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace !important;
        font-weight: 400 !important;
    }

    /* ===== Base layout ===== */
    .block-container{ padding-top: 2.5rem; padding-bottom: 1.5rem; max-width: 1400px; }
    .main { background: var(--bg) !important; }
    .stApp { background: var(--bg) !important; }
    [data-testid="stAppViewContainer"]{ background: var(--bg) !important; }
    [data-testid="stHeader"]{ background: var(--bg) !important; }
    [data-testid="stToolbar"]{ visibility: visible; }
    footer{ visibility: hidden; }
    [data-testid="stSidebarCollapsedControl"]{ visibility: visible !important; display: flex !important; z-index: 999999 !important; }

    /* ===== Scrollbars ===== */
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: #F1F5F9; border-radius: 99px; }
    ::-webkit-scrollbar-thumb { background: #CBD5E1; border-radius: 99px; }
    ::-webkit-scrollbar-thumb:hover { background: #94A3B8; }

    /* ===== Floating header ===== */
    .wp-header{
        background: linear-gradient(135deg, #7C5CFC 0%, #6366F1 100%);
        border: none;
        border-radius: var(--radius);
        padding: 18px 24px;
        margin-bottom: 20px;
        box-shadow: 0 4px 20px rgba(124,92,252,0.25);
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    .wp-header .title{
        font-size: 15px;
        letter-spacing: 0.06em;
        font-weight: 700;
        color: #ffffff;
        text-transform: uppercase;
    }
    .wp-header .subtitle{
        font-size: 12px;
        font-weight: 400;
        color: rgba(255,255,255,0.75);
        margin-top: 3px;
    }
    .wp-pill{
        display: inline-flex;
        align-items: center;
        gap: 8px;
        background: rgba(255,255,255,0.20);
        border: 1px solid rgba(255,255,255,0.30);
        color: #ffffff;
        padding: 8px 14px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 500;
    }

    /* ===== Tabs ===== */
    .stTabs [data-baseweb="tab-list"]{
        gap: 4px;
        background: #FFFFFF;
        padding: 6px;
        border-radius: var(--radius);
        border: 1px solid var(--border);
        box-shadow: var(--shadow);
        margin-bottom: 16px;
    }
    .stTabs [data-baseweb="tab"]{
        padding: 7px 16px;
        font-weight: 500;
        color: var(--muted);
        border-radius: 10px;
        font-size: 13px;
        height: 40px;
        transition: all 160ms ease;
    }
    .stTabs [data-baseweb="tab"]:hover{
        color: var(--text);
        background: var(--panel-2);
    }
    .stTabs [aria-selected="true"]{
        color: var(--accent) !important;
        background: rgba(124,92,252,0.08) !important;
        border: 1px solid rgba(124,92,252,0.18) !important;
        box-shadow: 0 2px 8px rgba(124,92,252,0.10) !important;
        font-weight: 600 !important;
    }

    /* ===== Section titles ===== */
    .section-title{
        color: var(--text);
        font-size: 13px;
        font-weight: 600;
        margin: 10px 0 10px;
        display: block;
        border-bottom: 2px solid rgba(124,92,252,0.15);
        padding-bottom: 8px;
        letter-spacing: 0.02em;
    }

    /* ===== Glass cards (now clean white cards) ===== */
    .glass-card{
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: var(--radius);
        box-shadow: var(--shadow);
        padding: 20px 20px 18px 20px;
        position: relative;
        overflow: hidden;
        transition: box-shadow 200ms ease, border-color 200ms ease;
    }
    .glass-card:hover{
        border-color: rgba(124,92,252,0.20);
        box-shadow: var(--shadow-hover);
    }
    .card-title{
        font-size: 13px;
        font-weight: 600;
        color: var(--text);
        margin: 0 0 12px 0;
        letter-spacing: 0.01em;
        display:flex;
        align-items:center;
        justify-content:space-between;
        gap: 10px;
    }
    .card-title .meta{
        font-size: 11px;
        color: var(--muted);
        font-weight: 400;
        background: var(--panel-2);
        padding: 2px 8px;
        border-radius: 999px;
    }

    /* ===== KPI cards ===== */
    .kpi-grid{ display:grid; grid-template-columns: repeat(4, 1fr); gap: 14px; }
    .kpi{
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: var(--radius);
        padding: 18px 18px 14px;
        position: relative;
        overflow:hidden;
        min-height: 84px;
        border-left: 4px solid var(--accent);
        box-shadow: var(--shadow);
        transition: box-shadow 200ms ease;
    }
    .kpi:hover{ box-shadow: var(--shadow-hover); }
    .kpi .label{
        font-size: 11px;
        color: var(--muted);
        font-weight: 600;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        display:flex;
        align-items:center;
        justify-content:space-between;
        gap: 10px;
    }
    .kpi .value{
        margin-top: 10px;
        font-size: 24px;
        font-weight: 700;
        color: var(--text);
        letter-spacing: -0.01em;
        font-family: "JetBrains Mono", ui-monospace, monospace !important;
    }
    .kpi .sub{
        margin-top: 4px;
        font-size: 11px;
        color: var(--muted2);
        font-weight: 400;
    }
    .kpi.good{ border-left-color: var(--good); }
    .kpi.good .value{ color: var(--good); }
    .kpi.bad{ border-left-color: var(--danger); }
    .kpi.bad .value{ color: var(--danger); }
    .kpi.accent{ border-left-color: var(--accent); background: rgba(124,92,252,0.03); }

    /* ===== Kebab (menu trigger) ===== */
    .kebab-wrap{ display:flex; align-items:center; gap:8px; }
    .kebab-hint{
        font-size: 11px;
        color: var(--muted2);
        font-weight: 400;
    }

    /* ===== Requisition item box ===== */
    .req-box{
        background: #FFFBEB;
        border-left: 3px solid var(--warn);
        padding: 10px 12px;
        margin: 6px 0;
        border-radius: 10px;
        font-size: 13px;
        line-height: 1.45;
        border: 1px solid rgba(245,158,11,0.18);
        color: var(--text);
    }

    /* ===== Activity list ===== */
    .log-container{
        max-height: 320px;
        overflow-y: auto;
        padding-right: 4px;
        border-radius: var(--radius);
        background: var(--panel);
        border: 1px solid var(--border);
    }
    .log-row{
        display:flex;
        justify-content:space-between;
        align-items:center;
        background: #FFFFFF;
        padding: 10px 12px;
        border-radius: 10px;
        margin: 5px;
        border-left: 3px solid var(--accent);
        border: 1px solid var(--border);
        transition: all 150ms ease;
    }
    .log-row:hover{ transform: translateY(-1px); box-shadow: var(--shadow); border-color: rgba(124,92,252,0.20); }
    .log-row-undone{ border-left: 3px solid var(--danger) !important; opacity: 0.60; }
    .log-info{ font-size: 12px; color: var(--text); line-height: 1.35; }
    .log-time{ font-size: 11px; color: var(--muted); margin-left: 6px; }

    /* ===== Sidebar ===== */
    [data-testid="stSidebar"]{
        background: #FFFFFF;
        border-right: 1px solid var(--border);
    }
    .sidebar-title{
        color: var(--text);
        font-weight: 700;
        font-size: 13px;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        margin-bottom: 10px;
    }

    /* ===== Sidebar toggle button (always visible) ===== */
    [data-testid="stSidebarCollapsedControl"] button {
        visibility: visible !important;
        color: var(--text) !important;
        background: #FFFFFF !important;
        border: 1px solid var(--border) !important;
        border-radius: 8px !important;
        box-shadow: var(--shadow) !important;
    }
    [data-testid="stSidebarCollapsedControl"] button:hover {
        background: rgba(124,92,252,0.06) !important;
        border-color: rgba(124,92,252,0.25) !important;
    }
    /* Also ensure the sidebar nav toggle and close button are visible */
    [data-testid="stSidebarNavToggle"],
    [data-testid="stSidebarNavToggle"] button,
    button[data-testid="stSidebarCollapseButton"] {
        visibility: visible !important;
        color: var(--text) !important;
    }
    /* Force all toggle SVG icons to use theme color */
    [data-testid="stSidebarCollapsedControl"] svg,
    button[data-testid="stSidebarCollapseButton"] svg,
    [data-testid="stSidebarNavToggle"] svg {
        fill: var(--text) !important;
        color: var(--text) !important;
        stroke: var(--text) !important;
    }

    /* ===== Buttons (global) ===== */
    .stButton>button, .stDownloadButton>button{
        border-radius: 10px !important;
        font-size: 13px !important;
        font-weight: 500 !important;
        padding: 8px 16px !important;
        border: 1px solid var(--border) !important;
        background: #FFFFFF !important;
        color: var(--text) !important;
        transition: all 150ms ease !important;
        box-shadow: 0 1px 2px rgba(0,0,0,0.05) !important;
    }
    .stButton>button:hover, .stDownloadButton>button:hover{
        border-color: rgba(124,92,252,0.30) !important;
        background: rgba(124,92,252,0.05) !important;
        color: var(--accent) !important;
        box-shadow: 0 2px 8px rgba(124,92,252,0.12) !important;
    }
    .stButton>button:focus{ box-shadow: 0 0 0 3px rgba(124,92,252,0.15) !important; }

    /* Primary buttons */
    .stButton>button[kind="primary"], .stButton>button[data-testid="baseButton-primary"]{
        background: linear-gradient(135deg, #7C5CFC, #6366F1) !important;
        border-color: transparent !important;
        color: #ffffff !important;
        box-shadow: 0 4px 14px rgba(124,92,252,0.30) !important;
    }
    .stButton>button[kind="primary"]:hover, .stButton>button[data-testid="baseButton-primary"]:hover{
        background: linear-gradient(135deg, #6B4EE0, #5558D9) !important;
        color: #ffffff !important;
        box-shadow: 0 6px 20px rgba(124,92,252,0.40) !important;
    }

    /* Inputs */
    .stTextInput input, .stNumberInput input, .stSelectbox div[role="combobox"], .stDateInput input{
        background: #FFFFFF !important;
        border: 1px solid var(--border) !important;
        border-radius: 10px !important;
        color: var(--text) !important;
        font-size: 13px !important;
        box-shadow: 0 1px 2px rgba(0,0,0,0.04) !important;
    }
    .stTextInput input:focus, .stNumberInput input:focus, .stDateInput input:focus{
        border-color: var(--accent) !important;
        box-shadow: 0 0 0 3px rgba(124,92,252,0.12) !important;
    }
    .stTextInput label, .stNumberInput label, .stSelectbox label, .stDateInput label{
        font-size: 12px !important;
        color: var(--muted) !important;
        font-weight: 500 !important;
    }

    /* Dataframe / editor */
    [data-testid="stDataFrame"]{
        border-radius: var(--radius) !important;
        border: 1px solid var(--border) !important;
        overflow:hidden !important;
        box-shadow: var(--shadow) !important;
    }

    /* Skeleton shimmer helper */
    .skeleton{
        background: linear-gradient(90deg, #F1F5F9 0%, #E2E8F0 50%, #F1F5F9 100%);
        background-size: 200% 100%;
        animation: shimmer 1.3s ease-in-out infinite;
        border: 1px solid var(--border);
        border-radius: 8px;
        height: 14px;
        margin: 8px 0;
    }
    @keyframes shimmer{
        0%{ background-position: 200% 0; }
        100%{ background-position: -200% 0; }
    }

    /* ===== Dashboard KPI horizontal row ===== */
    .dash-kpi-row{
        display: flex;
        flex-direction: row;
        gap: 10px;
        flex-wrap: wrap;
        padding-bottom: 5px;
    }
    .dash-kpi-box{
        flex: 1 1 calc(25% - 10px);
        min-width: 100px;
        background: #FFFFFF;
        border: 1px solid var(--border);
        border-radius: var(--radius);
        padding: 12px 10px 14px;
        text-align: center;
        box-shadow: var(--shadow);
        transition: box-shadow 200ms ease;
        overflow: hidden;
        word-break: break-word;
    }
    .dash-kpi-box:hover{ box-shadow: var(--shadow-hover); }
    .dash-kpi-box .kpi-icon{
        font-size: 18px;
        margin-bottom: 4px;
    }
    .dash-kpi-box .kpi-label{
        font-size: 10px;
        color: var(--muted);
        font-weight: 600;
        margin-bottom: 6px;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        line-height: 1.3;
    }
    .dash-kpi-box .kpi-value{
        font-size: 16px;
        font-weight: 700;
        color: var(--text);
        font-family: "JetBrains Mono", ui-monospace, monospace !important;
        letter-spacing: -0.01em;
        line-height: 1.2;
        word-break: break-all;
    }
    .dash-kpi-box .kpi-value.bad{ color: var(--danger); }
    .dash-kpi-box .kpi-value.good{ color: var(--good); }
    .dash-kpi-box .kpi-currency{
        font-size: 9px;
        color: var(--muted2);
        margin-top: 4px;
        font-weight: 400;
        line-height: 1.3;
    }

    /* ===== White-background chart card ===== */
    .dash-card-white{
        background: #FFFFFF;
        border: 1px solid var(--border);
        border-radius: var(--radius);
        box-shadow: var(--shadow);
        padding: 20px 20px 16px 20px;
        position: relative;
        margin-bottom: 16px;
        transition: box-shadow 200ms ease;
    }
    .dash-card-white:hover{ box-shadow: var(--shadow-hover); }

    /* ===== Streamlit bordered containers ===== */
    [data-testid="stVerticalBlockBorderWrapper"]{
        background: #FFFFFF !important;
        border: 1.5px solid #CBD5E1 !important;
        border-radius: 16px !important;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08), 0 6px 24px rgba(0,0,0,0.07) !important;
        padding: 20px !important;
        margin-bottom: 16px !important;
        position: relative;
        overflow: hidden;
        transition: box-shadow 220ms ease, border-color 220ms ease, transform 220ms ease;
    }
    [data-testid="stVerticalBlockBorderWrapper"]:hover{
        border-color: #94A3B8 !important;
        box-shadow: 0 4px 16px rgba(0,0,0,0.12), 0 10px 32px rgba(0,0,0,0.10) !important;
        transform: translateY(-2px);
    }
    [data-testid="stVerticalBlockBorderWrapper"] > div {
        padding: 0 !important;
    }

    /* ===== Dashboard title pill ===== */
    .dash-title-pill{
        display: inline-flex;
        align-items: center;
        justify-content: center;
        background: linear-gradient(135deg, #7C5CFC, #6366F1);
        border: none;
        color: #ffffff;
        padding: 8px 24px;
        border-radius: 999px;
        font-size: 14px;
        font-weight: 600;
        letter-spacing: 0.04em;
        margin-bottom: 16px;
        box-shadow: 0 4px 14px rgba(124,92,252,0.28);
    }
    .dash-note{
        font-size: 11px;
        color: var(--muted2);
        margin-top: 6px;
    }

    /* ===== Alert / status messages ===== */
    .stSuccess{ background: #ECFDF5 !important; color: #065F46 !important; border-left: 4px solid #10B981 !important; border-radius: 10px !important; }
    .stError{ background: #FEF2F2 !important; color: #991B1B !important; border-left: 4px solid #EF4444 !important; border-radius: 10px !important; }
    .stWarning{ background: #FFFBEB !important; color: #92400E !important; border-left: 4px solid #F59E0B !important; border-radius: 10px !important; }
    .stInfo{ background: #EFF6FF !important; color: #1E40AF !important; border-left: 4px solid #6366F1 !important; border-radius: 10px !important; }

    /* ===== Fix: secondary buttons — prevent dark/black rendering ===== */
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
        background: rgba(124,92,252,0.05) !important;
        border-color: rgba(124,92,252,0.30) !important;
        color: var(--accent) !important;
    }

    /* ===== Fix: remove double/black border on input, select, date fields ===== */
    [data-baseweb="input"],
    [data-baseweb="base-input"],
    [data-baseweb="select"] > div:first-child {
        background: #FFFFFF !important;
        border: 1px solid var(--border) !important;
        border-radius: 10px !important;
        box-shadow: 0 1px 2px rgba(0,0,0,0.04) !important;
    }
    [data-baseweb="input"]:focus-within,
    [data-baseweb="base-input"]:focus-within,
    [data-baseweb="select"]:focus-within > div:first-child {
        border-color: var(--accent) !important;
        box-shadow: 0 0 0 3px rgba(124,92,252,0.12) !important;
    }
    /* Remove inner outline added by Streamlit on top of baseweb border */
    .stTextInput > div > div,
    .stNumberInput > div > div,
    .stSelectbox > div > div,
    .stDateInput > div > div {
        border: none !important;
        box-shadow: none !important;
        outline: none !important;
    }
    /* Ensure date input container has clean border */
    [data-testid="stDateInput"] > div,
    [data-testid="stDateInput"] > label + div {
        border: none !important;
        box-shadow: none !important;
    }

    /* ===== Fix: Popover (⋮) card-settings button — compact & inline ===== */
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
        background: var(--panel-2) !important;
        border-color: var(--accent) !important;
        color: var(--accent) !important;
    }

    /* Popover content panel — outer shell */
    [data-testid="stPopover"] [data-testid="stPopoverBody"],
    div[data-baseweb="popover"] [data-testid="stPopoverBody"] {
        background: #FFFFFF !important;
        border: 1px solid var(--border) !important;
        border-radius: 14px !important;
        box-shadow: 0 12px 40px rgba(0,0,0,0.12), 0 2px 8px rgba(0,0,0,0.06) !important;
        color: var(--text) !important;
        padding: 18px 20px 14px !important;
        min-width: 230px !important;
        max-width: 270px !important;
    }

    /* ===== Popover interior — polished card settings ===== */
    /* Header caption */
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

    /* Selectbox controls */
    [data-testid="stPopoverBody"] div[data-baseweb="select"] > div {
        min-height: 34px !important;
        border-radius: 8px !important;
        font-size: 13px !important;
        border: 1.5px solid var(--border) !important;
        background: var(--panel-2, #F8FAFC) !important;
        transition: border-color 150ms ease, box-shadow 150ms ease !important;
    }
    [data-testid="stPopoverBody"] div[data-baseweb="select"] > div:hover {
        border-color: rgba(124,92,252,0.50) !important;
    }
    [data-testid="stPopoverBody"] div[data-baseweb="select"] > div:focus-within {
        border-color: var(--accent) !important;
        box-shadow: 0 0 0 3px rgba(124,92,252,0.10) !important;
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

    /* Refresh / action button inside popover */
    [data-testid="stPopoverBody"] button[data-testid="stBaseButton-secondary"] {
        width: 100% !important;
        margin-top: 12px !important;
        padding: 7px 14px !important;
        min-height: 34px !important;
        height: 34px !important;
        border-radius: 8px !important;
        font-size: 12px !important;
        font-weight: 600 !important;
        background: linear-gradient(135deg, #7C5CFC, #6366F1) !important;
        color: #FFFFFF !important;
        border: none !important;
        box-shadow: 0 2px 6px rgba(124,92,252,0.22) !important;
        transition: all 150ms ease !important;
        letter-spacing: 0.03em !important;
        text-transform: none !important;
    }
    [data-testid="stPopoverBody"] button[data-testid="stBaseButton-secondary"]:hover {
        background: linear-gradient(135deg, #6D4AE8, #5558E6) !important;
        box-shadow: 0 4px 12px rgba(124,92,252,0.32) !important;
        transform: translateY(-1px) !important;
    }

    /* ===== Fix: DataFrame / data_editor header — light background ===== */
    [data-testid="stDataFrame"] th,
    [data-testid="stDataFrameResizable"] th,
    [data-testid="stDataFrame"] [data-testid="glideDataEditorContainer"] .gdg-header,
    .dvn-stack .dvn-scroller th {
        background: #F1F5F9 !important;
        color: var(--text) !important;
        border-bottom: 1px solid var(--border) !important;
    }

    /* ===== Fix: Selectbox / dropdown selected value text ===== */
    div[data-baseweb="select"] * {
        color: var(--text) !important;
    }
    div[data-baseweb="select"] input::placeholder {
        color: var(--muted2) !important;
    }
    /* Dropdown menu options */
    div[data-baseweb="menu"] li,
    div[data-baseweb="popover"] li,
    [role="listbox"] li,
    [role="option"] {
        color: var(--text) !important;
    }

    /* ===== Fix: Radio button label text ===== */
    [data-testid="stRadio"] label,
    [data-testid="stRadio"] label span,
    [data-testid="stRadio"] div[role="radiogroup"] label {
        color: var(--text) !important;
    }

    /* ===== Fix: Widget label and form text ===== */
    [data-testid="stWidgetLabel"] label,
    [data-testid="stWidgetLabel"] p,
    .stSelectbox label,
    .stTextInput label,
    .stNumberInput label,
    .stDateInput label {
        color: var(--text) !important;
    }

    /* ===== Fix: Input placeholder text ===== */
    input::placeholder,
    textarea::placeholder {
        color: var(--muted2) !important;
    }

    /* ===== Compact expand button ===== */
    button[data-testid="stBaseButton-secondary"][kind="secondary"]:has(> div > p:only-child) {
        min-height: auto !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def _show_login_page():
    """Render a centered login / register form. Returns without stopping — caller must st.stop()."""
    st.markdown(
        """
        <div style="max-width:420px;margin:80px auto 0 auto;">
            <div style="text-align:center;margin-bottom:28px;">
                <span class="dash-title-pill">🔐 Warehouse Pro Cloud</span>
                <p style="color:var(--muted);font-size:13px;margin-top:8px;">Sign in to access your inventory</p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Center the form using columns
    _, center, _ = st.columns([1, 2, 1])
    with center:
        mode = st.radio("", ["Sign In", "Register"], horizontal=True, key="login_mode", label_visibility="collapsed")
        email = st.text_input("📧 Email", placeholder="you@example.com", key="login_email")
        password = st.text_input("🔑 Password", type="password", placeholder="••••••••", key="login_password")

        if mode == "Sign In":
            if st.button("🔓 Sign In", use_container_width=True, type="primary", key="signin_btn"):
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
                        if uid:
                            # Clear logged_out flag and populate session
                            if "logged_out" in st.session_state:
                                del st.session_state["logged_out"]
                            after_login_set_session(uid)
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
            if st.button("📝 Create Account", use_container_width=True, type="primary", key="signup_btn"):
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
                        if uid:
                            if "logged_out" in st.session_state:
                                del st.session_state["logged_out"]
                            after_login_set_session(uid)
                            st.success("✅ Account created! Welcome.")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.warning("Account created. Please check your email for a confirmation link, then sign in.")
                    else:
                        st.warning("Account may have been created. Please check your email and sign in.")
                except Exception as e:
                    st.error(f"❌ Registration failed: {e}")


# --- Ensure session state reflects current authenticated user ---
current_uid = get_current_user_id()  # uses helper defined earlier

if st.session_state.get("logged_out"):
    # User explicitly logged out — show login page, do not re-populate session
    _show_login_page()
    st.stop()
elif current_uid:
    # Active auth session — populate session state if not already done
    if st.session_state.get("user_id") != current_uid:
        try:
            after_login_set_session(current_uid)
        except Exception:
            st.session_state["user_id"] = current_uid
else:
    # No auth session at all — show login page
    _show_login_page()
    st.stop()


# --- ONBOARDING: show setup wizard if user is logged in but has no org ---
_uid = st.session_state.get("user_id")
_memberships = st.session_state.get("memberships", [])
if _uid and not _memberships:
    st.markdown(
        """
        <div style="max-width:500px;margin:60px auto 0 auto;text-align:center;">
            <span class="dash-title-pill">🏢 Set Up Your Organization</span>
            <p style="color:var(--muted);font-size:13px;margin-top:10px;">
                You're logged in but haven't set up an organization yet.<br>
                Fill in the details below to get started.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    _, center_col, _ = st.columns([1, 2, 1])
    with center_col:
        with st.form("onboarding_form"):
            _org_name = st.text_input("🏢 Organization Name", placeholder="e.g., My Warehouse Co.")
            _loc_name = st.text_input("📍 First Location Name", value="Main Warehouse", placeholder="e.g., Main Warehouse")
            _submitted = st.form_submit_button("🚀 Create Organization & Location", type="primary", use_container_width=True)
        if _submitted:
            if not _org_name.strip():
                st.error("Please enter an organization name.")
            else:
                _ok = onboard_new_user(
                    user_id=_uid,
                    org_name=_org_name.strip(),
                    initial_location_name=_loc_name.strip() or "Main Warehouse",
                )
                if _ok:
                    st.success("✅ Organization created! Loading your workspace...")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error("❌ Failed to create organization. Please try again or contact support.")
    st.stop()  # Don't render the rest of the app until onboarding is complete


# --- CORE CALCULATION ENGINE ---
def recalculate_item(df, item_name):
    if item_name not in df["Product Name"].values:
        return df
    idx = df[df["Product Name"] == item_name].index[0]
    day_cols = [str(i) for i in range(1, 32)]
    for col in day_cols:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    total_received = df.loc[idx, day_cols].sum()
    df.at[idx, "Total Received"] = total_received
    opening = pd.to_numeric(df.at[idx, "Opening Stock"], errors="coerce") or 0.0
    consumption = pd.to_numeric(df.at[idx, "Consumption"], errors="coerce") or 0.0
    closing = opening + total_received - consumption
    df.at[idx, "Closing Stock"] = closing

    if "Physical Count" in df.columns:
        physical_val = df.at[idx, "Physical Count"]
        if pd.notna(physical_val) and str(physical_val).strip() != "":
            physical = pd.to_numeric(physical_val, errors="coerce")
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
            if col_name not in df.columns:
                df[col_name] = 0.0
            current_val = pd.to_numeric(df.at[idx, col_name], errors="coerce") or 0.0
            df.at[idx, col_name] = current_val + float(qty)

        if not is_undo:
            # Insert only the new log row; id and user_id are included so the NOT NULL
            # constraints on activity_logs are satisfied without reloading the full table.
            new_log = pd.DataFrame(
                [
                    {
                        "id": str(uuid.uuid4()),
                        "LogID": str(uuid.uuid4())[:8],
                        "Timestamp": datetime.datetime.now().strftime("%H:%M:%S"),
                        "Item": item_name,
                        "Qty": qty,
                        "Day": day_num,
                        "Status": "Active",
                        "LogDate": datetime.date.today().strftime("%Y-%m-%d"),
                        "user_id": st.session_state.get("user_id"),
                    }
                ]
            )
            save_to_sheet(new_log, "activity_logs")

        df = recalculate_item(df, item_name)
        st.session_state.inventory = df
        # Save only the single modified inventory row to avoid sending id=null for other rows
        save_to_sheet(df.loc[[idx]].copy(), "persistent_inventory")
        return True
    return False

def undo_entry(log_id):
    logs = load_from_sheet("activity_logs")
    if not logs.empty and "LogID" in logs.columns and log_id in logs["LogID"].values:
        idx = logs[logs["LogID"] == log_id].index[0]
        if logs.at[idx, "Status"] == "Undone":
            return
        item, qty, day = logs.at[idx, "Item"], logs.at[idx, "Qty"], logs.at[idx, "Day"]
        if apply_transaction(item, day, -qty, is_undo=True):
            logs.at[idx, "Status"] = "Undone"
            # Save only the single updated log row (conflict on LogID upserts in place)
            save_to_sheet(logs.loc[[idx]].copy(), "activity_logs")
            st.rerun()

# --- MODALS ---
@st.dialog("🗂️ Manage Categories")
def manage_categories_modal():
    st.subheader("🗂️ Category Manager")

    meta_df = load_from_sheet("product_metadata")
    existing_categories = []
    if not meta_df.empty and "Category" in meta_df.columns:
        all_cats = meta_df["Category"].dropna().unique().tolist()
        existing_categories = sorted(
            [cat for cat in all_cats if not str(cat).startswith("CATEGORY_") and cat != "Supplier_Master" and cat != "General"]
        )

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

            new_category = pd.DataFrame(
                [
                    {
                        "Product Name": f"CATEGORY_{category_name}",
                        "UOM": "",
                        "Supplier": "",
                        "Contact": "",
                        "Email": "",
                        "Category": category_name,
                        "Lead Time": None,
                        "Price": 0,
                        "Currency": "",
                    }
                ]
            )
            if save_to_sheet(new_category, "product_metadata"):
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
                meta_df.loc[
                    (meta_df["Category"] == selected_cat) & (~meta_df["Product Name"].str.startswith("CATEGORY_", na=False)), "Category"
                ] = "General"
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
        new_row.update(
            {
                "Product Name": name,
                "UOM": uom,
                "Opening Stock": opening,
                "Total Received": 0.0,
                "Consumption": 0.0,
                "Closing Stock": opening,
                "Physical Count": None,
                "Variance": 0.0,
                "Category": category,
            }
        )
        new_row_df = pd.DataFrame([new_row])
        st.session_state.inventory = pd.concat([st.session_state.inventory, new_row_df], ignore_index=True)
        # Upsert only the new product row so existing rows with valid ids are untouched
        # and so that no id=null payload reaches persistent_inventory.
        inv_ok = save_to_sheet(new_row_df, "persistent_inventory")

        supplier_meta = pd.DataFrame(
            [
                {
                    "Product Name": name,
                    "UOM": uom,
                    "Supplier": supplier,
                    "Contact": contact,
                    "Email": email,
                    "Category": category,
                    "Lead Time": lead_time,
                    "Price": price,
                    "Currency": currency,
                }
            ]
        )
        # Upsert only the new metadata row (conflict on org_id,"Product Name" handles updates)
        meta_ok = save_to_sheet(supplier_meta, "product_metadata")

        if inv_ok and meta_ok:
            st.success(f"✅ Product '{name}' created with supplier '{supplier}' at {currency} {price}!")
            st.balloons()
            st.rerun()
        elif inv_ok and not meta_ok:
            st.warning(f"⚠️ Product '{name}' added to inventory but supplier metadata failed to save. Please update supplier info in the Supplier Directory.")
            st.rerun()
        elif not inv_ok:
            st.error("❌ Failed to save product to inventory.")

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

        supplier_entry = pd.DataFrame(
            [
                {
                    "Product Name": f"SUPPLIER_{supplier_name}",
                    "Supplier": supplier_name,
                    "Contact": contact,
                    "Email": email,
                    "Category": "Supplier_Master",
                    "UOM": "",
                    "Price": 0,
                    "Currency": "",
                    "Lead Time": None,
                }
            ]
        )

        if save_to_sheet(supplier_entry, "product_metadata"):
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
        selected_month = st.selectbox(
            "📅 Select Month Period",
            options=sorted(hist_df["Month_Period"].unique().tolist(), reverse=True),
            key="arch_month",
        )
        month_data = hist_df[hist_df["Month_Period"] == selected_month].drop(columns=["Month_Period"])
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
            month_data.to_excel(writer, index=False, sheet_name="Archive")
        st.download_button(
            label=f"📥 Download {selected_month}",
            data=buf.getvalue(),
            file_name=f"Inventory_{selected_month}.xlsx",
            use_container_width=True,
            type="primary",
        )
    else:
        st.info("📭 No records found.")

@st.dialog("🔒 Close Month & Rollover")
def close_month_modal():
    st.warning("⚠️ Physical Counts will become new Opening Stocks.")
    month_label = st.text_input("📅 Month Label", value=datetime.datetime.now().strftime("%b %Y"), key="month_label_input")
    if st.button("✅ Confirm Monthly Close", type="primary", use_container_width=True, key="close_month_btn"):
        df = st.session_state.inventory.copy()
        hist_df = load_from_sheet("monthly_history")
        archive_df = df.copy()
        archive_df["Month_Period"] = month_label
        save_to_sheet(pd.concat([hist_df, archive_df], ignore_index=True), "monthly_history")

        new_df = df.copy()
        for i in range(1, 32):
            new_df[str(i)] = 0.0
        for idx, row in new_df.iterrows():
            phys = row.get("Physical Count")
            new_df.at[idx, "Opening Stock"] = pd.to_numeric(phys) if pd.notna(phys) and str(phys).strip() != "" else row["Closing Stock"]

        new_df["Total Received"] = 0.0
        new_df["Consumption"] = 0.0
        new_df["Closing Stock"] = new_df["Opening Stock"]
        new_df["Physical Count"] = None
        new_df["Variance"] = 0.0
        save_to_sheet(new_df, "persistent_inventory")
        st.rerun()

# --- DASHBOARD HELPERS ---
TOP_15_CURRENCIES_PLUS_BHD = [
    "USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF", "CNY", "HKD", "SGD",
    "INR", "AED", "SAR", "KWD", "BHD",
]

def _currency_filtered_meta(meta_df, selected_currency):
    if meta_df is None or meta_df.empty:
        return meta_df
    if selected_currency == "All":
        return meta_df
    if "Currency" not in meta_df.columns:
        return meta_df.iloc[0:0]
    return meta_df[meta_df["Currency"].astype(str).str.upper() == str(selected_currency).upper()]

def _prepare_metadata():
    meta_df = load_from_sheet("product_metadata")
    if meta_df.empty:
        return meta_df

    for col in [
        "Product Name",
        "Category",
        "Price",
        "Currency",
        "Lead Time",
        "Reorder Qty",
        "Min Safety Stock",
        "Min Stock",
        "UOM",
        "Supplier",
    ]:
        if col not in meta_df.columns:
            meta_df[col] = None

    meta_df["Product Name"] = meta_df["Product Name"].astype(str).str.strip()
    meta_df["Category"] = meta_df["Category"].fillna("General").astype(str).str.strip()

    meta_df = meta_df[
        (~meta_df["Product Name"].str.startswith("CATEGORY_", na=False))
        & (~meta_df["Product Name"].str.startswith("SUPPLIER_", na=False))
    ]

    meta_df["Price"] = pd.to_numeric(meta_df["Price"], errors="coerce").fillna(0.0)
    meta_df["Lead Time"] = pd.to_numeric(meta_df["Lead Time"], errors="coerce").fillna(0.0)

    if "Reorder Qty" in meta_df.columns:
        meta_df["Reorder Qty"] = pd.to_numeric(meta_df["Reorder Qty"], errors="coerce").fillna(0.0)
    if "Min Safety Stock" in meta_df.columns:
        meta_df["Min Safety Stock"] = pd.to_numeric(meta_df["Min Safety Stock"], errors="coerce").fillna(0.0)
    if "Min Stock" in meta_df.columns:
        meta_df["Min Stock"] = pd.to_numeric(meta_df["Min Stock"], errors="coerce").fillna(0.0)

    meta_df["Currency"] = meta_df["Currency"].fillna("").astype(str).str.upper().str.strip()
    meta_df["UOM"] = meta_df["UOM"].fillna("").astype(str).str.strip()
    meta_df["Supplier"] = meta_df["Supplier"].fillna("").astype(str).str.strip()
    return meta_df

def _prepare_inventory(inv_df):
    if inv_df is None or inv_df.empty:
        return pd.DataFrame(columns=["Product Name", "Category", "Closing Stock", "UOM"])
    inv_df = inv_df.copy()
    for col in ["Product Name", "Category", "Closing Stock", "UOM"]:
        if col not in inv_df.columns:
            inv_df[col] = None
    inv_df["Product Name"] = inv_df["Product Name"].astype(str).str.strip()
    inv_df["Category"] = inv_df["Category"].fillna("General").astype(str).str.strip()
    inv_df["Closing Stock"] = pd.to_numeric(inv_df["Closing Stock"], errors="coerce").fillna(0.0)
    inv_df["UOM"] = inv_df["UOM"].fillna("").astype(str).str.strip()
    inv_df = inv_df[~inv_df["Product Name"].str.startswith("CATEGORY_", na=False)]
    return inv_df

def _prepare_reqs(req_df):
    if req_df is None or req_df.empty:
        return req_df
    df = req_df.copy()
    for col in ["Restaurant", "Item", "Qty", "DispatchQty", "Status", "RequestedDate", "Timestamp"]:
        if col not in df.columns:
            df[col] = None
    df["Restaurant"] = df["Restaurant"].fillna("").astype(str).str.strip()
    df["Item"] = df["Item"].fillna("").astype(str).str.strip()
    df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0.0)
    df["DispatchQty"] = pd.to_numeric(df["DispatchQty"], errors="coerce").fillna(0.0)
    df["Status"] = df["Status"].fillna("").astype(str).str.strip()
    df["RequestedDate"] = pd.to_datetime(df["RequestedDate"], errors="coerce").dt.date
    df["DispatchTS_Date"] = pd.to_datetime(df["Timestamp"], errors="coerce").dt.date
    return df

def _prepare_logs(log_df):
    """
    forces LogDateParsed as python datetime.date to compare with st.date_input values.
    """
    if log_df is None or log_df.empty:
        return log_df
    df = log_df.copy()
    for col in ["Item", "Qty", "Status", "LogDate", "Timestamp"]:
        if col not in df.columns:
            df[col] = None

    df["Item"] = df["Item"].fillna("").astype(str).str.strip()
    df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0.0)
    df["Status"] = df["Status"].fillna("").astype(str).str.strip()

    logdate = pd.to_datetime(df["LogDate"], errors="coerce")
    ts_fallback = pd.to_datetime(df["Timestamp"], errors="coerce")
    combined = logdate.fillna(ts_fallback)

    df["LogDateParsed"] = combined.dt.date  # python date
    return df

def _to_excel_bytes(sheets: dict):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        for name, df in sheets.items():
            safe_name = (str(name)[:31] if name else "Sheet")
            if df is None:
                continue
            (df if isinstance(df, pd.DataFrame) else pd.DataFrame(df)).to_excel(writer, index=False, sheet_name=safe_name)
    return buf.getvalue()

def _make_bar_chart(df, x_col, y_col):
    """
    Streamlit bar_chart may reorder categories (often alphabetically).
    Use Plotly for stable ordering and correct High→Low / Low→High display.
    """
    if df is None or df.empty or x_col not in df.columns or y_col not in df.columns:
        st.info("📭 No data for chart.")
        return

    chart_df = df[[x_col, y_col]].copy()
    chart_df[y_col] = pd.to_numeric(chart_df[y_col], errors="coerce").fillna(0.0)

    try:
        import plotly.express as px  # type: ignore

        fig = px.bar(chart_df, x=x_col, y=y_col)
        fig.update_traces(marker_color="rgba(124,92,252,0.75)", marker_line_color="rgba(124,92,252,0.0)")
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#64748B", family="Inter", size=12),
            xaxis=dict(
                categoryorder="array",
                categoryarray=chart_df[x_col].tolist(),
                showgrid=False,
                zeroline=False,
                tickfont=dict(size=10, color="#94A3B8"),
            ),
            yaxis=dict(showgrid=True, gridcolor="#F1F5F9", zeroline=False, tickfont=dict(size=10, color="#94A3B8")),
            margin=dict(l=10, r=10, t=10, b=10),
            height=360,
        )
        st.plotly_chart(fig, use_container_width=True)
    except Exception:
        chart_df = chart_df.set_index(x_col)
        st.bar_chart(chart_df, y=y_col)

def _make_pie_chart(df, label_col, value_col, top_n=None, show_legend=False, label_mode="%"):
    """
    Pie should respect the Top-N user selection (top_n).
    If top_n is None, default to 10 (sane default for readability).
    show_legend=False (default) hides the legend for compact card view.
    show_legend=True shows the legend on the right for fullscreen/dialog view.
    """
    if df is None or df.empty or label_col not in df.columns or value_col not in df.columns:
        st.info("📭 No data for chart.")
        return

    if top_n is None:
        top_n = 10

    pie_df = df[[label_col, value_col]].copy()
    pie_df[value_col] = pd.to_numeric(pie_df[value_col], errors="coerce").fillna(0.0)

    pie_df = pie_df[pie_df[value_col] > 0].head(int(top_n))

    if pie_df.empty:
        st.info("📭 No non-zero values for pie chart.")
        return

    try:
        import plotly.express as px  # type: ignore
        fig = px.pie(pie_df, names=label_col, values=value_col, hole=0.38, color_discrete_sequence=_CHART_PALETTE)
        if label_mode == "Qty":
            fig.update_traces(
                textposition="inside",
                textinfo="value+label",
                texttemplate="%{label}<br>%{value:.0f}",
            )
        elif label_mode == "Amount":
            fig.update_traces(
                textposition="inside",
                textinfo="value+label",
                texttemplate="%{label}<br>%{value:.2f}",
            )
        else:  # "%"
            fig.update_traces(textposition="inside", textinfo="percent+label")
        if show_legend:
            legend_cfg = dict(
                visible=True,
                orientation="v",
                yanchor="middle",
                y=0.5,
                xanchor="left",
                x=1.05,
                font=dict(size=12, color="#64748B"),
            )
            chart_height = _CHART_HEIGHT_FULLSCREEN
        else:
            legend_cfg = dict(visible=False)
            chart_height = _CHART_HEIGHT_COMPACT
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#64748B", family="Inter", size=12),
            margin=dict(l=0, r=0, t=30, b=0),
            height=chart_height,
            autosize=True,
            legend=legend_cfg,
        )
        st.plotly_chart(fig, use_container_width=True)
    except Exception:
        st.info("Plotly not installed. Showing table instead (install: `pip install plotly`).")
        st.dataframe(pie_df, use_container_width=True, hide_index=True)

def _make_horiz_bar_chart(df, label_col, value_col):
    """
    Horizontal bar chart (Plotly) with cyan/teal bars and value labels.
    Used for the 'Total Purchase From Supplier' card.
    """
    if df is None or df.empty or label_col not in df.columns or value_col not in df.columns:
        st.info("📭 No data for chart.")
        return

    chart_df = df[[label_col, value_col]].copy()
    chart_df[value_col] = pd.to_numeric(chart_df[value_col], errors="coerce").fillna(0.0)

    try:
        import plotly.express as px  # type: ignore

        fig = px.bar(
            chart_df,
            x=value_col,
            y=label_col,
            orientation="h",
            text=chart_df[value_col].apply(lambda v: f"{v:.1f}"),
        )
        fig.update_traces(
            marker_color="rgba(124,92,252,0.75)",
            marker_line_color="rgba(0,0,0,0)",
            textposition="outside",
            textfont=dict(size=11, color="#64748B"),
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#64748B", family="Inter", size=12),
            xaxis=dict(showgrid=True, gridcolor="#F1F5F9", zeroline=False, tickfont=dict(size=10, color="#94A3B8")),
            yaxis=dict(
                categoryorder="array",
                categoryarray=chart_df[label_col].tolist(),
                showgrid=False,
                zeroline=False,
                tickfont=dict(size=10, color="#94A3B8"),
            ),
            margin=dict(l=10, r=60, t=10, b=10),
            height=max(200, len(chart_df) * 38 + 30),
        )
        st.plotly_chart(fig, use_container_width=True)
    except Exception:
        chart_df = chart_df.set_index(label_col)
        st.bar_chart(chart_df, y=value_col)

# --- Chart height constants ---
_CHART_HEIGHT_COMPACT = 320
_CHART_HEIGHT_FULLSCREEN = 500

# --- Chart color palette (modern light theme) ---
_CHART_PALETTE = ["#7C5CFC", "#10B981", "#F59E0B", "#EF4444", "#6366F1", "#06B6D4", "#F97316", "#8B5CF6", "#14B8A6", "#EC4899"]

# --- Column abbreviations for compact table views ---
_COL_ABBREV = {
    "Received Qty": "Rec Qty",
    "Purchase Value": "Pur Val",
    "Dispatched Qty": "Disp Qty",
    "Sales Value": "Sale Val",
    "Purchase Amount": "Pur Amt",
}

def _abbreviate_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Rename verbose column headers to short versions for table display."""
    return df.rename(columns={k: v for k, v in _COL_ABBREV.items() if k in df.columns})


@st.dialog("📊 Expanded View", width="large")
def _show_fullscreen_card(title: str, df: pd.DataFrame, label_col: str, value_col: str, chart_type: str, top_n: int, label_mode: str = "%"):
    """
    Fullscreen dialog for a dashboard card.
    Pie charts show the legend on the right; tables use abbreviated column names.
    """
    st.markdown(f"### {title}")
    if df is None or df.empty:
        st.info("📭 No data available.")
        return
    if chart_type == "Table":
        st.dataframe(_abbreviate_cols(df), use_container_width=True, hide_index=True)
    elif chart_type == "Bar Chart":
        if label_col == "Supplier":
            _make_horiz_bar_chart(df, label_col, value_col)
        else:
            _make_bar_chart(df, label_col, value_col)
    else:
        _make_pie_chart(df, label_col, value_col, top_n=top_n, show_legend=True, label_mode=label_mode)


# FIX 1: Added missing function definition line
def _init_card_state(card_id, default_sort="High → Low", default_topn=10, default_view="Quantity"):
    """
    Per-card settings stored in st.session_state.dash_cards[card_id]
    """
    if "dash_cards" not in st.session_state:
        st.session_state.dash_cards = {}

    if card_id not in st.session_state.dash_cards:
        st.session_state.dash_cards[card_id] = {
            "sort": default_sort,
            "topn": default_topn,
            "view": default_view,  # Quantity/Value (used by some cards)
            "chart_type": "Pie Chart",
            "label_mode": "%",
        }

def _card_controls(card_id: str, allow_view_mode: bool = False, allow_chart_type: bool = False):
    """
    Kebab menu per card (⋮): sort, item count, optional view mode, optional chart type.
    """
    _init_card_state(card_id)
    state = st.session_state.dash_cards[card_id]

    with st.popover("⋮", use_container_width=False):
        st.caption("⚙  CARD SETTINGS")
        state["sort"] = st.selectbox(
            "Sort order",
            options=["High → Low", "Low → High"],
            index=0 if state["sort"] == "High → Low" else 1,
            key=f"{card_id}_sort",
        )
        state["topn"] = st.selectbox(
            "Item count",
            options=[3, 5, 10, 25, 50, 100],
            index=[3, 5, 10, 25, 50, 100].index(state["topn"]) if state["topn"] in [3, 5, 10, 25, 50, 100] else 2,
            key=f"{card_id}_topn",
        )
        if allow_view_mode:
            state["view"] = st.selectbox(
                "View mode",
                options=["Quantity", "Value"],
                index=0 if state["view"] == "Quantity" else 1,
                key=f"{card_id}_view",
            )
        if allow_chart_type:
            _chart_opts = ["Pie Chart", "Table", "Bar Chart"]
            _chart_idx = _chart_opts.index(state["chart_type"]) if state["chart_type"] in _chart_opts else 0
            state["chart_type"] = st.selectbox(
                "Chart type",
                options=_chart_opts,
                index=_chart_idx,
                key=f"{card_id}_chart_type",
            )

        # Show label mode only when Pie Chart is selected
        _current_chart = st.session_state.get(f"{card_id}_chart_type", state["chart_type"])
        if allow_chart_type and _current_chart == "Pie Chart":
            _lm_opts = ["%", "Qty", "Amount"]
            _lm_idx = _lm_opts.index(state["label_mode"]) if state.get("label_mode") in _lm_opts else 0
            state["label_mode"] = st.selectbox(
                "Pie label",
                options=_lm_opts,
                index=_lm_idx,
                key=f"{card_id}_label_mode",
            )

        if st.button("🔄  Refresh", key=f"{card_id}_refresh_btn"):
            st.cache_data.clear()
            st.rerun()

    # Ensure the dict reflects latest widget keys
    state["sort"] = st.session_state.get(f"{card_id}_sort", state["sort"])
    state["topn"] = st.session_state.get(f"{card_id}_topn", state["topn"])
    if allow_view_mode:
        state["view"] = st.session_state.get(f"{card_id}_view", state["view"])
    if allow_chart_type:
        state["chart_type"] = st.session_state.get(f"{card_id}_chart_type", state["chart_type"])
    state["label_mode"] = st.session_state.get(f"{card_id}_label_mode", state.get("label_mode", "%"))

    st.session_state.dash_cards[card_id] = state
    return state

def _add_amount_col(df: pd.DataFrame, item_col: str, qty_col: str, meta_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds an 'Amount' column = qty_col × Price from meta_df.
    df must have `item_col` matching 'Product Name' in meta_df.
    """
    if df is None or df.empty:
        return df
    df = df.copy()
    if meta_df is not None and not meta_df.empty and "Price" in meta_df.columns:
        price_map = meta_df.dropna(subset=["Product Name", "Price"]).set_index("Product Name")["Price"].to_dict()
        df["Price"] = df[item_col].map(price_map).fillna(0.0)
        df["Price"] = pd.to_numeric(df["Price"], errors="coerce").fillna(0.0)
        df["Amount"] = (pd.to_numeric(df[qty_col], errors="coerce").fillna(0.0) * df["Price"]).round(2)
    else:
        df["Amount"] = 0.0
    return df


def _sum_purchase_from_logs(logs_filtered: pd.DataFrame, meta_df: pd.DataFrame):
    """
    Purchase inferred from activity_logs (received qty) * unit price.
    Supplier comes from product_metadata.
    """
    if logs_filtered is None or logs_filtered.empty:
        return pd.DataFrame(columns=["Supplier", "Purchase Amount"])

    if meta_df is None or meta_df.empty:
        return pd.DataFrame(columns=["Supplier", "Purchase Amount"])

    # Join logs Item -> meta Product Name
    join_df = pd.merge(
        logs_filtered.rename(columns={"Item": "Product Name"})[["Product Name", "Qty"]],
        meta_df[["Product Name", "Supplier", "Price", "Currency"]],
        on="Product Name",
        how="left",
    )
    join_df["Price"] = pd.to_numeric(join_df["Price"], errors="coerce").fillna(0.0)
    join_df["Qty"] = pd.to_numeric(join_df["Qty"], errors="coerce").fillna(0.0)
    join_df["Supplier"] = join_df["Supplier"].fillna("Unknown").astype(str).str.strip().replace("", "Unknown")

    join_df["Purchase Amount"] = (join_df["Qty"] * join_df["Price"]).fillna(0.0)

    out = (
        join_df.groupby("Supplier", as_index=False)["Purchase Amount"]
        .sum()
        .sort_values("Purchase Amount", ascending=False)
    )
    return out

def _build_master_template_xlsx() -> bytes:
    """Generate the downloadable Master Inventory Template as xlsx bytes."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        # Sheet 1: MASTER_TEMPLATE — headers only, no example rows
        template_cols = [
            "Product Name", "UOM", "Opening Stock", "Category",
            "Supplier", "Contact", "Email", "Lead Time", "Price", "Currency",
        ]
        pd.DataFrame(columns=template_cols).to_excel(
            writer, index=False, sheet_name="MASTER_TEMPLATE"
        )

        # Sheet 2: INSTRUCTIONS
        instructions = pd.DataFrame({
            "Instructions": [
                "• Required columns: Product Name, UOM, Opening Stock",
                "• Opening Stock must be numeric and >= 0",
                "• Do not rename or reorder column headers",
                "• One row = one product",
                "• Category defaults to 'General' if left empty",
                "• Currency defaults to 'USD' if left empty",
                "• Lead Time and Price must be numeric and >= 0 if provided",
                "• Product Name must be unique (case-insensitive) within the file",
            ]
        })
        instructions.to_excel(writer, index=False, sheet_name="INSTRUCTIONS")
    return buf.getvalue()


def _validate_master_template(df: pd.DataFrame):
    """
    Validate a parsed MASTER_TEMPLATE DataFrame.
    Returns (cleaned_df, errors_list).
    errors_list items: {"Row": int, "Product Name": str, "Error": str}
    """
    errors = []
    required_cols = {"Product Name", "UOM", "Opening Stock"}
    missing = required_cols - set(df.columns)
    if missing:
        errors.append({"Row": "-", "Product Name": "-", "Error": f"Missing required columns: {', '.join(sorted(missing))}"})
        return df, errors

    df = df.copy()

    # Trim string fields
    for col in ["Product Name", "UOM", "Category", "Supplier", "Contact", "Email", "Currency"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace({"nan": "", "None": ""})

    # Drop fully empty rows
    df = df[df["Product Name"].str.len() > 0].reset_index(drop=True)

    # Check for duplicates (case-insensitive)
    seen_names: dict[str, int] = {}
    for i, row in df.iterrows():
        name = row["Product Name"]
        name_lower = name.lower()
        if name_lower in seen_names:
            errors.append({"Row": i + 1, "Product Name": name, "Error": f"Duplicate of row {seen_names[name_lower] + 1}"})
        else:
            seen_names[name_lower] = i

    for i, row in df.iterrows():
        name = row["Product Name"]
        row_num = i + 1

        # UOM required
        uom = str(row.get("UOM", "")).strip()
        if not uom or uom in ("", "nan"):
            errors.append({"Row": row_num, "Product Name": name, "Error": "UOM is required"})

        # Opening Stock numeric >= 0
        opening = pd.to_numeric(row.get("Opening Stock"), errors="coerce")
        if pd.isna(opening) or opening < 0:
            errors.append({"Row": row_num, "Product Name": name, "Error": "Opening Stock must be numeric and >= 0"})

        # Price numeric >= 0 if provided
        price_raw = row.get("Price")
        if price_raw is not None and str(price_raw).strip() not in ("", "nan"):
            price_val = pd.to_numeric(price_raw, errors="coerce")
            if pd.isna(price_val) or price_val < 0:
                errors.append({"Row": row_num, "Product Name": name, "Error": "Price must be numeric and >= 0"})

        # Lead Time numeric >= 0 if provided
        lt_raw = row.get("Lead Time")
        if lt_raw is not None and str(lt_raw).strip() not in ("", "nan"):
            lt_val = pd.to_numeric(lt_raw, errors="coerce")
            if pd.isna(lt_val) or lt_val < 0:
                errors.append({"Row": row_num, "Product Name": name, "Error": "Lead Time must be numeric and >= 0"})

    # Apply defaults
    df["Category"] = df["Category"].apply(lambda v: "General" if (not v or v in ("nan", "")) else v)
    if "Currency" in df.columns:
        df["Currency"] = df["Currency"].apply(lambda v: "USD" if (not v or v in ("nan", "")) else str(v).upper())
    else:
        df["Currency"] = "USD"

    return df, errors


@st.dialog("📦 Bulk Upload", width="large")
def bulk_upload_modal():
    # ── Section A: Download Master Template ──────────────────────────────
    st.subheader("⬇️ Master Inventory Template")
    st.caption("Download, fill in your products, then upload below to initialize your inventory.")
    template_bytes = _build_master_template_xlsx()
    st.download_button(
        label="⬇️ Download Master Template",
        data=template_bytes,
        file_name="Master_Inventory_Template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        key="dl_master_template",
    )

    st.divider()

    # ── Section B: Upload + validate master template ──────────────────────
    st.subheader("📤 Upload Master Template")
    master_file = st.file_uploader("Upload Master Template", type=["xlsx"], key="master_upload_modal")

    if master_file:
        try:
            raw_df = pd.read_excel(master_file, sheet_name="MASTER_TEMPLATE")
        except Exception as e:
            st.error(f"Could not read MASTER_TEMPLATE sheet: {e}")
            return

        cleaned_df, errors = _validate_master_template(raw_df)

        if errors:
            st.error(f"❌ Validation failed — {len(errors)} error(s). Fix the file and re-upload.")
            st.dataframe(pd.DataFrame(errors), use_container_width=True, hide_index=True)
            return

        # Show count summary
        st.success(f"✅ Validation passed — {len(cleaned_df)} product(s) ready to import.")
        col_a, col_b = st.columns(2)
        with col_a:
            st.metric("Total Products", len(cleaned_df))
        with col_b:
            cats = cleaned_df["Category"].nunique() if "Category" in cleaned_df.columns else 0
            st.metric("Categories", cats)

        # Preview first 20 rows
        st.caption("Preview (first 20 rows):")
        st.dataframe(cleaned_df.head(20), use_container_width=True, hide_index=True)

        st.divider()

        # ── Section C: Confirm Import ─────────────────────────────────────
        org_id = st.session_state.get("org_id")
        location_id = st.session_state.get("location_id")

        if not org_id or not location_id:
            st.error("⚠️ org_id or location_id not found in session. Please log in and select a location first.")
            return

        if st.button("✅ Import Products", type="primary", use_container_width=True, key="import_master_btn"):
            # 1) Upsert product_metadata (all columns except Opening Stock)
            meta_cols = ["Product Name", "UOM", "Supplier", "Contact", "Email", "Category", "Lead Time", "Price", "Currency"]
            meta_df = cleaned_df[[c for c in meta_cols if c in cleaned_df.columns]].copy()
            # Fill missing optional columns with empty string / NaN
            for col in meta_cols:
                if col not in meta_df.columns:
                    meta_df[col] = None
            save_to_sheet(meta_df, "product_metadata", pk='org_id,"Product Name"')

            # 2) Build inventory rows — only for products that do NOT already exist
            existing_inv = load_from_sheet("persistent_inventory")
            if not existing_inv.empty and "Product Name" in existing_inv.columns:
                existing_names = set(existing_inv["Product Name"].astype(str).str.strip().str.lower())
            else:
                existing_names = set()

            new_rows_mask = cleaned_df["Product Name"].str.strip().str.lower().apply(lambda n: n not in existing_names)
            new_products_df = cleaned_df[new_rows_mask].copy()

            if new_products_df.empty:
                st.info("ℹ️ All products already exist in inventory — no new rows created. Metadata was updated.")
            else:
                inv_df = pd.DataFrame()
                inv_df["Product Name"] = new_products_df["Product Name"].values
                inv_df["UOM"] = new_products_df["UOM"].values
                inv_df["Opening Stock"] = pd.to_numeric(new_products_df["Opening Stock"], errors="coerce").fillna(0.0).values
                inv_df["Category"] = new_products_df["Category"].values
                for i in range(1, 32):
                    inv_df[str(i)] = 0.0
                inv_df["Total Received"] = 0.0
                inv_df["Consumption"] = 0.0
                inv_df["Closing Stock"] = inv_df["Opening Stock"]
                inv_df["Physical Count"] = None
                inv_df["Variance"] = 0.0

                save_to_sheet(inv_df, "persistent_inventory", pk='org_id,location_id,"Product Name"')
                st.success(f"✅ {len(inv_df)} new product(s) added to inventory.")

            st.cache_data.clear()
            st.rerun()

    st.divider()

    # ── Legacy: raw XLSX/CSV inventory sync ──────────────────────────────
    st.subheader("📦 Inventory Master Sync (Legacy)")
    inv_file = st.file_uploader("Upload XLSX/CSV", type=["csv", "xlsx"], key="inv_upload_modal")
    if inv_file:
        try:
            raw = pd.read_excel(inv_file, skiprows=4, header=None) if inv_file.name.endswith(".xlsx") else pd.read_csv(inv_file, skiprows=4, header=None)
            new_inv = pd.DataFrame()
            new_inv["Product Name"] = raw[1]
            new_inv["UOM"] = raw[2]
            new_inv["Opening Stock"] = pd.to_numeric(raw[3], errors="coerce").fillna(0.0)
            for i in range(1, 32):
                new_inv[str(i)] = 0.0
            new_inv["Total Received"] = 0.0
            new_inv["Consumption"] = 0.0
            new_inv["Closing Stock"] = new_inv["Opening Stock"]
            new_inv["Category"] = "General"
            if st.button("🚀 Push Inventory", type="primary", use_container_width=True, key="push_inv_modal"):
                save_to_sheet(new_inv.dropna(subset=["Product Name"]), "persistent_inventory")
                st.rerun()
        except Exception as e:
            st.error(f"Error: {e}")

    st.divider()

    st.subheader("📞 Supplier Metadata Sync (Legacy)")
    meta_file = st.file_uploader("Upload Product Data", type=["csv", "xlsx"], key="meta_upload_modal")
    if meta_file:
        try:
            new_meta = pd.read_excel(meta_file) if meta_file.name.endswith(".xlsx") else pd.read_csv(meta_file)
            if st.button("🚀 Push Metadata", type="primary", use_container_width=True, key="push_meta_modal"):
                save_to_sheet(new_meta, "product_metadata")
                st.rerun()
        except Exception as e:
            st.error(f"Error: {e}")

# --- INITIALIZATION ---
if "inventory" not in st.session_state:
    st.session_state.inventory = load_from_sheet("persistent_inventory")
if "log_page" not in st.session_state:
    st.session_state.log_page = 0

# --- COMPACT TOP TOOLBAR ---
# Inject CSS to style the toolbar row as a single purple bar
st.markdown(
    """
    <style>
    /* Purple toolbar wrapper */
    div[data-testid="stHorizontalBlock"]:has(> div[data-testid="stColumn"] .toolbar-title) {
        background: linear-gradient(135deg, #7C5CFC 0%, #6366F1 100%) !important;
        border-radius: 12px !important;
        padding: 6px 12px !important;
        box-shadow: 0 4px 20px rgba(124,92,252,0.25) !important;
        margin-bottom: 12px !important;
        align-items: center !important;
    }
    /* Make buttons inside toolbar white/translucent */
    div[data-testid="stHorizontalBlock"]:has(> div[data-testid="stColumn"] .toolbar-title) .stButton > button {
        background: rgba(255,255,255,0.18) !important;
        border: 1px solid rgba(255,255,255,0.30) !important;
        color: #ffffff !important;
        font-size: 12px !important;
        font-weight: 500 !important;
        padding: 6px 14px !important;
        border-radius: 8px !important;
        box-shadow: none !important;
        min-height: 36px !important;
    }
    div[data-testid="stHorizontalBlock"]:has(> div[data-testid="stColumn"] .toolbar-title) .stButton > button:hover {
        background: rgba(255,255,255,0.30) !important;
        border-color: rgba(255,255,255,0.50) !important;
        color: #ffffff !important;
        box-shadow: 0 2px 8px rgba(255,255,255,0.15) !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)
_tb0, _tb1, _tb2, _tb3 = st.columns([4, 1.2, 1.2, 1])
with _tb0:
    st.markdown(
        '<div class="toolbar-title" style="display:flex;align-items:center;gap:10px;padding:4px 0;">'
        '<span style="font-size:14px;font-weight:700;color:#fff;letter-spacing:0.06em;text-transform:uppercase;">'
        'Warehouse Pro Cloud</span>'
        '<span style="font-size:11px;color:rgba(255,255,255,0.60);font-weight:400;">v8.6</span>'
        '</div>',
        unsafe_allow_html=True,
    )
with _tb1:
    if st.button("📦 Bulk Upload", use_container_width=True, key="bulk_upload_btn"):
        try:
            bulk_upload_modal()
        except Exception:
            st.warning("Bulk upload modal not available.")
with _tb2:
    if st.button("🔄 Refresh Data", use_container_width=True, key="refresh_all"):
        try:
            if hasattr(st, "cache_data"):
                st.cache_data.clear()
        except Exception:
            pass
        safe_rerun()
with _tb3:
    if st.button("🔓 Logout", use_container_width=True, key="logout_btn"):
        st.session_state["_show_lss_fullscreen"] = False
        _logout_confirm_dialog()

# ===================== SIDEBAR =====================
with st.sidebar:
    st.markdown('<h2 class="sidebar-title">☁️ Data Management</h2>', unsafe_allow_html=True)

    st.divider()

    with st.expander("📦 Inventory Master Sync"):
        inv_file = st.file_uploader("Upload XLSX/CSV", type=["csv", "xlsx"], key="inv_upload")
        if inv_file:
            try:
                raw = pd.read_excel(inv_file, skiprows=4, header=None) if inv_file.name.endswith(".xlsx") else pd.read_csv(inv_file, skiprows=4, header=None)
                new_inv = pd.DataFrame()
                new_inv["Product Name"] = raw[1]
                new_inv["UOM"] = raw[2]
                new_inv["Opening Stock"] = pd.to_numeric(raw[3], errors="coerce").fillna(0.0)
                for i in range(1, 32):
                    new_inv[str(i)] = 0.0
                new_inv["Total Received"] = 0.0
                new_inv["Consumption"] = 0.0
                new_inv["Closing Stock"] = new_inv["Opening Stock"]
                new_inv["Category"] = "General"

                if st.button("🚀 Push Inventory", type="primary", use_container_width=True, key="push_inv"):
                    save_to_sheet(new_inv.dropna(subset=["Product Name"]), "persistent_inventory")
                    st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

    with st.expander("📞 Supplier Metadata Sync"):
        meta_file = st.file_uploader("Upload Product Data", type=["csv", "xlsx"], key="meta_upload")
        if meta_file:
            try:
                new_meta = pd.read_excel(meta_file) if meta_file.name.endswith(".xlsx") else pd.read_csv(meta_file)
                if st.button("🚀 Push Metadata", type="primary", use_container_width=True, key="push_meta"):
                    save_to_sheet(new_meta, "product_metadata")
                    st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

    st.markdown("<hr>", unsafe_allow_html=True)
    if st.button("🗑️ Clear Cache", use_container_width=True, key="clear_cache"):
        st.cache_data.clear()
        st.rerun()

tab_ops, tab_req, tab_sup, tab_dash, tab_restaurants = st.tabs(["📊 Operations", "🚚 Requisitions", "📞 Suppliers", "📊 Dashboard", "🍴 Restaurants"])

# --- LSS Formatting State & Style Helper ---
_LSS_DISP_COLS = ["Product Name", "Category", "UOM", "Opening Stock", "Total Received",
                  "Closing Stock", "Consumption", "Physical Count", "Variance",
                  "Price", "Total Amount"]
_LSS_NUM_COLS = ["Opening Stock", "Total Received", "Closing Stock", "Consumption",
                 "Physical Count", "Variance", "Price", "Total Amount"]

if "_lss_fmt" not in st.session_state:
    st.session_state["_lss_fmt"] = {
        "align": "right",
        "wrap": False,
        "rules": [],
    }

# Sort state: persists across expanded/shrunk views
if "_lss_sort" not in st.session_state:
    st.session_state["_lss_sort"] = {"col": None, "asc": True}

_LSS_TEXT_COLS = {"Product Name", "Category", "UOM"}


def _enrich_lss_with_price(df):
    """Merge Price from product_metadata and compute Total Amount = Price × Closing Stock."""
    _meta = load_from_sheet("product_metadata")
    if _meta is not None and not _meta.empty and "Price" in _meta.columns and "Product Name" in _meta.columns:
        price_map = _meta[["Product Name", "Price"]].drop_duplicates(subset="Product Name")
        price_map["Price"] = pd.to_numeric(price_map["Price"], errors="coerce").fillna(0.0)
        if "Price" in df.columns:
            df = df.drop(columns=["Price"])
        df = df.merge(price_map, on="Product Name", how="left")
        df["Price"] = df["Price"].fillna(0.0)
    else:
        if "Price" not in df.columns:
            df["Price"] = 0.0
    df["Closing Stock"] = pd.to_numeric(df["Closing Stock"], errors="coerce").fillna(0.0)
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce").fillna(0.0)
    df["Total Amount"] = (df["Price"] * df["Closing Stock"]).round(2)
    return df


def _apply_lss_sort(df):
    """Sort dataframe based on saved LSS sort state. Returns sorted copy."""
    sort = st.session_state.get("_lss_sort", {})
    col = sort.get("col")
    if col and col in df.columns:
        asc = sort.get("asc", True)
        return df.sort_values(by=col, ascending=asc, ignore_index=True)
    return df


def _lss_sort_bar(key_suffix=""):
    """Render a compact sort bar. Reads/writes _lss_sort in session state."""
    sort = st.session_state.get("_lss_sort", {"col": None, "asc": True})
    current_col = sort.get("col")
    current_asc = sort.get("asc", True)

    all_cols = list(_LSS_DISP_COLS)
    options = ["None"] + all_cols
    idx = 0
    if current_col and current_col in all_cols:
        idx = all_cols.index(current_col) + 1

    sc1, sc2, sc3 = st.columns([2.5, 2, 0.8])
    with sc1:
        picked = st.selectbox(
            "Sort by", options, index=idx,
            key=f"lss_sort_col_{key_suffix}", label_visibility="collapsed",
        )
    with sc2:
        if picked != "None":
            is_text = picked in _LSS_TEXT_COLS
            if is_text:
                dir_options = ["A → Z", "Z → A"]
                dir_idx = 0 if current_asc else 1
            else:
                dir_options = ["Ascending ↑", "Descending ↓"]
                dir_idx = 0 if current_asc else 1
            direction = st.selectbox(
                "Dir", dir_options, index=dir_idx,
                key=f"lss_sort_dir_{key_suffix}", label_visibility="collapsed",
            )
            new_asc = (direction == dir_options[0])
        else:
            st.markdown("<div style='height:1px;'></div>", unsafe_allow_html=True)
            new_asc = True
    with sc3:
        if st.button("🔃", key=f"lss_sort_apply_{key_suffix}", help="Apply sort"):
            new_col = None if picked == "None" else picked
            st.session_state["_lss_sort"] = {"col": new_col, "asc": new_asc}

_LSS_QUICK_RULES = [
    {"label": "🔴 Zero Closing Stock",   "col": "Closing Stock", "cond": "=",  "val": 0,  "bg": "#FFCDD2", "fc": "#B71C1C"},
    {"label": "🟡 Low Stock (< 5)",      "col": "Closing Stock", "cond": "<",  "val": 5,  "bg": "#FFF9C4", "fc": "#F57F17"},
    {"label": "🔴 Negative Variance",    "col": "Variance",      "cond": "<",  "val": 0,  "bg": "#FFCDD2", "fc": "#B71C1C"},
    {"label": "🟢 High Stock (> 50)",    "col": "Closing Stock", "cond": ">",  "val": 50, "bg": "#C8E6C9", "fc": "#1B5E20"},
    {"label": "🔵 High Consumption (>10)","col": "Consumption",  "cond": ">",  "val": 10, "bg": "#BBDEFB", "fc": "#0D47A1"},
    {"label": "🟠 No Receipts",          "col": "Total Received","cond": "=",  "val": 0,  "bg": "#FFE0B2", "fc": "#E65100"},
]


def _build_lss_html(df, cols, height=300):
    """Build an HTML table string with user-configured LSS formatting."""
    import html as _html
    fmt = st.session_state.get("_lss_fmt", {})
    align = fmt.get("align", "right")
    wrap = fmt.get("wrap", False)
    rules = fmt.get("rules", [])

    text_cols = {"Product Name", "Category", "UOM"}

    # Pre-compute per-cell styles
    def _cell_style(col_name, value):
        parts = []
        # Alignment
        if col_name in text_cols:
            parts.append("text-align:left")
        else:
            parts.append(f"text-align:{align}")
        # Wrap
        if wrap:
            parts.append("white-space:normal;word-wrap:break-word")
        # Conditional rules
        if col_name not in text_cols:
            try:
                nv = float(value)
                for r in rules:
                    if r.get("col") != col_name:
                        continue
                    cond = r.get("cond", ">")
                    rv = float(r.get("val", 0))
                    hit = (
                        (cond == ">"  and nv > rv)  or
                        (cond == ">=" and nv >= rv) or
                        (cond == "<"  and nv < rv)  or
                        (cond == "<=" and nv <= rv) or
                        (cond == "="  and nv == rv) or
                        (cond == "!=" and nv != rv)
                    )
                    if hit:
                        bg = r.get("bg", "")
                        fc = r.get("fc", "")
                        if bg:
                            parts.append(f"background-color:{bg}")
                        if fc:
                            parts.append(f"color:{fc}")
            except (ValueError, TypeError):
                pass
        return ";".join(parts)

    # Format value
    def _fmt_val(col_name, value):
        if col_name in text_cols:
            return _html.escape(str(value) if value is not None else "")
        try:
            return f"{float(value):.2f}"
        except (ValueError, TypeError):
            return _html.escape(str(value) if value is not None else "0.00")

    # Build HTML
    sort = st.session_state.get("_lss_sort", {})
    sort_col = sort.get("col")
    sort_asc = sort.get("asc", True)

    h = []
    h.append(f'<div style="max-height:{height}px;overflow:auto;border:1px solid var(--border);border-radius:10px;">')
    h.append('<table style="width:100%;border-collapse:collapse;font-size:13px;font-family:Inter,sans-serif;">')
    # Header
    h.append('<thead><tr>')
    for c in cols:
        ta = "left" if c in text_cols else align
        # Sort indicator
        indicator = ""
        if c == sort_col:
            indicator = " ↑" if sort_asc else " ↓"
            indicator = f"<span style='color:var(--accent);font-size:11px;'>{indicator}</span>"
        h.append(
            f'<th style="position:sticky;top:0;z-index:1;background:#F1F5F9;padding:8px 10px;'
            f'text-align:{ta};font-weight:600;font-size:12px;color:var(--muted);'
            f'border-bottom:2px solid var(--border);white-space:nowrap;">'
            f'{_html.escape(c)}{indicator}</th>'
        )
    h.append('</tr></thead>')
    # Body
    h.append('<tbody>')
    for row_idx in range(len(df)):
        bg_row = "#FFFFFF" if row_idx % 2 == 0 else "#F8FAFC"
        h.append(f'<tr style="background:{bg_row};">')
        for c in cols:
            val = df[c].iloc[row_idx]
            style = _cell_style(c, val)
            display = _fmt_val(c, val)
            h.append(f'<td style="padding:7px 10px;border-bottom:1px solid #E2E8F0;{style}">{display}</td>')
        h.append('</tr>')
    # Footer: Total Amount sum
    if "Total Amount" in cols:
        try:
            grand_total = df["Total Amount"].astype(float).sum()
        except Exception:
            grand_total = 0.0
        ta_idx = cols.index("Total Amount")
        h.append('<tr style="background:#F1F5F9;border-top:2px solid var(--border);">')
        for ci, c in enumerate(cols):
            if ci == ta_idx - 1:
                h.append(
                    f'<td style="padding:8px 10px;font-weight:700;font-size:12px;color:var(--text);'
                    f'text-align:right;border-top:2px solid var(--border);">Total =</td>'
                )
            elif ci == ta_idx:
                h.append(
                    f'<td style="padding:8px 10px;font-weight:700;font-size:13px;color:var(--accent);'
                    f'text-align:{align};border-top:2px solid var(--border);'
                    f'font-family:JetBrains Mono,monospace;">{grand_total:,.2f}</td>'
                )
            else:
                h.append(f'<td style="padding:8px 10px;border-top:2px solid var(--border);"></td>')
        h.append('</tr>')
    h.append('</tbody></table></div>')
    return "".join(h)


def _render_lss_table(df, cols, height=300, key_suffix=""):
    """Render sort bar + LSS formatted HTML table via st.markdown."""
    _lss_sort_bar(key_suffix=key_suffix)
    sorted_df = _apply_lss_sort(df)
    html_str = _build_lss_html(sorted_df, cols, height=height)
    st.markdown(html_str, unsafe_allow_html=True)


# --- Fullscreen dialog for Live Stock Status ---
@st.dialog("📊 Live Stock Status", width="large")
def _lss_fullscreen_dialog():
    _df = st.session_state.get("inventory")
    if _df is None or _df.empty:
        st.info("No inventory data available.")
        return
    _df = _df.copy()
    _df = _enrich_lss_with_price(_df)
    for c in _LSS_DISP_COLS:
        if c not in _df.columns:
            _df[c] = 0.0

    # Work with a pending copy so changes don't require rerun
    if "_lss_fmt_pending" not in st.session_state:
        st.session_state["_lss_fmt_pending"] = dict(st.session_state.get("_lss_fmt", {}))
        st.session_state["_lss_fmt_pending"]["rules"] = list(
            st.session_state.get("_lss_fmt", {}).get("rules", [])
        )

    pending = st.session_state["_lss_fmt_pending"]
    _rules = pending.get("rules", [])

    # ── Formatting Panel ──
    with st.expander("🎨 Format & Style", expanded=False):
        # Row 1: Alignment + Wrap + Quick Rules all on one line concept
        _r1c1, _r1c2, _r1c3, _r1c4 = st.columns([1.5, 1, 1.5, 1.5])
        with _r1c1:
            _align = st.selectbox(
                "Align", ["left", "center", "right"],
                index=["left", "center", "right"].index(pending.get("align", "right")),
                key="fs_align",
            )
            pending["align"] = _align
        with _r1c2:
            st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
            _wrap = st.checkbox("Wrap", value=pending.get("wrap", False), key="fs_wrap")
            pending["wrap"] = _wrap
        with _r1c3:
            if st.button("✅ Apply", key="fs_apply", use_container_width=True, type="primary"):
                st.session_state["_lss_fmt"] = {
                    "align": pending.get("align", "right"),
                    "wrap": pending.get("wrap", False),
                    "rules": list(pending.get("rules", [])),
                }
                st.toast("✅ Formatting applied!", icon="🎨")
        with _r1c4:
            if st.button("🗑️ Clear All", key="fs_clear_all", use_container_width=True):
                pending["align"] = "right"
                pending["wrap"] = False
                pending["rules"] = []
                _rules.clear()
                st.session_state["_lss_fmt"] = {"align": "right", "wrap": False, "rules": []}
                st.toast("🗑️ All formatting cleared!")

        # ── Quick Rules: 6 buttons in one row ──
        st.markdown(
            "<div style='font-size:11px;color:var(--muted);font-weight:600;margin:6px 0 2px;'>⚡ QUICK RULES</div>",
            unsafe_allow_html=True,
        )
        _q1, _q2, _q3, _q4, _q5, _q6 = st.columns(6)
        for qi, (_qcol, qr) in enumerate(zip([_q1, _q2, _q3, _q4, _q5, _q6], _LSS_QUICK_RULES)):
            with _qcol:
                if st.button(qr["label"], key=f"qr_{qi}", use_container_width=True):
                    _rules.append({k: qr[k] for k in ("col", "cond", "val", "bg", "fc")})
                    pending["rules"] = _rules

        # ── Active Rules: compact HTML chips + remove control ──
        if _rules:
            st.markdown(
                "<div style='font-size:11px;color:var(--muted);font-weight:600;margin:8px 0 4px;'>📋 ACTIVE RULES</div>",
                unsafe_allow_html=True,
            )
            _rule_chips_html = []
            for idx, r in enumerate(_rules):
                _rbg = r.get("bg", "#eee")
                _rfc = r.get("fc", "#000")
                _rule_chips_html.append(
                    f"<span style='display:inline-flex;align-items:center;gap:5px;background:#F8FAFC;"
                    f"border:1px solid #CBD5E1;border-radius:8px;padding:4px 10px;font-size:11px;"
                    f"margin:2px 4px 2px 0;white-space:nowrap;'>"
                    f"<b>{r['col']}</b> {r['cond']} {r['val']}"
                    f" <span style='width:12px;height:12px;border-radius:3px;background:{_rbg};"
                    f"border:1px solid #aaa;display:inline-block;'></span>"
                    f" <span style='width:12px;height:12px;border-radius:3px;background:{_rfc};"
                    f"border:1px solid #aaa;display:inline-block;'></span>"
                    f" <span style='color:#EF4444;font-weight:700;font-size:13px;cursor:default;margin-left:2px;'>✕</span>"
                    f"</span>"
                )
            st.markdown(
                "<div style='display:flex;flex-wrap:wrap;'>" + "".join(_rule_chips_html) + "</div>",
                unsafe_allow_html=True,
            )
            # Compact remove row
            _rm_labels = [f"#{i+1} {r['col']} {r['cond']} {r['val']}" for i, r in enumerate(_rules)]
            _rmc1, _rmc2 = st.columns([4, 1])
            with _rmc1:
                _rm_pick = st.selectbox("Remove", _rm_labels, key="fs_rm_pick", label_visibility="collapsed")
            with _rmc2:
                if st.button("🗑️", key="fs_rm_btn", help="Remove selected rule"):
                    _rm_idx = _rm_labels.index(_rm_pick)
                    _rules.pop(_rm_idx)
                    pending["rules"] = _rules

        # ── Custom Rule: single compact row ──
        st.markdown(
            "<div style='font-size:11px;color:var(--muted);font-weight:600;margin:8px 0 2px;'>➕ CUSTOM RULE</div>",
            unsafe_allow_html=True,
        )
        nc1, nc2, nc3, nc4, nc5, nc6 = st.columns([2, 1.5, 1.2, 0.8, 0.8, 1])
        with nc1:
            _new_col = st.selectbox("Column", _LSS_NUM_COLS, key="fs_new_col", label_visibility="collapsed")
        with nc2:
            _new_cond = st.selectbox("Cond", [">", ">=", "<", "<=", "=", "!="], key="fs_new_cond", label_visibility="collapsed")
        with nc3:
            _new_val = st.number_input("Val", value=0.0, step=1.0, key="fs_new_val", label_visibility="collapsed")
        with nc4:
            _new_bg = st.color_picker("BG", "#FFCDD2", key="fs_new_bg")
        with nc5:
            _new_fc = st.color_picker("FC", "#B71C1C", key="fs_new_fc")
        with nc6:
            if st.button("➕ Add", key="fs_add_rule", use_container_width=True):
                _rules.append({"col": _new_col, "cond": _new_cond, "val": _new_val, "bg": _new_bg, "fc": _new_fc})
                pending["rules"] = _rules

    # ── Formatted table (uses saved/applied formatting) ──
    _render_lss_table(_df, _LSS_DISP_COLS, height=500, key_suffix="fs")

    if st.button("Close", key="close_lss_fs", use_container_width=True):
        # Clean up pending state
        if "_lss_fmt_pending" in st.session_state:
            del st.session_state["_lss_fmt_pending"]
        st.session_state["_show_lss_fullscreen"] = False
        st.rerun()


if st.session_state.get("_show_lss_fullscreen"):
    try:
        _lss_fullscreen_dialog()
    except Exception:
        pass
    # Clear flag after dialog closes (handles both X button and Close button)
    st.session_state["_show_lss_fullscreen"] = False

# ===================== OPERATIONS TAB =====================
with tab_ops:
    col_receipt_main, col_quick_main = st.columns([3, 1])

    with col_receipt_main:
        st.markdown('<span class="section-title">📥 Daily Receipt Portal</span>', unsafe_allow_html=True)
        if not st.session_state.inventory.empty:
            c1, c2, c3, c4 = st.columns([2, 0.8, 0.8, 1])
            with c1:
                sel_item = st.selectbox(
                    "🔍 Item",
                    options=[""] + sorted(st.session_state.inventory["Product Name"].unique().tolist()),
                    key="receipt_item",
                    label_visibility="collapsed",
                )
            with c2:
                day_in = st.number_input("Day", 1, 31, datetime.datetime.now().day, key="receipt_day", label_visibility="collapsed")
            with c3:
                qty_in = st.number_input("Qty", min_value=0.0, key="receipt_qty", label_visibility="collapsed")
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
            if st.button("➕ Item", use_container_width=True, help="New Product", key="btn_add_item"):
                add_item_modal()
        with ac2:
            if st.button("🗂️ Cat", use_container_width=True, help="Manage Categories", key="btn_add_cat"):
                manage_categories_modal()
        with ac3:
            if st.button("📂 Exp", use_container_width=True, help="Explorer", key="btn_exp"):
                archive_explorer_modal()
        with ac4:
            if st.button("🔒 Close", use_container_width=True, type="primary", help="Close Month", key="btn_close"):
                close_month_modal()

    st.markdown("<hr>", unsafe_allow_html=True)

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
                is_undone = row.get("Status", "") == "Undone"
                row_class = "log-row-undone" if is_undone else ""

                c_row = st.container()
                c_txt, c_undo = c_row.columns([4, 1])
                with c_txt:
                    h_item, h_qty, h_day, h_time = row.get("Item", ""), row.get("Qty", ""), row.get("Day", ""), row.get("Timestamp", "")
                    l_html = (
                        f'<div class="log-row {row_class}"><div class="log-info"><b>{h_item}</b><br>'
                        f'{h_qty} | D{h_day} <span class="log-time">{h_time}</span></div></div>'
                    )
                    st.markdown(l_html, unsafe_allow_html=True)
                with c_undo:
                    if (not is_undone) and str(row.get("LogID", "")).strip():
                        if st.button("↩", key=f"rev_{row['LogID']}", use_container_width=True):
                            undo_entry(row["LogID"])
            st.markdown("</div>", unsafe_allow_html=True)

            p_prev, p_next = st.columns(2)
            with p_prev:
                if st.button("◀", disabled=st.session_state.log_page == 0, use_container_width=True, key="log_prev"):
                    st.session_state.log_page -= 1
                    st.rerun()
            with p_next:
                if st.button("▶", disabled=st.session_state.log_page >= total_pages - 1, use_container_width=True, key="log_next"):
                    st.session_state.log_page += 1
                    st.rerun()
        else:
            st.caption("📭 No logs.")

    with stat_col:
        _lss_title_col, _lss_btn_col = st.columns([8, 1])
        with _lss_title_col:
            st.markdown('<span class="section-title">📊 Live Stock Status</span>', unsafe_allow_html=True)
        with _lss_btn_col:
            if st.button("⛶", key="expand_lss", help="Expand fullscreen"):
                st.session_state["_show_lss_fullscreen"] = True
                st.rerun()
        df_status = st.session_state.inventory.copy()
        df_status = _enrich_lss_with_price(df_status)
        disp_cols = ["Product Name", "Category", "UOM", "Opening Stock", "Total Received", "Closing Stock", "Consumption", "Physical Count", "Variance", "Price", "Total Amount"]
        for col in disp_cols:
            if col not in df_status.columns:
                df_status[col] = 0.0

        _has_fmt_rules = bool(st.session_state.get("_lss_fmt", {}).get("rules"))

        # Sort bar (always visible)
        _lss_sort_bar(key_suffix="sm")
        _sorted_status = _apply_lss_sort(df_status)

        if _has_fmt_rules:
            # Show formatted read-only HTML view (sort bar already shown above, skip inner one)
            html_str = _build_lss_html(_sorted_status, disp_cols, height=300)
            st.markdown(html_str, unsafe_allow_html=True)
            edited_df = df_status[disp_cols]
        else:
            # No formatting rules — show editable data_editor (sorted)
            edited_df = st.data_editor(
                _sorted_status[disp_cols],
                height=300,
                use_container_width=True,
                disabled=["Product Name", "Category", "UOM", "Total Received", "Closing Stock", "Variance", "Price", "Total Amount"],
                hide_index=True,
            )

        sc1, sc2, sc3 = st.columns(3)
        with sc1:
            if st.button("💾 Update Stock", use_container_width=True, type="primary", key="update_stock"):
                df_status.update(edited_df)
                for item in df_status["Product Name"]:
                    df_status = recalculate_item(df_status, item)
                save_to_sheet(df_status, "persistent_inventory")
                st.rerun()
        with sc2:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
                _exp_df = df_status[disp_cols].copy()
                # Append grand total row
                _total_row = {c: "" for c in disp_cols}
                _total_row["Price"] = "Total ="
                try:
                    _total_row["Total Amount"] = round(pd.to_numeric(_exp_df["Total Amount"], errors="coerce").fillna(0).sum(), 2)
                except Exception:
                    _total_row["Total Amount"] = 0.0
                _exp_df = pd.concat([_exp_df, pd.DataFrame([_total_row])], ignore_index=True)
                _exp_df.to_excel(writer, index=False, sheet_name="Summary")
            st.download_button("📥 Summary", data=buf.getvalue(), file_name="Summary.xlsx", use_container_width=True, key="dl_summary")
        with sc3:
            day_cols = [str(i) for i in range(1, 32)]
            existing_day_cols = [col for col in day_cols if col in df_status.columns]
            full_cols = ["Product Name", "Category", "UOM", "Opening Stock"] + existing_day_cols + [
                "Total Received",
                "Consumption",
                "Closing Stock",
                "Physical Count",
                "Variance",
                "Price",
                "Total Amount",
            ]
            full_cols = [col for col in full_cols if col in df_status.columns]

            if full_cols:
                buf_f = io.BytesIO()
                with pd.ExcelWriter(buf_f, engine="xlsxwriter") as writer:
                    _exp_full = df_status[full_cols].copy()
                    # Append grand total row
                    _total_row_f = {c: "" for c in full_cols}
                    if "Price" in full_cols:
                        _total_row_f["Price"] = "Total ="
                    if "Total Amount" in full_cols:
                        try:
                            _total_row_f["Total Amount"] = round(pd.to_numeric(_exp_full["Total Amount"], errors="coerce").fillna(0).sum(), 2)
                        except Exception:
                            _total_row_f["Total Amount"] = 0.0
                    _exp_full = pd.concat([_exp_full, pd.DataFrame([_total_row_f])], ignore_index=True)
                    _exp_full.to_excel(writer, index=False, sheet_name="Details")
                st.download_button("📂 Details", data=buf_f.getvalue(), file_name="Full_Report.xlsx", use_container_width=True, key="dl_details")
            else:
                st.warning("⚠️ No data columns available for export")

    with st.expander("📈 Weekly Par Analysis", expanded=False):
        df_hist = load_from_sheet("monthly_history")
        if not df_hist.empty and not st.session_state.inventory.empty:
            df_hist["Consumption"] = pd.to_numeric(df_hist["Consumption"], errors="coerce").fillna(0)
            avg_cons = df_hist.groupby("Product Name")["Consumption"].mean().reset_index()
            df_par = pd.merge(
                st.session_state.inventory[["Product Name", "UOM", "Closing Stock"]],
                avg_cons,
                on="Product Name",
                how="left",
            ).fillna(0)
            df_par["Weekly Usage"] = (df_par["Consumption"] / 4.33).round(2)
            df_par["Min (50%)"] = (df_par["Weekly Usage"] * 0.5).round(2)
            df_par["Max (150%)"] = (df_par["Weekly Usage"] * 1.5).round(2)
            st.dataframe(df_par, use_container_width=True, hide_index=True, height=250)
        else:
            st.info("Historical data required.")

# ===================== REQUISITIONS TAB =====================
with tab_req:
    st.markdown('<span class="section-title">🚚 Restaurant Requisitions</span>', unsafe_allow_html=True)

    if st.button("🔄 Refresh Requisitions", use_container_width=True, key="refresh_reqs"):
        st.cache_data.clear()
        st.rerun()

    all_reqs = load_from_sheet(
        "restaurant_requisitions",
        ["ReqID", "Restaurant", "Item", "Qty", "Status", "DispatchQty", "Timestamp", "RequestedDate", "FollowupSent"],
    )

    if not all_reqs.empty:
        if "FollowupSent" not in all_reqs.columns:
            all_reqs["FollowupSent"] = False

        status_filter = st.selectbox("Filter by Status", ["All", "Pending", "Dispatched", "Completed"], key="req_status_filter", label_visibility="collapsed")

        display_reqs = all_reqs if status_filter == "All" else all_reqs[all_reqs["Status"] == status_filter]

        if not display_reqs.empty:
            display_reqs = display_reqs.copy()
            display_reqs["RequestedDate"] = pd.to_datetime(display_reqs["RequestedDate"], errors="coerce")
            display_reqs = display_reqs[display_reqs["RequestedDate"].notna()]

            if not display_reqs.empty:
                display_reqs = display_reqs.sort_values("RequestedDate", ascending=False)
                unique_dates = sorted(display_reqs["RequestedDate"].unique(), reverse=True)

                for req_date in unique_dates:
                    try:
                        date_str = pd.Timestamp(req_date).strftime("%d/%m/%Y")
                    except Exception:
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
                                submitted_by_email = row.get("submitted_by_email", "") or ""

                                stock_info = st.session_state.inventory[st.session_state.inventory["Product Name"] == item_name]
                                available_qty = float(stock_info["Closing Stock"].values[0]) if not stock_info.empty else 0.0

                                status_color = "🟡" if status == "Pending" else "🟠" if status == "Dispatched" else "🔵"
                                followup_text = " ⚠️" if followup_sent else ""
                                submitter_text = f" | 👤 {submitted_by_email}" if submitted_by_email else ""

                                st.markdown(
                                    f"""
                                    <div class="req-box">
                                        <b>{status_color} {item_name}</b> | Req:{req_qty} | Got:{dispatch_qty} | Rem:{remaining_qty} | Avail:{available_qty}{followup_text}{submitter_text}
                                    </div>
                                    """,
                                    unsafe_allow_html=True,
                                )

                                if status == "Pending":
                                    c1, c2, c3 = st.columns([2, 1, 1])
                                    with c1:
                                        default_dispatch = min(req_qty, available_qty)
                                        dispatch_qty_input = st.number_input(
                                            "Dispatch",
                                            min_value=0.0,
                                            max_value=available_qty,
                                            value=default_dispatch,
                                            key=f"dispatch_{req_id}",
                                            label_visibility="collapsed",
                                        )
                                    with c2:
                                        if st.button("🚀 Dispatch", key=f"dispatch_btn_{req_id}", use_container_width=True):
                                            if dispatch_qty_input > 0:
                                                all_reqs.at[idx, "DispatchQty"] = dispatch_qty_input
                                                all_reqs.at[idx, "Status"] = "Dispatched"
                                                all_reqs.at[idx, "Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                                # Update warehouse consumption
                                                inv_df = st.session_state.inventory
                                                inv_match = inv_df[inv_df["Product Name"] == item_name]
                                                if not inv_match.empty:
                                                    inv_idx = inv_match.index[0]
                                                    current_consumption = pd.to_numeric(inv_df.at[inv_idx, "Consumption"], errors="coerce") or 0.0
                                                    inv_df.at[inv_idx, "Consumption"] = current_consumption + dispatch_qty_input
                                                    inv_df = recalculate_item(inv_df, item_name)
                                                    st.session_state.inventory = inv_df
                                                    save_to_sheet(inv_df, "persistent_inventory")
                                                if save_to_sheet(all_reqs, "restaurant_requisitions"):
                                                    st.success(f"✅ Dispatched {dispatch_qty_input}")
                                                    st.cache_data.clear()
                                                    st.rerun()
                                    with c3:
                                        if st.button("❌ Cancel", key=f"cancel_btn_{req_id}", use_container_width=True):
                                            all_reqs = all_reqs.drop(idx)
                                            save_to_sheet(all_reqs, "restaurant_requisitions")
                                            st.warning("❌ Cancelled")
                                            st.rerun()

                                elif status == "Dispatched":
                                    if remaining_qty > 0:
                                        c1, c2, c3 = st.columns([2, 1, 1])
                                        with c1:
                                            additional_dispatch = st.number_input(
                                                "Additional Dispatch",
                                                min_value=0.0,
                                                max_value=min(remaining_qty, available_qty),
                                                value=min(remaining_qty, available_qty),
                                                key=f"add_dispatch_{req_id}",
                                                label_visibility="collapsed",
                                            )
                                        with c2:
                                            if st.button("🚀 Send More", key=f"add_dispatch_btn_{req_id}", use_container_width=True):
                                                if additional_dispatch > 0:
                                                    new_total_dispatch = dispatch_qty + additional_dispatch
                                                    all_reqs.at[idx, "DispatchQty"] = new_total_dispatch
                                                    new_remaining = req_qty - new_total_dispatch
                                                    if new_remaining <= 0:
                                                        all_reqs.at[idx, "Status"] = "Completed"
                                                    all_reqs.at[idx, "Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                                    # Update warehouse consumption
                                                    inv_df = st.session_state.inventory
                                                    inv_match = inv_df[inv_df["Product Name"] == item_name]
                                                    if not inv_match.empty:
                                                        inv_idx = inv_match.index[0]
                                                        current_consumption = pd.to_numeric(inv_df.at[inv_idx, "Consumption"], errors="coerce") or 0.0
                                                        inv_df.at[inv_idx, "Consumption"] = current_consumption + additional_dispatch
                                                        inv_df = recalculate_item(inv_df, item_name)
                                                        st.session_state.inventory = inv_df
                                                        save_to_sheet(inv_df, "persistent_inventory")
                                                    if save_to_sheet(all_reqs, "restaurant_requisitions"):
                                                        st.success(f"✅ Dispatched additional {additional_dispatch}")
                                                        st.cache_data.clear()
                                                        st.rerun()
                                        with c3:
                                            if st.button("🚩 Follow-up", key=f"followup_{idx}", use_container_width=True):
                                                all_reqs.at[idx, "FollowupSent"] = True
                                                all_reqs.at[idx, "Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                                save_to_sheet(all_reqs, "restaurant_requisitions")
                                                st.success("✅ Follow-up sent!")
                                                st.rerun()
                                    else:
                                        st.caption(f"✅ Fully Dispatched: {dispatch_qty}")
            else:
                st.info("📭 No valid dates found in requisitions")
        else:
            st.info(f"📭 No {status_filter.lower()} requisitions found")
    else:
        st.info("📭 No requisitions yet")

# ===================== SUPPLIERS TAB =====================
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
        filtered = meta[
            ~meta["Product Name"].str.startswith("CATEGORY_", na=False)
            & ~meta["Product Name"].str.startswith("SUPPLIER_", na=False)
        ]
        if search:
            filtered = filtered[
                filtered["Product Name"].str.lower().str.contains(search.lower(), na=False)
                | filtered["Supplier"].str.lower().str.contains(search.lower(), na=False)
            ]
    else:
        filtered = meta

    if not filtered.empty:
        display_cols = [
            "Product Name",
            "Category",
            "Supplier",
            "Contact",
            "Email",
            "Price",
            "Currency",
            "Lead Time",
            "UOM",
            "Min Stock",
            "Reorder Qty",
            "Min Safety Stock",
        ]
        available_cols = [col for col in display_cols if col in filtered.columns]
        filtered_display = filtered[available_cols]
    else:
        filtered_display = filtered

    edited_meta = st.data_editor(filtered_display, num_rows="dynamic", use_container_width=True, hide_index=True, height=400, key="sup_editor")
    if st.button("💾 Save Directory", use_container_width=True, type="primary", key="save_sup_dir"):
        save_to_sheet(edited_meta, "product_metadata")
        st.rerun()

# ===================== DASHBOARD TAB =====================
with tab_dash:
    st.markdown('<div style="text-align:center;"><span class="dash-title-pill">📊 Warehouse Dashboard</span></div>', unsafe_allow_html=True)

    # Hardcoded defaults (filters removed from UI; each card has its own kebab controls)
    dashboard_view = "Pie Charts"
    restaurant_filter = "All"
    currency_choice = "All"
    legacy_top_n = 10
    dispatch_date_basis = "RequestedDate"

    # --- Single control row: From | To | Quick preset | Refresh | Export ---
    today = datetime.date.today()
    _QUICK_OPTIONS = ["last 2 days", "last 7 days", "last 14 days", "last 30 days", "last 90 days", "Custom"]
    _QUICK_DAYS = {"last 2 days": 2, "last 7 days": 7, "last 14 days": 14, "last 30 days": 30, "last 90 days": 90}

    d1, d2, d3, d4, d5 = st.columns([1.4, 1.4, 1.4, 0.8, 1.2])
    with d3:
        quick_preset = st.selectbox(
            "Quick",
            options=_QUICK_OPTIONS,
            index=3,
            key="dash_quick_preset",
            label_visibility="collapsed",
        )
    # When a non-Custom preset is selected, programmatically update the start date in session state
    if quick_preset != "Custom":
        _preset_days = _QUICK_DAYS[quick_preset]
        st.session_state["dash_start"] = today - datetime.timedelta(days=_preset_days)
        st.session_state["dash_end"] = today

    with d1:
        start_date = st.date_input("From", value=today - datetime.timedelta(days=30), key="dash_start", label_visibility="collapsed")
    with d2:
        end_date = st.date_input("To", value=today, key="dash_end", label_visibility="collapsed")
    with d4:
        if st.button("🔄 Refresh", use_container_width=True, key="dash_refresh"):
            st.cache_data.clear()
            st.rerun()
    with d5:
        # Export button placeholder — actual download button rendered after data is computed below
        export_placeholder = st.empty()


    if start_date > end_date:
        st.warning("⚠️ Start date is after end date. Please fix the date range.")
    else:
        start_date = pd.to_datetime(start_date).date()
        end_date = pd.to_datetime(end_date).date()

        # Data
        inv_df = _prepare_inventory(load_from_sheet("persistent_inventory"))
        meta_df = _prepare_metadata()
        req_df = _prepare_reqs(load_from_sheet("restaurant_requisitions"))
        log_df = _prepare_logs(load_from_sheet("activity_logs"))

        meta_cur = _currency_filtered_meta(meta_df, currency_choice)

        meta_all = meta_df.copy() if meta_df is not None else pd.DataFrame()
        if meta_all is None or meta_all.empty:
            meta_all = pd.DataFrame(columns=["Product Name", "UOM", "Category", "Price", "Currency", "Supplier"])

        _inv_for_merge = inv_df if (inv_df is not None and not inv_df.empty) else pd.DataFrame(columns=["Product Name", "Category", "Closing Stock", "UOM"])
        inv_join = pd.merge(
            _inv_for_merge,
            meta_all[["Product Name", "Category", "UOM"]].drop_duplicates("Product Name"),
            on="Product Name",
            how="left",
        )
        inv_join = pd.merge(
            inv_join,
            (
                meta_cur[["Product Name", "Price", "Currency"]].drop_duplicates("Product Name")
                if meta_cur is not None and not meta_cur.empty
                else pd.DataFrame(columns=["Product Name", "Price", "Currency"])
            ),
            on="Product Name",
            how="left",
        )

        if "UOM_x" in inv_join.columns and "UOM_y" in inv_join.columns:
            inv_join["UOM"] = inv_join["UOM_y"].fillna("").astype(str).str.strip()
            inv_join.loc[inv_join["UOM"] == "", "UOM"] = inv_join["UOM_x"].fillna("").astype(str).str.strip()
            inv_join = inv_join.drop(columns=["UOM_x", "UOM_y"])
        else:
            if "UOM" not in inv_join.columns:
                inv_join["UOM"] = ""

        if "Category_x" in inv_join.columns and "Category_y" in inv_join.columns:
            inv_join["Category"] = inv_join["Category_y"].fillna("General").astype(str).str.strip()
            inv_join.loc[inv_join["Category"] == "", "Category"] = inv_join["Category_x"].fillna("General").astype(str).str.strip()
            inv_join = inv_join.drop(columns=["Category_x", "Category_y"])

        inv_join["Price"] = pd.to_numeric(inv_join.get("Price", 0), errors="coerce").fillna(0.0)
        inv_join["Closing Stock"] = pd.to_numeric(inv_join.get("Closing Stock", 0), errors="coerce").fillna(0.0)
        inv_join["Stock Value"] = (inv_join["Closing Stock"] * inv_join["Price"]).round(2)

        # Requisitions filter
        req_filtered = req_df.copy() if req_df is not None and not req_df.empty else pd.DataFrame(
            columns=["Restaurant", "Item", "Qty", "DispatchQty", "Status", "RequestedDate", "DispatchTS_Date"]
        )
        if not req_filtered.empty:
            if restaurant_filter != "All":
                req_filtered = req_filtered[req_filtered["Restaurant"] == restaurant_filter]

            date_col = "DispatchTS_Date" if dispatch_date_basis == "Dispatch Timestamp" else "RequestedDate"
            req_filtered = req_filtered[req_filtered[date_col].notna()]
            req_filtered = req_filtered[(req_filtered[date_col] >= start_date) & (req_filtered[date_col] <= end_date)]

        # Logs filter (Received)
        logs_filtered = log_df.copy() if log_df is not None and not log_df.empty else pd.DataFrame(columns=["Item", "Qty", "Status", "LogDateParsed"])
        if not logs_filtered.empty:
            logs_filtered = logs_filtered[logs_filtered["Status"] == "Active"]
            logs_filtered = logs_filtered[logs_filtered["LogDateParsed"].notna()]
            logs_filtered = logs_filtered[
                (logs_filtered["LogDateParsed"] >= start_date) & (logs_filtered["LogDateParsed"] <= end_date)
            ]

        total_ordered_qty = float(req_filtered["Qty"].sum()) if not req_filtered.empty else 0.0
        total_dispatched_qty = (
            float(req_filtered[req_filtered["Status"].isin(["Dispatched", "Completed"])]["DispatchQty"].sum()) if not req_filtered.empty else 0.0
        )
        total_received_qty = float(logs_filtered["Qty"].sum()) if not logs_filtered.empty else 0.0

        stock_inhand_qty = float(inv_join["Closing Stock"].sum()) if not inv_join.empty else 0.0
        stock_inhand_value = float(inv_join["Stock Value"].sum()) if not inv_join.empty else 0.0

        # --- Dashboard layout: flat 3-column grid ---
        col1, col2, col3 = st.columns([1, 1, 1.2])

        # ===== Col1: Top Purchased (QTY) + Top Purchased (Value) =====
        with col1:
            # --- Top Purchased Product (QTY) ---
            with st.container(border=True):
                st.markdown('<div class="card-title">Top Purchased (QTY) <span class="meta">receipts</span></div>', unsafe_allow_html=True)
                s = _card_controls("card_purchased_qty", allow_view_mode=False, allow_chart_type=True)
                asc = (s["sort"] == "Low → High")
                topn = int(s["topn"])
                chart_type = s.get("chart_type", "Pie Chart")
                label_mode = s.get("label_mode", "%")

                purchased_qty = pd.DataFrame(columns=["Item", "Received Qty"])
                if not logs_filtered.empty:
                    purchased_qty = (
                        logs_filtered.groupby("Item", as_index=False)["Qty"]
                        .sum()
                        .rename(columns={"Qty": "Received Qty"})
                        .sort_values("Received Qty", ascending=asc)
                        .head(topn)
                    )

                if label_mode == "Amount":
                    purchased_qty = _add_amount_col(purchased_qty, "Item", "Received Qty", meta_df)
                    _display_col = "Amount"
                else:
                    _display_col = "Received Qty"

                if chart_type == "Table":
                    st.dataframe(_abbreviate_cols(purchased_qty[["Item", _display_col]]), use_container_width=True, hide_index=True, height=260)
                elif chart_type == "Bar Chart":
                    _make_bar_chart(purchased_qty, "Item", _display_col)
                else:
                    _make_pie_chart(purchased_qty, "Item", _display_col, top_n=topn, label_mode=label_mode)
                if st.button("⛶ Expand", key="expand_card_purchased_qty", use_container_width=True):
                    _show_fullscreen_card("Top Purchased (QTY)", purchased_qty[["Item", _display_col]], "Item", _display_col, chart_type, topn, label_mode=label_mode)

            # --- Top Purchased Product (Value) ---
            with st.container(border=True):
                st.markdown('<div class="card-title">Top Purchased (Value) <span class="meta">Qty × Price</span></div>', unsafe_allow_html=True)
                s = _card_controls("card_purchased_val", allow_view_mode=False, allow_chart_type=True)
                asc = (s["sort"] == "Low → High")
                topn = int(s["topn"])
                chart_type = s.get("chart_type", "Pie Chart")
                label_mode = s.get("label_mode", "%")

                purchased_val = pd.DataFrame(columns=["Item", "Purchase Value"])
                if not logs_filtered.empty and meta_df is not None and not meta_df.empty:
                    tmp = pd.merge(
                        logs_filtered.rename(columns={"Item": "Product Name"})[["Product Name", "Qty"]],
                        meta_df[["Product Name", "Price"]],
                        on="Product Name",
                        how="left",
                    )
                    tmp["Qty"] = pd.to_numeric(tmp["Qty"], errors="coerce").fillna(0.0)
                    tmp["Price"] = pd.to_numeric(tmp["Price"], errors="coerce").fillna(0.0)
                    tmp["Purchase Value"] = tmp["Qty"] * tmp["Price"]
                    purchased_val = (
                        tmp.groupby("Product Name", as_index=False)["Purchase Value"]
                        .sum()
                        .rename(columns={"Product Name": "Item"})
                        .sort_values("Purchase Value", ascending=asc)
                        .head(topn)
                    )

                if label_mode == "Qty" and not logs_filtered.empty:
                    _pv_display = (
                        logs_filtered.groupby("Item", as_index=False)["Qty"]
                        .sum()
                        .rename(columns={"Qty": "Received Qty"})
                        .sort_values("Received Qty", ascending=asc)
                        .head(topn)
                    )
                    _display_col_pv = "Received Qty"
                else:
                    _pv_display = purchased_val
                    _display_col_pv = "Purchase Value"

                if chart_type == "Table":
                    st.dataframe(_abbreviate_cols(_pv_display[["Item", _display_col_pv]]), use_container_width=True, hide_index=True, height=260)
                elif chart_type == "Bar Chart":
                    _make_bar_chart(_pv_display, "Item", _display_col_pv)
                else:
                    _make_pie_chart(_pv_display, "Item", _display_col_pv, top_n=topn, label_mode=label_mode)
                if st.button("⛶ Expand", key="expand_card_purchased_val", use_container_width=True):
                    _show_fullscreen_card("Top Purchased (Value)", _pv_display[["Item", _display_col_pv]], "Item", _display_col_pv, chart_type, topn, label_mode=label_mode)

        # ===== Col2: Top Selling (QTY) + Top Selling (Value) =====
        with col2:
            # --- Top Selling Product (QTY) ---
            with st.container(border=True):
                st.markdown('<div class="card-title">Top Selling (QTY) <span class="meta">dispatch</span></div>', unsafe_allow_html=True)
                s = _card_controls("card_selling_qty", allow_view_mode=False, allow_chart_type=True)
                asc = (s["sort"] == "Low → High")
                topn = int(s["topn"])
                chart_type = s.get("chart_type", "Pie Chart")
                label_mode = s.get("label_mode", "%")

                selling_qty = pd.DataFrame(columns=["Item", "Dispatched Qty"])
                if not req_filtered.empty:
                    disp_only = req_filtered[req_filtered["Status"].isin(["Dispatched", "Completed"])]
                    selling_qty = (
                        disp_only.groupby("Item", as_index=False)["DispatchQty"]
                        .sum()
                        .rename(columns={"DispatchQty": "Dispatched Qty"})
                        .sort_values("Dispatched Qty", ascending=asc)
                        .head(topn)
                    )

                if label_mode == "Amount":
                    selling_qty = _add_amount_col(selling_qty, "Item", "Dispatched Qty", meta_df)
                    _display_col = "Amount"
                else:
                    _display_col = "Dispatched Qty"

                if chart_type == "Table":
                    st.dataframe(_abbreviate_cols(selling_qty[["Item", _display_col]]), use_container_width=True, hide_index=True, height=260)
                elif chart_type == "Bar Chart":
                    _make_bar_chart(selling_qty, "Item", _display_col)
                else:
                    _make_pie_chart(selling_qty, "Item", _display_col, top_n=topn, label_mode=label_mode)
                if st.button("⛶ Expand", key="expand_card_selling_qty", use_container_width=True):
                    _show_fullscreen_card("Top Selling (QTY)", selling_qty[["Item", _display_col]], "Item", _display_col, chart_type, topn, label_mode=label_mode)

            # --- Top Selling Product (Value) ---
            with st.container(border=True):
                st.markdown('<div class="card-title">Top Selling (Value) <span class="meta">DispatchQty × Price</span></div>', unsafe_allow_html=True)
                s = _card_controls("card_selling_val", allow_view_mode=False, allow_chart_type=True)
                asc = (s["sort"] == "Low → High")
                topn = int(s["topn"])
                chart_type = s.get("chart_type", "Pie Chart")
                label_mode = s.get("label_mode", "%")

                selling_val = pd.DataFrame(columns=["Item", "Sales Value"])
                if not req_filtered.empty and meta_df is not None and not meta_df.empty:
                    disp_only = req_filtered[req_filtered["Status"].isin(["Dispatched", "Completed"])][["Item", "DispatchQty"]].copy()
                    disp_only["DispatchQty"] = pd.to_numeric(disp_only["DispatchQty"], errors="coerce").fillna(0.0)

                    tmp = pd.merge(
                        disp_only.rename(columns={"Item": "Product Name"}),
                        meta_df[["Product Name", "Price"]],
                        on="Product Name",
                        how="left",
                    )
                    tmp["Price"] = pd.to_numeric(tmp["Price"], errors="coerce").fillna(0.0)
                    tmp["Sales Value"] = tmp["DispatchQty"] * tmp["Price"]
                    selling_val = (
                        tmp.groupby("Product Name", as_index=False)["Sales Value"]
                        .sum()
                        .rename(columns={"Product Name": "Item"})
                        .sort_values("Sales Value", ascending=asc)
                        .head(topn)
                    )

                if label_mode == "Qty" and not req_filtered.empty:
                    disp_only_qty = req_filtered[req_filtered["Status"].isin(["Dispatched", "Completed"])][["Item", "DispatchQty"]].copy()
                    _sv_display = (
                        disp_only_qty.groupby("Item", as_index=False)["DispatchQty"]
                        .sum()
                        .rename(columns={"DispatchQty": "Dispatched Qty"})
                        .sort_values("Dispatched Qty", ascending=asc)
                        .head(topn)
                    )
                    _display_col_sv = "Dispatched Qty"
                else:
                    _sv_display = selling_val
                    _display_col_sv = "Sales Value"

                if chart_type == "Table":
                    st.dataframe(_abbreviate_cols(_sv_display[["Item", _display_col_sv]]), use_container_width=True, hide_index=True, height=260)
                elif chart_type == "Bar Chart":
                    _make_bar_chart(_sv_display, "Item", _display_col_sv)
                else:
                    _make_pie_chart(_sv_display, "Item", _display_col_sv, top_n=topn, label_mode=label_mode)
                if st.button("⛶ Expand", key="expand_card_selling_val", use_container_width=True):
                    _show_fullscreen_card("Top Selling (Value)", _sv_display[["Item", _display_col_sv]], "Item", _display_col_sv, chart_type, topn, label_mode=label_mode)

        # ===== Col3: Summary KPI + Total Purchase From Supplier =====
        with col3:
            # --- Compute KPI values ---
            purchase_total_val = float(purchased_val["Purchase Value"].sum()) if not purchased_val.empty else 0.0
            if purchase_total_val == 0.0 and meta_df is not None:
                purchase_total_val = float(_sum_purchase_from_logs(logs_filtered, meta_df)["Purchase Amount"].sum())
            sales_total_val = float(selling_val["Sales Value"].sum()) if not selling_val.empty else 0.0
            pnl = sales_total_val - purchase_total_val
            pnl_value_class = "good" if pnl >= 0 else "bad"
            cur_label = "All" if currency_choice == "All" else currency_choice

            # --- Summary card with horizontal KPI row ---
            with st.container(border=True):
                st.markdown('<div class="card-title">Summary <span class="meta">live</span></div>', unsafe_allow_html=True)
                _card_controls("card_summary", allow_view_mode=False)
                st.markdown(
                    f"""
                    <div class="dash-kpi-row">
                        <div class="dash-kpi-box">
                            <div class="kpi-icon">🧾</div>
                            <div class="kpi-label">Purchase</div>
                            <div class="kpi-value">{purchase_total_val:.2f}</div>
                            <div class="kpi-currency">{cur_label}</div>
                        </div>
                        <div class="dash-kpi-box">
                            <div class="kpi-icon">🪙</div>
                            <div class="kpi-label">Sales</div>
                            <div class="kpi-value">{sales_total_val:.2f}</div>
                            <div class="kpi-currency">{cur_label}</div>
                        </div>
                        <div class="dash-kpi-box">
                            <div class="kpi-icon">📊</div>
                            <div class="kpi-label">P&amp;L</div>
                            <div class="kpi-value {pnl_value_class}">{pnl:.2f}</div>
                            <div class="kpi-currency">Sales − Purchase</div>
                        </div>
                        <div class="dash-kpi-box">
                            <div class="kpi-icon">📦</div>
                            <div class="kpi-label">Stock In Hand</div>
                            <div class="kpi-value">{stock_inhand_value:.2f}</div>
                            <div class="kpi-currency">{cur_label}</div>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            # --- Total Purchase From Supplier card ---
            with st.container(border=True):
                st.markdown('<div class="card-title">Total Purchase From Supplier <span class="meta">Qty × Price</span></div>', unsafe_allow_html=True)
                s = _card_controls("card_supplier_purchase", allow_view_mode=False, allow_chart_type=True)
                asc = (s["sort"] == "Low → High")
                topn = int(s["topn"])
                chart_type = s.get("chart_type", "Pie Chart")
                label_mode = s.get("label_mode", "%")

                supplier_df = _sum_purchase_from_logs(logs_filtered, meta_df)
                if not supplier_df.empty:
                    supplier_df = supplier_df.sort_values("Purchase Amount", ascending=asc).head(topn)

                if label_mode == "Qty" and not logs_filtered.empty and meta_df is not None and not meta_df.empty:
                    _supp_qty = pd.merge(
                        logs_filtered.rename(columns={"Item": "Product Name"})[["Product Name", "Qty"]],
                        meta_df[["Product Name", "Supplier"]],
                        on="Product Name",
                        how="left",
                    )
                    _supp_qty["Supplier"] = _supp_qty["Supplier"].fillna("Unknown").astype(str).str.strip().replace("", "Unknown")
                    _supp_display = (
                        _supp_qty.groupby("Supplier", as_index=False)["Qty"]
                        .sum()
                        .sort_values("Qty", ascending=asc)
                        .head(topn)
                    )
                    _display_col_sup = "Qty"
                else:
                    _supp_display = supplier_df
                    _display_col_sup = "Purchase Amount"

                if chart_type == "Table":
                    st.dataframe(_abbreviate_cols(_supp_display), use_container_width=True, hide_index=True, height=360)
                elif chart_type == "Pie Chart":
                    _make_pie_chart(_supp_display, "Supplier", _display_col_sup, top_n=topn, label_mode=label_mode)
                else:
                    # Bar Chart (horizontal bar chart for this supplier card)
                    if _supp_display.empty:
                        st.info("📭 No supplier purchase data.")
                    else:
                        bar = _supp_display.rename(columns={_display_col_sup: "Amount"})
                        _make_horiz_bar_chart(bar, "Supplier", "Amount")
                if st.button("⛶ Expand", key="expand_card_supplier_purchase", use_container_width=True):
                    _show_fullscreen_card("Total Purchase From Supplier", _supp_display, "Supplier", _display_col_sup, chart_type, topn, label_mode=label_mode)

            # --- Export (compute bytes and render in the top placeholder + here as fallback) ---
            # FIX 2: Restored truncated dictionary values for export sheets
            stock_qty_top = (
                inv_join[["Product Name", "UOM", "Closing Stock"]]
                .sort_values("Closing Stock", ascending=False)
                .head(int(legacy_top_n))
                if not inv_join.empty
                else pd.DataFrame(columns=["Product Name", "UOM", "Closing Stock"])
            )
            stock_val_top = (
                inv_join[["Product Name", "UOM", "Closing Stock", "Price", "Currency", "Stock Value"]]
                .sort_values("Stock Value", ascending=False)
                .head(int(legacy_top_n))
                if not inv_join.empty
                else pd.DataFrame(columns=["Product Name", "UOM", "Closing Stock", "Price", "Currency", "Stock Value"])
            )

            export_bytes = _to_excel_bytes(
                {
                    "KPIs": pd.DataFrame(
                        [
                            {
                                "Start": start_date,
                                "End": end_date,
                                "Restaurant": restaurant_filter,
                                "Currency": currency_choice,
                                "Ordered Qty": total_ordered_qty,
                                "Dispatched Qty": total_dispatched_qty,
                                "Received Qty": total_received_qty,
                                "Stock In Hand Qty": stock_inhand_qty,
                                "Stock In Hand Value": stock_inhand_value,
                                "Purchase Total (approx)": purchase_total_val,
                                "Sales Total (approx)": sales_total_val,
                                "P&L (approx)": pnl,
                                "Dispatch Date Basis": dispatch_date_basis,
                            }
                        ]
                    ),
                    "Top Purchased Qty": purchased_qty,
                    "Top Selling Qty": selling_qty,
                    "Top Purchased Value": purchased_val,
                    "Top Selling Value": selling_val,
                    "Supplier Purchase": supplier_df,
                    "Stock In Hand Qty (Top)": stock_qty_top,
                    "Stock In Hand Value (Top)": stock_val_top,
                }
            )
            # Render export button in the top placeholder (header area)
            export_placeholder.download_button(
                "📤 Export Dashboard",
                data=export_bytes,
                file_name=f"Warehouse_Dashboard_{start_date}_to_{end_date}.xlsx",
                use_container_width=True,
                key="dash_export_excel_top",
                type="primary",
            )
            st.markdown(
                '<div class="dash-note">Note: Value metrics use the selected currency filter (no exchange-rate conversion). Items with Price=0 contribute 0 to value.</div>',
                unsafe_allow_html=True,
            )


# ===================== RESTAURANTS TAB =====================
with tab_restaurants:
    st.markdown('<span class="section-title">🍴 Manage Restaurants</span>', unsafe_allow_html=True)

    _mgr_org_id = st.session_state.get("org_id")
    _mgr_user_id = st.session_state.get("user_id")

    if not _mgr_org_id:
        st.warning("No organization found. Please complete onboarding first.")
        st.stop()

    # ── Section 1: Add Restaurant ──────────────────────────────────────────────
    with st.expander("➕ Add Restaurant", expanded=False):
        r_name = st.text_input("🏪 Restaurant Name", placeholder="e.g., Restaurant 01 - Downtown", key="new_rest_name")
        r_max_uses = st.number_input("👥 Max Manager Logins", min_value=1, max_value=20, value=5, key="new_rest_max_uses")
        if st.button("✅ Create Restaurant", type="primary", key="create_rest_btn"):
            if not r_name.strip():
                st.error("Please enter a restaurant name.")
            else:
                _result = create_restaurant_with_invite(
                    org_id=_mgr_org_id,
                    restaurant_name=r_name.strip(),
                    created_by=_mgr_user_id,
                    max_uses=int(r_max_uses),
                )
                if _result:
                    _inv_code = _result["invite_code"]["code"]
                    st.success(f"✅ Restaurant **{r_name.strip()}** created!")
                    st.info(f"📋 Invite Code: **{_inv_code}** — Share this with the restaurant manager(s)")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error("❌ Failed to create restaurant. Please try again.")

    st.divider()

    # ── Section 2: Restaurant List ─────────────────────────────────────────────
    st.markdown('<span class="section-title">🏪 Your Restaurants</span>', unsafe_allow_html=True)

    if st.button("🔄 Refresh List", key="refresh_rest_list"):
        st.cache_data.clear()
        st.rerun()

    _restaurants = get_org_restaurants(_mgr_org_id)

    if not _restaurants:
        st.info("📭 No restaurants yet. Create one above!")
    else:
        for _rest in _restaurants:
            _lid = _rest.get("id", "")
            _lname = _rest.get("name", "Unknown")
            _lactive = _rest.get("active", True)

            _status_badge = "🟢 Active" if _lactive else "🔴 Inactive"
            _border_color = "rgba(16,185,129,0.25)" if _lactive else "rgba(239,68,68,0.25)"

            with st.container(border=True):
                h_col, badge_col = st.columns([3, 1])
                with h_col:
                    st.markdown(f"**🏪 {_lname}**")
                with badge_col:
                    st.markdown(
                        f'<span style="font-size:12px;font-weight:600;color:{"#10B981" if _lactive else "#EF4444"};">'
                        f"{_status_badge}</span>",
                        unsafe_allow_html=True,
                    )

                # Invite codes for this location
                _codes = get_invite_codes_for_location(_lid)
                _active_codes = [c for c in _codes if c.get("active")]
                if _active_codes:
                    _c = _active_codes[0]
                    st.markdown(
                        f"📋 **Invite Code:** `{_c['code']}` &nbsp;|&nbsp; "
                        f"Uses: {_c['used_count']}/{_c['max_uses']}",
                        unsafe_allow_html=True,
                    )
                else:
                    st.caption("No active invite code.")

                # Members count
                _members = get_location_members(_lid)
                st.caption(f"👥 {len(_members)} manager(s) linked")

                # Action buttons
                btn_c1, btn_c2, btn_c3 = st.columns(3)
                with btn_c1:
                    if st.button("🔄 Regenerate Code", key=f"regen_{_lid}", use_container_width=True):
                        _new_code = regenerate_invite_code(
                            org_id=_mgr_org_id,
                            location_id=_lid,
                            created_by=_mgr_user_id,
                        )
                        if _new_code:
                            st.success(f"✅ New code: **{_new_code['code']}**")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("❌ Failed to regenerate code.")
                with btn_c2:
                    if _lactive:
                        if st.button("🔒 Deactivate", key=f"deact_{_lid}", use_container_width=True):
                            if deactivate_restaurant(_lid):
                                st.warning(f"🔒 **{_lname}** deactivated.")
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.error("❌ Failed to deactivate.")
                    else:
                        if st.button("🔓 Reactivate", key=f"react_{_lid}", use_container_width=True):
                            if reactivate_restaurant(_lid):
                                st.success(f"🔓 **{_lname}** reactivated.")
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.error("❌ Failed to reactivate.")
                with btn_c3:
                    if _active_codes:
                        _copy_code = _active_codes[0]["code"]
                        st.code(_copy_code, language=None)
