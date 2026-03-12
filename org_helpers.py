import streamlit as st
from st_supabase_connection import SupabaseConnection
from typing import Optional

# Reuse a Supabase connection inside this helper module.
# It's okay that app.py also creates a connection; this module establishes its own.
conn = st.connection("supabase", type=SupabaseConnection)


def create_organization(owner_user_id: str, org_name: str) -> Optional[dict]:
    """Create an organization and return the inserted row (dict) or None on failure."""
    try:
        payload = {"name": org_name, "owner_id": owner_user_id}
        resp = conn.table("organizations").insert(payload).execute()
        return resp.data[0] if resp.data else None
    except Exception as e:
        st.error(f"Failed to create organization: {e}")
        return None


def create_location(org_id: str, name: str, loc_type: str = "warehouse") -> Optional[dict]:
    """Create a location (warehouse/outlet) and return the inserted row or None on failure."""
    try:
        payload = {"org_id": org_id, "name": name, "type": loc_type}
        resp = conn.table("locations").insert(payload).execute()
        return resp.data[0] if resp.data else None
    except Exception as e:
        st.error(f"Failed to create location: {e}")
        return None


def add_membership(user_id: str, org_id: str, location_id: Optional[str] = None, role: str = "owner") -> Optional[dict]:
    """Add a membership for a user to an organization (and optional location)."""
    try:
        payload = {"user_id": user_id, "org_id": org_id, "location_id": location_id, "role": role}
        resp = conn.table("user_memberships").insert(payload).execute()
        return resp.data[0] if resp.data else None
    except Exception as e:
        st.error(f"Failed to add membership: {e}")
        return None


def get_user_memberships(user_id: str):
    """Return a list of membership dicts for the specified user (or empty list)."""
    try:
        resp = conn.table("user_memberships").select("*").eq("user_id", user_id).execute()
        return resp.data if resp.data else []
    except Exception as e:
        st.warning(f"Failed to fetch memberships: {e}")
        return []
