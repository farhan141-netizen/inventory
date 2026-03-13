import random
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


def generate_invite_code_string() -> str:
    """Generate a simple 6-digit numeric invite code."""
    return str(random.randint(100000, 999999))


def create_restaurant_with_invite(org_id: str, restaurant_name: str, created_by: str, max_uses: int = 5) -> Optional[dict]:
    """
    Create a restaurant location + invite code in one call.
    Returns {"location": {...}, "invite_code": {...}} or None on failure.
    """
    loc = create_location(org_id=org_id, name=restaurant_name, loc_type="restaurant")
    if not loc:
        return None

    # Generate a unique 6-digit code (retry if collision)
    for _ in range(10):
        code = generate_invite_code_string()
        try:
            payload = {
                "org_id": org_id,
                "location_id": loc["id"],
                "code": code,
                "role": "restaurant",
                "max_uses": max_uses,
                "used_count": 0,
                "active": True,
                "created_by": created_by,
            }
            resp = conn.table("invite_codes").insert(payload).execute()
            if resp.data:
                return {"location": loc, "invite_code": resp.data[0]}
        except Exception:
            continue  # code collision, retry

    st.error("Failed to generate unique invite code after multiple attempts.")
    return None


def validate_invite_code(code: str) -> Optional[dict]:
    """
    Validate an invite code. Returns the invite_code row dict if valid, None otherwise.
    Checks: exists, active, used_count < max_uses, and location is active.
    """
    try:
        resp = conn.table("invite_codes").select("*").eq("code", code.strip()).eq("active", True).execute()
        if not resp.data:
            return None
        invite = resp.data[0]
        if invite["used_count"] >= invite["max_uses"]:
            return None
        # Check location is active
        loc_resp = conn.table("locations").select("id, active, name").eq("id", invite["location_id"]).execute()
        if not loc_resp.data or not loc_resp.data[0].get("active", True):
            return None
        invite["_location_name"] = loc_resp.data[0].get("name", "")
        return invite
    except Exception:
        return None


def redeem_invite_code(code: str, user_id: str, user_email: str = "") -> Optional[dict]:
    """
    Redeem an invite code: create membership + increment used_count.
    Returns the membership dict or None on failure.
    """
    invite = validate_invite_code(code)
    if not invite:
        return None

    # Check if user already has a membership for this location
    existing = get_user_memberships(user_id)
    for m in existing:
        if m.get("location_id") == invite["location_id"]:
            return m  # Already a member, return existing membership

    # Create membership
    mem = add_membership(
        user_id=user_id,
        org_id=invite["org_id"],
        location_id=invite["location_id"],
        role=invite.get("role", "restaurant"),
    )
    if not mem:
        return None

    # Increment used_count
    try:
        conn.table("invite_codes").update({"used_count": invite["used_count"] + 1}).eq("id", invite["id"]).execute()
    except Exception:
        pass  # non-critical

    return mem


def get_org_restaurants(org_id: str) -> list:
    """Get all restaurant locations for an org (both active and inactive)."""
    try:
        resp = conn.table("locations").select("*").eq("org_id", org_id).eq("type", "restaurant").execute()
        return resp.data or []
    except Exception:
        return []


def get_invite_codes_for_location(location_id: str) -> list:
    """Get all invite codes for a specific location."""
    try:
        resp = conn.table("invite_codes").select("*").eq("location_id", location_id).execute()
        return resp.data or []
    except Exception:
        return []


def deactivate_restaurant(location_id: str) -> bool:
    """Soft-delete: set location.active = false and deactivate its invite codes."""
    try:
        conn.table("locations").update({"active": False}).eq("id", location_id).execute()
        conn.table("invite_codes").update({"active": False}).eq("location_id", location_id).execute()
        return True
    except Exception:
        return False


def reactivate_restaurant(location_id: str) -> bool:
    """Undo soft-delete: set location.active = true. Invite codes stay deactivated (owner can generate new ones)."""
    try:
        conn.table("locations").update({"active": True}).eq("id", location_id).execute()
        return True
    except Exception:
        return False


def is_location_active(location_id: str) -> bool:
    """Check if a location is active."""
    try:
        resp = conn.table("locations").select("active").eq("id", location_id).execute()
        if resp.data:
            return resp.data[0].get("active", True)
        return True  # default to active if not found
    except Exception:
        return True


def regenerate_invite_code(org_id: str, location_id: str, created_by: str, max_uses: int = 5) -> Optional[dict]:
    """Generate a new invite code for a location (deactivates old ones)."""
    try:
        # Deactivate existing codes for this location
        conn.table("invite_codes").update({"active": False}).eq("location_id", location_id).execute()
    except Exception:
        pass

    for _ in range(10):
        code = generate_invite_code_string()
        try:
            payload = {
                "org_id": org_id,
                "location_id": location_id,
                "code": code,
                "role": "restaurant",
                "max_uses": max_uses,
                "used_count": 0,
                "active": True,
                "created_by": created_by,
            }
            resp = conn.table("invite_codes").insert(payload).execute()
            if resp.data:
                return resp.data[0]
        except Exception:
            continue
    return None


def get_location_members(location_id: str) -> list:
    """Get all user memberships for a specific location."""
    try:
        resp = conn.table("user_memberships").select("*").eq("location_id", location_id).execute()
        return resp.data or []
    except Exception:
        return []
