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
    Stores user_email in the membership row so it can be displayed in the admin UI.
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

    # Build membership payload — always store user_email for admin display
    payload = {
        "user_id": user_id,
        "org_id": invite["org_id"],
        "location_id": invite["location_id"],
        "role": invite.get("role", "restaurant"),
    }
    if user_email:
        payload["user_email"] = user_email

    try:
        resp = conn.table("user_memberships").insert(payload).execute()
        mem = resp.data[0] if resp.data else None
    except Exception as e:
        st.error(f"Failed to create membership: {e}")
        return None

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
    """
    Generate a new invite code for a location (deactivates old ones).
    The new code's used_count reflects the number of managers already linked,
    so the Uses counter stays accurate and doesn't reset to 0/5.
    """
    # Count how many managers are already linked to this location
    actual_member_count = 0
    try:
        resp = conn.table("user_memberships").select("id", count="exact").eq("location_id", location_id).execute()
        actual_member_count = resp.count if resp.count is not None else len(resp.data or [])
    except Exception:
        pass

    # Deactivate existing codes for this location
    try:
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
                "used_count": actual_member_count,  # carry over existing member count
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


def get_member_email(user_id: str) -> str:
    """
    Try to fetch a user's email. Checks in this order:
    1. user_memberships.user_email column (fastest, stored at redeem time)
    2. profiles table (standard Supabase pattern)
    3. Returns shortened user_id as last resort fallback.
    """
    # 1. Check user_memberships.user_email — stored when user redeems invite code
    try:
        resp = (
            conn.table("user_memberships")
            .select("user_email")
            .eq("user_id", user_id)
            .not_.is_("user_email", "null")
            .limit(1)
            .execute()
        )
        if resp.data and resp.data[0].get("user_email"):
            return resp.data[0]["user_email"]
    except Exception:
        pass

    # 2. Try profiles table (common Supabase pattern)
    try:
        resp = conn.table("profiles").select("email").eq("id", user_id).execute()
        if resp.data and resp.data[0].get("email"):
            return resp.data[0]["email"]
    except Exception:
        pass

    # 3. Fallback: show partial user ID
    return f"user-{str(user_id)[:8]}…"


def get_location_members_with_email(location_id: str) -> list:
    """
    Get all memberships for a location, enriched with user email.
    Returns list of dicts with extra key: 'email'
    """
    members = get_location_members(location_id)
    for m in members:
        m["email"] = get_member_email(m.get("user_id", ""))
    return members


def update_member_role(membership_id: str, new_role: str) -> bool:
    """Update the role of a membership (e.g. 'restaurant', 'read_only', 'held')."""
    try:
        conn.table("user_memberships").update({"role": new_role}).eq("id", membership_id).execute()
        return True
    except Exception:
        return False


def delete_membership(membership_id: str) -> bool:
    """Permanently delete a membership record."""
    try:
        conn.table("user_memberships").delete().eq("id", membership_id).execute()
        return True
    except Exception:
        return False
