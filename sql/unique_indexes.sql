-- unique_indexes.sql
--
-- Run this script once in the Supabase SQL Editor to create the unique indexes
-- required for safe org-scoped upserts (ON CONFLICT handling).
--
-- Notes:
--   * Supabase / Postgres versions used here do NOT support the
--     `ADD CONSTRAINT IF NOT EXISTS` syntax, so we use
--     `CREATE UNIQUE INDEX IF NOT EXISTS` instead, which is idempotent
--     and achieves the same result for upsert conflict resolution.
--   * Unique indexes on the same columns as the on_conflict target are what
--     Postgres (and therefore Supabase) uses to detect and resolve conflicts
--     during an UPSERT (INSERT ... ON CONFLICT DO UPDATE).

-- product_metadata: unique per organisation by product name
create unique index if not exists ux_product_metadata_org_product
    on public.product_metadata(org_id, "Product Name");

-- persistent_inventory: unique per organisation + location by product name
create unique index if not exists ux_persistent_inventory_org_loc_product
    on public.persistent_inventory(org_id, location_id, "Product Name");

-- user_memberships: unique per user + organisation + location
create unique index if not exists ux_user_memberships_user_org_loc
    on public.user_memberships(user_id, org_id, location_id);

-- activity_logs: unique per organisation + location + log entry
-- Enables tenant-safe upserts so re-saving the same log entry never creates duplicates.
-- LogID is an 8-char identifier generated per log row; combined with org_id + location_id
-- it forms a stable, collision-free composite key.
create unique index if not exists ux_activity_logs_org_loc_logid
    on public.activity_logs(org_id, location_id, "LogID");
