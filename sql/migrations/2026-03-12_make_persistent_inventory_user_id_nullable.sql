-- Fix persistent_inventory schema so user_id can be nullable
-- and uniqueness is enforced by org_id+location_id+Product Name.

-- 1) Ensure UUID generation is available
create extension if not exists pgcrypto;

-- 2) Drop old PK that includes user_id (prevents user_id from being nullable)
alter table public.persistent_inventory
  drop constraint if exists persistent_inventory_pkey;

-- 3) Ensure id exists and is usable as primary key
alter table public.persistent_inventory
  add column if not exists id uuid default gen_random_uuid();

update public.persistent_inventory
set id = gen_random_uuid()
where id is null;

alter table public.persistent_inventory
  alter column id set not null;

alter table public.persistent_inventory
  add constraint persistent_inventory_pkey primary key (id);

-- 4) Now user_id can be nullable
alter table public.persistent_inventory
  alter column user_id drop not null;

-- 5) Enforce tenant-safe uniqueness for upserts
create unique index if not exists ux_persistent_inventory_org_loc_product
  on public.persistent_inventory (org_id, location_id, "Product Name");
