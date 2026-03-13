-- invite_codes table for restaurant onboarding
-- Run this in Supabase SQL Editor

create table if not exists public.invite_codes (
    id uuid default gen_random_uuid() primary key,
    org_id uuid not null,
    location_id uuid not null,
    code text not null unique,
    role text not null default 'restaurant',
    max_uses int not null default 5,
    used_count int not null default 0,
    active boolean not null default true,
    created_at timestamptz default now(),
    created_by uuid
);

-- Unique index to enforce code uniqueness and enable fast lookups
create unique index if not exists ux_invite_codes_code on public.invite_codes(code);

-- Add 'active' column to locations table if it doesn't exist
-- (run this separately if needed, or wrap in DO block)
do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_schema = 'public' and table_name = 'locations' and column_name = 'active'
    ) then
        alter table public.locations add column active boolean not null default true;
    end if;
end $$;

-- Add submitted_by and submitted_by_email to restaurant_requisitions if not exist
do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_schema = 'public' and table_name = 'restaurant_requisitions' and column_name = 'submitted_by'
    ) then
        alter table public.restaurant_requisitions add column submitted_by uuid;
    end if;
    if not exists (
        select 1 from information_schema.columns
        where table_schema = 'public' and table_name = 'restaurant_requisitions' and column_name = 'submitted_by_email'
    ) then
        alter table public.restaurant_requisitions add column submitted_by_email text;
    end if;
end $$;
