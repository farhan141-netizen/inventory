-- Migration: make persistent_inventory.user_id nullable
--
-- Why: When a new product is added via the app (e.g. through the Daily Receipt Portal
-- or Master Template upload), a row is inserted into persistent_inventory without a
-- user_id value.  The previous NOT NULL constraint on that column causes an error:
--   "persistent_inventory.user_id violates not-null constraint"
-- Making the column nullable allows rows to be inserted/upserted without a user_id
-- while still recording the user when the value is available.

ALTER TABLE public.persistent_inventory
    ALTER COLUMN user_id DROP NOT NULL;
