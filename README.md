# Warehouse Pro Cloud

A multi-tenant inventory management application built with Streamlit and Supabase.

## Getting Started

### 1. Deploy to Streamlit Cloud

Push this repository to GitHub, then connect it to [Streamlit Cloud](https://streamlit.io/cloud). Add your Supabase credentials as secrets:

```toml
# .streamlit/secrets.toml
[connections.supabase]
SUPABASE_URL = "https://<your-project>.supabase.co"
SUPABASE_KEY = "<your-anon-or-service-role-key>"
```

### 2. Run the SQL migration (required before first use)

Open the **Supabase SQL Editor** for your project and run the contents of [`sql/unique_indexes.sql`](sql/unique_indexes.sql).

This creates four idempotent unique indexes that enable safe org-scoped upserts for the following tables:

| Table | Unique index columns |
|---|---|
| `product_metadata` | `org_id`, `"Product Name"` |
| `persistent_inventory` | `org_id`, `location_id`, `"Product Name"` |
| `user_memberships` | `user_id`, `org_id`, `location_id` |
| `activity_logs` | `org_id`, `location_id`, `"LogID"` |

The `activity_logs` index ensures that re-saving the same log entry (identified by `LogID`) for the same organisation and location never creates duplicate rows. This makes activity-log upserts fully tenant-safe and idempotent.

> **Why `CREATE UNIQUE INDEX` instead of `ADD CONSTRAINT`?**  
> Some Supabase/Postgres versions do not support the `ADD CONSTRAINT IF NOT EXISTS` syntax and will return a syntax error. `CREATE UNIQUE INDEX IF NOT EXISTS` is fully idempotent and compatible, and Postgres uses unique indexes to resolve `ON CONFLICT` clauses in upserts.

### 3. Register and onboard

1. Open the app and register with your email and password.
2. On first login you will be prompted to create your **Organisation** and first **Location**.
3. Once onboarded, use **Bulk Upload → Download Master Template** to initialise your product catalogue and opening stock.

## Bulk Inventory Initialisation

Download the master template from the Bulk Upload section of the app. Fill in:

| Column | Required | Notes |
|---|---|---|
| Product Name | ✅ | Must be unique within your org |
| UOM | ✅ | Unit of measure (kg, pcs, ltr…) |
| Opening Stock | ✅ | Numeric; defaults to 0 |
| Category | optional | Defaults to "General" |
| Supplier | optional | |
| Contact | optional | |
| Email | optional | |
| Lead Time | optional | Days |
| Price | optional | Numeric |
| Currency | optional | Defaults to "USD" |

When you upload the filled template the app will:

1. Validate all rows (duplicate names, non-numeric numbers, etc.)
2. Show a preview for confirmation
3. Upsert **`product_metadata`** (everything except Opening Stock)
4. Upsert **`persistent_inventory`** (Opening Stock + calculated fields)

All data is scoped to your logged-in organisation and location — different organisations with the same product name (e.g. "Tomato") are stored completely separately.
