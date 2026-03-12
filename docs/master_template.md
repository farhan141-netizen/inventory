# Master Inventory Template

## Overview

The **Master Inventory Template** is a single Excel file (`.xlsx`) that lets a new organisation
initialise its entire product catalogue — including supplier metadata and opening stock — in one
guided step.

When uploaded, the app:

1. Validates every row and reports errors before writing anything to the database.
2. Splits the data into two DataFrames and upserts them into separate Supabase tables,
   both scoped to the current `org_id` and `location_id`.
3. Re-running the same upload updates existing products instead of creating duplicates.

---

## Workflow

```
Download template → Fill in Excel → Upload → Validate → Preview → Confirm Import
```

| Step | UI Element | Description |
|------|-----------|-------------|
| 1 | **⬇️ Download Master Template** button | Generates and downloads `master_inventory_template.xlsx` |
| 2 | **Upload Master Template** file uploader | Accepts a filled-in `.xlsx` file |
| 3 | Automatic validation | Checks required fields, data types, and duplicate product names |
| 4 | Preview table | Shows first 20 rows of validated data |
| 5 | **✅ Import Products** button | Writes data to Supabase |

---

## Template Columns

### `MASTER_TEMPLATE` sheet

| Column | Required | Type | Default | Notes |
|--------|----------|------|---------|-------|
| `Product Name` | ✅ Yes | Text | — | Unique within the file (case-insensitive) |
| `UOM` | ✅ Yes | Text | — | Unit of measure (e.g. `kg`, `pcs`, `ltr`) |
| `Opening Stock` | ✅ Yes | Number ≥ 0 | — | Current stock on hand; use `0` if unknown |
| `Category` | No | Text | `General` | Product category |
| `Supplier` | No | Text | — | Supplier / vendor name |
| `Contact` | No | Text | — | Supplier contact person |
| `Email` | No | Text | — | Supplier email address |
| `Lead Time` | No | Number ≥ 0 | — | Order lead time in days |
| `Price` | No | Number ≥ 0 | — | Unit price |
| `Currency` | No | Text | `USD` | Currency code (e.g. `USD`, `EUR`); normalised to uppercase |

### `INSTRUCTIONS` sheet

Contains human-readable bullet instructions embedded in the template file itself.

---

## Validation Rules

- All text fields are trimmed of leading/trailing whitespace.
- `Product Name` must be non-empty and unique within the uploaded file (case-insensitive).
- `UOM` must be non-empty.
- `Opening Stock` must be a number ≥ 0.
- `Price` must be a number ≥ 0 if provided.
- `Lead Time` must be a number ≥ 0 if provided.
- `Currency` is normalised to uppercase; defaults to `USD` if blank.
- `Category` defaults to `General` if blank.

If any validation errors are found, a table of errors is displayed and **no data is written
to the database** until all errors are resolved.

---

## Database Tables

### `product_metadata`

Populated from template columns: `Product Name`, `UOM`, `Supplier`, `Contact`, `Email`,
`Category`, `Lead Time`, `Price`, `Currency`.

**Upsert conflict key:** `(org_id, "Product Name")`

### `persistent_inventory`

Populated from template columns: `Product Name`, `UOM`, `Opening Stock`, `Category`.

Additional columns initialised automatically:
- Day columns `"1"`–`"31"`: `0.0`
- `Total Received`: `0.0`
- `Consumption`: `0.0`
- `Closing Stock`: equal to `Opening Stock`
- `Physical Count`: `null`
- `Variance`: `0.0`

**Upsert conflict key:** `(org_id, location_id, "Product Name")`

---

## Multi-Tenant Isolation

Every write is scoped to the authenticated user's `org_id` (and `location_id` for inventory).
This means:

- "Tomato" in organisation **Trendfull** and "Tomato" in organisation **Cine Cafe** are
  completely separate records.
- Re-uploading the master template updates only the products belonging to the **current**
  organisation and location — other organisations are never affected.

---

## Required Supabase Constraints

For upserts to be correctly isolated per org/location, the following unique constraints must
exist in Supabase:

```sql
-- product_metadata: one row per (org, product)
ALTER TABLE public.product_metadata
  ADD CONSTRAINT uq_product_metadata_org_product
  UNIQUE (org_id, "Product Name");

-- persistent_inventory: one row per (org, location, product)
ALTER TABLE public.persistent_inventory
  ADD CONSTRAINT uq_persistent_inventory_org_loc_product
  UNIQUE (org_id, location_id, "Product Name");
```

---

## What Is NOT Populated

The master import intentionally does **not** write to transactional tables:

- `activity_logs` — generated automatically by normal operations
- `monthly_history` — generated on month-close
- `orders_db` / `restaurant_requisitions` — operational tables, start empty

---

## Notes

- The import reads the `MASTER_TEMPLATE` sheet only; the `INSTRUCTIONS` sheet is ignored.
- Do not rename or reorder the column headers.
- Leave optional cells blank rather than entering `N/A` or `-`.
- Implementation uses `xlsxwriter` for template generation and `pandas` for parsing and validation.
