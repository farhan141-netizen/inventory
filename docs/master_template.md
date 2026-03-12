# Master Inventory Template

The Master Inventory Template is a single Excel file that lets you initialize your product catalog, opening stock, and supplier metadata for your organization in one step.

---

## 1. Download the Template

1. Open the app and click **📦 Bulk Upload** in the header.
2. Click **⬇️ Download Master Template**.
3. Save `Master_Inventory_Template.xlsx` to your computer.

The file contains two sheets:

| Sheet | Purpose |
|---|---|
| `MASTER_TEMPLATE` | Fill in one row per product |
| `INSTRUCTIONS` | Quick reference rules |

---

## 2. Fill in the Template

Open `MASTER_TEMPLATE` and add one row per product. Column reference:

| Column | Required | Notes |
|---|---|---|
| **Product Name** | ✅ | Must be unique within the file (case-insensitive) |
| **UOM** | ✅ | Unit of measure (e.g. `kg`, `pcs`, `ltr`) |
| **Opening Stock** | ✅ | Numeric, must be >= 0 |
| Category | optional | Defaults to `General` if left empty |
| Supplier | optional | Supplier company name |
| Contact | optional | Supplier contact person |
| Email | optional | Supplier email address |
| Lead Time | optional | Days; must be numeric and >= 0 if provided |
| Price | optional | Unit price; must be numeric and >= 0 if provided |
| Currency | optional | Defaults to `USD` (uppercased automatically) |

> **Do not rename or reorder headers.** The import relies on exact column names.

---

## 3. Upload the Template

1. In the **📦 Bulk Upload** modal, scroll to **📤 Upload Master Template**.
2. Click **Browse files** and select your filled template.
3. The app validates all rows immediately:
   - If there are errors, a table of row-level errors is shown. Fix the file and re-upload — nothing is written to the database until all rows are valid.
   - If validation passes, a preview of the first 20 rows is shown.

---

## 4. Import Products

After a successful validation:

1. Review the preview.
2. Click **✅ Import Products**.

The import performs two writes, both scoped to your current **org** and **location**:

### `product_metadata` — always upserted
All columns except `Opening Stock` are written (or updated) for every row in the file. If a product already exists in your org's metadata, its supplier info, price, category, etc. are refreshed.

### `persistent_inventory` — only new products
- The app checks which products already exist in inventory for your org/location.
- **Existing products are not touched** — their opening stock, daily receipt columns, consumption, and closing stock are preserved exactly as they are.
- Only products that do **not** yet exist in inventory receive a new row with:
  - Opening Stock from your template
  - Day columns 1–31 initialized to 0
  - Total Received, Consumption, Variance = 0
  - Closing Stock = Opening Stock

This "safe mode" means you can re-upload an updated metadata sheet at any time without resetting your live inventory numbers.

---

## 5. After Import

- The page refreshes automatically.
- Your products appear in the inventory grid and supplier directory.
- To update supplier details later, re-upload the template (only metadata is overwritten; inventory quantities remain safe).
