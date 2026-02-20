# Warehouse Pro Cloud — Inventory Management System

A Streamlit-based inventory and requisition management platform backed by **Google Sheets**, designed for warehouse operations and connected restaurant outlets.

---

## Project Structure

| File | Description |
|---|---|
| `app.py` | Main warehouse management portal (Warehouse Pro Cloud v8.5) |
| `restaurant_01.py` | Restaurant 01 operations portal |
| `utils.py` | Shared Google Sheets connection and helper utilities |
| `requirements.txt` | Python dependencies |

---

## Features

### Warehouse (`app.py`)
- **Daily Receipt Portal** — log incoming stock by item and day
- **Live Stock Status** — real-time view with editable Consumption and Physical Count columns
- **Activity Log** — paginated transaction log with per-entry undo support
- **Monthly Close & Rollover** — archive current month and roll Physical Counts into new Opening Stocks
- **Archive Explorer** — browse and download historical monthly snapshots as Excel
- **Weekly Par Analysis** — average consumption-based min/max par level suggestions
- **Requisition System** — create purchase orders linked to supplier metadata
- **Supplier Directory** — manage product-to-supplier mappings
- **Excel/CSV Import** — bulk load inventory or supplier metadata from uploaded files

### Restaurant Portal (`restaurant_01.py`)
- **Daily Stock Take** — enter Physical Counts per category; auto-calculates Consumption and Closing Stock
- **Requisition Cart** — search products, add to cart, and submit orders directly to the warehouse
- **Pending Orders** — view submitted requisitions, mark as Received, or request a follow-up

---

## Google Sheets Structure

The app reads and writes the following worksheets within the connected Google Sheet:

| Worksheet | Used by | Purpose |
|---|---|---|
| `persistent_inventory` | `app.py` | Main warehouse stock ledger |
| `activity_logs` | `app.py` | Transaction history |
| `monthly_history` | `app.py` | Monthly archive snapshots |
| `orders_db` | `app.py`, `restaurant_01.py` | Purchase / requisition orders |
| `product_metadata` | `app.py` | Supplier directory |
| `rest_01_inventory` | `restaurant_01.py` | Restaurant 01 stock counts |

---

## Setup

### 1. Google Sheets Connection

Add a `.streamlit/secrets.toml` file with your Google service account credentials:

```toml
[connections.gsheets]
spreadsheet = "https://docs.google.com/spreadsheets/d/<YOUR_SHEET_ID>"
type = "service_account"
project_id = "..."
private_key_id = "..."
private_key = "-----BEGIN RSA PRIVATE KEY-----\n..."
client_email = "..."
client_id = "..."
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the App

**Warehouse portal:**
```bash
streamlit run app.py
```

**Restaurant portal:**
```bash
streamlit run restaurant_01.py
```

---

## Dev Container

This repository includes a `.devcontainer` configuration for GitHub Codespaces. Opening the repo in Codespaces will automatically install all dependencies and launch the warehouse portal at port **8501**.

---

## Inventory Template (Excel Import)

When uploading a bulk inventory file, the app expects:

- **Row 5 onwards** — data rows (first 4 rows are skipped)
- **Column B** — Product Name
- **Column C** — Unit of Measure (UOM)
- **Column D** — Opening Stock
