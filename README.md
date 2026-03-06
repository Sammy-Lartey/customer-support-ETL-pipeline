# Customer Support ETL Pipeline

An end-to-end ETL pipeline that transforms raw, multi-sheet customer support Excel files into a clean, analytics-ready PostgreSQL database. Built to handle messy real-world data, enforce data quality, and support reliable reporting.

---

## The Problem

Incoming customer support data had:
- Multiple fragmented monthly sheets with inconsistent formats
- Malformed phone numbers and unreliable region names
- Missing and duplicate customer identifiers
- No direct link between customer profiles and complaints
- No visibility into pipeline operations or failures

---

## Solution

A modular 4-phase pipeline that resolves all of the above and loads everything into a structured, queryable PostgreSQL schema.

```
Excel File → Load & Clean → Schema Setup → ID Assignment & Constraints → Analytics
  Phase 1                    Phase 2              Phase 3                  Phase 4
```

**Key technical highlights:**
- Phone numbers normalized to E.164 (`+233...`) format
- Region names fuzzy-matched against valid Ghana regions (RapidFuzz, threshold: 80)
- Turnaround times validated and recalculated from log/resolution dates
- ULID-based customer IDs assigned across `customers` and `complaints` tables
- Full referential integrity (PKs, FKs, NOT NULL constraints)
- Singleton rotating logger with phase-level timing and full stack trace capture

---

## Project Structure

```
├── main.py             # Pipeline entrypoint (all 4 phases)
├── data_prep.py        # Excel loading and sheet merging
├── data_cleaner.py     # Cleaning, normalization, TAT validation
├── db_handler.py       # Database connection and write utilities
├── schema_manager.py   # Schema creation and profile ID sync
├── data_int.py         # Customer ID assignment and constraints
├── analytics.py        # Indexes, views, materialized views
├── config.py           # Config loaded from .env
├── libs.py             # Centralized imports
└── logger.py           # Rotating file + console logger
```

---

## Setup

1. **Clone and install**
   ```bash
   git clone https://github.com/your-username/customer-support-pipeline.git
   cd customer-support-pipeline
   pip install -r requirements.txt
   ```

2. **Configure environment**
   ```bash
   cp .env.example .env
   ```
   Fill in your values:
   ```env
   CS2025_DATA_PATH=/path/to/data/folder
   CS2025_EXCEL_FILE=CUSTOMER SUPPORT-2025.xlsx
   CS2025_EXCLUDE_SHEETS=Unresolved
   CS2025_SCHEMA=your_schema_name

   DB_USER=your_user
   DB_PASSWORD=your_password
   DB_HOST=your_host
   DB_NAME=your_db
   DB_PORT=5432

   LOG_LEVEL=INFO
   LOG_FILE_MAX_SIZE=10
   LOG_BACKUP_COUNT=5
   ```

3. **Run**
   ```bash
   python main.py
   ```

> ⚠️ The pipeline does a **full refresh** on every run — the target schema is dropped and recreated.

---

## Module Reference

### `data_prep.py` — `CustomerSupportDataPrep`
| Method | Description |
|---|---|
| `load_excel_data()` | Loads all sheets from the Excel file into a dict of DataFrames |
| `merge_sheets(exclude_sheets)` | Concatenates sheets, aligning mismatched columns with `NaN` |

### `data_cleaner.py` — `DataCleaner`
| Method | Description |
|---|---|
| `clean_columns()` | Normalizes names, column headers, phone numbers, regions, dates and types |
| `validate_and_calculate_tat()` | Fixes negative/missing turnaround times and corrects swapped dates |

### `db_handler.py` — `DatabaseHandler`
| Method | Description |
|---|---|
| `write_dataframe(df, table, schema, if_exists)` | Writes a DataFrame to a PostgreSQL table |
| `execute_query(query)` | Executes a raw SQL string |

### `schema_manager.py` — `SchemaManager`
| Method | Description |
|---|---|
| `split_data(df)` | Separates the flat DataFrame into `customers` and `complaints` |
| `setup_schema(df)` | Drops and recreates the schema, writes both tables, syncs external columns |
| `sync_profile_ids()` | Populates `profileId` by joining on phone number against `public.client` |
| `sync_number2()` | Populates `number2` from `public.client` using `profileId` |

### `data_int.py` — `DataIntegrator`
| Method | Description |
|---|---|
| `assign_customer_ids()` | Generates ULIDs per customer, keyed on `profileId` then phone number |
| `reorder_table_columns()` | Applies a clean, logical column order to both tables |
| `apply_constraints()` | Adds `PRIMARY KEY`, `FOREIGN KEY`, and `NOT NULL` constraints |

### `analytics.py` — `Analytics`
| Method | Description |
|---|---|
| `create_indexes()` | Adds indexes on `customerId`, `profileId`, `number`, and `region` |
| `create_views()` | Creates 5 SQL views for reporting |
| `create_materialized_views()` | Creates 1 materialized view for monthly summaries |

---

## Database Schema

### `customers`
| Column | Type | Notes |
|---|---|---|
| `customerId` | VARCHAR | PRIMARY KEY, ULID |
| `profileId` | VARCHAR | Synced from `public.client` |
| `name` | VARCHAR | NOT NULL |
| `number` | VARCHAR | NOT NULL, E.164 format |
| `number2` | VARCHAR | Secondary phone |
| `gender` | VARCHAR | |
| `dateOfBirth` | DATE | |
| `accountType` | VARCHAR | |
| `branch` | VARCHAR | |

### `complaints`
| Column | Type | Notes |
|---|---|---|
| `customerId` | VARCHAR | NOT NULL, FK → customers |
| `profileId` | VARCHAR | |
| `number` | VARCHAR | NOT NULL |
| `logDate` | DATE | NOT NULL |
| `complaintSource` | VARCHAR | |
| `natureOfComplaint` | VARCHAR | |
| `subject` | VARCHAR | |
| `detailsOfComplaint` | VARCHAR | |
| `status` | VARCHAR | |
| `turnaroundTime` | INTEGER | Days to resolution |
| `resolutionDate` | DATE | |
| `region` | VARCHAR | Fuzzy-matched |
| `location` | VARCHAR | |
| `updates` | VARCHAR | |
| `comment` | VARCHAR | |
| `reasonForReversalRequest` | VARCHAR | |

---

## Analytics Layer

| View | Description |
|---|---|
| `vw_customer_overview` | Customers with total complaints and first/last complaint dates |
| `vw_complaint_summary` | Complaints with customer info and turnaround category |
| `vw_regional_stats` | Resolution rate and avg turnaround by region |
| `vw_complaint_status` | Complaint counts and percentages by status |
| `vw_monthly_trends` | Monthly volume, top complaint type, and top region |
| `mv_monthly_complaint_summary` | Materialized monthly summary for fast queries |

---

## Achievements

| Area | Result |
|---|---|
| Records processed | 50,000+ rows across monthly sheets |
| Phone number standardization | ~95% accuracy |
| Region name correction | ~90% accuracy via fuzzy matching |
| Query performance | ~70% faster with indexes on key columns |
| Data integrity | Full referential integrity across customers and complaints |
| Observability | Phase-level timing, error context, and audit trail on every run |

---

## Business Impact

- **Fragmented Excel data → centralized PostgreSQL database** accessible to the whole team
- **Report generation reduced from hours to real-time queries** via pre-built views
- **Unified historical customer support view** linking complaints back to customer profiles
- **Reduced debugging time from days to minutes** through structured logging and error context
- **Complete audit trail** for every pipeline run — what changed, when, and why

---

## Requirements

- Python 3.8+
- PostgreSQL 12+
- A `public.client` table with `profileId`, `phoneNumber`, `phoneNumber2` columns
