# EPA_GHG – Snowflake Ingestion

This folder contains the **Snowflake SQL scripts** used for setting up the database, warehouse, schemas, and raw ingestion tables for the U.S. Greenhouse Gas Emissions project.

---

## 📌 Overview
- Creates the Snowflake **database** (`EPA_GHG`) and **warehouse** (`WH_GHG`).
- Defines schema lifecycle: **RAW → STG → CORE → MART**.
- Creates the **RAW_GHG_EMISSIONS** table to store ingested data from EPA FLIGHT.

---

## 📂 Files
- `data_ingestion.sql` →  
  - Creates database & warehouse.  
  - Initializes schemas.  
  - Creates raw ingestion table with metadata tracking.  

---

## ⚙️ Usage
Run the ingestion script inside your Snowflake console:

```sql
USE ROLE ACCOUNTADMIN;
RUN SCRIPT data_ingestion.sql;
