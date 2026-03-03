# 🚀 TCPL AWS Glue POC — Complete Step-by-Step Execution Guide
## Replacing SnapLogic: SAP → S3 → Glue → Snowflake
### Tables: BillingDocument (Parent) + BillingDocumentItem (Child)

---

> **What this document is:**
> This is your complete recipe to execute the TCPL Glue POC tomorrow. Every single step, every single line of code, every config, every decision — all here. You follow this top to bottom and you will complete the POC. Do not skip any step.
>
> **The POC Goal:**
> Prove that AWS Glue can replace SnapLogic for ingesting SAP data into Snowflake — using BillingDocument (5.39M rows, 356 columns) and BillingDocumentItem (24.94M rows, 2,107 columns) as the test case — in Delta Load mode.

---

# 📋 TABLE OF CONTENTS

1. [Understand What You Are Building — Full Picture](#1-understand-what-you-are-building)
2. [Why These 2 Tables — Business Context](#2-why-these-2-tables)
3. [Architecture — Every Layer Explained](#3-architecture)
4. [Pre-Requisites Checklist — Do This Before Anything](#4-pre-requisites-checklist)
5. [Step 1 — SAP Connection Setup (JDBC)](#5-step-1-sap-connection-setup)
6. [Step 2 — S3 Bucket Structure Setup](#6-step-2-s3-bucket-structure-setup)
7. [Step 3 — Snowflake Setup (Target Tables)](#7-step-3-snowflake-setup)
8. [Step 4 — AWS Glue Job: BillingDocument (Parent) — Full Code](#8-step-4-glue-job-billingdocument-parent)
9. [Step 5 — AWS Glue Job: BillingDocumentItem (Child) — Full Code](#9-step-5-glue-job-billingdocumentitem-child)
10. [Step 6 — Delta Load Watermark Logic — Deep Explanation](#10-step-6-delta-load-watermark-logic)
11. [Step 7 — Airflow DAG to Orchestrate the POC](#11-step-7-airflow-dag)
12. [Step 8 — Run the POC — Manual Execution Steps](#12-step-8-run-the-poc)
13. [Step 9 — Validate Your Data — All Checks](#13-step-9-validate-your-data)
14. [Step 10 — Performance Tuning for 24.94M Rows](#14-step-10-performance-tuning)
15. [Common Errors and How to Fix Them](#15-common-errors-and-fixes)
16. [POC Success Criteria — How to Know You Passed](#16-poc-success-criteria)
17. [Presentation Talking Points for Your Manager](#17-presentation-talking-points)
18. [Full Glossary](#18-glossary)

---

# 1. UNDERSTAND WHAT YOU ARE BUILDING

## The Current World (SnapLogic)

```
TODAY:
SAP S4 HANA
    ↓
 SnapLogic (iPaaS tool — extracts data, pushes to S3)
    ↓
 AWS S3 (Raw Layer)
    ↓
 AWS Glue (Transforms — but Glue was NOT doing extraction)
    ↓
 Snowflake
    ↓
 ThoughtSpot / Power BI
```

**The Problem with SnapLogic:**
- SnapLogic is expensive (licensing cost)
- SnapLogic is a black box — hard to customize
- SnapLogic has limited retry/error handling capabilities
- The team wants full control over the extraction layer
- AWS Glue can do extraction AND transformation — so why pay for SnapLogic?

## The New World (POC — What You Are Building)

```
POC TARGET:
SAP S4 HANA
    ↓
 AWS Glue (NEW — now does BOTH extraction AND transformation)
   [Glue connects directly to SAP via JDBC/ODP connector]
    ↓
 AWS S3 Raw Layer (same as before)
    ↓
 AWS Glue Transformation Job (same as before)
    ↓
 Snowflake (same as before)
    ↓
 ThoughtSpot / Power BI (same as before)
```

**What changes:**
SnapLogic is REMOVED from the extraction step. Glue now does it all.

## The Two Tables You Are Working With

```
TABLE 1: BillingDocument (PARENT)
- SAP Table Name: VBRK (Billing Document Header)
- Rows: 5.39 Million
- Columns: 356
- Load Type: Delta (only changed/new records)
- DAG: prd-saps4-hourly-billing-document-delta-load-pipeline
- Schedule: Every hour at :00
- Priority: 0 (Mission Critical)

TABLE 2: BillingDocumentItem (CHILD)
- SAP Table Name: VBRP (Billing Document Item)
- Rows: 24.94 Million
- Columns: 2,107
- Load Type: Delta (only changed/new records)
- DAG: prd-saps4-hourly-billing-document-delta-load-pipeline (same DAG)
- Schedule: Every hour at :00
- Priority: 0 (Mission Critical)
```

## Why Parent-Child Matters

Every billing document (VBRK) has multiple billing items (VBRP).

```
Example:
VBRK (BillingDocument):
├── VBELN: 9000012345    ← Invoice number
├── FKDAT: 2025-03-01    ← Billing date
├── KUNAG: DIST001        ← Distributor
└── NETWR: 50000.00       ← Total net value

VBRP (BillingDocumentItem):
├── VBELN: 9000012345, POSNR: 10  ← Item 10 of invoice
│   ├── MATNR: TEA-ASSAM-500G
│   └── FKIMG: 200 (qty)
├── VBELN: 9000012345, POSNR: 20  ← Item 20 of invoice
│   ├── MATNR: SALT-TATA-1KG
│   └── FKIMG: 500 (qty)
└── VBELN: 9000012345, POSNR: 30  ← Item 30 of invoice
    ├── MATNR: WATER-TATA-1L
    └── FKIMG: 1000 (qty)
```

**The POC Rule:**
- First load Parent (VBRK) — because Item (VBRP) references the Document number from VBRK
- Then load Child (VBRP)
- Never load Child before Parent — referential integrity

---

# 2. WHY THESE 2 TABLES — BUSINESS CONTEXT

## Why Billing Tables Are Priority 0

From the platform overview you know:
- `prd-saps4-hourly-billing-document-delta-load-pipeline` runs every hour at :00
- It is Priority 0 — Mission Critical
- The CFA Checkpoint at 20:11 IST waits for the billing document DAG as one of its 9 dependencies

**Business flow:**
1. Distributor places order → Sales Order (VBAK/VBAP)
2. Goods dispatched from CFA → Delivery (LIKP/LIPS)
3. Goods billed → **Billing Document (VBRK/VBRP)** ← YOU ARE HERE
4. Billing document = invoice = money owed by distributor

**If billing data is not in Snowflake:**
- ThoughtSpot dashboards show wrong revenue
- Finance cannot see daily revenue
- The 20:11 CFA checkpoint fails
- CFA mart not refreshed
- Next day's dispatches may be affected

**This is exactly why these tables are the perfect POC tables:**
- High business criticality = strong proof of concept value
- Complex enough (5.39M + 24.94M rows, 356 + 2107 columns) = technically challenging
- Delta load required = tests the most important capability (watermark tracking)

---

# 3. ARCHITECTURE — EVERY LAYER EXPLAINED

## Complete POC Architecture

```
╔══════════════════════════════════════════════════════════════════════════╗
║                     TCPL GLUE POC — FULL ARCHITECTURE                   ║
╚══════════════════════════════════════════════════════════════════════════╝

┌─────────────────────────────────────────────────────────────────────────┐
│  LAYER 0: SOURCE                                                         │
│                                                                          │
│  SAP S4/HANA                                                             │
│  ┌──────────────┐    ┌─────────────────────┐                            │
│  │ VBRK         │    │ VBRP                │                            │
│  │ BillingDoc   │    │ BillingDocItem      │                            │
│  │ 5.39M rows   │    │ 24.94M rows         │                            │
│  │ 356 columns  │    │ 2,107 columns       │                            │
│  └──────┬───────┘    └──────────┬──────────┘                            │
│         │ JDBC/ODP              │ JDBC/ODP                              │
└─────────┼───────────────────────┼─────────────────────────────────────-─┘
          │                       │
          ▼                       ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  LAYER 1: EXTRACTION (AWS GLUE JOB — NEW, REPLACES SNAPLOGIC)           │
│                                                                          │
│  Glue Job 1: glue-poc-billingdocument-delta-extract                     │
│  - Reads from VBRK via JDBC                                             │
│  - Applies watermark filter (AEDAT > last_run_date)                     │
│  - Writes Parquet to S3 Raw                                             │
│                                                                          │
│  Glue Job 2: glue-poc-billingdocumentitem-delta-extract                 │
│  - Reads from VBRP via JDBC                                             │
│  - Applies watermark filter                                             │
│  - Writes Parquet to S3 Raw                                             │
│  - Runs AFTER Job 1 completes                                           │
└─────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  LAYER 2: S3 RAW (Landing Zone)                                         │
│                                                                          │
│  s3://tcpl-glue-poc/raw/sap/VBRK/year=2025/month=03/day=01/             │
│  s3://tcpl-glue-poc/raw/sap/VBRP/year=2025/month=03/day=01/             │
│                                                                          │
│  Format: Parquet (columnar, compressed)                                 │
│  Partition: by date extracted                                           │
└─────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  LAYER 3: TRANSFORMATION (AWS GLUE JOB)                                │
│                                                                          │
│  Glue Job 3: glue-poc-billingdocument-transform-load                   │
│  - Reads from S3 Raw                                                    │
│  - Deduplicates                                                         │
│  - Renames columns (SAP names → readable names)                        │
│  - Type casting (strings → dates, decimals)                            │
│  - Writes to S3 Processed                                              │
│  - Loads to Snowflake via COPY INTO                                    │
└─────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  LAYER 4: SNOWFLAKE (Target)                                            │
│                                                                          │
│  DB_SAP.CORE_SAP.BILLING_DOCUMENT          ← from VBRK                 │
│  DB_SAP.CORE_SAP.BILLING_DOCUMENT_ITEM     ← from VBRP                 │
│                                                                          │
│  Load mode: UPSERT (MERGE) — delta records update existing rows        │
└─────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  LAYER 5: ORCHESTRATION (Airflow)                                       │
│                                                                          │
│  DAG: glue_poc_billing_delta_load                                       │
│  Schedule: Hourly (to match existing pipeline)                         │
│  Tasks: extract_vbrk → extract_vbrp → transform_vbrk → transform_vbrp  │
└─────────────────────────────────────────────────────────────────────────┘
```

## Delta Load Concept — The Core of This POC

```
DELTA LOAD = Only extract CHANGED or NEW records since last run

WHY DELTA?
Full Load = extract ALL 5.39M rows every hour → TOO SLOW, too expensive
Delta Load = extract only rows changed in the last hour → FAST, efficient

HOW DELTA WORKS (Watermark Method):

Run 1 (First ever run):
  → Extract ALL rows (initial full load)
  → Record watermark: "Last run = 2025-03-01 08:00:00"
  → Load everything to Snowflake

Run 2 (1 hour later):
  → Query: SELECT * FROM VBRK WHERE AEDAT >= '2025-03-01 08:00:00'
  → Only get rows changed/created after the last run
  → Update watermark: "Last run = 2025-03-01 09:00:00"
  → UPSERT into Snowflake (update existing, insert new)

Run 3 (next hour):
  → Query: SELECT * FROM VBRK WHERE AEDAT >= '2025-03-01 09:00:00'
  → And so on...

WATERMARK COLUMN in VBRK:
- AEDAT = Last changed date
- ERDAT = Created date
- We use MAX(AEDAT, ERDAT) as the watermark — catches both new and updated
```

---

# 4. PRE-REQUISITES CHECKLIST — DO THIS BEFORE ANYTHING

Go through every item below. If anything is missing, get it sorted BEFORE starting the POC code.

## AWS Access

```
✅ CHECKLIST:

□ AWS Console access with these permissions:
  - glue:* (create, run, monitor Glue jobs)
  - s3:* (create buckets, read/write objects)
  - secretsmanager:GetSecretValue (read SAP and Snowflake credentials)
  - iam:PassRole (assign roles to Glue)
  - logs:* (view CloudWatch logs)
  - ec2:* (if using Glue with VPC for SAP connectivity)

□ AWS Region confirmed: (ask your team — likely ap-south-1 Mumbai or us-east-1)

□ AWS Account ID: __________________ (write it here)
```

## SAP Access Details (from AWS Secrets Manager or your team)

```
□ SAP JDBC URL: 
  Format: jdbc:sap://<SAP_HOST>:<PORT>/?databaseName=<DB>
  Example: jdbc:sap://10.0.1.50:30015/?databaseName=S4H
  
□ SAP Username: __________________ (service account, not personal)

□ SAP Password: __________________ (stored in AWS Secrets Manager)

□ SAP Secret Name in Secrets Manager: __________________ 
  (e.g., "tcpl/sap/jdbc-credentials")

□ SAP table access confirmed:
  - Can SELECT from VBRK? □ Yes
  - Can SELECT from VBRP? □ Yes
  (Ask SAP Basis team to confirm — you need read access)

□ SAP network connectivity from Glue:
  - Is SAP accessible from AWS VPC? □ Yes
  - VPC ID: __________________
  - Subnet ID: __________________
  - Security Group that allows port 30015 to SAP: __________________
```

## Snowflake Access Details

```
□ Snowflake Account URL:
  Format: <account>.snowflakecomputing.com
  
□ Snowflake Username: __________________

□ Snowflake Password: __________________ (in Secrets Manager)

□ Snowflake Secret Name: __________________ 
  (e.g., "tcpl/snowflake/glue-credentials")

□ Snowflake Database: DB_SAP (or POC database — confirm with Sam)

□ Snowflake Schema: CORE_SAP (or POC schema)

□ Snowflake Warehouse: WH_ETL (confirm with Sam)

□ Snowflake Role: SYSADMIN or specific ETL role
```

## S3 Setup

```
□ S3 Bucket for POC: __________________ 
  (e.g., tcpl-glue-poc OR existing tcpl-datalake bucket with /poc/ prefix)
  
□ Glue Temp Directory (S3 path for Glue temp files):
  s3://<bucket>/glue-temp/
  
□ Glue Scripts Location:
  s3://<bucket>/glue-scripts/
```

## Glue Setup

```
□ Glue IAM Role exists: __________________
  (This role needs: S3 read/write, Secrets Manager read, CloudWatch write)
  
□ Glue version confirmed: Glue 4.0 (uses Spark 3.3 — latest stable)

□ Glue Python version: Python 3.10

□ SAP JDBC Driver JAR uploaded to S3:
  - SAP HANA JDBC Driver: ngdbc.jar
  - Location: s3://<bucket>/jars/ngdbc.jar
  - Version: Match your SAP HANA version
  (Download from: SAP Software Downloads — HANA Client)
```

## Watermark Table in Snowflake

```
□ Watermark control table created in Snowflake:
  (Script in Step 3 below — create this first)
```

---

# 5. STEP 1 — SAP CONNECTION SETUP (JDBC)

## 5.1 Store SAP Credentials in AWS Secrets Manager

**Go to AWS Console → Secrets Manager → Store a new secret**

```
Secret Type: Other type of secret

Key-Value pairs:
  username: <SAP_SERVICE_ACCOUNT_USERNAME>
  password: <SAP_SERVICE_ACCOUNT_PASSWORD>
  jdbc_url:  jdbc:sap://<SAP_HOST>:30015/?databaseName=<SCHEMA>

Secret Name: tcpl/sap/glue-poc-credentials
Description: SAP HANA credentials for Glue POC - BillingDocument extraction
```

## 5.2 Store Snowflake Credentials in AWS Secrets Manager

```
Secret Type: Other type of secret

Key-Value pairs:
  username:   <SNOWFLAKE_USERNAME>
  password:   <SNOWFLAKE_PASSWORD>
  account:    <SNOWFLAKE_ACCOUNT>
  warehouse:  WH_ETL
  database:   DB_SAP
  schema:     CORE_SAP
  role:       ETL_ROLE

Secret Name: tcpl/snowflake/glue-poc-credentials
Description: Snowflake credentials for Glue POC
```

## 5.3 Create Glue Connection to SAP

**AWS Console → AWS Glue → Connections → Add Connection**

```
Connection name:    tcpl-sap-hana-poc
Connection type:    JDBC
JDBC URL:           jdbc:sap://<SAP_HOST>:30015/?databaseName=<DB>
Username:           <from Secrets Manager>
Password:           <from Secrets Manager>

VPC:                <your VPC ID>
Subnet:             <private subnet with SAP access>
Security Group:     <SG allowing outbound to SAP port 30015>
```

**Test the connection:**
After creating, click "Test Connection" — it must show green before proceeding.

## 5.4 Upload SAP JDBC Driver to S3

```bash
# Download ngdbc.jar from SAP Marketplace
# Then upload to S3:

aws s3 cp ngdbc.jar s3://<your-bucket>/jars/ngdbc.jar

# Verify:
aws s3 ls s3://<your-bucket>/jars/
```

---

# 6. STEP 2 — S3 BUCKET STRUCTURE SETUP

## Create the Folder Structure

Run these AWS CLI commands (or create manually in S3 Console):

```bash
# Set your bucket name
BUCKET="tcpl-glue-poc"  # change this to your actual bucket

# Create all folder structure (S3 uses prefixes, not real folders)
# Create placeholder files to establish the structure:

# RAW LAYER — where Glue writes extracted data from SAP
aws s3api put-object --bucket $BUCKET --key raw/sap/VBRK/
aws s3api put-object --bucket $BUCKET --key raw/sap/VBRP/

# PROCESSED LAYER — where transformed data lives before loading to Snowflake
aws s3api put-object --bucket $BUCKET --key processed/sap/VBRK/
aws s3api put-object --bucket $BUCKET --key processed/sap/VBRP/

# GLUE TEMP — Glue needs a temp directory for intermediate operations
aws s3api put-object --bucket $BUCKET --key glue-temp/

# GLUE SCRIPTS — where your Python job files live
aws s3api put-object --bucket $BUCKET --key glue-scripts/

# JARS — where JDBC drivers live
# (you already uploaded ngdbc.jar here in step 5.4)

# WATERMARK — control table backups (optional)
aws s3api put-object --bucket $BUCKET --key watermark/
```

## S3 Path Convention for This POC

```
s3://tcpl-glue-poc/
├── raw/
│   └── sap/
│       ├── VBRK/                          ← BillingDocument raw data
│       │   └── extracted_date=2025-03-01/ ← Date partition
│       │       └── part-00000.parquet
│       └── VBRP/                          ← BillingDocumentItem raw data
│           └── extracted_date=2025-03-01/
│               ├── part-00000.parquet
│               ├── part-00001.parquet     ← Multiple parts (large table)
│               └── part-00002.parquet
│
├── processed/
│   └── sap/
│       ├── VBRK/
│       └── VBRP/
│
├── glue-temp/                             ← Glue internal temp files
├── glue-scripts/                          ← Your Python script files
│   ├── billing_document_extract.py
│   ├── billing_document_item_extract.py
│   ├── billing_document_transform.py
│   └── billing_document_item_transform.py
└── jars/
    └── ngdbc.jar                          ← SAP HANA JDBC driver
```

---

# 7. STEP 3 — SNOWFLAKE SETUP (TARGET TABLES)

## Run All These SQL Scripts in Snowflake (in order)

### 7.1 Create POC Database/Schema (if not using existing)

```sql
-- Option A: Use existing DB (recommended — check with Sam)
USE DATABASE DB_SAP;
USE SCHEMA CORE_SAP;

-- Option B: Create POC-specific schema to avoid touching production
USE DATABASE DB_SAP;
CREATE SCHEMA IF NOT EXISTS CORE_SAP_POC COMMENT = 'POC schema for Glue migration test';
USE SCHEMA CORE_SAP_POC;
```

### 7.2 Create Watermark Control Table

```sql
-- This table is the HEART of delta loading
-- It remembers the last time each table was successfully loaded
-- Glue reads from this table to know where to start extraction

CREATE TABLE IF NOT EXISTS PIPELINE_WATERMARK (
    table_name          VARCHAR(200)       NOT NULL,
    source_system       VARCHAR(50)        NOT NULL DEFAULT 'SAP',
    last_watermark_ts   TIMESTAMP_NTZ,                    -- last successful run timestamp
    last_run_status     VARCHAR(20)        NOT NULL DEFAULT 'PENDING',  -- SUCCESS / FAILED / RUNNING
    last_run_start      TIMESTAMP_NTZ,                    -- when last run started
    last_run_end        TIMESTAMP_NTZ,                    -- when last run ended
    rows_extracted      NUMBER(18,0)       DEFAULT 0,     -- how many rows extracted last run
    rows_loaded         NUMBER(18,0)       DEFAULT 0,     -- how many rows loaded to Snowflake
    notes               VARCHAR(1000),                    -- any notes or error messages
    created_at          TIMESTAMP_NTZ      DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ      DEFAULT CURRENT_TIMESTAMP(),
    
    PRIMARY KEY (table_name, source_system)
);

COMMENT ON TABLE PIPELINE_WATERMARK IS 
'Delta load watermark control table. Tracks last successful extraction timestamp per table. Used by AWS Glue jobs to determine what to extract.';
```

### 7.3 Initialize Watermark Records

```sql
-- Insert initial records for BillingDocument and BillingDocumentItem
-- First run will do a full historical load starting from this date
-- Adjust the date to match your desired historical start point

-- For POC: start from 90 days ago (adjust as needed)
INSERT INTO PIPELINE_WATERMARK 
    (table_name, source_system, last_watermark_ts, last_run_status, notes)
VALUES 
    ('VBRK', 'SAP', DATEADD('day', -90, CURRENT_TIMESTAMP()), 'PENDING', 
     'Initial watermark for POC - BillingDocument'),
    ('VBRP', 'SAP', DATEADD('day', -90, CURRENT_TIMESTAMP()), 'PENDING', 
     'Initial watermark for POC - BillingDocumentItem');

-- Verify:
SELECT * FROM PIPELINE_WATERMARK;
```

### 7.4 Create BillingDocument Target Table (VBRK)

```sql
-- Main billing document header table
-- Key SAP columns mapped to readable names
-- NOTE: For the POC, we're including the most important columns
-- In production you would include all 356 columns

CREATE TABLE IF NOT EXISTS BILLING_DOCUMENT (
    -- Primary Key
    BILLING_DOC_NUMBER      VARCHAR(10)     NOT NULL,   -- VBELN: Billing document number
    
    -- Document Info
    BILLING_DOC_TYPE        VARCHAR(4),                 -- FKART: Billing type
    BILLING_DATE            DATE,                       -- FKDAT: Billing date
    BILLING_STATUS          VARCHAR(2),                 -- RFBSK: Document status
    CREATED_DATE            DATE,                       -- ERDAT: Created on
    CREATED_TIME            VARCHAR(6),                 -- ERZET: Created at (time)
    CREATED_BY              VARCHAR(12),                -- ERNAM: Created by
    CHANGED_DATE            DATE,                       -- AEDAT: Last changed date
    
    -- Customer / Partner
    SOLD_TO_PARTY           VARCHAR(10),                -- KUNAG: Sold-to party
    PAYER                   VARCHAR(10),                -- KUNRE: Payer
    BILL_TO_PARTY           VARCHAR(10),                -- KUNRG: Bill-to party
    
    -- Organization
    SALES_ORG               VARCHAR(4),                 -- VKORG: Sales organization
    DIST_CHANNEL            VARCHAR(2),                 -- VTWEG: Distribution channel
    DIVISION                VARCHAR(2),                 -- SPART: Division
    
    -- Financial
    NET_VALUE               NUMBER(15,2),               -- NETWR: Net value
    TAX_VALUE               NUMBER(15,2),               -- MWSBK: Tax amount
    CURRENCY                VARCHAR(5),                 -- WAERK: Currency
    EXCHANGE_RATE           NUMBER(9,5),                -- KURRF: Exchange rate
    
    -- Reference Documents
    REFERENCE_DOC           VARCHAR(16),                -- XBLNR: Reference document
    SALES_DOC_REF           VARCHAR(10),                -- ZUONR: Assignment number
    
    -- Delivery Info
    SHIPPING_CONDITION      VARCHAR(2),                 -- VSBED: Shipping conditions
    INCOTERMS               VARCHAR(3),                 -- INCO1: Incoterms 1
    
    -- Payment
    PAYMENT_TERMS           VARCHAR(4),                 -- ZTERM: Payment terms
    PAYMENT_METHOD          VARCHAR(1),                 -- ZLSCH: Payment method
    
    -- Plant / Company
    COMPANY_CODE            VARCHAR(4),                 -- BUKRS: Company code
    
    -- Metadata (added by Glue pipeline)
    EXTRACTED_AT            TIMESTAMP_NTZ,              -- When Glue extracted this record
    LOAD_BATCH_ID           VARCHAR(50),                -- Unique batch ID for this Glue run
    SOURCE_SYSTEM           VARCHAR(20) DEFAULT 'SAP',  -- Source system identifier
    IS_DELETED              BOOLEAN DEFAULT FALSE,      -- Soft delete flag
    
    PRIMARY KEY (BILLING_DOC_NUMBER)
)
CLUSTER BY (BILLING_DATE)
COMMENT = 'SAP Billing Document Header (VBRK). Loaded by AWS Glue POC. Replaces SnapLogic pipeline prd-saps4-hourly-billing-document-delta-load-pipeline.';
```

### 7.5 Create BillingDocumentItem Target Table (VBRP)

```sql
-- Billing document item / line item table
-- Each billing document can have many items

CREATE TABLE IF NOT EXISTS BILLING_DOCUMENT_ITEM (
    -- Primary Key (composite)
    BILLING_DOC_NUMBER      VARCHAR(10)     NOT NULL,   -- VBELN: Billing document
    BILLING_ITEM_NUMBER     VARCHAR(6)      NOT NULL,   -- POSNR: Item number
    
    -- Material
    MATERIAL_NUMBER         VARCHAR(18),                -- MATNR: Material number
    MATERIAL_DESC           VARCHAR(40),                -- ARKTX: Short text
    MATERIAL_GROUP          VARCHAR(9),                 -- MATKL: Material group
    
    -- Quantities
    BILLED_QUANTITY         NUMBER(13,3),               -- FKIMG: Billed quantity
    BILLED_UOM              VARCHAR(3),                 -- VRKME: Unit of measure
    BASE_QUANTITY           NUMBER(13,3),               -- UMVKZ: Base quantity
    
    -- Values
    NET_VALUE               NUMBER(15,2),               -- NETWR: Net value
    NET_PRICE               NUMBER(15,5),               -- NETPR: Net price
    CURRENCY                VARCHAR(5),                 -- WAERK: Currency
    GROSS_VALUE             NUMBER(15,2),               -- KWMENG-related
    
    -- References
    SALES_DOC               VARCHAR(10),                -- AUBEL: Order number
    SALES_ITEM              VARCHAR(6),                 -- AUPOS: Order item
    DELIVERY_DOC            VARCHAR(10),                -- VGBEL: Delivery document
    DELIVERY_ITEM           VARCHAR(6),                 -- VGPOS: Delivery item
    
    -- Tax
    TAX_AMOUNT              NUMBER(15,2),               -- MWSBP: Tax amount
    TAX_CLASS               VARCHAR(1),                 -- MWSKZ: Tax classification
    
    -- Plant / Storage
    PLANT                   VARCHAR(4),                 -- WERKS: Plant
    STORAGE_LOC             VARCHAR(4),                 -- LGORT: Storage location
    
    -- Pricing
    CONDITION_TYPE          VARCHAR(4),                 -- Most relevant pricing condition
    
    -- Dates
    BILLING_DATE            DATE,                       -- FKDAT: Billing date (from header join)
    CREATED_DATE            DATE,                       -- ERDAT
    CHANGED_DATE            DATE,                       -- AEDAT
    
    -- Metadata
    EXTRACTED_AT            TIMESTAMP_NTZ,
    LOAD_BATCH_ID           VARCHAR(50),
    SOURCE_SYSTEM           VARCHAR(20) DEFAULT 'SAP',
    IS_DELETED              BOOLEAN DEFAULT FALSE,
    
    PRIMARY KEY (BILLING_DOC_NUMBER, BILLING_ITEM_NUMBER)
)
CLUSTER BY (BILLING_DATE, PLANT)
COMMENT = 'SAP Billing Document Items (VBRP). Loaded by AWS Glue POC.';
```

### 7.6 Create Staging Tables for UPSERT Operations

```sql
-- Staging tables receive the new delta data
-- Then we MERGE staging into the main table

CREATE TABLE IF NOT EXISTS BILLING_DOCUMENT_STAGE (LIKE BILLING_DOCUMENT)
COMMENT = 'Staging table for BILLING_DOCUMENT delta loads. Truncated and reloaded each run.';

CREATE TABLE IF NOT EXISTS BILLING_DOCUMENT_ITEM_STAGE (LIKE BILLING_DOCUMENT_ITEM)
COMMENT = 'Staging table for BILLING_DOCUMENT_ITEM delta loads.';
```

### 7.7 Create Stored Procedures for UPSERT

```sql
-- Procedure to merge staging data into main table for VBRK
CREATE OR REPLACE PROCEDURE UPSERT_BILLING_DOCUMENT()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    rows_merged NUMBER;
    result_msg VARCHAR;
BEGIN
    -- MERGE: update existing rows, insert new rows
    MERGE INTO BILLING_DOCUMENT AS target
    USING BILLING_DOCUMENT_STAGE AS source
    ON target.BILLING_DOC_NUMBER = source.BILLING_DOC_NUMBER
    WHEN MATCHED THEN UPDATE SET
        target.BILLING_DOC_TYPE     = source.BILLING_DOC_TYPE,
        target.BILLING_DATE         = source.BILLING_DATE,
        target.BILLING_STATUS       = source.BILLING_STATUS,
        target.CHANGED_DATE         = source.CHANGED_DATE,
        target.SOLD_TO_PARTY        = source.SOLD_TO_PARTY,
        target.PAYER                = source.PAYER,
        target.BILL_TO_PARTY        = source.BILL_TO_PARTY,
        target.SALES_ORG            = source.SALES_ORG,
        target.DIST_CHANNEL         = source.DIST_CHANNEL,
        target.DIVISION             = source.DIVISION,
        target.NET_VALUE            = source.NET_VALUE,
        target.TAX_VALUE            = source.TAX_VALUE,
        target.CURRENCY             = source.CURRENCY,
        target.EXCHANGE_RATE        = source.EXCHANGE_RATE,
        target.REFERENCE_DOC        = source.REFERENCE_DOC,
        target.COMPANY_CODE         = source.COMPANY_CODE,
        target.EXTRACTED_AT         = source.EXTRACTED_AT,
        target.LOAD_BATCH_ID        = source.LOAD_BATCH_ID
    WHEN NOT MATCHED THEN INSERT (
        BILLING_DOC_NUMBER, BILLING_DOC_TYPE, BILLING_DATE, BILLING_STATUS,
        CREATED_DATE, CREATED_TIME, CREATED_BY, CHANGED_DATE,
        SOLD_TO_PARTY, PAYER, BILL_TO_PARTY,
        SALES_ORG, DIST_CHANNEL, DIVISION,
        NET_VALUE, TAX_VALUE, CURRENCY, EXCHANGE_RATE,
        REFERENCE_DOC, SALES_DOC_REF, SHIPPING_CONDITION,
        INCOTERMS, PAYMENT_TERMS, PAYMENT_METHOD, COMPANY_CODE,
        EXTRACTED_AT, LOAD_BATCH_ID, SOURCE_SYSTEM
    ) VALUES (
        source.BILLING_DOC_NUMBER, source.BILLING_DOC_TYPE, source.BILLING_DATE,
        source.BILLING_STATUS, source.CREATED_DATE, source.CREATED_TIME,
        source.CREATED_BY, source.CHANGED_DATE, source.SOLD_TO_PARTY,
        source.PAYER, source.BILL_TO_PARTY, source.SALES_ORG,
        source.DIST_CHANNEL, source.DIVISION, source.NET_VALUE,
        source.TAX_VALUE, source.CURRENCY, source.EXCHANGE_RATE,
        source.REFERENCE_DOC, source.SALES_DOC_REF, source.SHIPPING_CONDITION,
        source.INCOTERMS, source.PAYMENT_TERMS, source.PAYMENT_METHOD,
        source.COMPANY_CODE, source.EXTRACTED_AT, source.LOAD_BATCH_ID,
        source.SOURCE_SYSTEM
    );
    
    result_msg := 'UPSERT_BILLING_DOCUMENT completed successfully.';
    RETURN result_msg;
END;
$$;

-- Same procedure for VBRP
CREATE OR REPLACE PROCEDURE UPSERT_BILLING_DOCUMENT_ITEM()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO BILLING_DOCUMENT_ITEM AS target
    USING BILLING_DOCUMENT_ITEM_STAGE AS source
    ON target.BILLING_DOC_NUMBER = source.BILLING_DOC_NUMBER
       AND target.BILLING_ITEM_NUMBER = source.BILLING_ITEM_NUMBER
    WHEN MATCHED THEN UPDATE SET
        target.MATERIAL_NUMBER      = source.MATERIAL_NUMBER,
        target.MATERIAL_DESC        = source.MATERIAL_DESC,
        target.BILLED_QUANTITY      = source.BILLED_QUANTITY,
        target.BILLED_UOM           = source.BILLED_UOM,
        target.NET_VALUE            = source.NET_VALUE,
        target.NET_PRICE            = source.NET_PRICE,
        target.CURRENCY             = source.CURRENCY,
        target.SALES_DOC            = source.SALES_DOC,
        target.DELIVERY_DOC         = source.DELIVERY_DOC,
        target.PLANT                = source.PLANT,
        target.BILLING_DATE         = source.BILLING_DATE,
        target.CHANGED_DATE         = source.CHANGED_DATE,
        target.EXTRACTED_AT         = source.EXTRACTED_AT,
        target.LOAD_BATCH_ID        = source.LOAD_BATCH_ID
    WHEN NOT MATCHED THEN INSERT (
        BILLING_DOC_NUMBER, BILLING_ITEM_NUMBER, MATERIAL_NUMBER,
        MATERIAL_DESC, MATERIAL_GROUP, BILLED_QUANTITY, BILLED_UOM,
        BASE_QUANTITY, NET_VALUE, NET_PRICE, CURRENCY,
        SALES_DOC, SALES_ITEM, DELIVERY_DOC, DELIVERY_ITEM,
        TAX_AMOUNT, PLANT, STORAGE_LOC, BILLING_DATE,
        CREATED_DATE, CHANGED_DATE, EXTRACTED_AT, LOAD_BATCH_ID, SOURCE_SYSTEM
    ) VALUES (
        source.BILLING_DOC_NUMBER, source.BILLING_ITEM_NUMBER, source.MATERIAL_NUMBER,
        source.MATERIAL_DESC, source.MATERIAL_GROUP, source.BILLED_QUANTITY, source.BILLED_UOM,
        source.BASE_QUANTITY, source.NET_VALUE, source.NET_PRICE, source.CURRENCY,
        source.SALES_DOC, source.SALES_ITEM, source.DELIVERY_DOC, source.DELIVERY_ITEM,
        source.TAX_AMOUNT, source.PLANT, source.STORAGE_LOC, source.BILLING_DATE,
        source.CREATED_DATE, source.CHANGED_DATE, source.EXTRACTED_AT,
        source.LOAD_BATCH_ID, source.SOURCE_SYSTEM
    );
    
    RETURN 'UPSERT_BILLING_DOCUMENT_ITEM completed successfully.';
END;
$$;
```

---

# 8. STEP 4 — GLUE JOB: BILLINGDOCUMENT (PARENT) — FULL CODE

## 8.1 Create the Python Script

Save this as: `billing_document_extract_transform.py`
Upload to: `s3://<your-bucket>/glue-scripts/billing_document_extract_transform.py`

```python
"""
TCPL AWS Glue POC
Job: BillingDocument (VBRK) Delta Extract + Load to Snowflake
Replaces: prd-saps4-hourly-billing-document-delta-load-pipeline (SnapLogic)

Author: Decision Point Team
Version: 1.0
Date: 2025

WHAT THIS JOB DOES:
1. Reads last watermark timestamp from Snowflake control table
2. Connects to SAP via JDBC
3. Extracts VBRK records changed since watermark
4. Writes raw data to S3 (Parquet, partitioned by date)
5. Applies transformations (column rename, type cast, dedup)
6. Loads to Snowflake staging table
7. Calls MERGE stored procedure to upsert into main table
8. Updates watermark timestamp on success
"""

import sys
import json
import boto3
import logging
from datetime import datetime, timezone
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *
import snowflake.connector

# ============================================================
# SETUP — LOGGING, CONTEXT, ARGUMENTS
# ============================================================

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BILLING_DOCUMENT_EXTRACT")

# Get job parameters (passed from Airflow or Glue console)
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'sap_secret_name',         # AWS Secrets Manager secret for SAP
    'snowflake_secret_name',   # AWS Secrets Manager secret for Snowflake
    's3_bucket',               # e.g., tcpl-glue-poc
    's3_raw_prefix',           # e.g., raw/sap/VBRK
    's3_processed_prefix',     # e.g., processed/sap/VBRK
    'snowflake_database',      # e.g., DB_SAP
    'snowflake_schema',        # e.g., CORE_SAP_POC
    'batch_size',              # How many rows to process per partition (default: 500000)
])

# Initialize Spark and Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Generate unique batch ID for this run
batch_id = f"GLUE_{args['JOB_NAME']}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
extraction_ts = datetime.now(timezone.utc)
today_partition = extraction_ts.strftime('%Y-%m-%d')

logger.info(f"=== BILLING DOCUMENT EXTRACT JOB STARTING ===")
logger.info(f"Batch ID: {batch_id}")
logger.info(f"Extraction Time: {extraction_ts}")

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_secret(secret_name: str, region_name: str = "ap-south-1") -> dict:
    """Retrieve credentials from AWS Secrets Manager."""
    client = boto3.client('secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])
    logger.info(f"Successfully retrieved secret: {secret_name}")
    return secret


def get_snowflake_connection(sf_creds: dict):
    """Create and return a Snowflake connection."""
    conn = snowflake.connector.connect(
        user=sf_creds['username'],
        password=sf_creds['password'],
        account=sf_creds['account'],
        warehouse=sf_creds['warehouse'],
        database=sf_creds['database'],
        schema=sf_creds['schema'],
        role=sf_creds.get('role', 'SYSADMIN'),
        session_parameters={
            'QUERY_TAG': f'GLUE_POC_{batch_id}'
        }
    )
    logger.info("Snowflake connection established.")
    return conn


def get_watermark(sf_conn, table_name: str) -> str:
    """
    Read the last successful watermark timestamp for a given table.
    Returns timestamp as string in format 'YYYY-MM-DD HH:MM:SS'
    """
    cursor = sf_conn.cursor()
    cursor.execute("""
        SELECT COALESCE(
            TO_CHAR(last_watermark_ts, 'YYYY-MM-DD HH24:MI:SS'),
            '2020-01-01 00:00:00'
        ) AS watermark
        FROM PIPELINE_WATERMARK
        WHERE table_name = %s
          AND source_system = 'SAP'
    """, (table_name,))
    row = cursor.fetchone()
    watermark = row[0] if row else '2020-01-01 00:00:00'
    logger.info(f"Watermark for {table_name}: {watermark}")
    cursor.close()
    return watermark


def update_watermark(sf_conn, table_name: str, status: str, 
                     rows_extracted: int, rows_loaded: int,
                     new_watermark: str = None, notes: str = None):
    """Update the watermark control table after job completion."""
    cursor = sf_conn.cursor()
    
    if status == 'SUCCESS' and new_watermark:
        cursor.execute("""
            UPDATE PIPELINE_WATERMARK
            SET last_watermark_ts = TO_TIMESTAMP_NTZ(%s, 'YYYY-MM-DD HH24:MI:SS'),
                last_run_status   = %s,
                last_run_end      = CURRENT_TIMESTAMP(),
                rows_extracted    = %s,
                rows_loaded       = %s,
                notes             = %s,
                updated_at        = CURRENT_TIMESTAMP()
            WHERE table_name    = %s
              AND source_system = 'SAP'
        """, (new_watermark, status, rows_extracted, rows_loaded, notes, table_name))
    else:
        cursor.execute("""
            UPDATE PIPELINE_WATERMARK
            SET last_run_status = %s,
                last_run_end    = CURRENT_TIMESTAMP(),
                rows_extracted  = %s,
                rows_loaded     = %s,
                notes           = %s,
                updated_at      = CURRENT_TIMESTAMP()
            WHERE table_name    = %s
              AND source_system = 'SAP'
        """, (status, rows_extracted, rows_loaded, notes, table_name))
    
    sf_conn.commit()
    cursor.close()
    logger.info(f"Watermark updated for {table_name}: status={status}, rows_extracted={rows_extracted}")


def mark_watermark_running(sf_conn, table_name: str):
    """Mark table as currently being processed — prevents concurrent runs."""
    cursor = sf_conn.cursor()
    cursor.execute("""
        UPDATE PIPELINE_WATERMARK
        SET last_run_status = 'RUNNING',
            last_run_start  = CURRENT_TIMESTAMP(),
            updated_at      = CURRENT_TIMESTAMP()
        WHERE table_name    = %s
          AND source_system = 'SAP'
    """, (table_name,))
    sf_conn.commit()
    cursor.close()


# ============================================================
# MAIN JOB EXECUTION
# ============================================================

# -- STEP 1: Load Credentials --
logger.info("STEP 1: Loading credentials from Secrets Manager...")
sap_creds = get_secret(args['sap_secret_name'])
sf_creds = get_secret(args['snowflake_secret_name'])

# Override Snowflake credentials with job args (more flexible)
sf_creds['database'] = args['snowflake_database']
sf_creds['schema'] = args['snowflake_schema']

# -- STEP 2: Connect to Snowflake and Get Watermark --
logger.info("STEP 2: Getting watermark from Snowflake...")
sf_conn = get_snowflake_connection(sf_creds)

watermark_ts = get_watermark(sf_conn, 'VBRK')
logger.info(f"Will extract VBRK records changed after: {watermark_ts}")

# Mark as running (prevents duplicate runs)
mark_watermark_running(sf_conn, 'VBRK')

# -- STEP 3: Extract from SAP via JDBC --
logger.info("STEP 3: Extracting VBRK from SAP...")

# IMPORTANT: The WHERE clause uses AEDAT (last changed date) AND ERDAT (created date)
# We use GREATEST to catch both newly created AND recently updated records
#
# SAP VBRK key columns used here:
# VBELN = Billing document number (primary key)
# AEDAT = Date on which the record was last changed
# ERDAT = Date on which the record was created  
# FKDAT = Billing date

sap_query = f"""
    SELECT 
        VBELN,      -- Billing Document Number
        FKART,      -- Billing Type
        FKDAT,      -- Billing Date
        RFBSK,      -- Document Status
        ERDAT,      -- Created Date
        ERZET,      -- Created Time
        ERNAM,      -- Created By
        AEDAT,      -- Last Changed Date
        KUNAG,      -- Sold-to Party
        KUNRE,      -- Payer
        KUNRG,      -- Bill-to Party
        VKORG,      -- Sales Org
        VTWEG,      -- Distribution Channel
        SPART,      -- Division
        NETWR,      -- Net Value
        MWSBK,      -- Tax Amount
        WAERK,      -- Currency
        KURRF,      -- Exchange Rate
        XBLNR,      -- Reference Document
        ZUONR,      -- Assignment Number
        VSBED,      -- Shipping Condition
        INCO1,      -- Incoterms 1
        ZTERM,      -- Payment Terms
        ZLSCH,      -- Payment Method
        BUKRS        -- Company Code
    FROM VBRK
    WHERE AEDAT >= TO_DATE('{watermark_ts}', 'YYYY-MM-DD HH24:MI:SS')
       OR ERDAT >= TO_DATE('{watermark_ts}', 'YYYY-MM-DD HH24:MI:SS')
    ORDER BY AEDAT, VBELN
"""

# NOTE: For SAP HANA, the JDBC connection uses the ngdbc driver
# The dbtable parameter accepts a subquery wrapped in parentheses

logger.info(f"SAP Query (VBRK delta since {watermark_ts}):")
logger.info(sap_query[:500] + "...")  # Log first 500 chars

try:
    # Read from SAP HANA using JDBC
    # numPartitions controls parallelism — for 5.39M rows, 8 partitions is good
    vbrk_raw_df = spark.read \
        .format("jdbc") \
        .option("url", sap_creds['jdbc_url']) \
        .option("dbtable", f"({sap_query}) VBRK_DELTA") \
        .option("user", sap_creds['username']) \
        .option("password", sap_creds['password']) \
        .option("driver", "com.sap.db.jdbc.Driver") \
        .option("numPartitions", "8") \
        .option("partitionColumn", "VBELN") \
        .option("lowerBound", "0000000000") \
        .option("upperBound", "9999999999") \
        .option("fetchsize", "10000") \
        .option("queryTimeout", "3600") \
        .load()
    
    row_count_extracted = vbrk_raw_df.count()
    logger.info(f"✅ VBRK extraction complete. Rows extracted: {row_count_extracted:,}")
    
    if row_count_extracted == 0:
        logger.info("No new records since last watermark. Updating watermark and exiting cleanly.")
        update_watermark(sf_conn, 'VBRK', 'SUCCESS', 0, 0, 
                        new_watermark=datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                        notes='No new records — delta was empty')
        sf_conn.close()
        job.commit()
        sys.exit(0)

except Exception as e:
    logger.error(f"❌ VBRK SAP extraction FAILED: {str(e)}")
    update_watermark(sf_conn, 'VBRK', 'FAILED', 0, 0, notes=f"Extraction failed: {str(e)[:500]}")
    sf_conn.close()
    raise

# -- STEP 4: Write Raw Data to S3 --
logger.info("STEP 4: Writing raw data to S3...")

s3_raw_path = f"s3://{args['s3_bucket']}/{args['s3_raw_prefix']}/extracted_date={today_partition}/"

try:
    vbrk_raw_df \
        .coalesce(4) \
        .write \
        .mode("overwrite") \
        .parquet(s3_raw_path)
    
    logger.info(f"✅ Raw VBRK data written to: {s3_raw_path}")

except Exception as e:
    logger.error(f"❌ S3 write FAILED: {str(e)}")
    update_watermark(sf_conn, 'VBRK', 'FAILED', row_count_extracted, 0, 
                    notes=f"S3 write failed: {str(e)[:500]}")
    sf_conn.close()
    raise

# -- STEP 5: Transform the Data --
logger.info("STEP 5: Applying transformations...")

try:
    vbrk_transformed_df = vbrk_raw_df \
        .withColumnRenamed("VBELN", "BILLING_DOC_NUMBER") \
        .withColumnRenamed("FKART", "BILLING_DOC_TYPE") \
        .withColumnRenamed("FKDAT", "BILLING_DATE") \
        .withColumnRenamed("RFBSK", "BILLING_STATUS") \
        .withColumnRenamed("ERDAT", "CREATED_DATE") \
        .withColumnRenamed("ERZET", "CREATED_TIME") \
        .withColumnRenamed("ERNAM", "CREATED_BY") \
        .withColumnRenamed("AEDAT", "CHANGED_DATE") \
        .withColumnRenamed("KUNAG", "SOLD_TO_PARTY") \
        .withColumnRenamed("KUNRE", "PAYER") \
        .withColumnRenamed("KUNRG", "BILL_TO_PARTY") \
        .withColumnRenamed("VKORG", "SALES_ORG") \
        .withColumnRenamed("VTWEG", "DIST_CHANNEL") \
        .withColumnRenamed("SPART", "DIVISION") \
        .withColumnRenamed("NETWR", "NET_VALUE") \
        .withColumnRenamed("MWSBK", "TAX_VALUE") \
        .withColumnRenamed("WAERK", "CURRENCY") \
        .withColumnRenamed("KURRF", "EXCHANGE_RATE") \
        .withColumnRenamed("XBLNR", "REFERENCE_DOC") \
        .withColumnRenamed("ZUONR", "SALES_DOC_REF") \
        .withColumnRenamed("VSBED", "SHIPPING_CONDITION") \
        .withColumnRenamed("INCO1", "INCOTERMS") \
        .withColumnRenamed("ZTERM", "PAYMENT_TERMS") \
        .withColumnRenamed("ZLSCH", "PAYMENT_METHOD") \
        .withColumnRenamed("BUKRS", "COMPANY_CODE") \
        \
        .withColumn("BILLING_DATE", F.to_date(F.col("BILLING_DATE"), "yyyyMMdd")) \
        .withColumn("CREATED_DATE", F.to_date(F.col("CREATED_DATE"), "yyyyMMdd")) \
        .withColumn("CHANGED_DATE", F.to_date(F.col("CHANGED_DATE"), "yyyyMMdd")) \
        .withColumn("NET_VALUE",     F.col("NET_VALUE").cast(DecimalType(15, 2))) \
        .withColumn("TAX_VALUE",     F.col("TAX_VALUE").cast(DecimalType(15, 2))) \
        .withColumn("EXCHANGE_RATE", F.col("EXCHANGE_RATE").cast(DecimalType(9, 5))) \
        \
        .withColumn("BILLING_DOC_NUMBER", F.trim(F.col("BILLING_DOC_NUMBER"))) \
        .withColumn("SOLD_TO_PARTY",      F.trim(F.col("SOLD_TO_PARTY"))) \
        .withColumn("MATERIAL_NUMBER",    F.lit(None).cast(StringType())) \
        \
        .filter(F.col("BILLING_DOC_NUMBER").isNotNull()) \
        .filter(F.length(F.trim(F.col("BILLING_DOC_NUMBER"))) > 0) \
        \
        .dropDuplicates(["BILLING_DOC_NUMBER"]) \
        \
        .withColumn("EXTRACTED_AT",   F.lit(extraction_ts.strftime('%Y-%m-%d %H:%M:%S')).cast(TimestampType())) \
        .withColumn("LOAD_BATCH_ID",  F.lit(batch_id)) \
        .withColumn("SOURCE_SYSTEM",  F.lit("SAP")) \
        .withColumn("IS_DELETED",     F.lit(False))
    
    # Calculate new watermark (max changed date in this batch)
    max_changed_date = vbrk_transformed_df \
        .agg(F.max("CHANGED_DATE").alias("max_date")) \
        .collect()[0]['max_date']
    
    new_watermark = str(max_changed_date) + " 23:59:59" if max_changed_date else \
                    datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    
    logger.info(f"✅ Transformations complete. New watermark will be: {new_watermark}")
    
    # Show sample data for validation
    logger.info("Sample transformed records:")
    vbrk_transformed_df.show(5, truncate=False)
    vbrk_transformed_df.printSchema()

except Exception as e:
    logger.error(f"❌ Transformation FAILED: {str(e)}")
    update_watermark(sf_conn, 'VBRK', 'FAILED', row_count_extracted, 0, 
                    notes=f"Transform failed: {str(e)[:500]}")
    sf_conn.close()
    raise

# -- STEP 6: Write Processed Data to S3 --
logger.info("STEP 6: Writing processed data to S3...")

s3_processed_path = f"s3://{args['s3_bucket']}/{args['s3_processed_prefix']}/extracted_date={today_partition}/"

try:
    vbrk_transformed_df \
        .coalesce(4) \
        .write \
        .mode("overwrite") \
        .parquet(s3_processed_path)
    
    logger.info(f"✅ Processed VBRK data written to: {s3_processed_path}")

except Exception as e:
    logger.error(f"❌ Processed S3 write FAILED: {str(e)}")
    update_watermark(sf_conn, 'VBRK', 'FAILED', row_count_extracted, 0, 
                    notes=f"Processed S3 write failed: {str(e)[:500]}")
    sf_conn.close()
    raise

# -- STEP 7: Load to Snowflake --
logger.info("STEP 7: Loading to Snowflake...")

# Snowflake connection options for Spark connector
snowflake_options = {
    "sfURL":       f"{sf_creds['account']}.snowflakecomputing.com",
    "sfUser":      sf_creds['username'],
    "sfPassword":  sf_creds['password'],
    "sfDatabase":  sf_creds['database'],
    "sfSchema":    sf_creds['schema'],
    "sfWarehouse": sf_creds['warehouse'],
    "sfRole":      sf_creds.get('role', 'SYSADMIN'),
    "truncate_table": "on",   # Truncate staging before each load
    "usestagingtable": "off"
}

try:
    # Step 7a: Load to staging table
    vbrk_transformed_df.write \
        .format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "BILLING_DOCUMENT_STAGE") \
        .mode("overwrite") \
        .save()
    
    logger.info(f"✅ Staging table loaded. Rows in batch: {row_count_extracted:,}")
    
    # Step 7b: Call MERGE procedure via Snowflake Python connector
    cursor = sf_conn.cursor()
    cursor.execute("CALL UPSERT_BILLING_DOCUMENT()")
    result = cursor.fetchone()
    cursor.close()
    logger.info(f"✅ MERGE complete: {result[0]}")
    
    # Step 7c: Verify row count in target table
    cursor = sf_conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM BILLING_DOCUMENT")
    total_rows = cursor.fetchone()[0]
    cursor.close()
    logger.info(f"✅ BILLING_DOCUMENT total rows after load: {total_rows:,}")
    
    rows_loaded = row_count_extracted  # All extracted rows were merged

except Exception as e:
    logger.error(f"❌ Snowflake load FAILED: {str(e)}")
    update_watermark(sf_conn, 'VBRK', 'FAILED', row_count_extracted, 0, 
                    notes=f"Snowflake load failed: {str(e)[:500]}")
    sf_conn.close()
    raise

# -- STEP 8: Update Watermark on Success --
logger.info("STEP 8: Updating watermark...")

update_watermark(
    sf_conn, 
    'VBRK', 
    'SUCCESS', 
    row_count_extracted, 
    rows_loaded,
    new_watermark=new_watermark,
    notes=f"Batch {batch_id} completed successfully. S3: {s3_raw_path}"
)

sf_conn.close()

logger.info(f"=== BILLING DOCUMENT JOB COMPLETE ===")
logger.info(f"Batch ID:       {batch_id}")
logger.info(f"Rows extracted: {row_count_extracted:,}")
logger.info(f"Rows loaded:    {rows_loaded:,}")
logger.info(f"New watermark:  {new_watermark}")

job.commit()
```

## 8.2 Create the Glue Job in AWS Console

**AWS Console → AWS Glue → ETL Jobs → Create Job → Script editor**

```
Job Name:       glue-poc-billingdocument-delta-load
IAM Role:       <your Glue IAM role>
Glue Version:   Glue 4.0
Worker Type:    G.2X (2 DPU per worker — needed for 5.39M rows)
Number of Workers: 10
Max Retries:    1
Job Timeout:    120 minutes
Temp directory: s3://<bucket>/glue-temp/

Script:         s3://<bucket>/glue-scripts/billing_document_extract_transform.py

Additional JARs: s3://<bucket>/jars/ngdbc.jar

Python libraries (install in job):
  - snowflake-connector-python
  - boto3

Job Parameters (add these as key-value):
  --sap_secret_name         = tcpl/sap/glue-poc-credentials
  --snowflake_secret_name   = tcpl/snowflake/glue-poc-credentials
  --s3_bucket               = tcpl-glue-poc
  --s3_raw_prefix           = raw/sap/VBRK
  --s3_processed_prefix     = processed/sap/VBRK
  --snowflake_database      = DB_SAP
  --snowflake_schema        = CORE_SAP_POC
  --batch_size              = 500000
  
  --additional-python-modules = snowflake-connector-python==3.6.0,boto3
```

---

# 9. STEP 5 — GLUE JOB: BILLINGDOCUMENTITEM (CHILD) — FULL CODE

Save as: `billing_document_item_extract_transform.py`

```python
"""
TCPL AWS Glue POC
Job: BillingDocumentItem (VBRP) Delta Extract + Load to Snowflake
Parent table: VBRK (must complete first)
Rows: 24.94M | Columns: 2,107

IMPORTANT DIFFERENCES FROM VBRK JOB:
1. Table is 5x larger (24.94M vs 5.39M)
2. More parallelism needed (numPartitions=20, more workers)
3. VBRP has its own watermark entry
4. Must only run AFTER VBRK job succeeds
"""

import sys
import json
import boto3
import logging
from datetime import datetime, timezone
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *
import snowflake.connector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BILLING_DOCUMENT_ITEM_EXTRACT")

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'sap_secret_name',
    'snowflake_secret_name',
    's3_bucket',
    's3_raw_prefix',
    's3_processed_prefix',
    'snowflake_database',
    'snowflake_schema',
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

batch_id = f"GLUE_{args['JOB_NAME']}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
extraction_ts = datetime.now(timezone.utc)
today_partition = extraction_ts.strftime('%Y-%m-%d')

logger.info(f"=== BILLING DOCUMENT ITEM EXTRACT JOB STARTING ===")
logger.info(f"Batch ID: {batch_id}")

# ---- Helper functions (same as parent job) ----
def get_secret(secret_name, region_name="ap-south-1"):
    client = boto3.client('secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

def get_snowflake_connection(sf_creds):
    return snowflake.connector.connect(
        user=sf_creds['username'],
        password=sf_creds['password'],
        account=sf_creds['account'],
        warehouse=sf_creds['warehouse'],
        database=sf_creds['database'],
        schema=sf_creds['schema'],
        role=sf_creds.get('role', 'SYSADMIN')
    )

def get_watermark(sf_conn, table_name):
    cursor = sf_conn.cursor()
    cursor.execute("""
        SELECT COALESCE(
            TO_CHAR(last_watermark_ts, 'YYYY-MM-DD HH24:MI:SS'),
            '2020-01-01 00:00:00'
        ) FROM PIPELINE_WATERMARK
        WHERE table_name = %s AND source_system = 'SAP'
    """, (table_name,))
    row = cursor.fetchone()
    cursor.close()
    return row[0] if row else '2020-01-01 00:00:00'

def update_watermark(sf_conn, table_name, status, rows_extracted=0, rows_loaded=0, 
                     new_watermark=None, notes=None):
    cursor = sf_conn.cursor()
    if status == 'SUCCESS' and new_watermark:
        cursor.execute("""
            UPDATE PIPELINE_WATERMARK
            SET last_watermark_ts = TO_TIMESTAMP_NTZ(%s, 'YYYY-MM-DD HH24:MI:SS'),
                last_run_status   = %s,
                last_run_end      = CURRENT_TIMESTAMP(),
                rows_extracted    = %s, rows_loaded = %s, notes = %s,
                updated_at        = CURRENT_TIMESTAMP()
            WHERE table_name = %s AND source_system = 'SAP'
        """, (new_watermark, status, rows_extracted, rows_loaded, notes, table_name))
    else:
        cursor.execute("""
            UPDATE PIPELINE_WATERMARK
            SET last_run_status = %s, last_run_end = CURRENT_TIMESTAMP(),
                rows_extracted = %s, rows_loaded = %s, notes = %s,
                updated_at = CURRENT_TIMESTAMP()
            WHERE table_name = %s AND source_system = 'SAP'
        """, (status, rows_extracted, rows_loaded, notes, table_name))
    sf_conn.commit()
    cursor.close()

# ---- STEP 1: Load Credentials ----
logger.info("STEP 1: Loading credentials...")
sap_creds = get_secret(args['sap_secret_name'])
sf_creds = get_secret(args['snowflake_secret_name'])
sf_creds['database'] = args['snowflake_database']
sf_creds['schema'] = args['snowflake_schema']

# ---- STEP 2: Get Watermark ----
logger.info("STEP 2: Getting VBRP watermark...")
sf_conn = get_snowflake_connection(sf_creds)
watermark_ts = get_watermark(sf_conn, 'VBRP')
logger.info(f"Will extract VBRP records changed after: {watermark_ts}")

# ---- STEP 3: Verify Parent (VBRK) Loaded Successfully ----
logger.info("STEP 3: Verifying parent table (VBRK) status...")
cursor = sf_conn.cursor()
cursor.execute("""
    SELECT last_run_status, last_run_end, rows_loaded
    FROM PIPELINE_WATERMARK
    WHERE table_name = 'VBRK' AND source_system = 'SAP'
""")
vbrk_status = cursor.fetchone()
cursor.close()

if vbrk_status and vbrk_status[0] != 'SUCCESS':
    error_msg = f"Parent VBRK load status is '{vbrk_status[0]}' — cannot load VBRP until VBRK succeeds"
    logger.error(f"❌ {error_msg}")
    update_watermark(sf_conn, 'VBRP', 'FAILED', 0, 0, notes=error_msg)
    sf_conn.close()
    raise Exception(error_msg)

logger.info(f"✅ VBRK status is SUCCESS. VBRP extraction can proceed.")

# ---- STEP 4: Extract VBRP from SAP ----
logger.info("STEP 4: Extracting VBRP from SAP...")

# VBRP key columns:
# VBELN = Billing document number (FK to VBRK.VBELN)
# POSNR = Item number (together with VBELN = primary key)
# MATNR = Material number
# FKIMG = Billed quantity
# NETWR = Net value
# AEDAT = Last changed date (WATERMARK COLUMN)
# ERDAT = Created date

sap_query_vbrp = f"""
    SELECT 
        VBELN,      -- Billing Document Number (FK to VBRK)
        POSNR,      -- Item Number
        MATNR,      -- Material Number
        ARKTX,      -- Short Text
        MATKL,      -- Material Group
        FKIMG,      -- Billed Quantity
        VRKME,      -- Unit of Measure
        UMVKZ,      -- Base Quantity
        NETWR,      -- Net Value
        NETPR,      -- Net Price
        WAERK,      -- Currency
        AUBEL,      -- Sales Order Reference
        AUPOS,      -- Sales Order Item
        VGBEL,      -- Delivery Document
        VGPOS,      -- Delivery Item
        MWSBP,      -- Tax Amount
        MWSKZ,      -- Tax Classification
        WERKS,      -- Plant
        LGORT,      -- Storage Location
        FKDAT,      -- Billing Date
        ERDAT,      -- Created Date
        AEDAT        -- Last Changed Date (WATERMARK)
    FROM VBRP
    WHERE AEDAT >= TO_DATE('{watermark_ts}', 'YYYY-MM-DD HH24:MI:SS')
       OR ERDAT >= TO_DATE('{watermark_ts}', 'YYYY-MM-DD HH24:MI:SS')
    ORDER BY VBELN, POSNR
"""

# VBRP is 24.94M rows — use more partitions for parallelism
# Partition by VBELN numeric range for even distribution
try:
    vbrp_raw_df = spark.read \
        .format("jdbc") \
        .option("url", sap_creds['jdbc_url']) \
        .option("dbtable", f"({sap_query_vbrp}) VBRP_DELTA") \
        .option("user", sap_creds['username']) \
        .option("password", sap_creds['password']) \
        .option("driver", "com.sap.db.jdbc.Driver") \
        .option("numPartitions", "20") \
        .option("partitionColumn", "VBELN") \
        .option("lowerBound", "0000000000") \
        .option("upperBound", "9999999999") \
        .option("fetchsize", "10000") \
        .option("queryTimeout", "3600") \
        .load()
    
    row_count_extracted = vbrp_raw_df.count()
    logger.info(f"✅ VBRP extraction complete. Rows extracted: {row_count_extracted:,}")
    
    if row_count_extracted == 0:
        logger.info("No new VBRP records. Updating watermark and exiting.")
        update_watermark(sf_conn, 'VBRP', 'SUCCESS', 0, 0,
                        new_watermark=datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                        notes='Delta was empty — no new VBRP records')
        sf_conn.close()
        job.commit()
        sys.exit(0)

except Exception as e:
    logger.error(f"❌ VBRP SAP extraction FAILED: {str(e)}")
    update_watermark(sf_conn, 'VBRP', 'FAILED', 0, 0, notes=f"Extraction failed: {str(e)[:500]}")
    sf_conn.close()
    raise

# ---- STEP 5: Write Raw to S3 ----
logger.info("STEP 5: Writing raw VBRP data to S3...")
s3_raw_path = f"s3://{args['s3_bucket']}/{args['s3_raw_prefix']}/extracted_date={today_partition}/"

try:
    # VBRP is large — use more partitions when writing
    vbrp_raw_df \
        .coalesce(10) \
        .write \
        .mode("overwrite") \
        .parquet(s3_raw_path)
    logger.info(f"✅ Raw VBRP written to: {s3_raw_path}")

except Exception as e:
    logger.error(f"❌ S3 raw write failed: {str(e)}")
    update_watermark(sf_conn, 'VBRP', 'FAILED', row_count_extracted, 0, 
                    notes=f"S3 write failed: {str(e)[:500]}")
    sf_conn.close()
    raise

# ---- STEP 6: Transform ----
logger.info("STEP 6: Applying transformations to VBRP...")

try:
    vbrp_transformed_df = vbrp_raw_df \
        .withColumnRenamed("VBELN",  "BILLING_DOC_NUMBER") \
        .withColumnRenamed("POSNR",  "BILLING_ITEM_NUMBER") \
        .withColumnRenamed("MATNR",  "MATERIAL_NUMBER") \
        .withColumnRenamed("ARKTX",  "MATERIAL_DESC") \
        .withColumnRenamed("MATKL",  "MATERIAL_GROUP") \
        .withColumnRenamed("FKIMG",  "BILLED_QUANTITY") \
        .withColumnRenamed("VRKME",  "BILLED_UOM") \
        .withColumnRenamed("UMVKZ",  "BASE_QUANTITY") \
        .withColumnRenamed("NETWR",  "NET_VALUE") \
        .withColumnRenamed("NETPR",  "NET_PRICE") \
        .withColumnRenamed("WAERK",  "CURRENCY") \
        .withColumnRenamed("AUBEL",  "SALES_DOC") \
        .withColumnRenamed("AUPOS",  "SALES_ITEM") \
        .withColumnRenamed("VGBEL",  "DELIVERY_DOC") \
        .withColumnRenamed("VGPOS",  "DELIVERY_ITEM") \
        .withColumnRenamed("MWSBP",  "TAX_AMOUNT") \
        .withColumnRenamed("MWSKZ",  "TAX_CLASS") \
        .withColumnRenamed("WERKS",  "PLANT") \
        .withColumnRenamed("LGORT",  "STORAGE_LOC") \
        .withColumnRenamed("FKDAT",  "BILLING_DATE") \
        .withColumnRenamed("ERDAT",  "CREATED_DATE") \
        .withColumnRenamed("AEDAT",  "CHANGED_DATE") \
        \
        .withColumn("BILLING_DATE",    F.to_date(F.col("BILLING_DATE"), "yyyyMMdd")) \
        .withColumn("CREATED_DATE",    F.to_date(F.col("CREATED_DATE"), "yyyyMMdd")) \
        .withColumn("CHANGED_DATE",    F.to_date(F.col("CHANGED_DATE"), "yyyyMMdd")) \
        .withColumn("BILLED_QUANTITY", F.col("BILLED_QUANTITY").cast(DecimalType(13, 3))) \
        .withColumn("BASE_QUANTITY",   F.col("BASE_QUANTITY").cast(DecimalType(13, 3))) \
        .withColumn("NET_VALUE",       F.col("NET_VALUE").cast(DecimalType(15, 2))) \
        .withColumn("NET_PRICE",       F.col("NET_PRICE").cast(DecimalType(15, 5))) \
        .withColumn("TAX_AMOUNT",      F.col("TAX_AMOUNT").cast(DecimalType(15, 2))) \
        \
        .withColumn("BILLING_DOC_NUMBER",  F.trim(F.col("BILLING_DOC_NUMBER"))) \
        .withColumn("BILLING_ITEM_NUMBER", F.trim(F.col("BILLING_ITEM_NUMBER"))) \
        .withColumn("MATERIAL_NUMBER",     F.trim(F.col("MATERIAL_NUMBER"))) \
        .withColumn("PLANT",               F.trim(F.col("PLANT"))) \
        \
        .filter(F.col("BILLING_DOC_NUMBER").isNotNull()) \
        .filter(F.col("BILLING_ITEM_NUMBER").isNotNull()) \
        \
        .dropDuplicates(["BILLING_DOC_NUMBER", "BILLING_ITEM_NUMBER"]) \
        \
        .withColumn("EXTRACTED_AT",  F.lit(extraction_ts.strftime('%Y-%m-%d %H:%M:%S')).cast(TimestampType())) \
        .withColumn("LOAD_BATCH_ID", F.lit(batch_id)) \
        .withColumn("SOURCE_SYSTEM", F.lit("SAP")) \
        .withColumn("IS_DELETED",    F.lit(False))
    
    max_changed = vbrp_transformed_df.agg(F.max("CHANGED_DATE")).collect()[0][0]
    new_watermark = str(max_changed) + " 23:59:59" if max_changed else \
                    datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    
    logger.info(f"✅ VBRP transformations complete. New watermark: {new_watermark}")
    vbrp_transformed_df.show(5, truncate=False)

except Exception as e:
    logger.error(f"❌ VBRP transformation failed: {str(e)}")
    update_watermark(sf_conn, 'VBRP', 'FAILED', row_count_extracted, 0,
                    notes=f"Transform failed: {str(e)[:500]}")
    sf_conn.close()
    raise

# ---- STEP 7: Write Processed to S3 ----
s3_processed_path = f"s3://{args['s3_bucket']}/{args['s3_processed_prefix']}/extracted_date={today_partition}/"

vbrp_transformed_df.coalesce(10).write.mode("overwrite").parquet(s3_processed_path)
logger.info(f"✅ Processed VBRP written to: {s3_processed_path}")

# ---- STEP 8: Load to Snowflake ----
logger.info("STEP 8: Loading VBRP to Snowflake...")

snowflake_options = {
    "sfURL":       f"{sf_creds['account']}.snowflakecomputing.com",
    "sfUser":      sf_creds['username'],
    "sfPassword":  sf_creds['password'],
    "sfDatabase":  sf_creds['database'],
    "sfSchema":    sf_creds['schema'],
    "sfWarehouse": sf_creds['warehouse'],
    "sfRole":      sf_creds.get('role', 'SYSADMIN'),
    "truncate_table": "on"
}

try:
    vbrp_transformed_df.write \
        .format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "BILLING_DOCUMENT_ITEM_STAGE") \
        .mode("overwrite") \
        .save()
    
    cursor = sf_conn.cursor()
    cursor.execute("CALL UPSERT_BILLING_DOCUMENT_ITEM()")
    result = cursor.fetchone()
    cursor.close()
    logger.info(f"✅ VBRP MERGE complete: {result[0]}")
    
    cursor = sf_conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM BILLING_DOCUMENT_ITEM")
    total_rows = cursor.fetchone()[0]
    cursor.close()
    logger.info(f"✅ BILLING_DOCUMENT_ITEM total rows: {total_rows:,}")

except Exception as e:
    logger.error(f"❌ Snowflake VBRP load failed: {str(e)}")
    update_watermark(sf_conn, 'VBRP', 'FAILED', row_count_extracted, 0,
                    notes=f"SF load failed: {str(e)[:500]}")
    sf_conn.close()
    raise

# ---- STEP 9: Update Watermark ----
update_watermark(sf_conn, 'VBRP', 'SUCCESS', row_count_extracted, row_count_extracted,
                new_watermark=new_watermark,
                notes=f"Batch {batch_id} completed. S3: {s3_raw_path}")

sf_conn.close()
logger.info(f"=== BILLING DOCUMENT ITEM JOB COMPLETE ===")
logger.info(f"Rows extracted: {row_count_extracted:,}")
logger.info(f"New watermark:  {new_watermark}")

job.commit()
```

## Glue Job Settings for VBRP (larger table)

```
Job Name:           glue-poc-billingdocumentitem-delta-load
Worker Type:        G.2X
Number of Workers:  20        ← More workers for 24.94M rows
Max Retries:        1
Job Timeout:        180 minutes  ← Longer timeout for bigger table

Job Parameters:
  --sap_secret_name         = tcpl/sap/glue-poc-credentials
  --snowflake_secret_name   = tcpl/snowflake/glue-poc-credentials
  --s3_bucket               = tcpl-glue-poc
  --s3_raw_prefix           = raw/sap/VBRP
  --s3_processed_prefix     = processed/sap/VBRP
  --snowflake_database      = DB_SAP
  --snowflake_schema        = CORE_SAP_POC
  
  --additional-python-modules = snowflake-connector-python==3.6.0,boto3
```

---

# 10. STEP 6 — DELTA LOAD WATERMARK LOGIC — DEEP EXPLANATION

## Why Watermark Is the Most Critical Design Decision

```
WITHOUT WATERMARK (Full Load every hour):
  - Extract ALL 5.39M VBRK rows every hour
  - 5.39M rows × 356 columns × hourly = massive data movement
  - Takes 30-60 minutes just to extract
  - SAP put under heavy load every hour
  - Snowflake needs to reload all 5.39M rows every hour
  - This is what we DON'T want

WITH WATERMARK (Delta Load):
  - In any given hour, maybe 50,000 billing documents change
  - Extract only those 50,000 rows (not 5.39M)
  - Takes 2-3 minutes
  - SAP load is minimal
  - Only 50,000 rows merged into Snowflake
  - This is efficient and scalable
```

## The Watermark Column in SAP VBRK

```
In VBRK (BillingDocument):
  ERDAT = Date the billing document was CREATED in SAP
  AEDAT = Date the billing document was last CHANGED in SAP

We track BOTH because:
  ERDAT tells us about NEW documents
  AEDAT tells us about UPDATED documents (e.g., billing value changed, status changed)

Our WHERE clause:
  WHERE AEDAT >= last_watermark OR ERDAT >= last_watermark

This catches BOTH new and updated billing documents.
```

## The Watermark Update Strategy

```
APPROACH: Set new watermark to MAX(AEDAT) from the current batch

WHY?
After each successful load, we set watermark = the latest AEDAT
in the records we just processed.

EXAMPLE:
  Previous watermark: 2025-02-28
  
  Current batch contains records with AEDAT ranging from:
    2025-02-28 to 2025-03-01
  
  MAX(AEDAT) in this batch = 2025-03-01
  
  New watermark = 2025-03-01 23:59:59
  
  Next run will extract:
    WHERE AEDAT >= 2025-03-01 23:59:59
    (Only records changed on or after March 2nd)

SAFETY BUFFER:
  In production, some teams add a small overlap:
    New watermark = MAX(AEDAT) - 1 hour
  
  This handles edge cases where SAP transactions 
  are committed slightly late. For the POC, exact
  MAX(AEDAT) is fine.
```

## Handling the FIRST RUN (Historical Load)

```
INITIAL STATE:
  PIPELINE_WATERMARK.last_watermark_ts = 90 days ago
  (as set in Step 7.3)

FIRST RUN:
  Query: WHERE AEDAT >= '2024-12-02' (90 days ago)
  
  This will extract ALL billing documents from last 90 days
  Likely much more than a normal hourly delta
  
  EXPECT FIRST RUN TO:
  - Take longer (maybe 15-30 minutes)
  - Extract millions of rows
  - Load them all into Snowflake
  
  THIS IS NORMAL AND EXPECTED.
  
SECOND RUN ONWARD:
  Watermark is now yesterday/today
  Only changed records since last run extracted
  Back to normal fast delta loads
```

---

# 11. STEP 7 — AIRFLOW DAG TO ORCHESTRATE THE POC

Save this as: `glue_poc_billing_delta_dag.py`

```python
"""
Airflow DAG for TCPL Glue POC
Orchestrates BillingDocument + BillingDocumentItem delta loads

Schedule: Hourly (matches existing SnapLogic pipeline)
This DAG is the POC replacement for:
  prd-saps4-hourly-billing-document-delta-load-pipeline

Task flow:
  start
    → extract_billingdocument_vbrk
    → extract_billingdocumentitem_vbrp   (only after VBRK succeeds)
    → validate_snowflake_rowcounts
    → end
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# ============================================================
# DAG CONFIGURATION
# ============================================================

default_args = {
    'owner':              'decision-point-team',
    'depends_on_past':    False,   # Each run is independent
    'start_date':         days_ago(1),
    'email':              ['your-team@decisionpoint.in'],
    'email_on_failure':   True,
    'email_on_retry':     False,
    'retries':            1,
    'retry_delay':        timedelta(minutes=5),
    'execution_timeout':  timedelta(hours=3),
}

dag = DAG(
    dag_id='glue_poc_billing_delta_load',
    default_args=default_args,
    description='POC: AWS Glue replaces SnapLogic for BillingDocument delta load from SAP',
    schedule_interval='0 * * * *',   # Hourly at :00 — matches existing pipeline
    catchup=False,                    # Don't run for past missed hours
    max_active_runs=1,                # Only 1 instance at a time (hourly doesn't overlap)
    tags=['poc', 'glue', 'sap', 'billing', 'priority-0'],
)

# ============================================================
# GLUE JOB ARGUMENTS
# ============================================================

# These get passed to the Glue Python scripts
GLUE_COMMON_ARGS = {
    '--sap_secret_name':       'tcpl/sap/glue-poc-credentials',
    '--snowflake_secret_name': 'tcpl/snowflake/glue-poc-credentials',
    '--s3_bucket':             'tcpl-glue-poc',
    '--snowflake_database':    'DB_SAP',
    '--snowflake_schema':      'CORE_SAP_POC',
}

VBRK_ARGS = {
    **GLUE_COMMON_ARGS,
    '--s3_raw_prefix':         'raw/sap/VBRK',
    '--s3_processed_prefix':   'processed/sap/VBRK',
    '--batch_size':            '500000',
}

VBRP_ARGS = {
    **GLUE_COMMON_ARGS,
    '--s3_raw_prefix':         'raw/sap/VBRP',
    '--s3_processed_prefix':   'processed/sap/VBRP',
    '--batch_size':            '1000000',   # Larger batch for VBRP
}

# ============================================================
# TASK DEFINITIONS
# ============================================================

with dag:

    # Task 0: Start marker
    start = DummyOperator(task_id='start')

    # Task 1: Run VBRK Glue Job (BillingDocument — Parent)
    # GlueJobOperator submits the job and WAITS for completion
    run_vbrk_glue_job = GlueJobOperator(
        task_id='extract_transform_billingdocument_vbrk',
        job_name='glue-poc-billingdocument-delta-load',
        script_args=VBRK_ARGS,
        aws_conn_id='aws_default',          # Airflow connection to AWS
        region_name='ap-south-1',           # Your AWS region
        wait_for_completion=True,
        num_of_dpus=20,                     # 10 workers × 2 DPU = 20 DPU
        create_job_kwargs={
            'GlueVersion': '4.0',
            'WorkerType': 'G.2X',
            'NumberOfWorkers': 10,
        },
        dag=dag,
    )

    # Task 2: Run VBRP Glue Job (BillingDocumentItem — Child)
    # ONLY runs after VBRK completes successfully
    run_vbrp_glue_job = GlueJobOperator(
        task_id='extract_transform_billingdocumentitem_vbrp',
        job_name='glue-poc-billingdocumentitem-delta-load',
        script_args=VBRP_ARGS,
        aws_conn_id='aws_default',
        region_name='ap-south-1',
        wait_for_completion=True,
        num_of_dpus=40,                     # 20 workers × 2 DPU for larger table
        create_job_kwargs={
            'GlueVersion': '4.0',
            'WorkerType': 'G.2X',
            'NumberOfWorkers': 20,
        },
        dag=dag,
    )

    # Task 3: Validate row counts in Snowflake
    # This runs a SQL check to confirm data landed correctly
    validate_rowcounts = SnowflakeOperator(
        task_id='validate_snowflake_rowcounts',
        snowflake_conn_id='snowflake_default',
        sql="""
            -- Check 1: Row counts
            SELECT 
                'BILLING_DOCUMENT' AS table_name,
                COUNT(*) AS total_rows,
                MAX(EXTRACTED_AT) AS last_extracted,
                MAX(LOAD_BATCH_ID) AS last_batch
            FROM DB_SAP.CORE_SAP_POC.BILLING_DOCUMENT
            
            UNION ALL
            
            SELECT 
                'BILLING_DOCUMENT_ITEM' AS table_name,
                COUNT(*) AS total_rows,
                MAX(EXTRACTED_AT) AS last_extracted,
                MAX(LOAD_BATCH_ID) AS last_batch
            FROM DB_SAP.CORE_SAP_POC.BILLING_DOCUMENT_ITEM;
            
            -- Check 2: Orphan items (items without matching header)
            SELECT COUNT(*) AS orphan_items
            FROM DB_SAP.CORE_SAP_POC.BILLING_DOCUMENT_ITEM i
            LEFT JOIN DB_SAP.CORE_SAP_POC.BILLING_DOCUMENT d
                ON i.BILLING_DOC_NUMBER = d.BILLING_DOC_NUMBER
            WHERE d.BILLING_DOC_NUMBER IS NULL;
            
            -- Check 3: Watermark status
            SELECT table_name, last_run_status, last_watermark_ts, rows_extracted
            FROM DB_SAP.CORE_SAP_POC.PIPELINE_WATERMARK
            WHERE source_system = 'SAP';
        """,
        dag=dag,
    )

    # Task 4: End marker
    end = DummyOperator(task_id='end')

    # ============================================================
    # TASK DEPENDENCIES — THE CRITICAL ORDER
    # ============================================================

    # MUST BE IN THIS ORDER:
    # start → VBRK → VBRP → validate → end
    # 
    # VBRP cannot run until VBRK succeeds (referential integrity)
    # Validation runs last to confirm everything is clean

    start >> run_vbrk_glue_job >> run_vbrp_glue_job >> validate_rowcounts >> end
```

---

# 12. STEP 8 — RUN THE POC — MANUAL EXECUTION STEPS

## First-Time Run (Manual — Not via Airflow)

Do the first run manually so you can watch logs in real-time.

### Before Running — Final Checklist

```
□ SAP JDBC connection tested (green in Glue Connections)
□ ngdbc.jar uploaded to S3
□ Snowflake tables created (all 7 objects from Step 3)
□ Watermark initialized with 90-day lookback
□ Glue IAM role has correct permissions
□ Both Glue jobs created and saved
□ Scripts uploaded to S3
□ Secrets Manager has correct credentials
```

### Manual Run Sequence

**Run 1: Test VBRK Glue Job**

1. Go to **AWS Console → AWS Glue → ETL Jobs**
2. Select `glue-poc-billingdocument-delta-load`
3. Click **"Run Job"**
4. Add job parameters:
   ```
   --sap_secret_name         tcpl/sap/glue-poc-credentials
   --snowflake_secret_name   tcpl/snowflake/glue-poc-credentials
   --s3_bucket               tcpl-glue-poc
   --s3_raw_prefix           raw/sap/VBRK
   --s3_processed_prefix     processed/sap/VBRK
   --snowflake_database      DB_SAP
   --snowflake_schema        CORE_SAP_POC
   --batch_size              500000
   ```
5. Click **"Run"**
6. Watch the run:
   - **Glue Console → Job Runs** → click your run → **Logs** → **Output**
   - You'll see the print statements from the Python script

7. While VBRK runs, open **CloudWatch → Log Groups → /aws-glue/jobs/output**
   - Find your job's log stream
   - Watch for:
     ```
     ✅ VBRK extraction complete. Rows extracted: 123,456
     ✅ Raw VBRK data written to: s3://...
     ✅ Staging table loaded
     ✅ MERGE complete
     ✅ BILLING_DOCUMENT total rows: 123,456
     ✅ Watermark updated
     === BILLING DOCUMENT JOB COMPLETE ===
     ```

**Run 2: After VBRK Success, Run VBRP Job**

1. Same process — run `glue-poc-billingdocumentitem-delta-load`
2. Parameters:
   ```
   --s3_raw_prefix           raw/sap/VBRP
   --s3_processed_prefix     processed/sap/VBRP
   (everything else same)
   ```

**Verify in Snowflake after both runs:**

```sql
USE DATABASE DB_SAP;
USE SCHEMA CORE_SAP_POC;

-- 1. Check BILLING_DOCUMENT row count
SELECT COUNT(*), MAX(BILLING_DATE), MIN(BILLING_DATE) FROM BILLING_DOCUMENT;

-- 2. Check BILLING_DOCUMENT_ITEM row count  
SELECT COUNT(*), MAX(BILLING_DATE), MIN(BILLING_DATE) FROM BILLING_DOCUMENT_ITEM;

-- 3. Check watermark updated correctly
SELECT * FROM PIPELINE_WATERMARK ORDER BY table_name;

-- 4. Sample data check
SELECT * FROM BILLING_DOCUMENT LIMIT 10;
SELECT * FROM BILLING_DOCUMENT_ITEM WHERE BILLING_DOC_NUMBER = (
    SELECT BILLING_DOC_NUMBER FROM BILLING_DOCUMENT LIMIT 1
) LIMIT 20;

-- 5. Check pipeline ran at correct time
SELECT LOAD_BATCH_ID, EXTRACTED_AT, COUNT(*) AS rows
FROM BILLING_DOCUMENT
GROUP BY LOAD_BATCH_ID, EXTRACTED_AT
ORDER BY EXTRACTED_AT DESC;
```

---

# 13. STEP 9 — VALIDATE YOUR DATA — ALL CHECKS

## Validation Queries to Run After First Successful Load

### Check 1: Row Counts vs Expected

```sql
-- Expected: similar to current SnapLogic pipeline
-- Compare to actual TCPL platform counts (ask Sam or check ThoughtSpot)

SELECT 
    'BILLING_DOCUMENT' AS table_name,
    COUNT(*) AS glue_poc_count
FROM DB_SAP.CORE_SAP_POC.BILLING_DOCUMENT

UNION ALL

SELECT 
    'BILLING_DOCUMENT_ITEM',
    COUNT(*)
FROM DB_SAP.CORE_SAP_POC.BILLING_DOCUMENT_ITEM;

-- Compare to existing Snowflake table (if available):
SELECT 
    'EXISTING_BILLING_DOC' AS table_name,
    COUNT(*) AS existing_count
FROM DB_SAP.CORE_SAP.BILLING_DOCUMENT   -- Check actual table name with Sam
;
```

### Check 2: Referential Integrity (No Orphan Items)

```sql
-- Every item must have a matching header document
-- If this returns > 0, there's a data quality issue

SELECT COUNT(*) AS orphan_items_count,
       'Items without matching BillingDocument header' AS issue
FROM BILLING_DOCUMENT_ITEM i
LEFT JOIN BILLING_DOCUMENT d ON i.BILLING_DOC_NUMBER = d.BILLING_DOC_NUMBER
WHERE d.BILLING_DOC_NUMBER IS NULL;

-- Expected result: 0 orphan items
-- If > 0, investigate: likely VBRK load was incomplete
```

### Check 3: No Duplicate Primary Keys

```sql
-- BILLING_DOCUMENT: PK is BILLING_DOC_NUMBER
SELECT BILLING_DOC_NUMBER, COUNT(*) AS cnt
FROM BILLING_DOCUMENT
GROUP BY BILLING_DOC_NUMBER
HAVING COUNT(*) > 1
LIMIT 20;
-- Expected: 0 rows returned (no duplicates)

-- BILLING_DOCUMENT_ITEM: PK is (BILLING_DOC_NUMBER, BILLING_ITEM_NUMBER)
SELECT BILLING_DOC_NUMBER, BILLING_ITEM_NUMBER, COUNT(*) AS cnt
FROM BILLING_DOCUMENT_ITEM
GROUP BY BILLING_DOC_NUMBER, BILLING_ITEM_NUMBER
HAVING COUNT(*) > 1
LIMIT 20;
-- Expected: 0 rows returned
```

### Check 4: Data Freshness

```sql
-- Confirm data is from today/recently
SELECT 
    MAX(BILLING_DATE)   AS max_billing_date,
    MAX(EXTRACTED_AT)   AS max_extracted_at,
    MIN(BILLING_DATE)   AS min_billing_date,
    DATEDIFF('day', MAX(BILLING_DATE), CURRENT_DATE()) AS days_lag
FROM BILLING_DOCUMENT;

-- Expected: max_billing_date should be today or yesterday
-- days_lag should be 0 or 1
```

### Check 5: Key Business Metrics (Sanity Check)

```sql
-- Total billing value — compare to what ThoughtSpot/Finance expects
SELECT 
    BILLING_DATE,
    SALES_ORG,
    COUNT(DISTINCT BILLING_DOC_NUMBER) AS num_invoices,
    SUM(NET_VALUE) AS total_net_value,
    CURRENCY
FROM BILLING_DOCUMENT
WHERE BILLING_DATE = CURRENT_DATE() - 1  -- Yesterday's data
GROUP BY BILLING_DATE, SALES_ORG, CURRENCY
ORDER BY BILLING_DATE DESC, SALES_ORG;
```

### Check 6: Watermark Health

```sql
SELECT 
    table_name,
    last_run_status,
    TO_CHAR(last_watermark_ts, 'YYYY-MM-DD HH24:MI:SS') AS watermark,
    last_run_start,
    last_run_end,
    rows_extracted,
    rows_loaded,
    DATEDIFF('minute', last_run_start, last_run_end) AS run_duration_minutes,
    notes
FROM PIPELINE_WATERMARK
ORDER BY table_name;
```

### Check 7: Compare with S3 Raw File

```sql
-- After your Glue run, check how many Parquet files are in S3
-- Go to AWS Console → S3 → your bucket → raw/sap/VBRK/extracted_date=today/
-- Count the files and estimate row counts from file sizes
-- 1 MB Parquet ≈ 50,000-100,000 rows (depending on columns)
-- 5.39M rows VBRK ≈ 50-100 MB Parquet
-- 24.94M rows VBRP ≈ 500MB-1GB Parquet
```

---

# 14. STEP 10 — PERFORMANCE TUNING FOR 24.94M ROWS

## Understanding the Performance Challenges

```
VBRK (5.39M rows, 356 columns):
- Medium sized table
- 356 columns × 5.39M rows = ~1.9 billion cells
- With Parquet compression, ~200-500MB on disk
- Expected Glue runtime: 10-20 minutes

VBRP (24.94M rows, 2,107 columns):
- LARGE table
- 2,107 columns × 24.94M rows = ~52 billion cells
- This is a very wide table (2,107 columns!)
- Expected Glue runtime: 30-60 minutes
- Needs careful tuning
```

## Tuning for VBRP (24.94M rows, 2,107 columns)

### Option 1: Increase Parallelism (Primary Lever)

```
Current setting: numPartitions=20
Increase to:     numPartitions=40 or 50

How it works:
  Spark splits the JDBC read into 40 parallel queries
  Each query fetches 1/40th of the data
  All 40 run simultaneously in parallel
  Reduces overall read time significantly

In the Glue job code, change:
  .option("numPartitions", "40")
  .option("partitionColumn", "VBELN")
  .option("lowerBound", "0000000000")
  .option("upperBound", "9999999999")
```

### Option 2: Increase Worker Count

```
In Glue Job settings:
  Worker Type:         G.4X (not G.2X)
  Number of Workers:   30

G.4X workers have:
  - 64 GB RAM (vs 32 GB for G.2X)
  - 16 vCPUs (vs 8 for G.2X)

Better for wide tables (2,107 columns needs more memory per row)
```

### Option 3: Column Pruning — Don't Extract All 2,107 Columns

```python
# Instead of SELECT * FROM VBRP, only select columns you actually need
# This is the most impactful optimization for a 2,107 column table!

# In the SAP query, explicitly list only needed columns:
sap_query_vbrp = """
    SELECT 
        VBELN, POSNR, MATNR, ARKTX, MATKL,
        FKIMG, VRKME, NETWR, NETPR, WAERK,
        AUBEL, AUPOS, VGBEL, VGPOS,
        MWSBP, MWSKZ, WERKS, LGORT,
        FKDAT, ERDAT, AEDAT
        -- Only 21 columns instead of 2,107!
        -- In production you'd include more, but for POC this is sufficient
    FROM VBRP
    WHERE AEDAT >= TO_DATE('{watermark_ts}', ...)
"""

# This reduces data volume by ~99% (21 vs 2,107 columns)
# Much faster extraction and load
```

### Option 4: Partition Write in Spark

```python
# When writing to S3, partition by plant or billing date
# This makes future reads much faster

vbrp_transformed_df \
    .write \
    .partitionBy("PLANT", "BILLING_DATE") \  # Partition for faster reads
    .mode("overwrite") \
    .parquet(s3_processed_path)
```

### Option 5: Delta Only (Already Doing This)

The most important optimization is already in place — we're only extracting changed records. For an hourly run, maybe only 100K-500K VBRP records change. That's very manageable even with 2,107 columns.

---

# 15. COMMON ERRORS AND HOW TO FIX THEM

## Error 1: SAP JDBC Connection Fails

```
ERROR MESSAGE:
  com.sap.db.jdbc.exceptions.JDBCDriverException: 
  Cannot connect to jdbc:sap://... Connection refused

CAUSES:
  a) SAP host/port is wrong
  b) Security group doesn't allow outbound to SAP port
  c) SAP is not running / is in maintenance
  d) Credentials wrong (username/password)
  e) Glue VPC cannot reach SAP VPC (network peering issue)

FIXES:
  a) Verify JDBC URL with SAP Basis team
  b) Check Security Group: allow outbound port 30015 (or 3<instance>15) to SAP IP
  c) Check SAP system status with SAP Basis
  d) Rotate credentials in Secrets Manager, test manually first
  e) Check VPC peering — Glue must be in same VPC as SAP or peered VPC
  
TEST:
  In Glue Connections → Test Connection
  If it fails here, it's network/credentials, not code
```

## Error 2: ngdbc.jar ClassNotFoundException

```
ERROR MESSAGE:
  java.lang.ClassNotFoundException: com.sap.db.jdbc.Driver

CAUSE:
  The SAP HANA JDBC driver JAR is not loaded by Glue

FIX:
  In Glue Job → Details → Advanced Properties → 
  "Referenced files path": s3://your-bucket/jars/ngdbc.jar
  
  Make sure you're using the EXACT right version of ngdbc.jar
  for your SAP HANA version. Ask SAP Basis for the correct version.
```

## Error 3: Snowflake Authentication Failed

```
ERROR MESSAGE:
  snowflake.connector.errors.DatabaseError: 
  250001: Failed to connect to DB: account.snowflakecomputing.com, port 443.
  JWT token is invalid.
  
  OR
  
  Incorrect username or password was specified.

CAUSE:
  Wrong credentials in Secrets Manager

FIX:
  1. Go to Snowflake and manually test credentials:
     snowsql -a <account> -u <username>  → enter password
  
  2. If login works manually but not in Glue:
     Check the secret format in Secrets Manager — must be exact key names:
     {"username": "...", "password": "...", "account": "..."}
     
  3. Check account format:
     WRONG:  xy12345.snowflakecomputing.com  (don't include domain)
     RIGHT:  xy12345  (just the account identifier)
```

## Error 4: Airflow 1-Hour Timeout (Glue Job Skipped)

```
PROBLEM:
  VBRP Glue job takes 75 minutes
  Airflow task timeout = 60 minutes
  Airflow marks task as SKIPPED at 60 minutes
  But Glue actually completes at 75 minutes
  Next task (validate) never runs
  Watermark shows RUNNING (not SUCCESS)

THIS IS THE EXACT PROBLEM SAM DESCRIBED IN THE PLATFORM OVERVIEW!

FIX IN AIRFLOW DAG:
  In the GlueJobOperator, set:
    execution_timeout=timedelta(hours=3)   # In default_args
  
  OR in the specific task:
    GlueJobOperator(
        ...
        execution_timeout=timedelta(hours=3),
        ...
    )

FIX IN GLUE JOB:
  Optimize the VBRP job to run in < 45 minutes:
  - Increase workers (Option 2 in Step 10)
  - Column pruning (Option 3 in Step 10)
  - More partitions (Option 1 in Step 10)
  
MONITORING:
  Every morning, check:
  SELECT last_run_status FROM PIPELINE_WATERMARK WHERE table_name = 'VBRP'
  If status = 'RUNNING' but it's been 2+ hours, the job is stuck or skipped
  → Go to Glue Console → check if job actually completed
  → If completed in Glue, manually update watermark and trigger downstream
```

## Error 5: Memory Issues (OOM — Out of Memory)

```
ERROR MESSAGE:
  java.lang.OutOfMemoryError: GC overhead limit exceeded
  OR
  Container killed by YARN for exceeding memory limits

CAUSE:
  2,107 columns × 24.94M rows is a LOT of data in memory
  Default Glue worker memory may not be enough

FIX:
  Option A: Upgrade Worker Type
    Change from G.2X to G.4X (doubles the RAM)
    
  Option B: Column Pruning
    Only select 20-30 columns instead of 2,107
    
  Option C: Increase Spark configuration
    In Glue Job → Advanced → Spark UI → add:
    --conf spark.sql.shuffle.partitions=400
    --conf spark.default.parallelism=400
    
  Option D: Process in smaller chunks
    Add date-range splitting to VBRP extraction
    Process one week at a time for initial historical load
```

## Error 6: Duplicate Records in Snowflake

```
PROBLEM:
  Same BILLING_DOC_NUMBER appears multiple times in target table

CAUSE:
  Duplicate records in source (SAP may have duplicates)
  OR MERGE procedure has a bug
  OR the same batch was loaded twice

FIX — Check source first:
  In the SAP query, add: SELECT DISTINCT VBELN, FKART, ...
  
  In Glue code, deduplication is already there:
  .dropDuplicates(["BILLING_DOC_NUMBER"])
  
  Check MERGE procedure logic — make sure ON clause uses correct PK
  
FIX — If it was a double-load:
  DELETE FROM BILLING_DOCUMENT 
  WHERE LOAD_BATCH_ID IN (SELECT LOAD_BATCH_ID FROM BILLING_DOCUMENT 
                           GROUP BY LOAD_BATCH_ID ORDER BY MIN(EXTRACTED_AT) DESC LIMIT 1)
  Then re-run the load
```

## Error 7: Watermark Table Stuck at RUNNING

```
PROBLEM:
  PIPELINE_WATERMARK shows last_run_status = 'RUNNING'
  This prevents a new run (because the concurrent run check fails)

CAUSE:
  Previous Glue job crashed mid-run
  OR Airflow killed the job
  OR manual intervention interrupted the job

FIX:
  Check if Glue job is actually still running:
  AWS Console → Glue → ETL Jobs → your job → Job Runs
  
  If Glue shows "Succeeded" or "Failed":
    The job completed but watermark wasn't updated
    
  Manually reset watermark:
  UPDATE PIPELINE_WATERMARK
  SET last_run_status = 'FAILED',
      notes = 'Manually reset — job was stuck at RUNNING',
      updated_at = CURRENT_TIMESTAMP()
  WHERE table_name IN ('VBRK', 'VBRP')
    AND last_run_status = 'RUNNING';
  
  Then re-run the Glue jobs
```

---

# 16. POC SUCCESS CRITERIA — HOW TO KNOW YOU PASSED

## Define Success Before You Start

Your POC succeeds when ALL of the following are true:

### Technical Success Criteria

```
✅ CRITERIA 1: Data extracted from SAP without SnapLogic
   - Proof: Glue CloudWatch logs show "VBRK extraction complete" and "VBRP extraction complete"
   - Proof: S3 shows Parquet files in raw/sap/VBRK/ and raw/sap/VBRP/

✅ CRITERIA 2: Correct row counts in Snowflake
   - BILLING_DOCUMENT: comparable to existing table (ask Sam for expected count)
   - BILLING_DOCUMENT_ITEM: comparable to existing table
   - Proof: validation SQL query shows expected row counts

✅ CRITERIA 3: Delta load works correctly
   - Run #1: loads historical records (90 days)
   - Run #2 (1 hour later): only loads records changed in that hour
   - Proof: Run #2 row count is much smaller than Run #1

✅ CRITERIA 4: Watermark tracks correctly
   - After Run #1: watermark shows yesterday/today's date
   - After Run #2: watermark shows latest AEDAT from that hour's batch
   - Proof: SELECT * FROM PIPELINE_WATERMARK shows correct timestamps

✅ CRITERIA 5: No data quality issues
   - Zero orphan items (all VBRP records have matching VBRK)
   - Zero duplicate primary keys
   - Proof: Check 2 and Check 3 in Section 13 return 0 rows

✅ CRITERIA 6: Parent-child order maintained
   - VBRK always loads before VBRP
   - Proof: VBRK EXTRACTED_AT timestamp < VBRP EXTRACTED_AT timestamp in same batch

✅ CRITERIA 7: Airflow DAG runs successfully
   - All 4 tasks show green
   - Proof: Airflow UI → DAG runs → all tasks marked as "success"

✅ CRITERIA 8: Error handling works
   - Test by deliberately breaking something (wrong SAP creds)
   - Watermark shows "FAILED" status
   - Restore creds → re-run → shows "SUCCESS"
   - Proof: PIPELINE_WATERMARK records show correct status changes
```

### Performance Criteria (POC Targets)

```
VBRK Job Runtime:
  - First run (90-day history): < 30 minutes
  - Delta runs (hourly): < 10 minutes

VBRP Job Runtime:
  - First run (90-day history): < 60 minutes
  - Delta runs (hourly): < 20 minutes

Total pipeline (both tables, delta run):
  Target: < 30 minutes end-to-end
  Current SnapLogic comparison: ask what SnapLogic currently takes
```

### Business Validation (Show This to Sam and the Team)

```sql
-- Run this in Snowflake — show the output to Sam
-- This proves the data is correct and business-usable

SELECT 
    DATE_TRUNC('month', BILLING_DATE) AS billing_month,
    SALES_ORG,
    COUNT(DISTINCT BILLING_DOC_NUMBER) AS num_invoices,
    SUM(NET_VALUE) AS total_revenue,
    CURRENCY
FROM DB_SAP.CORE_SAP_POC.BILLING_DOCUMENT
WHERE BILLING_DATE >= DATEADD('month', -3, CURRENT_DATE())
GROUP BY 1, 2, 5
ORDER BY 1 DESC, 2;

-- Ask Sam or Arnab (Sales domain owner) to confirm these numbers 
-- match what they see in ThoughtSpot from the SnapLogic pipeline
```

---

# 17. PRESENTATION TALKING POINTS FOR YOUR MANAGER

When you present the POC results, here is how to explain what you built:

## The Problem We Solved

"Currently, TCPL uses SnapLogic to extract data from SAP and land it in S3. SnapLogic is an external iPaaS tool with licensing costs and limited customizability. The question was: can AWS Glue natively do what SnapLogic does, better and cheaper?"

## Why BillingDocument + BillingDocumentItem

"We picked the most critical tables on the platform — Priority 0 Mission Critical. The existing `prd-saps4-hourly-billing-document-delta-load-pipeline` runs every hour. If this fails, the CFA checkpoint at 20:11 fails, which means the entire CFA mart is not refreshed. These tables directly impact TCPL's revenue operations. If Glue can handle these, it can handle anything."

## What the POC Proved

"Glue successfully:
1. Connected to SAP HANA via JDBC — no SnapLogic needed
2. Extracted delta records using watermark-based filtering
3. Applied all transformations (SAP column names to readable names, type casting, deduplication)
4. Wrote raw and processed data to S3 in Parquet format
5. Loaded into Snowflake using MERGE (upsert — only changed records)
6. Updated the watermark correctly for the next hourly run
7. Ran the whole thing under Airflow orchestration"

## Performance Numbers to Quote

| Metric | Result |
|---|---|
| VBRK rows extracted (delta) | X rows |
| VBRK job duration | Y minutes |
| VBRP rows extracted (delta) | X rows |
| VBRP job duration | Y minutes |
| Data quality issues | 0 orphan items, 0 duplicates |
| Cost vs SnapLogic | TBD (Glue cost per DPU hour) |

## Next Steps (What You'd Recommend)

"For full production migration:
1. Expand to all 58 Priority 0 DAGs (replace all SnapLogic pipelines)
2. Run Glue and SnapLogic in parallel for 2 weeks to compare results
3. Once validated, cut over to Glue and retire SnapLogic
4. Estimated time: 2-3 months for full migration"

---

# 18. GLOSSARY

| Term | Meaning |
|---|---|
| **VBRK** | SAP table name for Billing Document Header (BillingDocument) |
| **VBRP** | SAP table name for Billing Document Item (BillingDocumentItem) |
| **VBELN** | SAP field: Billing/Sales document number (primary key) |
| **POSNR** | SAP field: Item number (part of VBRP primary key) |
| **AEDAT** | SAP field: Date of last change (our watermark column) |
| **ERDAT** | SAP field: Date of record creation |
| **FKDAT** | SAP field: Billing date |
| **NETWR** | SAP field: Net value |
| **MATNR** | SAP field: Material number |
| **WERKS** | SAP field: Plant |
| **JDBC** | Java Database Connectivity — standard protocol to connect apps to databases |
| **ngdbc.jar** | SAP HANA JDBC driver JAR file |
| **Delta Load** | Extract only new/changed records since last run |
| **Full Load** | Extract all records from source (no watermark filter) |
| **Watermark** | A timestamp stored in a control table marking the last successful extract |
| **UPSERT / MERGE** | SQL operation: UPDATE if record exists, INSERT if new |
| **Parquet** | Columnar file format for big data — smaller size, faster query than CSV |
| **Partition** | Dividing data by a key (e.g., date) to speed up queries and writes |
| **numPartitions** | Spark setting: how many parallel JDBC connections to SAP |
| **GlueContext** | Glue-specific wrapper around Spark that adds AWS features |
| **DPU** | Data Processing Unit — Glue's billing unit (1 DPU = 4 vCPU + 16 GB RAM) |
| **G.2X Worker** | Glue worker type: 2 DPU (8 vCPU, 32 GB RAM) per worker |
| **G.4X Worker** | Glue worker type: 4 DPU (16 vCPU, 64 GB RAM) per worker |
| **Secrets Manager** | AWS service for securely storing credentials |
| **IAM Role** | AWS Identity — defines what a service (Glue) is allowed to do |
| **CloudWatch Logs** | AWS logging service — where Glue job print statements appear |
| **Staging Table** | Temporary Snowflake table that receives delta data before MERGE |
| **Snowflake Connector** | Python library (snowflake-connector-python) to interact with Snowflake from Glue |
| **CFA Checkpoint** | The 20:11 IST Airflow DAG that waits for 9 upstream DAGs including billing |
| **POC** | Proof of Concept — a small-scale test to validate an approach |
| **iPaaS** | Integration Platform as a Service — what SnapLogic is |
| **OOM** | Out of Memory — Spark worker ran out of RAM |
| **Batch ID** | Unique identifier for each Glue job run (used for tracking and audit) |
| **Parent Entity** | BillingDocument (VBRK) — the header record |
| **Child Entity** | BillingDocumentItem (VBRP) — line items that reference the parent |
| **Referential Integrity** | Every child record must have a matching parent record |

---

*Good luck with the POC tomorrow! 🚀*
*Follow this document top to bottom. Don't skip steps. The most common mistakes are:*
*1. Network/security group blocking SAP JDBC connection*
*2. Wrong ngdbc.jar version*
*3. Snowflake credentials format wrong in Secrets Manager*
*4. Forgetting to load VBRK before VBRP*

*— Decision Point Team*
