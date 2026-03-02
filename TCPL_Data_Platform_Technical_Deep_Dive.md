# 🏗️ TCPL Data Platform — Complete Technical Deep Dive
## For Data Engineers at Decision Point
### *Understand the full platform architecture inside-out — every layer, every tool, every concept*

---

> **How to read this document:** This document covers the COMPLETE technical architecture of the TCPL Data Platform. It combines what was covered in Sam's session with deep explanations of WHY things are built the way they are. Read it section by section. By the end, you should be able to draw the full architecture on a whiteboard and explain every component to anyone.

---

# 📖 TABLE OF CONTENTS

1. [TCPL Formation — Why This Platform Was Even Built](#1-tcpl-formation)
2. [Platform Ownership — Who Owns What](#2-platform-ownership)
3. [Source Systems — Where Data Comes From](#3-source-systems)
4. [SnapLogic — The Ingestion Layer (Deep Dive)](#4-snaplogic-ingestion-layer)
5. [AWS Data Lake — The Storage Layers (Deep Dive)](#5-aws-data-lake)
6. [AWS Glue — The Transformation Layer (Deep Dive)](#6-aws-glue-transformation)
7. [Snowflake — The Data Warehouse (Deep Dive)](#7-snowflake-data-warehouse)
8. [Apache Airflow — The Orchestration Layer (Deep Dive)](#8-airflow-orchestration)
9. [BI Tools — ThoughtSpot and Power BI](#9-bi-tools)
10. [Machine Learning — SageMaker](#10-machine-learning)
11. [Security & Governance Layer](#11-security-governance)
12. [Deployment Governance — How Changes Are Released](#12-deployment-governance)
13. [Support Model — L1 and L2](#13-support-model)
14. [Failure Handling — What Happens When Things Break](#14-failure-handling)
15. [Full Architecture — Everything Together](#15-full-architecture)
16. [Data Engineer Day-in-the-Life — How You Fit In](#16-data-engineer-day-in-life)
17. [Master Glossary — Every Technical Term Explained](#17-master-glossary)

---

# 1. TCPL FORMATION — WHY THIS PLATFORM WAS EVEN BUILT

## 1.1 The Merger Story

TCPL (Tata Consumer Products Limited) was formed **about 5 years ago** by merging multiple separate Tata companies:

| Company Before Merger | What They Did |
|---|---|
| **Tata Salt** | Made and sold Tata Salt |
| **Tata Beverages** | Made and sold Tata Tea, Tetley |
| **Organic India** | Organic herbal teas, tulsi products |
| **Capital Foods** | Sauces, ready-to-eat (Ching's Secret, Smith & Jones) |

**The problem before the merger:**
Each of these companies had their OWN:
- Their own SAP system (or different version)
- Their own databases
- Their own data formats
- Their own reporting tools
- Their own definitions (e.g., "what counts as a sale" was defined differently in Tata Salt vs Tata Beverages)

**Result:** When TCPL was formed and someone asked "How much total revenue did TCPL make last month?", it was IMPOSSIBLE to answer quickly. Someone would have to go to 4 different systems, pull 4 different reports, manually combine in Excel, and spend 2-3 days just to produce ONE number.

## 1.2 Why a Unified Data Platform Was Needed

After the merger, TCPL leadership said: **"We need ONE single platform where ALL our data comes together."**

The goals were:
1. **Single source of truth** — one place for all data, no conflicting numbers
2. **Speed** — business users should get answers in seconds, not days
3. **Scale** — handle data from 30,000 distributors, 1.2 million outlets, millions of daily transactions
4. **Self-service** — 3,000+ users should be able to analyze data themselves without needing IT help every time
5. **Unified definitions** — "Primary Sale", "Revenue", "Active Distributor" means the SAME thing across all business units

## 1.3 The Scale of Data Generated

Because TCPL is so large, the data volumes are enormous:

| Business Area | Data Generated |
|---|---|
| Sales (Primary + Secondary) | Millions of billing documents per month |
| Distributor Orders | 30,000 distributors placing daily orders |
| Salesman Visits | Hundreds of thousands of visits per day |
| Products | 7,000-10,000 SKUs |
| Outlets | 1.2 million shops to track |
| Finance | Thousands of invoices, credit notes, debit notes daily |
| HR | Payroll, attendance, thousands of employees |
| International | Multiple countries, currencies |

This data volume is why you cannot use a simple Excel or a small database — you need an enterprise-grade cloud data platform.

---

# 2. PLATFORM OWNERSHIP — WHO OWNS WHAT

## 2.1 The Ownership Structure

The TCPL Data Platform is NOT owned by just one person. It is divided by business domain:

```
┌─────────────────────────────────────────────────────────┐
│              TCPL UNIFIED DATA PLATFORM                 │
│                                                         │
│  Overall Owner: SAM JOSEPH                              │
│  (Responsible for the entire platform, infrastructure,  │
│   governance, and cross-functional decisions)           │
│                                                         │
├───────────────┬──────────────────┬──────────────────────┤
│    ARNAB      │     ARITRA       │       SHALVI         │
│    Sales &    │   Operations &   │  Finance, HR &       │
│   Marketing   │  Supply Chain    │  International       │
│               │                  │                      │
│ • Primary     │ • Procurement    │ • Revenue accounts   │
│   sales data  │ • Inventory      │ • HR/payroll         │
│ • Secondary   │ • Production     │ • International      │
│   sales data  │   planning       │   business data      │
│ • Distributor │ • Logistics      │ • Month-end close    │
│   data        │ • Demand         │   reporting          │
└───────────────┴──────────────────┴──────────────────────┘
```

## 2.2 What "Domain Ownership" Means for You

As a Data Engineer, this structure matters because:

- When you build or modify something in **Sales & Marketing** domain → Arnab is your stakeholder
- When something breaks in **Finance** data → Shalvi's team raises the issue
- When you need platform-wide decisions (infra changes, new tools) → Sam approves
- **DT (Digital Transformation) teams** for each domain manage:
  - Dataset definitions (what exactly is each field?)
  - Refresh frequencies (how often should data update?)
  - Functional requirements (what do business users need?)

## 2.3 The Vendor Side — LatentView + Decision Point

The session also shows **LatentView** participants. Important to understand the vendor ecosystem:

- **LatentView** — another analytics/data vendor working on parts of the TCPL platform
- **Decision Point** — your company, also working on the TCPL platform
- Multiple vendors can work on the same client — different areas or different projects
- Coordination between vendors is important to avoid conflicts

---

# 3. SOURCE SYSTEMS — WHERE DATA COMES FROM

## 3.1 Overview of Data Producers

The document calls source systems **"Data Producers"** — they produce raw data that flows into the platform.

```
DATA PRODUCERS (Source Systems)
├── SAP ERP
├── Salesforce (Mavic/SFDC)
├── Nielsen
├── Tea Buying App
├── Order Management (OMS)
├── MDM (Master Data Management)
├── Vendor Payment System
└── MDM Tickets
```

## 3.2 SAP — The Biggest Data Producer

**What SAP covers for TCPL:**
- **Finance** — all financial transactions, journal entries, accounts payable/receivable
- **Supply Chain** — procurement from vendors, production planning, inventory movements
- **Sales** — primary sales orders, billing documents, pricing
- **Operations** — delivery orders, shipments, warehouse management

**Why SAP is special:**
SAP is the "system of record" — it is THE authoritative source for financial numbers. If there's a discrepancy between SAP and any other system, SAP is always considered correct.

**SAP data characteristics:**
- Very structured and standardized (SAP enforces its own data standards)
- Very high volume (every transaction creates multiple related records)
- Data is in SAP-specific table structures (VBAK, VBRK, etc.)
- Changes happen throughout the day in real-time
- SnapLogic extracts from SAP in batch (usually nightly)

## 3.3 Salesforce (Mavic) — Secondary Sales Data

Already covered in detail in the first document. From a technical perspective:

**Salesforce data characteristics:**
- Cloud-based (data lives in Salesforce's servers, accessed via API)
- More real-time than SAP (salesmen update on their phones instantly)
- API-based extraction (SnapLogic calls Salesforce REST APIs)
- Data is in Salesforce object structure (Accounts, Opportunities, Custom Objects)
- Can have data quality issues (field sales data entry is imperfect)

## 3.4 Nielsen — Market Intelligence Data

**What Nielsen provides:**
- Consumer/tertiary sales estimates
- Market share data
- Category performance
- Competitor intelligence

**Nielsen data characteristics:**
- NOT real-time — Nielsen provides periodic data files (weekly, monthly)
- File-based delivery (Nielsen sends CSV/Excel files, not a live API)
- Estimated data (based on sampling, not exact counts)
- Expensive — TCPL pays Nielsen for this data

## 3.5 Internal Apps — The Less Visible Sources

| App | What Data It Has |
|---|---|
| **Tea Buying App** | Data about purchasing tea leaves from tea estates — quantities, prices, quality grades |
| **Order Management (OMS)** | Distributor order placement data |
| **MDM (Master Data Management)** | Master data — product catalog, distributor list, outlet list, geography hierarchy |
| **Vendor Payment System** | Payments made to vendors/suppliers |
| **MDM Tickets** | Change requests for master data (e.g., "add a new distributor", "change a product name") |

## 3.6 What is MDM (Master Data Management)?

MDM is extremely important for data engineers — understand this deeply.

**What is Master Data?**
Master data = the reference data that everything else is built on. Examples:
- Product Master — the official list of all TCPL products (SKU code, name, category, brand, etc.)
- Distributor Master — the official list of all 30,000 distributors
- Outlet Master — the official list of all 1.2 million outlets
- Geography Master — the official hierarchy of countries, clusters, regions

**Why MDM matters:**
If the product master says "Tata Salt 1KG has SKU code TS001" but SAP uses "SALT1000" for the same product, then when you try to join sales data with product data, nothing will match!

MDM is the system that:
- Maintains the SINGLE official version of all master data
- Ensures SAP, Mavic, and all other systems use the SAME codes and names
- Processes change requests (when a new product is launched, someone adds it to MDM first, then it flows to SAP and Mavic)

**Your role with MDM:**
When you build dimension tables in Snowflake (dim_product, dim_distributor, etc.), MDM data is often the source. Keep dimension tables in sync with MDM.

---

# 4. SNAPLOGIC — THE INGESTION LAYER (DEEP DIVE)

## 4.1 What is SnapLogic and Why Is It Used?

**SnapLogic** is an **iPaaS (Integration Platform as a Service)** — a cloud-based tool that connects different systems and moves data between them.

Think of SnapLogic as a **network of pipes**. On one side, you have source systems (SAP, Mavic, Nielsen). On the other side, you have the destination (AWS S3). SnapLogic builds the pipes that carry data from source to destination.

**Why SnapLogic specifically and not custom code?**
You could write Python scripts to extract data from SAP and load to S3. But SnapLogic offers:
- **Pre-built connectors** — ready-made connections to SAP, Salesforce, Oracle, etc. No need to build from scratch
- **Visual pipeline builder** — drag and drop interface to build pipelines
- **Monitoring dashboard** — see which pipelines ran, which failed, when
- **Error handling** — built-in retry logic, error logging
- **Scalability** — can handle large data volumes
- **Reduced development time** — faster to build than writing custom code

## 4.2 How SnapLogic Pipelines Work — Step by Step

A **pipeline** in SnapLogic is a sequence of steps that:
1. Connects to a source system
2. Reads data
3. Does minimal transformation
4. Writes data to destination (S3)

```
SNAPLOGIC PIPELINE FLOW

┌──────────────────────────────────────────────────────────────────────┐
│                        SnapLogic Pipeline                            │
│                                                                      │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────────┐  │
│  │ Source   │ →  │  Read    │ →  │ Minimal  │ →  │  Write to    │  │
│  │Connector │    │  Data    │    │Transform │    │  AWS S3      │  │
│  │(SAP/SFDC)│    │(SQL/API) │    │(if any)  │    │  (Raw Zone)  │  │
│  └──────────┘    └──────────┘    └──────────┘    └──────────────┘  │
│                                                                      │
│  This whole thing is ONE Pipeline for ONE table                      │
└──────────────────────────────────────────────────────────────────────┘
```

TCPL has **many** such pipelines — one for each table/object being extracted from each source system.

## 4.3 Parameterization — The Smart Way SnapLogic is Built

### What is Parameterization?

Instead of building a completely separate pipeline for each and every table (which would mean hundreds of slightly different pipelines), TCPL built **parameterized pipelines**.

**The concept:**
One generic pipeline can handle many different tables. Instead of hardcoding the table name inside the pipeline, the table name is passed as a **parameter**.

**The Control Table:**
Parameters are stored in a **MySQL RDS database** called the **Control Table**.

### What is the Control Table?

The Control Table is a configuration database (MySQL on AWS RDS). It has rows like:

```
┌──────────────────┬────────────────┬──────────────────┬────────────────────┐
│  source_system   │  table_name    │  api_connection  │  pipeline_name     │
├──────────────────┼────────────────┼──────────────────┼────────────────────┤
│  SAP             │  VBRK          │  SAP_PROD_CONN   │  sap_extract_pipe  │
│  SAP             │  VBRP          │  SAP_PROD_CONN   │  sap_extract_pipe  │
│  SAP             │  VBAK          │  SAP_PROD_CONN   │  sap_extract_pipe  │
│  Salesforce      │  Account       │  SFDC_API_CONN   │  sfdc_extract_pipe │
│  Salesforce      │  Order__c      │  SFDC_API_CONN   │  sfdc_extract_pipe │
│  Nielsen         │  market_share  │  NIELSEN_CONN    │  nielsen_file_pipe │
└──────────────────┴────────────────┴──────────────────┴────────────────────┘
```

**How it works:**
1. Airflow (orchestrator) triggers a pipeline with parameters from the control table
2. SnapLogic pipeline receives: "extract table VBRK from SAP using connection SAP_PROD_CONN"
3. Pipeline connects to SAP, reads VBRK table, writes to S3
4. Next run: Airflow passes "extract table VBRP from SAP"
5. Same pipeline, different parameter — different table extracted

**Benefits of this approach:**
- Add a new table to extract? Just add a row to the control table — no code change needed
- Change connection details? Update the control table, all pipelines pick up the change
- One pipeline to maintain instead of hundreds

## 4.4 Incremental Load Handling — The Watermark System

### Full Load vs Incremental Load

**Full Load:**
Extract EVERYTHING from the source table every time.
- Problem: SAP billing table has 5 years of data = tens of millions of rows
- If you extract all of it every night, it takes hours and wastes compute/money

**Incremental Load (the smart way):**
Extract only the rows that are NEW or CHANGED since the last extraction.
- Only takes a few minutes
- Much less data transferred
- Efficient and cost-effective

### How Watermarks Work

A **watermark** is a marker that records "I last extracted data up to this point."

```
WATERMARK CONCEPT

SAP Billing Table (millions of rows):
─────────────────────────────────────────────────────────────
Row 1: Invoice date 2024-01-01  ← Already extracted
Row 2: Invoice date 2024-01-02  ← Already extracted
...
Row 999,000: Invoice date 2025-02-28  ← Already extracted
─ ─ ─ ─ ─ ─ ─ WATERMARK LINE ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
Row 999,001: Invoice date 2025-03-01  ← NEW — extract this
Row 999,002: Invoice date 2025-03-01  ← NEW — extract this
Row 999,003: Invoice date 2025-03-02  ← NEW — extract this
─────────────────────────────────────────────────────────────
```

**The watermark column:**
Usually a `last_modified_timestamp` or `created_date` column in the source table.

**The watermark process:**
1. SnapLogic checks the control table: "Last time I extracted VBRK, the maximum `posting_date` was 2025-02-28"
2. SnapLogic queries SAP: `SELECT * FROM VBRK WHERE posting_date > '2025-02-28'`
3. Gets only new/modified rows
4. After successful extraction, updates the watermark to `2025-03-02` (latest date extracted)
5. Next run starts from `2025-03-02`

**Important nuance — backdated records:**
Sometimes SAP users post entries with past dates (e.g., today is March 2 but they post an entry for Feb 28 due to a delay). This is a problem because the watermark has already passed Feb 28.

Solution: Use a slightly wider window — extract records modified in the last N days to catch backdated entries.

## 4.5 Monitoring & Change Control

### Daily Monitoring
The platform team monitors SnapLogic pipelines every day using the **SnapLogic dashboard**:
- Which pipelines ran successfully?
- Which pipelines failed?
- How long did each pipeline take?
- How many records were extracted?

If a pipeline fails → alert is sent to the support team → L1 support investigates.

### Change Control — Freshservice + CAB

**Freshservice:**
Freshservice is an IT Service Management (ITSM) tool. Whenever ANY change is made to the platform (new pipeline, modified pipeline, new table, etc.), a **Change Request (CR)** must be created in Freshservice.

The CR documents:
- What change is being made
- Why is it needed
- What is the risk
- How to rollback if something goes wrong
- Who approves it

**CAB = Change Advisory Board:**
The CAB is a group of people (technical leads, business representatives) who review and **approve** change requests before they are deployed.

**Why this process exists:**
Without it, someone could make a change to a pipeline on Friday evening, it breaks on Saturday, and the Monday morning ThoughtSpot dashboards show wrong data. 3,000 business users start their week with incorrect numbers. This is catastrophic for a company like TCPL.

The CR → CAB process ensures:
- Every change is reviewed by multiple people
- Risks are assessed before deployment
- Someone is accountable for each change
- There is documentation if something goes wrong

---

# 5. AWS DATA LAKE — THE STORAGE LAYERS (DEEP DIVE)

## 5.1 What is a Data Lake?

A **Data Lake** is a storage system that holds raw data in its native format until it's needed.

Think of it like this:
- A **database** is like a organized filing cabinet — everything is neatly sorted and structured
- A **data lake** is like a large warehouse — everything is stored, but not always perfectly organized
- The data lake stores everything first, then you figure out what to do with it

**AWS S3 (Simple Storage Service)** is Amazon's file/object storage service. TCPL uses S3 as their data lake.

## 5.2 The Four Layers of the TCPL Data Lake

TCPL's S3 data lake is organized into **4 distinct layers** (also called zones). Each layer has a specific purpose. Data flows from layer to layer.

```
DATA FLOW THROUGH S3 LAYERS

Source Systems
      ↓
┌─────────────────────────────────────────────────────────────────┐
│  LAYER 1: RAW LAYER                                             │
│  (SnapLogic dumps data here exactly as received)                │
│  S3 path: s3://tcpl-datalake/raw/sap/vbrk/2025/03/01/          │
│  Format: CSV, JSON, or Parquet (as extracted from source)       │
│  Transformation: NONE — raw, untouched data                     │
│  Retention: Long-term (kept for audit/reprocessing)             │
└──────────────────────────┬──────────────────────────────────────┘
                           ↓ (AWS Glue reads from here)
┌─────────────────────────────────────────────────────────────────┐
│  LAYER 2: REFINED / CURATED LAYER                               │
│  (Intermediate processing — partly cleaned, validated)          │
│  S3 path: s3://tcpl-datalake/refined/sap/billing/2025/03/01/   │
│  Format: Usually Parquet (columnar, compressed, fast to query)  │
│  Transformation: Basic cleaning, schema fixes, deduplication    │
│  Retention: Medium-term                                         │
└──────────────────────────┬──────────────────────────────────────┘
                           ↓ (More Glue processing)
┌─────────────────────────────────────────────────────────────────┐
│  LAYER 3: PROCESSED LAYER                                       │
│  (Final, clean data ready to load into Snowflake)               │
│  S3 path: s3://tcpl-datalake/processed/sales/billing/           │
│  Format: Parquet, partitioned by date/region for performance    │
│  Transformation: Full business logic applied, ready for DWH     │
│  Retention: Short-term (once in Snowflake, less critical)       │
└──────────────────────────┬──────────────────────────────────────┘
                           ↓ (Loaded into Snowflake)
                    ❄️ SNOWFLAKE

Also:
┌─────────────────────────────────────────────────────────────────┐
│  LAYER 4: ARCHIVAL LAYER (AWS Glacier)                          │
│  (Old data moved here for long-term cheap storage)              │
│  Old raw and processed data → moved to Glacier                  │
│  Glacier is very cheap but slow to retrieve                     │
│  Used for compliance / regulatory retention requirements        │
└─────────────────────────────────────────────────────────────────┘
```

## 5.3 Layer 1 — Raw Layer (Most Important to Understand)

**The Golden Rule of Raw Layer:**
> **NEVER modify, delete, or transform data in the raw layer. Store exactly as received from source.**

**Why this rule exists:**
- If something goes wrong in processing, you can always go back to the raw data and reprocess
- The raw data is your backup of what the source system originally said
- It serves as an audit trail (what did SAP actually have on this date?)
- You can rerun transformations from scratch if business logic changes

**What raw data looks like:**
When SnapLogic extracts SAP's VBRK table (billing document header), it dumps it as a file in S3. The file might look like:

```
VBELN,FKDAT,KUNAG,NETWR,WAERK,BUKRS
9001234567,20250301,DIST001,11800.00,INR,1000
9001234568,20250301,DIST002,24500.00,INR,1000
9001234569,20250301,DIST003,8200.00,INR,1000
```

This is raw SAP data — SAP column names, SAP data types. No renaming, no cleaning, just exactly what SAP had.

**File organization in S3 (Partitioning):**
Raw files are organized by date so you can find data for specific periods:
```
s3://tcpl-datalake/raw/
├── sap/
│   ├── vbrk/
│   │   ├── 2025/03/01/vbrk_20250301_batch1.parquet
│   │   ├── 2025/03/02/vbrk_20250302_batch1.parquet
│   └── vbak/
│       ├── 2025/03/01/vbak_20250301_batch1.parquet
├── salesforce/
│   ├── order/
│   └── account/
└── nielsen/
    └── market_share/
```

## 5.4 Layer 2 — Refined/Curated Layer

**Purpose:** Intermediate zone between raw and processed. Glue jobs read from raw, do basic cleaning, and write here.

**What happens here:**
- Remove obviously bad records (null primary keys, clearly invalid data)
- Fix data types (SAP sometimes sends dates as strings "20250301" — convert to proper date format)
- Basic deduplication (remove exact duplicate rows if they somehow got loaded twice)
- Schema normalization (rename columns from SAP codes to more readable names)

**Example transformation:**
Raw: `VBELN=9001234567, FKDAT=20250301, KUNAG=DIST001, NETWR=11800.00`
Refined: `billing_doc_number=9001234567, billing_date=2025-03-01, distributor_code=DIST001, net_value=11800.00`

## 5.5 Layer 3 — Processed Layer

**Purpose:** Final clean data, ready to be loaded into Snowflake.

**What happens here:**
- Full business logic applied
- Joins between tables (e.g., join billing with distributor master to get distributor name)
- KPI calculations
- Data partitioned optimally for Snowflake loading
- Quality checks passed

**After this layer:**
Data is loaded into Snowflake Core Layer. Job done for that pipeline run.

## 5.6 Layer 4 — Archival (AWS Glacier)

**What is Glacier?**
AWS Glacier is Amazon's **cold storage** service. It is:
- Extremely cheap (fraction of the cost of regular S3)
- Very slow to retrieve (could take hours to restore files)

**When is data moved to Glacier?**
Old raw data that is past the retention period (e.g., raw files older than 1 year) gets automatically moved to Glacier. You don't need daily access to 2-year-old raw SAP files, but you might need them if there's an audit or a major bug discovery.

**Lifecycle Policies:**
AWS has **S3 Lifecycle Policies** — automatic rules that move data between storage tiers based on age:
- Raw files > 90 days → move to Glacier
- Processed files > 30 days → delete (already in Snowflake)

---

# 6. AWS GLUE — THE TRANSFORMATION LAYER (DEEP DIVE)

## 6.1 What is AWS Glue?

**AWS Glue** is Amazon's **serverless ETL (Extract, Transform, Load) service**.

**What "serverless" means:**
You don't need to manage servers. AWS automatically allocates compute resources when a Glue job runs. When the job finishes, resources are released. You only pay for the time the job actually runs.

**What Glue does in TCPL:**
Glue jobs read data from S3 (Raw or Refined layers), apply transformations, and write the results back to S3 (Processed layer) or directly to Snowflake.

**Technology used:**
Glue jobs are written in **Python using PySpark** (Apache Spark for Python). Spark is a distributed data processing framework that can handle massive datasets efficiently.

## 6.2 All Transformation Steps in Glue — Each One Explained

### Step 1: Deduplication

**Problem:**
Sometimes the same record gets extracted twice. This happens because:
- A pipeline retried after a failure and re-extracted records
- A source system update changed a field, and the watermark picked up the record again
- SAP sometimes has duplicate billing documents during system corrections

**What deduplication does:**
Identifies and removes duplicate records, keeping only the latest/correct version.

**How it works technically:**
```python
# PySpark deduplication example concept
from pyspark.sql import Window
from pyspark.sql.functions import row_number, desc

window = Window.partitionBy("billing_doc_number").orderBy(desc("extraction_timestamp"))
df = df.withColumn("rank", row_number().over(window))
df_deduped = df.filter(df.rank == 1).drop("rank")
```

**Logic:** For each billing_doc_number, keep only the most recently extracted version. All older versions are dropped.

---

### Step 2: Schema Standardization

**Problem:**
Different source systems have different naming conventions:
- SAP uses German abbreviations: VBELN (billing doc number), FKDAT (billing date), KUNAG (customer)
- Salesforce uses its own naming: Account__c, Order_Number__c
- Nielsen uses its own format

**What schema standardization does:**
Renames ALL columns to a consistent, readable naming convention that your business users and Snowflake models understand.

**Example:**
| Source Column (SAP) | Standardized Column |
|---|---|
| VBELN | billing_document_number |
| FKDAT | billing_date |
| KUNAG | distributor_code |
| NETWR | net_value |
| WAERK | currency_code |
| BUKRS | company_code |

This standardization is defined in a mapping file/table that Glue reads. When SAP changes a column name (rare but happens), you update the mapping file, not the entire Glue job.

---

### Step 3: Data Validation and Cleansing

**What validation does:**
Checks that data meets expected quality standards before it enters Snowflake.

**Types of validations:**

| Validation Type | Example Check | Action if Fails |
|---|---|---|
| **Not Null** | billing_document_number must not be null | Reject record, log error |
| **Data Type** | billing_date must be a valid date | Try to fix, or reject |
| **Referential Integrity** | distributor_code must exist in distributor master | Flag as orphan record |
| **Business Rule** | net_value must be > 0 for standard billing | Flag for review |
| **Format Check** | Date must be in YYYY-MM-DD format | Convert or reject |
| **Range Check** | Quantity must be between 1 and 999,999 | Flag suspiciously large orders |

**What happens to failed records:**
- They go into an **error table** or **quarantine zone** in S3
- The support team is notified
- Someone reviews and either fixes or ignores them
- They are NOT loaded into Snowflake (bad data should never enter your warehouse)

**Cleansing:**
- Trim whitespace: `"  DIST001  "` → `"DIST001"`
- Standardize case: `"tata salt"` → `"Tata Salt"` (if needed)
- Fix date formats: `"01/03/2025"` → `"2025-03-01"`
- Handle special characters in product names

---

### Step 4: Type Casting

**Problem:**
When data is extracted from SAP or Salesforce, it often comes as strings (text) even when the data is actually a number or date.

SAP exports: `NETWR = "11800.00"` (string) — but you need it as a decimal/float
SAP exports: `FKDAT = "20250301"` (string) — but you need it as a date

**What type casting does:**
Converts data to the correct data type:
```python
# Type casting examples
df = df.withColumn("net_value", col("NETWR").cast("decimal(18,2)"))
df = df.withColumn("billing_date", to_date(col("FKDAT"), "yyyyMMdd"))
df = df.withColumn("quantity", col("MENGE").cast("integer"))
```

**Why this matters:**
- If billing date is stored as a string in Snowflake, you cannot do `WHERE billing_date BETWEEN '2025-01-01' AND '2025-03-01'` properly
- Math operations on string numbers give wrong results
- ThoughtSpot needs proper data types to create charts correctly

---

### Step 5: Partitioning

**What is partitioning?**
Partitioning means organizing data in S3 files by specific columns so that queries only read the data they need.

**Example:**
Without partitioning: one massive file with all billing data from 2020 to 2025.
With date partitioning:
```
s3://tcpl-datalake/processed/billing/
├── year=2025/
│   ├── month=01/
│   │   ├── part-0001.parquet
│   │   └── part-0002.parquet
│   ├── month=02/
│   └── month=03/
└── year=2024/
```

**Why partitioning is critical for performance:**
When a Glue job or Snowflake query needs "all billing data for March 2025", it reads ONLY the `year=2025/month=03/` folder. It doesn't touch any other data.

Without partitioning: reads 5 years of data, processes it, filters to March 2025.
With partitioning: reads ONLY March 2025 data. 

**Partition columns used in TCPL:**
- `date` — most common (daily batches)
- `instance` — if there are multiple SAP instances (e.g., India SAP, International SAP)
- `api` — for Salesforce, partitioned by API version or object type

---

### Step 6: Loading into Snowflake Core

After all transformations are done, the final step in the Glue job is loading data into the **Snowflake Core Layer**.

**How Glue loads to Snowflake:**
- Uses the **Snowflake Spark Connector** or **COPY INTO** command
- Glue writes processed Parquet files to S3 Processed zone
- Then triggers a `COPY INTO` command in Snowflake which loads from S3 to the Snowflake table

**Load modes:**
- **Append** — add new rows (used for transactional fact tables like billing)
- **Upsert (MERGE)** — insert new, update existing (used for dimension tables like distributor master)
- **Full Refresh** — truncate and reload entirely (used for small reference tables)

---

# 7. SNOWFLAKE DATA WAREHOUSE — DEEP DIVE

## 7.1 What is Snowflake and Why is it Used?

**Snowflake** is a cloud-based **Data Warehouse** — a highly optimized database designed for analytical queries.

**Regular database vs Data Warehouse:**

| Aspect | Regular Database (e.g., MySQL) | Data Warehouse (Snowflake) |
|---|---|---|
| Designed for | Operational queries (insert/update/delete fast) | Analytical queries (read millions of rows fast) |
| Data volume | Thousands to millions of rows | Hundreds of millions to billions of rows |
| Query type | Simple lookups, transactions | Complex aggregations, joins across huge tables |
| Users | Application code | Analysts, BI tools |
| Example query | "Find order ID 12345" | "Sum of all sales by region and month for last 2 years" |

**Why Snowflake specifically:**
- **Separates storage and compute** — you can scale compute (query speed) independently from storage (data size)
- **Auto-scaling** — when many users run queries simultaneously, Snowflake can add more compute automatically
- **Pay-per-use** — you pay only when queries are running, not for idle time
- **Zero copy cloning** — create development copies of production data without duplicating storage
- **Time travel** — query data as it was at any point in the past (useful for debugging)

## 7.2 Snowflake Architecture in TCPL — The Two-Layer Structure

TCPL's Snowflake is organized into a clear structure:

```
SNOWFLAKE DATABASES STRUCTURE

Snowflake Account
│
├── SOURCE SYSTEM DATABASES (one per source)
│   ├── DB_SAP              ← All SAP tables, cleaned
│   ├── DB_SALESFORCE       ← All Salesforce tables, cleaned
│   ├── DB_NIELSEN          ← Nielsen data
│   └── DB_INTERNAL_APPS    ← MDM, OMS, other apps
│
├── CORE LAYER (per domain)
│   ├── CORE_SALES          ← Cleaned sales data, close to source structure
│   ├── CORE_FINANCE        ← Cleaned finance data
│   ├── CORE_SUPPLY_CHAIN   ← Cleaned supply chain data
│   └── CORE_HR             ← Cleaned HR data
│
└── BUSINESS-READY LAYER (domain marts)
    ├── MART_SALES          ← Sales analytics models (fact + dim tables)
    ├── MART_FINANCE        ← Finance analytics models
    ├── MART_SUPPLY_CHAIN   ← Supply chain analytics models
    └── MART_HR             ← HR analytics models
```

## 7.3 The Core Layer — What It Is and Why It Exists

**Core Layer = Cleaned datasets that closely mirror the source system structure**

Think of Core as a "digital twin" of SAP inside Snowflake:
- SAP has a billing table → Core has a billing table with the same data, just cleaned
- Core does NOT change the structure or business logic
- Core tables use standardized column names (done by Glue before loading)
- Core is used by the Business-Ready layer as its source

**Why have a Core layer at all?**
- Separation of concerns — raw source data is separate from business models
- Multiple business-ready models can read from the same Core table
- If source system changes, you only update Core → Business-Ready tables stay stable
- Useful for debugging (can check Core to see exactly what came from SAP)

## 7.4 The Business-Ready Layer — Where the Magic Happens

**Business-Ready Layer = Domain-specific data marts combining data from multiple sources**

This is where you build:
- **Fact Tables** — transaction data (sales, orders, visits)
- **Dimension Tables** — reference data (products, distributors, geography)
- **KPI Tables** — pre-calculated metrics

**What "combining data from multiple sources" means:**
The Sales Mart combines:
- SAP billing data (primary sales)
- Salesforce secondary sales data
- MDM product master
- MDM distributor master
- Geography hierarchy
- Sales targets (from Excel uploads or planning system)
- Nielsen market share data

A single query in ThoughtSpot can answer "What is our market share for Tata Salt in North India this quarter?" because all this data is combined and modeled in the Business-Ready layer.

## 7.5 Snowflake Warehouses (Compute Clusters)

In Snowflake, **"warehouse"** has a dual meaning:
1. The overall data warehouse (the system)
2. A **compute warehouse** (a virtual cluster that runs queries)

TCPL likely has multiple compute warehouses:
- `WH_ETL` — used for loading data (Glue → Snowflake loads)
- `WH_REPORTING` — used by ThoughtSpot for queries
- `WH_POWERBI` — used by Power BI
- `WH_ADHOC` — used by analysts for ad-hoc queries

**Why separate warehouses?**
If ThoughtSpot's 3,000 users are running reports in the morning and a massive ETL load is also happening, they would compete for the same compute. Separate warehouses ensure reporting performance is not affected by loading, and vice versa.

## 7.6 Pre-Aggregated Data for Performance

The document mentions ThoughtSpot uses **"pre-aggregated data"**.

**What is pre-aggregation?**
Instead of computing summaries on-the-fly every time a user opens a dashboard, you pre-compute them and store the results.

**Example:**
Instead of running this every time a user opens a dashboard:
```sql
SELECT region, month, SUM(billing_value) as total_sales
FROM fact_primary_sales
WHERE year = 2025
GROUP BY region, month
-- This might scan 100 million rows every time
```

You pre-compute and store:
```sql
-- Runs once daily, stores results in an aggregated table
INSERT INTO agg_sales_by_region_month
SELECT region, month, SUM(billing_value) as total_sales
FROM fact_primary_sales
WHERE year = 2025
GROUP BY region, month;
```

Then ThoughtSpot queries the small aggregated table (thousands of rows) instead of the huge fact table (hundreds of millions of rows) → 100x faster.

---

# 8. APACHE AIRFLOW — THE ORCHESTRATION LAYER (DEEP DIVE)

## 8.1 What is Apache Airflow and Why is it Needed?

**Airflow** is a **workflow orchestration platform** — it schedules, runs, and monitors data pipelines.

**The problem Airflow solves:**
Imagine you have 50 jobs to run every night in a specific order:
1. First, run SnapLogic to extract SAP data → then
2. Run Glue job to clean SAP billing data → then
3. Load into Snowflake Core → then
4. Run Snowflake SQL to build Business-Ready table → then
5. Trigger ThoughtSpot cache refresh

If any step fails, the next step should NOT run. If you want to rerun just step 3 without re-running steps 1 and 2, you should be able to do that.

Without Airflow: You'd need someone to manually trigger each job in order, wait for it to finish, then trigger the next one. Impossible at scale.

With Airflow: You define the order and dependencies once, Airflow handles everything automatically.

## 8.2 DAGs — The Core Concept

**DAG = Directed Acyclic Graph**

A DAG is how Airflow defines a workflow. It's a map of tasks and their dependencies.

**"Directed"** = tasks have a specific direction (Task A → Task B, never B → A)
**"Acyclic"** = no loops (Task A cannot eventually point back to itself)
**"Graph"** = a network of connected tasks

**TCPL has 240 DAGs** — that's 240 separate workflows managing different parts of the platform.

**Example DAG (simplified):**
```
DAG: daily_sap_billing_pipeline

Task 1: extract_sap_billing (SnapLogic)
    ↓
Task 2: raw_to_refined_billing (Glue Job)
    ↓
Task 3: refined_to_processed_billing (Glue Job)
    ↓
Task 4: load_to_snowflake_core (Snowflake COPY INTO)
    ↓
Task 5: build_fact_primary_sales (Snowflake SQL)
    ↓
Task 6: build_agg_sales_tables (Snowflake SQL)
    ↓
Task 7: notify_thoughtspot_cache_refresh
    ↓
Task 8: send_success_notification
```

If Task 3 fails → Tasks 4, 5, 6, 7, 8 DO NOT RUN. This prevents bad data from flowing downstream.

## 8.3 Scheduling in Airflow

Every DAG has a **schedule** — when it should run.

**Common schedules in TCPL:**

| DAG | Schedule | Why |
|---|---|---|
| SAP billing extraction | `0 22 * * *` (10 PM daily) | SAP batch closes at 9 PM |
| Salesforce extraction | `0 23 * * *` (11 PM daily) | Run after SAP extraction |
| Snowflake models build | `0 2 * * *` (2 AM daily) | After all extractions complete |
| ThoughtSpot refresh | `0 4 * * *` (4 AM daily) | After Snowflake builds complete |
| Weekly aggregates | `0 6 * * 1` (6 AM Mondays) | Weekly summaries needed for Monday reports |
| Monthly aggregates | `0 6 1 * *` (6 AM 1st of month) | Month-end business reviews |

**Cron syntax explanation:**
`0 22 * * *` means: minute=0, hour=22, day=any, month=any, weekday=any → runs at 10:00 PM every day

## 8.4 Dependency Management

Airflow manages **dependencies** — the rules about which task must complete before another can start.

**Types of dependencies:**

**Sequential dependency:**
```
Task A → Task B → Task C
(A must complete before B starts, B must complete before C starts)
```

**Parallel dependencies:**
```
Task A → Task B ┐
                 ├→ Task D (D starts only after BOTH B and C complete)
Task A → Task C ┘
```

**Cross-DAG dependencies:**
Sometimes DAG 2 should only run after DAG 1 completes. Airflow handles this too.

**Real example in TCPL:**
The Snowflake Sales Mart build depends on:
- SAP billing Glue job completing ✅
- Salesforce secondary sales Glue job completing ✅
- MDM product master load completing ✅
- MDM distributor master load completing ✅

All four must succeed before the Sales Mart build starts. If ANY one fails, the Sales Mart build skips.

## 8.5 Failure Notifications

When a task fails, Airflow automatically sends notifications:

**Notification channels:**
- **Email** — platform team and relevant support staff receive failure emails
- **SNS (Amazon Simple Notification Service)** — messages sent to SNS which can trigger other actions
- **Slack** (if integrated) — alerts in relevant channels

**What a failure notification contains:**
- DAG name
- Task that failed
- Error message / traceback
- Timestamp
- Link to Airflow UI to investigate

## 8.6 Secrets Management — AWS Secrets Manager

**What are secrets?**
Secrets are sensitive credentials like:
- Database passwords (Snowflake username/password)
- API keys (Salesforce API token)
- SAP connection credentials
- AWS access keys

**The problem:**
You cannot put passwords in your code or in plain text files. If your code is in GitHub and has `snowflake_password = "MyPassword123"` — that's a major security breach.

**How AWS Secrets Manager works:**
- Credentials are stored encrypted in AWS Secrets Manager
- Airflow DAGs call Secrets Manager at runtime to get credentials
- Credentials are NEVER stored in code files
- Access to Secrets Manager is controlled via IAM roles

**Example in Airflow DAG:**
```python
# Instead of hardcoding:
snowflake_conn = "user=admin, password=MyPassword123, account=tcpl"

# Airflow gets it from Secrets Manager:
snowflake_conn = get_secret("tcpl/snowflake/prod/credentials")
```

## 8.7 Skipped Airflow Jobs — The Critical Issue

**The 1-Hour Timeout Rule:**
If any Airflow task runs for **more than 1 hour**, Airflow marks it as **"skipped"** and downstream tasks are also skipped.

**Why does this happen?**
- A Glue job gets stuck due to a data quality issue with one record
- SAP extractions take longer than usual due to large data volumes
- Network issues slow down the data transfer
- The job doesn't fail outright — it just never finishes within the time limit

**The Two Failure Scenarios:**

**Scenario 1 — Glue Job Fails Before Completing:**
```
Airflow Task: run_glue_billing_job
Status: FAILED (Glue job crashed)
Result: No data loaded to S3 Processed layer
Action: Fix Glue job → Rerun the Airflow task → Data loads → Continue
```

**Scenario 2 — The Tricky Case (Glue Succeeds BUT Airflow Skips):**
```
Airflow Task: run_glue_billing_job
What happened: Glue job ACTUALLY completed successfully — data IS in S3
But: Glue took 1h 15min — Airflow already marked task as SKIPPED
Airflow sees: Task SKIPPED → downstream tasks (Snowflake load) also SKIPPED
Result: Glue data is in S3 but Snowflake never got loaded!
Airflow thinks: nothing to do (task was "skipped" not "failed")
```

**This is dangerous** because:
- The data IS in S3 (Glue succeeded)
- But Snowflake doesn't have the latest data
- ThoughtSpot shows yesterday's data but nobody knows it's stale
- Users make decisions based on outdated information

**The Solution:**
When Airflow skips downstream tasks, the support team must **manually trigger the downstream tasks** (Snowflake load, mart builds) to ensure data is complete.

This is a key L1 support task — checking daily if any skipped jobs caused downstream data gaps.

---

# 9. BI TOOLS — THOUGHTSPOT AND POWER BI

## 9.1 ThoughtSpot — The Primary BI Tool

**What ThoughtSpot does:**
Already covered in the previous document from Arnab's session. Key technical additions:

**Pre-aggregated data architecture:**
ThoughtSpot works best with pre-computed aggregate tables. The Snowflake Business-Ready layer has:
- Daily aggregates (sales by day, region, distributor)
- Weekly aggregates
- Monthly aggregates
- YTD (Year-to-Date) aggregates

These pre-computed tables make ThoughtSpot dashboards load in 1-3 seconds instead of 30-60 seconds.

**ThoughtSpot coverage:**
- **70-80% of enterprise reporting** is done via ThoughtSpot
- 3,000+ users, 2,500+ Liveboards

## 9.2 Power BI — The Financial Reporting Tool

**Why Power BI exists alongside ThoughtSpot?**
Power BI is used mainly for **financial reporting** (~20-30% of reporting). Finance teams historically used Power BI before ThoughtSpot was introduced.

**Current Status:**
- ~120 Power BI reports currently exist
- These are being **migrated to ThoughtSpot** gradually
- Finance month-end reports (used for regulatory/audit purposes) are still in Power BI

**Key Difference between Power BI and ThoughtSpot:**
| Aspect | ThoughtSpot | Power BI |
|---|---|---|
| Query style | Natural language search | Visual drag-and-drop |
| Data freshness | Near real-time (cache-based) | Scheduled refresh |
| Users | Sales, operations, marketing | Finance, executives |
| Best for | Operational analytics | Formatted financial reports |

**Deployment Freeze connection:**
The **25th to 3rd/4th monthly deployment freeze** exists primarily for Power BI financial reports. Finance runs their month-end close during this period — any platform change that breaks a financial report during month-end close is catastrophic.

## 9.3 The Migration from Power BI to ThoughtSpot

**Why migrate?**
- Reduce tool sprawl (fewer tools = simpler maintenance)
- ThoughtSpot is more self-service (users don't need IT to build new reports)
- Unified platform for all business users

**What you need to know:**
During migration, both systems need to show the **same numbers**. Your Snowflake data must be reliable and match between ThoughtSpot and Power BI. If ThoughtSpot shows ₹100 Cr revenue and Power BI shows ₹98 Cr — that's a data issue you need to investigate.

---

# 10. MACHINE LEARNING — SAGEMAKER

## 10.1 Amazon SageMaker in TCPL

**What is SageMaker?**
AWS SageMaker is Amazon's managed **Machine Learning platform**. It allows data scientists to build, train, and deploy ML models.

**TCPL's ML Usage:**
Currently there are **~4 active ML models**, mostly in the **supply chain** domain.

## 10.2 What Supply Chain ML Models Do (Likely Use Cases)

**Demand Forecasting:**
Predicting how much of each product each region will need next week/month.
- Input: Historical sales, seasonal patterns, promotions, price changes
- Output: Predicted demand per SKU per region
- Business value: TCPL can pre-position stock at CFAs before demand spikes

**Auto Replenishment Optimization:**
The ARS (Auto Replenishment System) may use ML to decide when to trigger orders.
- Input: Distributor stock levels, historical sales velocity, lead times
- Output: Optimal reorder point and order quantity
- Business value: Prevents both stock-outs and excess inventory

**Tea Blending Optimization:**
Tea blending is a complex process — mixing different tea leaves to achieve a consistent taste profile at optimal cost.
- Input: Tea leaf quality data, price, availability
- Output: Optimal blend composition
- Business value: Cost reduction while maintaining product quality

## 10.3 How SageMaker Connects to the Data Platform

```
DATA FLOW FOR ML

Snowflake (Business-Ready Layer)
          ↓ (feature data exported)
AWS S3 (ML features bucket)
          ↓
Amazon SageMaker
(Model training & inference)
          ↓ (predictions output)
AWS S3 (predictions bucket)
          ↓
Snowflake (predictions table)
          ↓
ThoughtSpot (ML predictions visible in dashboards)
```

**As a Data Engineer, your ML responsibilities:**
- Ensure the feature data (inputs to ML models) is fresh and accurate in Snowflake
- Build the pipeline that exports features from Snowflake to S3 for SageMaker
- Load SageMaker predictions back into Snowflake
- Make predictions available in ThoughtSpot for business users

---

# 11. SECURITY & GOVERNANCE LAYER

## 11.1 IAM — Identity and Access Management

**What IAM does:**
AWS IAM controls **who can do what** on AWS services.

**Key IAM concepts:**
- **Users** — individual AWS accounts (usually not used for applications)
- **Roles** — permissions assigned to services (e.g., "Glue Role" that allows Glue to read from S3 and write to Snowflake)
- **Policies** — rules about what actions are allowed

**In TCPL:**
- The Glue service has an IAM role that allows it to: read from S3, write to S3, access Secrets Manager
- Airflow has an IAM role that allows it to: trigger Glue jobs, read from S3, send SNS notifications
- No service has more permissions than it needs (principle of least privilege)

## 11.2 AWS Secrets Manager

Already covered in the Airflow section. In the architecture diagram, it sits in the "Security and Governance Layer" — it stores all credentials encrypted.

**What's stored in Secrets Manager:**
- Snowflake connection credentials
- Salesforce API tokens
- SAP connection details
- SnapLogic API keys
- Any third-party service credentials

## 11.3 SNS — Simple Notification Service

**What SNS does:**
AWS SNS is a messaging service used for sending notifications/alerts.

**In TCPL:**
- Airflow failures → SNS topic → email/SMS to support team
- Data pipeline data quality failures → SNS → Slack alert
- Critical job success → SNS → confirmation to stakeholders

## 11.4 Data Governance

**What data governance means:**
Rules and processes that ensure data is accurate, consistent, and used appropriately.

**In TCPL:**
- **DT (Digital Transformation) teams** define what each dataset means and how it should be used
- **Freshservice CR process** ensures all platform changes are documented and approved
- **CAB approvals** ensure risky changes are reviewed before deployment
- **Deployment freeze** protects critical financial periods

---

# 12. DEPLOYMENT GOVERNANCE — HOW CHANGES ARE RELEASED

## 12.1 The Full Change Management Process

Any change to the TCPL data platform — whether it's a new SnapLogic pipeline, a Glue job modification, a new Snowflake table, or a ThoughtSpot dashboard — must follow this process:

```
CHANGE MANAGEMENT FLOW

Developer makes change (in DEV environment)
           ↓
Developer tests in DEV environment
           ↓
Developer creates FRESHSERVICE CHANGE REQUEST (CR)
           ↓
CR documents:
  - What change?
  - Why needed?
  - Business impact?
  - Risk assessment?
  - Rollback plan?
           ↓
CAB REVIEW MEETING
(Technical leads + Business representatives review CR)
           ↓
CAB APPROVES ✅
           ↓
Change deployed to PRODUCTION environment
           ↓
Post-deployment verification
(Check that ThoughtSpot shows correct data after change)
           ↓
CR closed in Freshservice
```

## 12.2 Environments — DEV, UAT, PROD

**Like most enterprise platforms, TCPL has multiple environments:**

| Environment | Purpose | Who Uses It |
|---|---|---|
| **DEV (Development)** | Developers build and test new features | Data Engineers (you) |
| **UAT (User Acceptance Testing)** | Business users test and validate before production | Business analysts, power users |
| **PROD (Production)** | Live environment serving all 3,000+ users | All business users |

**The rule:** Code flows from DEV → UAT → PROD. You NEVER directly modify PROD without going through the process. This protects the live platform.

## 12.3 Deployment Freeze — The Most Important Rule

**What is the deployment freeze?**
**From the 25th of each month to the 3rd or 4th of the next month — NO changes can be deployed to production.**

**Why?**
This period is **Finance Month-End Close**. During this time:
- Finance team is closing the books for the month
- They are running final reports for management and regulatory purposes
- Everything must be stable — no surprises from platform changes
- Even a small bug in a pipeline during month-end could delay financial reporting
- This can have serious implications (regulatory penalties, management decisions made on wrong data)

**What you need to know as a data engineer:**
- If you have a change ready by the 23rd — rush to get CAB approval and deploy before the 25th
- If it's not ready by the 25th — you wait until the 4th or 5th
- Exceptions can be granted for critical production fixes — but require Sam's approval
- Plan your development and testing cycles around this freeze

---

# 13. SUPPORT MODEL — L1 AND L2

## 13.1 Overview of the Support Tiers

The platform has a **two-tier support model** to handle issues systematically.

```
ISSUE REPORTED (pipeline failure, wrong data, missing data)
           ↓
      ┌────┴────┐
      │ L1 TEAM │  ← First line of response
      └────┬────┘
           ↓
   Can L1 resolve it?
      ├── YES → Resolved, ticket closed
      └── NO  → Escalate to L2
                     ↓
               ┌─────┴─────┐
               │  L2 TEAM  │  ← Experts
               └─────┬─────┘
                     ↓
              Root cause analysis
              + Fix implemented
              + Ticket closed
```

## 13.2 L1 Support — The First Responders

**Who does L1:**
Junior engineers or dedicated support staff who monitor the platform daily.

**L1 Responsibilities:**

### Daily Monitoring
Every morning, L1 checks:
- Which Airflow DAGs ran last night?
- Did any DAG fail or get skipped?
- Are all SnapLogic pipelines green?
- Does ThoughtSpot show fresh data (yesterday's data, not 2 days old)?
- Are there any data quality alerts in Snowflake error logs?

This is done BEFORE business users start their day (usually checked by 7-8 AM).

### Job Reruns
When a job fails for a known/simple reason (e.g., temporary network issue, SAP was down for maintenance):
- L1 reruns the failed Airflow task
- Verifies the rerun succeeds
- Confirms data is now correct in Snowflake and ThoughtSpot

**How to rerun an Airflow task:**
In the Airflow UI, navigate to the failed DAG → find the failed task → right-click → "Clear" → it reruns.

### Ticket Closure
When issues are reported by business users (via Freshservice tickets):
- L1 investigates
- If it's a simple issue (stale data, UI question, wrong filter in ThoughtSpot) — resolve and close
- If it needs code fix — escalate to L2

## 13.3 L2 Support — The Experts

**Who does L2:**
Senior data engineers with deep knowledge of SnapLogic, Glue, Snowflake internals.

**L2 Responsibilities:**

### Root Cause Analysis (RCA)
When a job fails repeatedly or data is wrong in a non-obvious way:
- L2 goes deep into logs (Glue logs in CloudWatch, SnapLogic pipeline logs, Airflow task logs)
- Identifies the root cause: "The Glue job failed because SAP added a new column to VBRK that our schema doesn't have"
- Documents the RCA

### Fixes in SnapLogic
- Modify pipeline logic to handle new scenarios
- Fix connector configurations
- Update control table parameters
- Add new pipelines for new data sources

### Fixes in Glue
- Fix Python/PySpark code in Glue jobs
- Handle new edge cases in data validation
- Optimize slow Glue jobs
- Fix schema mapping issues

### Fixes in Snowflake
- Fix SQL bugs in transformation views or stored procedures
- Update business logic in Snowflake models
- Performance tuning (slow queries, missing clustering keys)
- Fix data quality issues at the Snowflake level

---

# 14. FAILURE HANDLING — WHAT HAPPENS WHEN THINGS BREAK

## 14.1 Types of Failures

### Type 1: SnapLogic Pipeline Failure
**Cause:** SAP connection timeout, API rate limit on Salesforce, credentials expired, source system downtime.

**Impact:** Raw data not landed in S3 → All downstream jobs (Glue, Snowflake) have no new data to process.

**Resolution:**
1. L1 checks SnapLogic dashboard for error message
2. If simple (network timeout) → trigger rerun in SnapLogic
3. If complex (SAP schema change, credential expiry) → escalate to L2
4. After fix → rerun SnapLogic pipeline → verify data landed in S3
5. Then trigger downstream Glue + Snowflake jobs manually if Airflow window has passed

### Type 2: Glue Job Failure
**Cause:** Data quality issue (unexpected null in non-nullable column), schema mismatch (new column in source), out-of-memory error (unexpected data volume spike), PySpark code bug.

**Impact:** Data in S3 Raw/Refined not processed → Snowflake doesn't get latest data.

**Resolution:**
1. Check Glue job logs in AWS CloudWatch
2. Identify error line
3. Fix the issue (add null handling, update schema mapping, increase Glue DPU size)
4. Redeploy fixed Glue job
5. Rerun for the failed date

### Type 3: Snowflake Transformation Failure
**Cause:** SQL error in Snowflake stored procedure, wrong join logic, division by zero in KPI calculation, Snowflake warehouse suspended/out of credits.

**Impact:** Business-Ready tables not updated → ThoughtSpot shows stale data.

**Resolution:**
1. Check Airflow task log for Snowflake error
2. Connect to Snowflake and run the failing SQL manually to see error
3. Fix SQL logic
4. Rerun Snowflake task in Airflow

### Type 4: Airflow DAG Scheduling Issue
**Cause:** Airflow scheduler down, cron expression wrong, DAG code has Python syntax error.

**Impact:** Entire pipeline not triggered → all downstream jobs don't run.

**Resolution:**
1. Check Airflow web server
2. Check scheduler logs
3. Fix DAG code if there's a syntax error
4. Manually trigger missed DAG runs

### Type 5: ThoughtSpot Stale Data
**Cause:** Snowflake has latest data but ThoughtSpot cache hasn't refreshed.

**Impact:** Users see old data even though Snowflake is updated.

**Resolution:**
Manually trigger ThoughtSpot cache refresh for affected Liveboards/Worksheets.

## 14.2 The Skipped Job Problem — In Detail

This is the trickiest failure scenario, so let's go very deep on it.

**The scenario:**

```
DAG: daily_sap_billing
Schedule: 10 PM every night
Timeout: 1 hour per task

10:00 PM → Task 1: extract_sap (SnapLogic) → STARTS
10:45 PM → Task 1: extract_sap → COMPLETES ✅ (45 min, within 1 hr)

10:45 PM → Task 2: glue_billing_job → STARTS
           (Glue job runs... and runs... and runs)
11:45 PM → Airflow: "Task 2 has been running for 1 hour — marking as SKIPPED"

BUT... in reality, at 11:48 PM, the Glue job ACTUALLY COMPLETES SUCCESSFULLY.
The data IS in S3 Processed zone.

Airflow sees: Task 2 = SKIPPED
Airflow does NOT run: Task 3, Task 4, Task 5 (all skipped)

RESULT: 
✅ Data is in S3 (Glue succeeded)
❌ Snowflake was NOT updated (Task 3 never ran)
❌ Business-Ready tables NOT refreshed (Task 4 never ran)
❌ ThoughtSpot shows yesterday's data (Task 5 never ran)
```

**Why this is a common pain point:**
The volume of data TCPL processes can vary day-to-day. On a normal Tuesday, the billing Glue job takes 45 minutes. On a month-end day (25th), there are 3x more billing documents — the same Glue job takes 1.5 hours → triggers the skip.

**L1 must check for this EVERY morning:**
```
L1 Morning Check Procedure:
1. Open Airflow UI
2. Look for any DAGs with SKIPPED tasks
3. For each SKIPPED task, check: did the underlying Glue job actually succeed?
   - Go to AWS Glue → Job runs → check the job that was running during skip window
4. If Glue job shows "Succeeded":
   - The data IS in S3
   - Manually trigger all downstream tasks in Airflow
5. If Glue job shows "Failed":
   - Data is NOT in S3
   - Fix Glue job → rerun from the Glue task
```

---

# 15. FULL ARCHITECTURE — EVERYTHING TOGETHER

## 15.1 The Complete High-Level Architecture

```
═══════════════════════════════════════════════════════════════════════════════
DATA PRODUCERS (SOURCE SYSTEMS)
═══════════════════════════════════════════════════════════════════════════════

[SAP ERP]    [Salesforce/Mavic]    [Nielsen]    [Tea Buying App]
   |                |                  |               |
[OMS]         [Salesforce]          [Files]        [MDM]
                                                  [Vendor Pay]
                                                  [MDM Tickets]

═══════════════════════════════════════════════════════════════════════════════
INGESTION LAYER
═══════════════════════════════════════════════════════════════════════════════

                    [SnapLogic — Batch Ingestion]
                    • Parameterized pipelines
                    • MySQL RDS control tables
                    • Watermark-based incremental loads
                    • Monitored via SnapLogic Dashboard

═══════════════════════════════════════════════════════════════════════════════
AWS CLOUD — DATA LAKE (S3)
═══════════════════════════════════════════════════════════════════════════════

[RAW LAYER]          [REFINED/CURATED]       [PROCESSED LAYER]
• Exact copy         • Basic cleaning         • Fully transformed
  of source          • Schema fixes           • Business logic
• No changes         • Type casting           • Partitioned
• Long retention     • Deduplication          • Ready for Snowflake

          [ARCHIVAL — AWS GLACIER]
          • Old data cold storage

═══════════════════════════════════════════════════════════════════════════════
TRANSFORMATION LAYER
═══════════════════════════════════════════════════════════════════════════════

                    [AWS Glue — PySpark ETL Jobs]
                    • Reads from S3 Raw/Refined
                    • Applies all transformations
                    • Writes to S3 Processed
                    • Loads into Snowflake Core

═══════════════════════════════════════════════════════════════════════════════
ORCHESTRATION LAYER
═══════════════════════════════════════════════════════════════════════════════

                    [Apache Airflow — 240 DAGs]
                    • Schedules all pipelines
                    • Manages dependencies
                    • Triggers SnapLogic, Glue, Snowflake
                    • Failure notifications via SNS
                    • Credentials via AWS Secrets Manager

═══════════════════════════════════════════════════════════════════════════════
DATA WAREHOUSE — SNOWFLAKE
═══════════════════════════════════════════════════════════════════════════════

[SOURCE SYSTEM DBs]  →  [CORE LAYER]  →  [BUSINESS-READY LAYER]
SAP DB                   CORE_SALES        MART_SALES (Facts + Dims + KPIs)
SALESFORCE DB            CORE_FINANCE      MART_FINANCE
NIELSEN DB               CORE_SUPPLY       MART_SUPPLY_CHAIN
etc.                     CORE_HR           MART_HR

═══════════════════════════════════════════════════════════════════════════════
ML LAYER
═══════════════════════════════════════════════════════════════════════════════

            [Amazon SageMaker — ~4 Active Models]
            • Demand forecasting
            • Tea blending optimization
            • Supply chain models

═══════════════════════════════════════════════════════════════════════════════
VISUALIZATION / CONSUMPTION LAYER
═══════════════════════════════════════════════════════════════════════════════

[ThoughtSpot]              [Power BI]
70-80% reporting           ~120 finance reports
3,000+ users               (migrating to TS)
2,500+ Liveboards          Month-end reports

═══════════════════════════════════════════════════════════════════════════════
DATA CONSUMERS
═══════════════════════════════════════════════════════════════════════════════

Sales App   |   Tea Buying App   |   CFA Centralization   |   MDM
Auto Re-ordering System   |   Tea Blending Optimization

═══════════════════════════════════════════════════════════════════════════════
SECURITY & GOVERNANCE
═══════════════════════════════════════════════════════════════════════════════

[IAM — Access Control]    [Secrets Manager]    [SNS — Notifications]
[Freshservice — ITSM]     [CAB Process]        [Deployment Freeze]

═══════════════════════════════════════════════════════════════════════════════
```

---

# 16. DATA ENGINEER DAY-IN-THE-LIFE — HOW YOU FIT IN

## 16.1 Your Daily Routine (L1 Days)

On days when you're doing L1 support:

**7:00 AM — Morning Check**
1. Open Airflow UI
2. Review all DAG runs from last night
3. Check for failures, skips, unexpected long runtimes
4. For skipped tasks: check Glue logs to determine if data is actually in S3
5. Trigger any necessary reruns
6. Check SnapLogic dashboard for pipeline statuses
7. Verify ThoughtSpot shows today's data (yesterday's transactions visible)
8. Update L1 monitoring log

**9:00 AM — Ticket Review**
- Check Freshservice for new tickets from business users
- Investigate reported data issues
- Resolve L1-level issues (reruns, access issues, basic questions)
- Escalate L2 issues with proper documentation

**Rest of Day**
- Monitor for any daytime failures (some pipelines run during business hours)
- Support business users with data questions
- Document any new issues found

## 16.2 Your Development Work (Development Days)

On development days:

**Always start in DEV environment:**
1. Get requirements from Arnab/Aritra/Shalvi (domain owners)
2. Build in DEV (new SnapLogic pipeline / new Glue job / new Snowflake model)
3. Test thoroughly in DEV
4. Raise Freshservice CR with full documentation
5. Get CAB approval
6. Deploy to PROD (outside freeze window)
7. Verify in PROD after deployment

## 16.3 Key Technologies You Need to Master

| Technology | What You Need to Know |
|---|---|
| **SnapLogic** | Understanding pipeline structure, control table, monitoring dashboard, how to trigger/rerun pipelines |
| **AWS S3** | S3 paths and partitioning, how to navigate S3 console, checking if files are present |
| **AWS Glue** | How to read Glue job logs in CloudWatch, how to check job run history, basic PySpark transformations |
| **Apache Airflow** | DAG structure, task states (success/failed/skipped), how to rerun tasks, understanding DAG dependencies |
| **Snowflake** | SQL (joins, aggregations, window functions), understanding Core vs Business-Ready layers, COPY INTO, creating/modifying tables and views |
| **ThoughtSpot** | How to check data freshness, how to navigate Liveboards, understanding worksheet connections to Snowflake |
| **Freshservice** | How to create/update CRs, escalation process |
| **AWS CloudWatch** | Reading Glue job logs, setting up alerts |
| **MySQL** | Understanding the SnapLogic control table structure |

---

# 17. MASTER GLOSSARY — EVERY TECHNICAL TERM EXPLAINED

| Term | Full Form | What It Means |
|---|---|---|
| **TCPL** | Tata Consumer Products Limited | Your client |
| **Decision Point** | — | Your company (the vendor) |
| **LatentView** | — | Another analytics vendor working on TCPL |
| **DT Team** | Digital Transformation Team | TCPL's internal team managing each domain's data requirements |
| **SAP** | Systems, Applications & Products | TCPL's main ERP system |
| **SFDC** | Salesforce.com | Salesforce platform (on which Mavic is built) |
| **Mavic** | — | TCPL's Salesforce-based secondary sales system |
| **Nielsen** | — | Third-party market research company |
| **MDM** | Master Data Management | System managing all master/reference data |
| **OMS** | Order Management System | Mobile app for distributor orders |
| **SnapLogic** | — | Integration/ingestion platform (iPaaS) |
| **iPaaS** | Integration Platform as a Service | Cloud-based tool connecting different systems |
| **Pipeline** | — | A sequence of steps to move/transform data from source to destination |
| **Parameterization** | — | Making pipelines reusable by passing table/config details as parameters instead of hardcoding |
| **Control Table** | — | MySQL database storing SnapLogic pipeline configuration (table names, connections, etc.) |
| **MySQL RDS** | MySQL Relational Database Service | Amazon's managed MySQL database used for the control table |
| **Watermark** | — | A marker tracking the last successfully extracted record, used for incremental loads |
| **Incremental Load** | — | Extracting only new/changed data since last run (vs Full Load = extract everything) |
| **Full Load** | — | Extracting all data from source (done once at setup or for complete refresh) |
| **Freshservice** | — | ITSM (IT Service Management) tool for Change Requests and tickets |
| **CR** | Change Request | A formal documented request to make a change to the platform |
| **CAB** | Change Advisory Board | The approval committee that reviews Change Requests |
| **S3** | Simple Storage Service | Amazon's cloud file storage — used as the data lake |
| **Data Lake** | — | A storage system holding large volumes of raw data in its native format |
| **Raw Layer** | — | First S3 zone — exact copy of source data, no changes |
| **Refined/Curated Layer** | — | Second S3 zone — basic cleaning applied |
| **Processed Layer** | — | Third S3 zone — fully transformed, ready for Snowflake |
| **AWS Glacier** | — | Amazon's archival cold storage — cheap, slow to retrieve |
| **Lifecycle Policy** | — | AWS rule that auto-moves data between storage tiers based on age |
| **Glue** | AWS Glue | Amazon's serverless ETL service |
| **ETL** | Extract, Transform, Load | The process of getting data from source, cleaning it, and loading to destination |
| **PySpark** | Python + Apache Spark | Distributed data processing framework used in Glue jobs |
| **Serverless** | — | No server management needed — cloud provider allocates compute automatically |
| **Deduplication** | — | Removing duplicate records from a dataset |
| **Schema Standardization** | — | Renaming columns from source-specific names to consistent, readable names |
| **Type Casting** | — | Converting data from one data type to another (e.g., string "20250301" to date 2025-03-01) |
| **Partitioning** | — | Organizing data files by specific columns (like date) for faster querying |
| **DPU** | Data Processing Unit | Unit of compute in Glue — more DPUs = more power = faster but more expensive |
| **Snowflake** | — | Cloud data warehouse — the central analytical database |
| **Data Warehouse** | — | Database optimized for analytical queries on large datasets |
| **Core Layer** | — | Snowflake layer with cleaned data mirroring source structure |
| **Business-Ready Layer** | — | Snowflake layer with domain-specific models (facts, dimensions, KPIs) |
| **Fact Table** | — | Large table with transactional data (sales, orders, visits) |
| **Dimension Table** | — | Reference table with descriptive data (products, distributors, geography) |
| **Pre-aggregation** | — | Pre-computing summary calculations and storing them for faster querying |
| **COPY INTO** | — | Snowflake command to load data from S3 into a Snowflake table |
| **Upsert / MERGE** | — | Insert new records AND update existing records in one operation |
| **Time Travel** | — | Snowflake feature allowing you to query data as it was at a past point in time |
| **Virtual Warehouse** | — | Snowflake's compute cluster — separate from storage |
| **Airflow** | Apache Airflow | Workflow orchestration platform — schedules and runs pipelines |
| **DAG** | Directed Acyclic Graph | Airflow's way of defining a workflow — tasks and their order/dependencies |
| **Task** | — | One step within an Airflow DAG (e.g., run_glue_job, load_to_snowflake) |
| **Scheduler** | — | Airflow component that triggers DAGs based on their schedules |
| **Cron** | — | A syntax for expressing schedules (e.g., "0 22 * * *" = 10 PM daily) |
| **Downstream task** | — | A task that comes after another task in a DAG — depends on the previous task completing |
| **Skipped task** | — | In Airflow, when a task is skipped because it exceeded the timeout |
| **SNS** | Simple Notification Service | AWS messaging service used for alerts and notifications |
| **AWS Secrets Manager** | — | AWS service for securely storing and retrieving credentials/API keys |
| **IAM** | Identity and Access Management | AWS service controlling who/what can access AWS resources |
| **IAM Role** | — | A set of permissions assigned to an AWS service (not a person) |
| **CloudWatch** | AWS CloudWatch | AWS monitoring and logging service — where Glue job logs are stored |
| **ThoughtSpot** | — | BI (Business Intelligence) tool — 70-80% of TCPL reporting |
| **Power BI** | — | Microsoft's BI tool — used mainly for financial reporting (~120 reports) |
| **Liveboard** | — | ThoughtSpot's name for a dashboard |
| **SageMaker** | Amazon SageMaker | AWS machine learning platform |
| **DEV** | Development Environment | Where you build and test new features |
| **UAT** | User Acceptance Testing | Where business users validate before production |
| **PROD** | Production Environment | Live environment used by all business users |
| **Deployment Freeze** | — | Period (25th to 3rd/4th monthly) when NO changes can be deployed to PROD |
| **L1 Support** | Level 1 Support | Daily monitoring, job reruns, simple issue resolution |
| **L2 Support** | Level 2 Support | Root cause analysis, code fixes, complex issue resolution |
| **RCA** | Root Cause Analysis | Deep investigation to find the true cause of a problem |
| **Rerun** | — | Running a failed or skipped job again |
| **Backfill** | — | Running a pipeline for historical dates that were missed or need reprocessing |
| **CFA** | Carrying and Forwarding Agent | TCPL's warehouse/dispatch partner |
| **Auto Re-ordering** | — | System that automatically creates orders based on stock levels (ARS) |

---

## Final Summary — The Complete Technical Stack at TCPL

```
Source → SnapLogic → S3 (Raw → Refined → Processed) → Glue → Snowflake (Core → Business-Ready) → ThoughtSpot / Power BI

Orchestrated by: Airflow (240 DAGs)
Secured by: IAM + Secrets Manager
Monitored via: SnapLogic Dashboard + CloudWatch + SNS alerts
Changes governed by: Freshservice CR + CAB + Deployment Freeze
Supported by: L1 (daily monitoring) + L2 (fixes)
ML on: SageMaker (supply chain models)
```

---

*This document is prepared for Decision Point data engineers working on the TCPL project.*
*Based on: TCPL Data Platform Support — High Level Overview Session by Sam Joseph*
*Classification: INTERNAL*
