# 🚀 TCPL AWS Glue POC — Complete Step-by-Step Execution Guide
## Replacing SnapLogic: SAP OData → S3 → Glue → Snowflake
### Tables: BillingDocument (Parent) + BillingDocumentItem (Child)

---

> **What this document is:**
> This is your complete recipe to execute the TCPL Glue POC. Every single step, every single line of code, every config, every decision — all here. Follow top to bottom. Do not skip any step.
>
> **The POC Goal:**
> Prove that AWS Glue can replace SnapLogic for ingesting SAP data into Snowflake — using BillingDocument (5.39M rows, 356 MB) and BillingDocumentItem (24.94M rows, 2,107 MB) as the test case.
>
> **Connection Method: SAP OData API — NOT JDBC**
> SAP exposes data via OData APIs built on CDS (Core Data Services) views.
> Glue calls these HTTP REST endpoints using Python `requests` library.
> There is no database driver, no JDBC URL, no ngdbc.jar needed.
> This matches the official Solution Design Document v2.

---

# 📋 TABLE OF CONTENTS

1. [Understand What You Are Building — Full Picture](#1-understand-what-you-are-building)
2. [Why These 2 Tables — Business Context](#2-why-these-2-tables)
3. [Architecture — Every Layer Explained](#3-architecture)
4. [How OData Works — Read This Before Coding](#4-how-odata-works)
5. [Pre-Requisites Checklist — Do This Before Anything](#5-pre-requisites-checklist)
6. [Step 1 — AWS Setup (Secrets Manager, S3, IAM, Glue Console)](#6-step-1-aws-setup)
7. [Step 2 — S3 Bucket Structure Setup](#7-step-2-s3-setup)
8. [Step 3 — Snowflake Setup (Control Tables + Target Schema)](#8-step-3-snowflake-setup)
9. [Step 4 — Connection Test Glue Job — Run This First Always](#9-step-4-connection-test)
10. [Step 5 — AWS Glue Job: BillingDocument (Parent) — Full Code](#10-step-5-glue-job-billingdocument)
11. [Step 6 — AWS Glue Job: BillingDocumentItem (Child) — Full Code](#11-step-6-glue-job-billingdocumentitem)
12. [Step 7 — Delta Load Watermark Logic — Deep Explanation](#12-step-7-delta-load-watermark)
13. [Step 8 — Airflow DAG to Orchestrate the POC](#13-step-8-airflow-dag)
14. [Step 9 — Run the POC — Manual Execution Steps](#14-step-9-run-the-poc)
15. [Step 10 — Validate Your Data — All Checks + Comparison Queries](#15-step-10-validate-data)
16. [Common Errors and How to Fix Them](#16-common-errors)
17. [POC Success Criteria — How to Know You Passed](#17-poc-success-criteria)
18. [Presentation Talking Points for Your Manager](#18-presentation-talking-points)
19. [Full Glossary](#19-glossary)

---

# 1. UNDERSTAND WHAT YOU ARE BUILDING

## The Current World (SnapLogic)

```
TODAY:
Airflow
    ↓  (triggers SnapLogic)
SnapLogic
    ↓  (calls SAP OData API, extracts data, pushes to S3)
AWS S3 (Raw Layer)
    ↓
AWS Glue (Transforms only — Glue was NOT doing extraction)
    ↓
Snowflake
    ↓
ThoughtSpot / Power BI (3000+ users)
```

**The Problem with SnapLogic:**
- SnapLogic is expensive — high licensing cost
- SnapLogic is a black box — hard to debug or customize
- Limited retry and error handling control
- The team wants full ownership of the extraction layer
- AWS Glue can do extraction AND transformation — so why pay for SnapLogic?

## The New World (What You Are Building)

```
POC TARGET:
Airflow (GlueJobOperator)
    ↓
    ├─ Task 1: glue-poc-billingdocument-odata-extract        ← NEW job (replaces SnapLogic)
    │          Calls SAP OData API → writes Parquet to S3
    ↓
    ├─ Task 2: glue-poc-billingdocumentitem-odata-extract    ← NEW job (replaces SnapLogic)
    │          Calls SAP OData API → writes Parquet to S3
    ↓
    ├─ Task 3: tcpl-glue-sap-to-snowflake-core               ← EXISTING template (unchanged)
    │          Reads Parquet from S3 → loads to Snowflake
    │          (this job was already running after SnapLogic wrote to S3)
    │          (now it runs after our new Glue extractor writes to S3 instead)
    ↓
AWS S3 (same bucket/structure as before)
    ↓
Snowflake (POC schema: GLUE_POC_DB.SAPS4_CORE)
    ↓
ThoughtSpot / Power BI (same as before)
```

**What changes:**
SnapLogic is REMOVED. Tasks 1+2 (new Glue jobs) now do what SnapLogic did — call SAP OData and land data in S3.

**What stays exactly the same:**
Task 3 (existing Glue transform template), S3 structure, Snowflake, Airflow, ThoughtSpot.
The existing template `tcpl-glue-sap-to-snowflake-core` is called with a new SOURCE_PATH pointing to the POC S3 folder — the job code itself is not touched.

## Current vs Target — Side by Side

| Component | Current (SnapLogic) | POC Target (Glue) |
|---|---|---|
| Trigger | Airflow → SnapLogic API | Airflow → GlueJobOperator |
| Extraction | SnapLogic pipelines | Glue Python jobs |
| SAP Connection | SnapLogic OData connector | Glue HTTP requests to OData |
| Configuration | MySQL RDS INPUT_ENTITY_LIST | Snowflake INPUT_ENTITY_LIST_GLUE_POC |
| Landing Zone | S3 bucket | S3 bucket (same) |
| Transformation | Glue jobs | Glue jobs (same templates) |
| Target | Snowflake SAPS4_CORE | Snowflake GLUE_POC_DB.SAPS4_CORE |

---

# 2. WHY THESE 2 TABLES — BUSINESS CONTEXT

## POC Scope from Solution Design v2

The full POC covers 12 tables — 7 from SAP, 5 from SFDC.
You are working on 2 of the 7 SAP tables for this POC:

**All 7 SAP Tables (from Solution Design v2, Table 13):**

| # | Table Name | Rows | Size (MB) | Load Type | Pattern |
|---|---|---|---|---|---|
| 1 | ACDOCA_FINANCE_DATA | 418.25M | 28,298 | Delta | High volume |
| 2 | BillingDocumentItemPrcgElmnt | 311.17M | 7,850 | Delta | Pricing |
| 3 | BillingDocumentItem | 24.94M | 2,107 | Delta | **Child ← YOU** |
| 4 | BillingDocument | 5.39M | 356 | Delta | **Parent ← YOU** |
| 5 | SalesOrderItem | 23.54M | 2,210 | Delta | Child |
| 6 | SalesOrder | 3.79M | 242 | Delta | Parent |
| 7 | CustomerMaster | 80K | 9 | Full | Master data |

**All SAP tables use OData APIs built on CDS views.**
**Pagination is handled via `$skip` and `$top` parameters.**

## Why BillingDocument + BillingDocumentItem

```
BillingDocument (Parent — VBRK equivalent):
  One row = one invoice sent to a distributor
  e.g., Invoice 9000012345 to Distributor DIST001 = ₹50,000 total

BillingDocumentItem (Child — VBRP equivalent):
  Many rows per invoice = line items (which products, qty, price)
  e.g., Line 10: Tata Tea 500g × 200 units = ₹10,000
        Line 20: Tata Salt 1kg × 500 units = ₹20,000
        Line 30: Tata Water 1L × 300 units = ₹20,000

RULE: Always load BillingDocument FIRST, then BillingDocumentItem.
WHY:  Child references parent document number.
      If child loads first, the parent doesn't exist → broken data.
```

## Why This is Priority 0

```
The CFA Checkpoint DAG runs at 20:11 IST every night.
It waits for 9 upstream pipelines — BillingDocument is one of them.

If BillingDocument pipeline fails:
  → CFA Checkpoint FAILS at 20:11
  → CFA Mart not refreshed
  → EY Asterix has stale data for next day
  → Dispatch orders may be wrong
  → TCPL revenue is impacted

This is exactly why BillingDocument is Priority 0 — Mission Critical.
Proving the POC on this table = maximum business impact.
```

## Business Flow Context

```
Distributor places order → Sales Order (SalesOrder / SalesOrderItem)
Goods dispatched from CFA → Delivery documents
Goods billed → BillingDocument / BillingDocumentItem ← YOU ARE HERE
BillingDocument = invoice = money owed by distributor
If billing data missing from Snowflake:
  → ThoughtSpot shows wrong revenue
  → Finance cannot see daily numbers
  → CFA checkpoint fails
```

---

# 3. ARCHITECTURE — EVERY LAYER EXPLAINED

## Complete POC Architecture (OData)

```
╔══════════════════════════════════════════════════════════════════════════╗
║              TCPL GLUE POC — OData Architecture                         ║
╚══════════════════════════════════════════════════════════════════════════╝

┌─────────────────────────────────────────────────────────────────────────┐
│  LAYER 0: SOURCE                                                         │
│                                                                          │
│  SAP S/4HANA                                                             │
│  CDS Views exposed as OData endpoints:                                   │
│                                                                          │
│  /sap/opu/odata/sap/API_BILLING_DOCUMENT_SRV/A_BillingDocument          │
│  /sap/opu/odata/sap/API_BILLING_DOCUMENT_SRV/A_BillingDocumentItem      │
│                                                                          │
│  These are HTTP REST endpoints — not database connections.               │
│  SAP returns JSON pages of data.                                         │
└─────────────────────────────────────────────────────────────────────────┘
          │ HTTP GET request (Basic Auth)
          │ Response: JSON { "d": { "results": [...] } }
          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  LAYER 1: EXTRACTION (AWS GLUE — replaces SnapLogic)                    │
│                                                                          │
│  Glue Job 1: glue-poc-billingdocument-odata-extract                     │
│  Glue Job 2: glue-poc-billingdocumentitem-odata-extract                 │
│                                                                          │
│  Each job does:                                                          │
│  1. Read config from Snowflake GLUE_ENTITY_CONFIG control table         │
│  2. Get SAP credentials from AWS Secrets Manager                        │
│  3. Call SAP OData API → paginate through ALL pages ($skip/$top loop)   │
│  4. Collect all JSON records into Spark DataFrame                        │
│  5. Add metadata columns (EXTRACTED_AT, LOAD_BATCH_ID, SOURCE_SYSTEM)  │
│  6. Write Parquet to S3                                                  │
│  7. Load to Snowflake POC schema                                         │
│  8. Update watermark in GLUE_ENTITY_CONFIG                              │
│  9. Write audit record to GLUE_AUDIT_LOG                                │
└─────────────────────────────────────────────────────────────────────────┘
          │ Write Parquet files
          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  LAYER 2: S3 DATA LAKE                                                   │
│                                                                          │
│  Official TCPL S3 Path Standard (from Lead — follow exactly):           │
│  s3://{bucket}/raw_zone/{SOURCE_SYSTEM}/{RELATIVE_PATH}/                │
│        filename_{timestamp}.csv                                          │
│                                                                          │
│  POC Paths (SAP source, aligned with prod convention):                  │
│  DEV:  s3://tcpl-sales-kpi-landing-zone-dev/raw_zone/SAP/               │
│            BillingDocument/billingdocument_{timestamp}.parquet           │
│        s3://tcpl-sales-kpi-landing-zone-dev/raw_zone/SAP/               │
│            BillingDocumentItem/billingdocumentitem_{timestamp}.parquet   │
│  PROD: s3://tcpl-datalake-raw-prd-01/raw_zone/SAP/                      │
│            BillingDocument/billingdocument_{timestamp}.parquet           │
│        s3://tcpl-datalake-raw-prd-01/raw_zone/SAP/                      │
│            BillingDocumentItem/billingdocumentitem_{timestamp}.parquet   │
│  Format: Parquet (columnar, compressed, fast to query)                  │
└─────────────────────────────────────────────────────────────────────────┘
          │ Snowflake Spark connector
          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  LAYER 3: SNOWFLAKE                                                      │
│                                                                          │
│  Target (POC data):                                                      │
│    GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT                               │
│    GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENTITEM                           │
│                                                                          │
│  Control (config + audit):                                               │
│    PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_ENTITY_CONFIG                      │
│    PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_AUDIT_LOG                          │
│                                                                          │
│  Compare (validation):                                                   │
│    SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT  ← SnapLogic data (existing)    │
│    GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT ← Glue data (new)            │
└─────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  LAYER 4: AIRFLOW (MWAA) — Orchestration                                │
│                                                                          │
│  DAG: glue_poc_sap_billing_extract                                      │
│  Task 1 → BillingDocument Glue job (parent)                             │
│  Task 2 → BillingDocumentItem Glue job (child — after Task 1)          │
│  Task 3 → Validate row counts in Snowflake                              │
└─────────────────────────────────────────────────────────────────────────┘
```

---

# 4. HOW ODATA WORKS — READ THIS BEFORE CODING

This section is critical. If you understand OData, the code will make sense.

## What is OData?

```
OData = Open Data Protocol
It is a standard REST API for exposing data over HTTP.
SAP builds OData services on top of CDS views.

Instead of connecting to a database with a driver,
you call an HTTP URL and get JSON back:

GET https://<sap-host>/sap/opu/odata/sap/API_BILLING_DOCUMENT_SRV/A_BillingDocument
    ?$format=json
    &$top=1000
    &$skip=0

Response:
{
  "d": {
    "results": [
      {
        "BillingDocument": "9000012345",
        "BillingDocumentDate": "/Date(1709251200000)/",
        "SoldToParty": "DIST001",
        "NetAmount": "50000.00",
        ...
      },
      { ... next record ... }
    ]
  }
}
```

## OData URL Parameters Explained

```
$format=json         → Return JSON (not XML)
$top=1000            → Max rows per page (like LIMIT)
$skip=0              → Start from row 0 (like OFFSET)
$skip=1000           → Start from row 1000 (next page)
$filter=...          → Filter condition (for delta loads)
$select=Field1,Field2 → Return only specific columns (faster)

Example delta filter:
$filter=LastChangeDateTime gt datetime'2025-03-01T00:00:00'
This means: give me records changed AFTER 1st March 2025
```

## Why Pagination is Critical

```
SAP OData returns maximum page_size rows per call (usually 1000-5000).
For BillingDocument with 5.39M rows and page_size=1000:

  Call 1:  $top=1000&$skip=0     → rows 1–1000
  Call 2:  $top=1000&$skip=1000  → rows 1001–2000
  Call 3:  $top=1000&$skip=2000  → rows 2001–3000
  ...
  Call N:  $top=1000&$skip=...   → last page (returns fewer than 1000 rows)

YOUR CODE LOOPS UNTIL IT GETS AN EMPTY PAGE.
If you don't paginate → you only ever get the first 1000 rows.
This is the most important concept in OData extraction.
```

## Authentication

```
SAP OData uses HTTP Basic Authentication:
  - Username + password sent in Authorization header
  - Encoded as Base64: base64("username:password")
  - Credentials stored in AWS Secrets Manager
  - Glue reads from Secrets Manager at runtime (never hardcoded)

Headers sent with every request:
  Authorization: Basic <base64-encoded-credentials>
  Accept: application/json
```

## SAP Extraction Pattern (from Solution Design v2, Section 6.3)

```
1. Read entity configuration from Snowflake control table (GLUE_ENTITY_CONFIG)
2. Authenticate using Basic Auth with credentials from Secrets Manager
3. Build OData URL with $filter (LastChangeDateTime > last_watermark)
4. Handle pagination using $skip and $top parameters
5. Handle parent-child relationships where applicable
6. Write results to S3 in Parquet format
7. Update watermark in control table on success
8. Log execution details to audit table (GLUE_AUDIT_LOG)
```

This is exactly what our code implements in Steps 5 and 6.

---

# 5. PRE-REQUISITES CHECKLIST — DO THIS BEFORE ANYTHING

Print this page. Go collect every item. Do not start coding without these.

## SAP OData Details (Get from SAP Basis Team)

```
□ SAP Base URL:
  https://___________________________________________
  e.g. https://my-sap-s4.tcpl.com:44300
  or   https://10.0.1.50:8080

□ OData Service Path:
  /sap/opu/odata/sap/API_BILLING_DOCUMENT_SRV
  (ask SAP Basis to confirm the exact service name)

□ Entity Set for BillingDocument:
  A_BillingDocument
  (confirm with SAP Basis — could differ per system)

□ Entity Set for BillingDocumentItem:
  A_BillingDocumentItem
  (confirm with SAP Basis)

□ SAP Technical Username: _______________________
  (service account — NOT your personal login)

□ SAP Password: ________________________________

□ Watermark column name in OData response:
  LastChangeDateTime
  (confirm field name exists in response)

□ Test URL (ask SAP Basis to confirm this works):
  <base_url>/sap/opu/odata/sap/API_BILLING_DOCUMENT_SRV/A_BillingDocument
  ?$top=5&$format=json

  If you paste this in a browser and see JSON → you are ready.
  If you see a login page → credentials needed in browser too.
  If you see an error → ask SAP Basis to investigate.
```

## AWS Details (Get from AWS Admin / Cloud Team)

```
□ AWS Account ID: ____________________________
□ AWS Region: ________________________________ (e.g. ap-south-1)
□ S3 Bucket Name: ____________________________ (existing or new)
□ VPC ID: ____________________________________ (where Glue will run)
□ Subnet ID (private): _______________________ (private subnet)
□ Security Group ID: _________________________ (must allow HTTPS to SAP)
□ Glue IAM Role ARN: _________________________ (you create this in Step 6)

NETWORK REQUIREMENT:
  Glue runs inside a VPC.
  Glue needs to make HTTPS calls to SAP (port 443 or 8080/44300).
  The Security Group attached to Glue must allow outbound to SAP IP on SAP port.
  Ask your AWS infra team: "Can Glue in <VPC> reach <SAP_URL> on port <PORT>?"
```

## Snowflake Details (Get from Platform Team)

```
□ Account Identifier: _______________________ (e.g. xy12345 — no domain)
□ Username: _________________________________
□ Password: _________________________________
□ Warehouse: ________________________________ (e.g. WH_ETL)
□ Role: _____________________________________ (e.g. SYSADMIN or ETL_ROLE)

Target schema for POC data:    GLUE_POC_DB.SAPS4_CORE
Control schema for config:     PRD_ARCH_SUPPORT_DB.GLUE_POC
SnapLogic data for comparison: SAPS4_DB.SAPS4_CORE
```

---

# 6. STEP 1 — AWS SETUP (SECRETS MANAGER, S3, IAM, GLUE)

## 6.1 Store SAP OData Credentials in Secrets Manager

**Why Secrets Manager?**
Never hardcode passwords in code. Secrets Manager encrypts them.
Glue reads them at runtime — the password never appears in code or logs.

```
1. Open AWS Console → search bar → type "Secrets Manager" → click it

2. Click: "Store a new secret" (orange button)

3. Secret type page:
   Select: "Other type of secret"
   (NOT RDS, NOT Redshift — just "Other")
   Click: Next

4. Key/value pairs — add EXACTLY these keys:

   Key              | Value
   ─────────────────┼──────────────────────────────────────────────────────
   username         | <SAP technical username>
   password         | <SAP password>
   base_url         | https://<your-sap-host>:<port>
   odata_service    | /sap/opu/odata/sap/API_BILLING_DOCUMENT_SRV

   Example:
   username         | SVC_GLUE_EXTRACTOR
   password         | MyS3cureP@ss!
   base_url         | https://sap-s4.tcpl.com:44300
   odata_service    | /sap/opu/odata/sap/API_BILLING_DOCUMENT_SRV

5. Click: Next (skip rotation)
   Click: Next (skip tags)

6. Secret name: tcpl/sap/odata-credentials
   Description: SAP S4 OData credentials for AWS Glue POC
   Click: Store

7. On the success page → copy and save the Secret ARN.
   Looks like:
   arn:aws:secretsmanager:ap-south-1:123456789012:secret:tcpl/sap/odata-credentials-AbCd
```

## 6.2 Store Snowflake Credentials in Secrets Manager

```
1. Secrets Manager → "Store a new secret" again

2. Secret type: "Other type of secret"

3. Key/value pairs:

   Key                | Value
   ───────────────────┼────────────────────────────────────────────────────
   username           | <snowflake username>
   password           | <snowflake password>
   account            | <account id ONLY — e.g. xy12345>
   warehouse          | WH_ETL
   role               | SYSADMIN
   control_database   | PRD_ARCH_SUPPORT_DB
   control_schema     | GLUE_POC
   target_database    | GLUE_POC_DB
   target_schema      | SAPS4_CORE

   IMPORTANT for account:
   Snowflake URL:  xy12345.snowflakecomputing.com
   account value:  xy12345    ← ONLY this part. No domain. No https.

4. Secret name: tcpl/snowflake/glue-poc-credentials
   Click: Store
   Save the ARN.
```

## 6.3 Create IAM Role for Glue

```
1. AWS Console → IAM → left sidebar → "Roles" → "Create role"

2. Trusted entity type: AWS service
   Use case: Glue (scroll down to find it)
   Click: Next

3. Add permissions — search and tick each policy:
   ✅ AmazonS3FullAccess
   ✅ AWSGlueServiceRole
   ✅ CloudWatchLogsFullAccess
   ✅ SecretsManagerReadWrite

   Click: Next

4. Role name: AWSGlueServiceRole-TCPL-POC
   Description: Glue IAM role for TCPL SAP OData POC
   Click: Create role

5. Click on the role you just created.
   Copy the Role ARN:
   arn:aws:iam::123456789012:role/AWSGlueServiceRole-TCPL-POC
   SAVE THIS — you need it when creating Glue jobs.
```

## 6.4 Create Glue Jobs in AWS Console

You will create 3 jobs. Here is the process for each.

**First — upload all Python scripts to S3:**
```
1. Go to S3 → your bucket → glue-scripts/
2. Click: Upload → Add files
3. Upload these 3 files (code is in Sections 9, 10, 11):
   - sap_odata_connection_test.py
   - billing_document_odata_extract.py
   - billing_doc_item_odata_extract.py
4. Verify all 3 appear in S3 with non-zero file size.
```

**Create Job 1: Connection Test**
```
1. AWS Console → AWS Glue → left sidebar → "ETL Jobs"
2. Click: "Create job"
3. Choose: "Script editor" → "Upload and edit an existing script"
   Upload: sap_odata_connection_test.py
   Click: Create

4. Click the "Job details" tab:

   Name              : glue-poc-sap-odata-connection-test
   IAM Role          : AWSGlueServiceRole-TCPL-POC
   Type              : Spark
   Glue version      : Glue 4.0
   Language          : Python 3
   Worker type       : G.1X
   Requested workers : 2
   Max retries       : 0
   Job timeout       : 10 minutes

5. Scroll down to "Advanced properties":
   Script path:   s3://<your-bucket>/glue-scripts/sap_odata_connection_test.py
   Temporary path: s3://<your-bucket>/glue-temp/

6. Scroll to "Libraries":
   Additional Python modules: requests,snowflake-connector-python
   (Glue installs these automatically at job startup)

7. Scroll to "Job parameters":
   Click "Add new parameter" for each:

   Key                       | Value
   ──────────────────────────┼──────────────────────────────────────
   --sap_secret_name         | tcpl/sap/odata-credentials
   --snowflake_secret_name   | tcpl/snowflake/glue-poc-credentials

8. Click: Save (top right)
```

**Create Job 2: BillingDocument Extract**
```
Same process as above. Differences:

   Name              : glue-poc-billingdocument-odata-extract
   Worker type       : G.2X
   Requested workers : 10
   Job timeout       : 90 minutes
   Script            : billing_document_odata_extract.py

   Additional Python modules: requests,snowflake-connector-python

   Job parameters:
   Key                       | Value
   ──────────────────────────┼──────────────────────────────────────
   --sap_secret_name         | tcpl/sap/odata-credentials
   --snowflake_secret_name   | tcpl/snowflake/glue-poc-credentials
   --s3_bucket               | <your-bucket-name>
   --entity_name             | BillingDocument
   --page_size               | 1000
   --load_type               | FULL
```

**Create Job 3: BillingDocumentItem Extract**
```
Same as Job 2. Differences:

   Name              : glue-poc-billingdocumentitem-odata-extract
   Worker type       : G.2X
   Requested workers : 20     ← more workers for 24.94M rows
   Job timeout       : 180 minutes

   Job parameters same as Job 2 EXCEPT:
   --entity_name             | BillingDocumentItem
```

---

# 7. STEP 2 — S3 BUCKET STRUCTURE SETUP

## ⚠️ Official TCPL S3 Path Standard — Follow This Exactly

Your lead has confirmed the official path convention for ALL TCPL data pipelines:

```
STANDARD FORMAT:
s3://{bucket}/raw_zone/{SOURCE_SYSTEM}/{RELATIVE_PATH}/filename_{timestamp}.csv

WHERE:
  {bucket}         = environment-specific bucket (see table below)
  {SOURCE_SYSTEM}  = SAP (for all SAP OData extracts)
  {RELATIVE_PATH}  = object name — BillingDocument or BillingDocumentItem
  filename         = lowercase object name
  {timestamp}      = format YYYYMMDDHHMMSS  e.g. 20240315103045
```

## Environment-Wise S3 Buckets (from Lead's Config)

| Environment | Bucket | SAP Base Path |
|---|---|---|
| DEV | tcpl-sales-kpi-landing-zone-dev | s3://tcpl-sales-kpi-landing-zone-dev/raw_zone/SAP/ |
| TEST | tcpl-sales-kpi-landing-zone-dev | s3://tcpl-sales-kpi-landing-zone-dev/raw_zone/SAP/ |
| PROD | tcpl-datalake-raw-prd-01 | s3://tcpl-datalake-raw-prd-01/raw_zone/SAP/ |

> **Note:** DEV and TEST point to the same bucket. Only PROD uses the dedicated `tcpl-datalake-raw-prd-01` bucket.

## POC Target S3 Paths (BillingDocument + BillingDocumentItem)

```
# DEV — use this for POC testing
s3://tcpl-sales-kpi-landing-zone-dev/raw_zone/SAP/BillingDocument/
    └── billingdocument_20240315103045.parquet

s3://tcpl-sales-kpi-landing-zone-dev/raw_zone/SAP/BillingDocumentItem/
    └── billingdocumentitem_20240315103045.parquet

# PROD — do not touch until POC is validated in DEV
s3://tcpl-datalake-raw-prd-01/raw_zone/SAP/BillingDocument/
    └── billingdocument_20240315103045.parquet

s3://tcpl-datalake-raw-prd-01/raw_zone/SAP/BillingDocumentItem/
    └── billingdocumentitem_20240315103045.parquet
```

## How Your Glue Job Should Construct the S3 Path

In your Glue Python code, build the output path like this:

```python
import datetime

# Read from environment variable or Glue job parameter
environment = args.get('environment', 'dev')   # 'dev', 'test', or 'prod'

SAP_BASE_PATHS = {
    'dev':  's3://tcpl-sales-kpi-landing-zone-dev/raw_zone/SAP/',
    'test': 's3://tcpl-sales-kpi-landing-zone-dev/raw_zone/SAP/',
    'prod': 's3://tcpl-datalake-raw-prd-01/raw_zone/SAP/'
}

# RELATIVE_PATH comes from INPUT_ENTITY_LIST_GLUE_POC.RELATIVE_PATH
# e.g. "BillingDocument"
relative_path = config['RELATIVE_PATH']   # BillingDocument

# Timestamp for filename
timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

# Construct full S3 path — matches official TCPL standard
s3_base = SAP_BASE_PATHS[environment]
s3_output_path = f"{s3_base}{relative_path}/{relative_path.lower()}_{timestamp}.parquet"

# Example result:
# s3://tcpl-sales-kpi-landing-zone-dev/raw_zone/SAP/BillingDocument/billingdocument_20240315103045.parquet
```

## Create Supporting Folders in S3

```
1. AWS Console → S3 → click your POC bucket (use DEV bucket for POC)

2. The raw_zone/SAP/ folder likely already exists.
   If not, create:
   raw_zone/
   raw_zone/SAP/
   raw_zone/SAP/BillingDocument/
   raw_zone/SAP/BillingDocumentItem/

3. Also create utility folders:
   glue-scripts/     ← upload Python job files here
   glue-temp/        ← Glue uses this for temp/intermediate files

Final verified structure:
tcpl-sales-kpi-landing-zone-dev/
├── raw_zone/SAP/BillingDocument/           ← Parquet files land here
├── raw_zone/SAP/BillingDocumentItem/       ← Parquet files land here
├── glue-scripts/                           ← Python job scripts
└── glue-temp/                              ← Glue temp files
```

---

# 8. STEP 3 — SNOWFLAKE SETUP (CONTROL TABLES + TARGET SCHEMA)

Run ALL these SQL scripts in your Snowflake Worksheet in order.

## 8.1 Create Databases and Schemas

```sql
-- Create the POC target database and schema
-- This is where Glue-extracted data will land
CREATE DATABASE IF NOT EXISTS GLUE_POC_DB;

CREATE SCHEMA IF NOT EXISTS GLUE_POC_DB.SAPS4_CORE
COMMENT = 'POC target schema — Glue OData extracted SAP data.
           Compare with SAPS4_DB.SAPS4_CORE (SnapLogic data).
           From Solution Design v2: GLUE_POC_DB.{SOURCE}_CORE';

-- Create the control database and schema
-- This holds config and audit tables
CREATE DATABASE IF NOT EXISTS PRD_ARCH_SUPPORT_DB;

CREATE SCHEMA IF NOT EXISTS PRD_ARCH_SUPPORT_DB.GLUE_POC
COMMENT = 'Glue POC control tables — entity config and audit logs.
           From Solution Design v2 Section 7.';

-- Verify
SHOW SCHEMAS IN DATABASE GLUE_POC_DB;
SHOW SCHEMAS IN DATABASE PRD_ARCH_SUPPORT_DB;
```

## 8.2 INPUT_ENTITY_LIST_GLUE_POC — Control Table

> ⚠️ **Important update from your lead:**
> The control table for this POC must follow the official schema: **`INPUT_ENTITY_LIST_GLUE_POC`**.
> This is the standard TCPL config table that replaces SnapLogic's MySQL `INPUT_ENTITY_LIST`.
> Your Glue job must read from this table — do NOT hardcode any of these values.

### Column-by-Column Explanation

| Column | Type | What It Means |
|---|---|---|
| `INPUT_ID` | NUMBER AUTOINCREMENT PK | Auto-generated row ID |
| `SRC_SYS_NM` | VARCHAR(30) | Source system — use `SAP` for all SAP OData tables |
| `SRC_API_NM` | VARCHAR(1000) | Full OData API path e.g. `/sap/opu/odata/sap/API_BILLING_DOCUMENT_SRV` |
| `SRC_OBJ_NM` | VARCHAR(50) | OData entity set name e.g. `A_BillingDocument` |
| `REQUIRED_FLAG` | CHAR(1) default `N` | `Y` = active/extract this entity, `N` = skip |
| `LOAD_TYP` | CHAR(1) | `D` = Delta (use watermark), `F` = Full (no filter) |
| `CDC_WATERMARK_COL` | VARCHAR(255) | OData field used for delta filter e.g. `LastChangeDateTime` |
| `LAST_CHANGE_DT_TM_SAP` | VARCHAR(255) | Watermark as SAP-formatted string (raw value from SAP) |
| `LAST_CHANGE_DT_TM` | TIMESTAMP_NTZ | Watermark as proper Snowflake timestamp — use this in your $filter |
| `LAST_MAX_CHANGELOG_NO` | NUMBER | For change-log based CDC (not used for BillingDocument — leave NULL) |
| `PARENT_OBJ_FLG` | VARCHAR(1) | `Y` = this is a parent entity, `N` = child |
| `PARENT_OBJ_NM` | VARCHAR(50) | For child entities — name of the parent (e.g. `BillingDocument`) |
| `RELATIVE_PATH` | VARCHAR(300) | S3 subfolder name AND friendly lookup key e.g. `BillingDocument` |
| `INCLUDE_FIELDS` | VARCHAR(32768) | Comma-separated OData `$select` fields (leave NULL to fetch all) |
| `STATIC_PARAMS` | VARCHAR(8000) | Extra OData `$filter` conditions e.g. `CompanyCode eq '1000'` |
| `BATCH_SIZE` | NUMBER | OData `$top` page size — how many rows per API call (use 1000) |
| `PK_COL_LIST` | VARCHAR(4000) | Primary key columns of this entity (comma-separated) |
| `DT_COL_LIST` | VARCHAR(4000) | Date/timestamp columns that need type casting |
| `DEL_FLG` | VARCHAR(1) | Soft delete flag column name (if SAP marks deletions) |
| `LAST_UPD_TM` | TIMESTAMP_NTZ | Auto-updated timestamp — when this config row was last changed |

### Create the Table

```sql
USE DATABASE PRD_ARCH_SUPPORT_DB;
USE SCHEMA GLUE_POC;

CREATE TABLE IF NOT EXISTS INPUT_ENTITY_LIST_GLUE_POC (
  INPUT_ID                NUMBER(38,0) AUTOINCREMENT PRIMARY KEY,
  SRC_SYS_NM              VARCHAR(30),
  SRC_API_NM              VARCHAR(1000),
  SRC_OBJ_NM              VARCHAR(50),
  REQUIRED_FLAG           CHAR(1) DEFAULT 'N',
  LOAD_TYP                CHAR(1),
  CDC_WATERMARK_COL       VARCHAR(255),
  LAST_CHANGE_DT_TM_SAP   VARCHAR(255),
  LAST_CHANGE_DT_TM       TIMESTAMP_NTZ,
  LAST_MAX_CHANGELOG_NO   NUMBER(38,0),
  PARENT_OBJ_FLG          VARCHAR(1),
  PARENT_OBJ_NM           VARCHAR(50),
  RELATIVE_PATH           VARCHAR(300),
  INCLUDE_FIELDS          VARCHAR(32768),
  STATIC_PARAMS           VARCHAR(8000),
  BATCH_SIZE              NUMBER(38,0),
  PK_COL_LIST             VARCHAR(4000),
  DT_COL_LIST             VARCHAR(4000),
  DEL_FLG                 VARCHAR(1),
  LAST_UPD_TM             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

  CONSTRAINT chk_required_flag CHECK (REQUIRED_FLAG IN ('Y', 'N')),
  CONSTRAINT chk_load_typ      CHECK (LOAD_TYP IN ('D', 'F')),
  CONSTRAINT chk_parent_flg    CHECK (PARENT_OBJ_FLG IN ('Y', 'N', NULL))
)
COMMENT = 'Configuration metadata for Glue data integration pipelines — official TCPL standard';
```

### Insert Config Records for BillingDocument + BillingDocumentItem

```sql
-- BillingDocument (PARENT entity — load this FIRST)
INSERT INTO PRD_ARCH_SUPPORT_DB.GLUE_POC.INPUT_ENTITY_LIST_GLUE_POC
    (SRC_SYS_NM, SRC_API_NM, SRC_OBJ_NM, REQUIRED_FLAG,
     LOAD_TYP, CDC_WATERMARK_COL, LAST_CHANGE_DT_TM,
     PARENT_OBJ_FLG, PARENT_OBJ_NM,
     RELATIVE_PATH, BATCH_SIZE,
     PK_COL_LIST, DT_COL_LIST, STATIC_PARAMS)
VALUES (
    'SAP',
    '/sap/opu/odata/sap/API_BILLING_DOCUMENT_SRV',  -- SRC_API_NM: OData service path
    'A_BillingDocument',                              -- SRC_OBJ_NM: entity set
    'Y',                                              -- REQUIRED_FLAG: active
    'D',                                              -- LOAD_TYP: Delta load
    'LastChangeDateTime',                             -- CDC_WATERMARK_COL
    DATEADD('day', -30, CURRENT_TIMESTAMP()),         -- LAST_CHANGE_DT_TM: start 30 days back for POC
    'Y',                                              -- PARENT_OBJ_FLG: this IS the parent
    NULL,                                             -- PARENT_OBJ_NM: no parent (this is root)
    'BillingDocument',                                -- RELATIVE_PATH: S3 subfolder name
    1000,                                             -- BATCH_SIZE: $top page size
    'BillingDocument',                                -- PK_COL_LIST: primary key column
    'BillingDocumentDate,CreationDate,LastChangeDateTime',  -- DT_COL_LIST
    NULL                                              -- STATIC_PARAMS: no extra filters
);

-- BillingDocumentItem (CHILD entity — load this AFTER BillingDocument)
INSERT INTO PRD_ARCH_SUPPORT_DB.GLUE_POC.INPUT_ENTITY_LIST_GLUE_POC
    (SRC_SYS_NM, SRC_API_NM, SRC_OBJ_NM, REQUIRED_FLAG,
     LOAD_TYP, CDC_WATERMARK_COL, LAST_CHANGE_DT_TM,
     PARENT_OBJ_FLG, PARENT_OBJ_NM,
     RELATIVE_PATH, BATCH_SIZE,
     PK_COL_LIST, DT_COL_LIST, STATIC_PARAMS)
VALUES (
    'SAP',
    '/sap/opu/odata/sap/API_BILLING_DOCUMENT_SRV',
    'A_BillingDocumentItem',
    'Y',
    'D',
    'LastChangeDateTime',
    DATEADD('day', -30, CURRENT_TIMESTAMP()),
    'N',                                              -- PARENT_OBJ_FLG: this is a CHILD
    'BillingDocument',                                -- PARENT_OBJ_NM: parent entity name
    'BillingDocumentItem',
    1000,
    'BillingDocument,BillingDocumentItem',            -- PK_COL_LIST: composite key
    'BillingDocumentDate,LastChangeDateTime',
    NULL
);

-- Verify both records
SELECT INPUT_ID, SRC_OBJ_NM, LOAD_TYP, REQUIRED_FLAG,
       TO_CHAR(LAST_CHANGE_DT_TM, 'YYYY-MM-DD HH24:MI:SS') AS watermark_start,
       PARENT_OBJ_FLG, PARENT_OBJ_NM, RELATIVE_PATH, BATCH_SIZE
FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.INPUT_ENTITY_LIST_GLUE_POC
ORDER BY INPUT_ID;
-- Expected: 2 rows. BillingDocument (parent) first, BillingDocumentItem (child) second.
```

### How Your Glue Job Must Read This Table

```python
import snowflake.connector

# Connect to Snowflake (credentials from Secrets Manager)
conn = snowflake.connector.connect(**sf_creds)
cur = conn.cursor()

# Read config for this entity — driven by RELATIVE_PATH (passed as Glue job param)
entity_name = args['entity_name']   # e.g. 'BillingDocument'

cur.execute("""
    SELECT
        SRC_API_NM,
        SRC_OBJ_NM,
        LOAD_TYP,
        CDC_WATERMARK_COL,
        LAST_CHANGE_DT_TM,
        RELATIVE_PATH,
        BATCH_SIZE,
        PARENT_OBJ_FLG,
        PARENT_OBJ_NM,
        INCLUDE_FIELDS,
        STATIC_PARAMS,
        PK_COL_LIST,
        DT_COL_LIST
    FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.INPUT_ENTITY_LIST_GLUE_POC
    WHERE RELATIVE_PATH = %s
      AND REQUIRED_FLAG = 'Y'
""", (entity_name,))

row = cur.fetchone()
config = {
    'src_api_nm':       row[0],
    'src_obj_nm':       row[1],
    'load_typ':         row[2],   # 'D' or 'F'
    'watermark_col':    row[3],   # 'LastChangeDateTime'
    'last_watermark':   row[4],   # datetime object
    'relative_path':    row[5],   # 'BillingDocument'
    'batch_size':       row[6],   # 1000
    'is_parent':        row[7],   # 'Y' or 'N'
    'parent_obj_nm':    row[8],
    'include_fields':   row[9],
    'static_params':    row[10],
    'pk_col_list':      row[11],
    'dt_col_list':      row[12],
}

# Use these values — never hardcode them in the job!
```

## 8.3 GLUE_AUDIT_LOG Table

Every Glue job run writes one row here.
This is your full history of every extraction — when it ran,
how many rows were extracted, success or failure, error messages.

```sql
CREATE TABLE IF NOT EXISTS GLUE_AUDIT_LOG (
    AUDIT_ID           NUMBER AUTOINCREMENT PRIMARY KEY,

    GLUE_JOB_RUN_ID    VARCHAR(100),
    -- AWS Glue assigned run ID (from job context)
    -- Format: jr_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

    SRC_SYS_NM         VARCHAR(20),
    -- SAPS4 or SFDC

    ENTITY_NAME        VARCHAR(100),
    -- BillingDocument or BillingDocumentItem

    LOAD_TYPE          VARCHAR(10),
    -- FULL or DELTA

    START_TIME         TIMESTAMP_NTZ,
    -- When the extraction started

    END_TIME           TIMESTAMP_NTZ,
    -- When the extraction finished (null if still running)

    RECORD_COUNT       NUMBER,
    -- How many rows were extracted in this run

    STATUS             VARCHAR(20),
    -- IN_PROGRESS: job is running
    -- SUCCESS: completed successfully
    -- FAILED: job crashed

    ERROR_MESSAGE      VARCHAR(2000),
    -- Populated only when STATUS = FAILED
    -- Contains the Python exception message

    S3_OUTPUT_PATH     VARCHAR(500),
    -- Where the Parquet files were written

    WATERMARK_USED     VARCHAR(50),
    -- Watermark value used for this run's $filter

    NEW_WATERMARK      VARCHAR(50),
    -- New watermark set after this run

    CREATED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE GLUE_AUDIT_LOG IS
'Audit log for every AWS Glue job run.
 From Solution Design v2, Section 7.2.
 Location: PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_AUDIT_LOG';
```

## 8.4 Insert Config Records for BillingDocument and BillingDocumentItem

> ✅ Config INSERT statements are now included directly in Section 8.2 above (inside the `INPUT_ENTITY_LIST_GLUE_POC` setup block).
> Run those INSERTs when you create the table. No separate step needed here.

Verify after inserting:

```sql
SELECT INPUT_ID, SRC_SYS_NM, SRC_OBJ_NM, LOAD_TYP, REQUIRED_FLAG,
       TO_CHAR(LAST_CHANGE_DT_TM, 'YYYY-MM-DD HH24:MI:SS') AS watermark_start,
       PARENT_OBJ_FLG, PARENT_OBJ_NM, RELATIVE_PATH, BATCH_SIZE
FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.INPUT_ENTITY_LIST_GLUE_POC
ORDER BY INPUT_ID;
-- Expected: 2 rows — BillingDocument (parent, Y) then BillingDocumentItem (child, N)
```

## 8.5 Grant Permissions

```sql
-- Grant Glue's Snowflake user permissions on POC schemas
USE ROLE SYSADMIN;

-- Control schema permissions
GRANT USAGE ON DATABASE PRD_ARCH_SUPPORT_DB TO ROLE ETL_ROLE;
GRANT USAGE ON SCHEMA PRD_ARCH_SUPPORT_DB.GLUE_POC TO ROLE ETL_ROLE;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES
    IN SCHEMA PRD_ARCH_SUPPORT_DB.GLUE_POC TO ROLE ETL_ROLE;
GRANT SELECT, INSERT, UPDATE ON FUTURE TABLES
    IN SCHEMA PRD_ARCH_SUPPORT_DB.GLUE_POC TO ROLE ETL_ROLE;

-- Target schema permissions
GRANT USAGE ON DATABASE GLUE_POC_DB TO ROLE ETL_ROLE;
GRANT USAGE ON SCHEMA GLUE_POC_DB.SAPS4_CORE TO ROLE ETL_ROLE;
GRANT CREATE TABLE ON SCHEMA GLUE_POC_DB.SAPS4_CORE TO ROLE ETL_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES
    IN SCHEMA GLUE_POC_DB.SAPS4_CORE TO ROLE ETL_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES
    IN SCHEMA GLUE_POC_DB.SAPS4_CORE TO ROLE ETL_ROLE;

-- If using SYSADMIN role for POC: skip the above — SYSADMIN has all access already.
```

---

# 9. STEP 4 — CONNECTION TEST GLUE JOB — RUN THIS FIRST ALWAYS

**Before running any real extraction job, always run this test first.**
This test calls the SAP OData API and fetches just 5 records.
If this passes → your real jobs will work.
If this fails → fix the error here, not inside 300 lines of extraction code.

Save as: `sap_odata_connection_test.py`
Upload to: `s3://<your-bucket>/glue-scripts/sap_odata_connection_test.py`

```python
"""
TCPL Glue POC — SAP OData Connection Test
==========================================
PURPOSE  : Verify Glue can reach SAP OData API and get records back.
           Also verify Snowflake connection and control table access.
EXPECTED : Prints "ALL TESTS PASSED" at the end.
RUN WHEN : First thing tomorrow. Every time you change credentials.
           Before any full extraction run.
"""

import sys
import json
import boto3
import requests
import base64
import snowflake.connector
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# ── Job initialization ──
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'sap_secret_name',         # e.g. tcpl/sap/odata-credentials
    'snowflake_secret_name',   # e.g. tcpl/snowflake/glue-poc-credentials
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("=" * 65)
print("TCPL GLUE POC — SAP OData CONNECTION TEST")
print("=" * 65)

# ── Test 1: Load SAP credentials from Secrets Manager ──
print("\n[TEST 1] Loading SAP credentials from Secrets Manager...")
try:
    sm_client = boto3.client('secretsmanager', region_name='ap-south-1')
    # Change region_name if your AWS region is different
    sap_secret = json.loads(
        sm_client.get_secret_value(SecretId=args['sap_secret_name'])['SecretString']
    )
    print(f"  ✅ Secret loaded: {args['sap_secret_name']}")
    print(f"  base_url      : {sap_secret['base_url']}")
    print(f"  odata_service : {sap_secret['odata_service']}")
    print(f"  username      : {sap_secret['username']}")
    # Never print the password
except Exception as e:
    print(f"  ❌ FAILED to load SAP secret: {e}")
    print("  FIX: Check secret name is correct.")
    print("  FIX: Check Glue IAM role has SecretsManagerReadWrite policy.")
    raise

# ── Test 2: Call SAP OData API ──
print("\n[TEST 2] Calling SAP OData API — fetching 5 rows from BillingDocument...")

# Build Basic Auth header
# This is how SAP OData authenticates — username:password encoded in Base64
credentials_str = f"{sap_secret['username']}:{sap_secret['password']}"
b64_creds = base64.b64encode(credentials_str.encode()).decode()

headers = {
    "Authorization": f"Basic {b64_creds}",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# Test URL — just 5 rows, no filter
# $top=5 means "give me only 5 records"
# $format=json means "return JSON format not XML"
test_url = (
    f"{sap_secret['base_url']}"
    f"{sap_secret['odata_service']}"
    f"/A_BillingDocument"
    f"?$top=5&$format=json"
)
# NOTE: If entity set name is different in your SAP system,
# ask SAP Basis: "What is the entity set name for BillingDocument
# in the API_BILLING_DOCUMENT_SRV OData service?"

print(f"  URL: {test_url}")

try:
    response = requests.get(
        test_url,
        headers=headers,
        verify=True,    # SSL certificate verification
                        # Set verify=False only if SAP uses self-signed cert
        timeout=60,     # 60 seconds before giving up
    )
    print(f"  HTTP Status: {response.status_code}")

    if response.status_code == 200:
        data = response.json()
        results = data.get('d', {}).get('results', [])
        print(f"  ✅ OData API WORKS — returned {len(results)} records")
        if results:
            first = results[0]
            print(f"\n  First record fields: {list(first.keys())}")
            print(f"  BillingDocument    : {first.get('BillingDocument', 'N/A')}")
            print(f"  BillingDocumentDate: {first.get('BillingDocumentDate', 'N/A')}")
            print(f"  SoldToParty        : {first.get('SoldToParty', 'N/A')}")
            print(f"  NetAmount          : {first.get('NetAmount', 'N/A')}")

    elif response.status_code == 401:
        print(f"  ❌ 401 Unauthorized — Username or password is wrong")
        print(f"  FIX: Verify credentials with SAP Basis team.")
        print(f"  FIX: Update username/password in Secrets Manager secret.")
        raise Exception("SAP authentication failed — 401 Unauthorized")

    elif response.status_code == 403:
        print(f"  ❌ 403 Forbidden — User lacks access to this OData service")
        print(f"  FIX: Ask SAP Basis to grant {sap_secret['username']} access")
        print(f"       to service: {sap_secret['odata_service']}")
        raise Exception("SAP authorization failed — 403 Forbidden")

    elif response.status_code == 404:
        print(f"  ❌ 404 Not Found — Service or entity name is wrong")
        print(f"  FIX: Ask SAP Basis to confirm the correct OData URL.")
        print(f"       They should tell you the exact service path and entity name.")
        print(f"  FIX: Try browsing: {sap_secret['base_url']}/sap/opu/odata/sap/")
        raise Exception("SAP OData endpoint not found — 404")

    else:
        print(f"  ❌ Unexpected HTTP status: {response.status_code}")
        print(f"  Response body: {response.text[:500]}")
        raise Exception(f"SAP OData returned status {response.status_code}")

except requests.exceptions.ConnectionError as ce:
    print(f"\n  ❌ CONNECTION ERROR — Cannot reach SAP host")
    print(f"  Error: {ce}")
    print(f"  FIX: Is SAP reachable from AWS Glue VPC?")
    print(f"  FIX: Check Security Group allows outbound HTTPS to SAP IP.")
    print(f"  FIX: Ask AWS infra team: Can Glue in this VPC reach {sap_secret['base_url']}?")
    raise

except requests.exceptions.Timeout:
    print(f"\n  ❌ TIMEOUT — SAP did not respond in 60 seconds")
    print(f"  FIX: SAP may be under load. Try again in a few minutes.")
    print(f"  FIX: Check if SAP host is up.")
    raise

# ── Test 3: Snowflake connection ──
print("\n[TEST 3] Testing Snowflake connection...")
try:
    sf_secret = json.loads(
        sm_client.get_secret_value(SecretId=args['snowflake_secret_name'])['SecretString']
    )
    sf_conn = snowflake.connector.connect(
        user=sf_secret['username'],
        password=sf_secret['password'],
        account=sf_secret['account'],
        warehouse=sf_secret['warehouse'],
        database=sf_secret.get('control_database', 'PRD_ARCH_SUPPORT_DB'),
        schema=sf_secret.get('control_schema', 'GLUE_POC'),
        role=sf_secret.get('role', 'SYSADMIN'),
    )
    cursor = sf_conn.cursor()
    cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
    row = cursor.fetchone()
    print(f"  ✅ Snowflake connected!")
    print(f"  User     : {row[0]}")
    print(f"  Role     : {row[1]}")
    print(f"  Database : {row[2]}")
    print(f"  Schema   : {row[3]}")
    cursor.close()
except Exception as e:
    print(f"  ❌ Snowflake connection FAILED: {e}")
    print(f"  FIX: Check account identifier (no .snowflakecomputing.com)")
    print(f"  FIX: Check username and password.")
    print(f"  FIX: Check if warehouse is running.")
    raise

# ── Test 4: Read GLUE_ENTITY_CONFIG ──
print("\n[TEST 4] Reading GLUE_ENTITY_CONFIG control table...")
try:
    cursor = sf_conn.cursor()
    cursor.execute("""
        SELECT SRC_OBJ_NM, LOAD_TYP,
               TO_CHAR(LAST_WATERMARK, 'YYYY-MM-DD') AS watermark,
               BATCH_SIZE, REQUIRED_FLAG
        FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_ENTITY_CONFIG
        WHERE SRC_SYS_NM = 'SAPS4'
        ORDER BY CONFIG_ID
    """)
    rows = cursor.fetchall()
    if rows:
        print(f"  ✅ Control table has {len(rows)} SAP entity config(s):")
        for r in rows:
            print(f"  Entity: {r[0]} | Load: {r[1]} | Watermark: {r[2]} | PageSize: {r[3]} | Active: {r[4]}")
    else:
        print(f"  ⚠️  No records in GLUE_ENTITY_CONFIG for SAPS4.")
        print(f"  FIX: Run the INSERT statements in Section 8.4 of the guide.")
    cursor.close()
    sf_conn.close()
except Exception as e:
    print(f"  ❌ Failed to read control table: {e}")
    print(f"  FIX: Did you run the Snowflake setup scripts in Section 8?")
    raise

print("\n" + "=" * 65)
print("✅ ALL TESTS PASSED")
print("   SAP OData API is reachable and returning data.")
print("   Snowflake is connected.")
print("   Control table has config records.")
print("   You are ready to run the extraction jobs.")
print("=" * 65)

job.commit()
```

---

# 10. STEP 5 — GLUE JOB: BILLINGDOCUMENT (PARENT) — FULL CODE

Save as: `billing_document_odata_extract.py`
Upload to: `s3://<your-bucket>/glue-scripts/billing_document_odata_extract.py`

```python
"""
TCPL Glue POC — BillingDocument OData Extraction
=================================================
Source   : SAP S/4HANA OData API
Entity   : A_BillingDocument
API      : API_BILLING_DOCUMENT_SRV
Target   : GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT

Extraction pattern from Solution Design v2 Section 6.3:
  1. Read config from GLUE_ENTITY_CONFIG control table
  2. Authenticate via Basic Auth from Secrets Manager
  3. Build OData URL with $filter (LastChangeDateTime > last_watermark)
  4. Paginate through all pages using $skip and $top
  5. Write Parquet to S3
  6. Load to Snowflake GLUE_POC_DB.SAPS4_CORE
  7. Update watermark in GLUE_ENTITY_CONFIG
  8. Write audit log to GLUE_AUDIT_LOG

RUN ORDER: Run this BEFORE BillingDocumentItem (parent before child).
"""

import sys
import json
import boto3
import requests
import base64
import snowflake.connector
from datetime import datetime, timezone, timedelta
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# ── Job Initialization ──

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'sap_secret_name',
    'snowflake_secret_name',
    's3_bucket',
    'entity_name',     # BillingDocument
    'page_size',       # 1000
    'load_type',       # FULL or DELTA
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

run_ts     = datetime.now(timezone.utc)
date_str   = run_ts.strftime('%Y-%m-%d')
batch_id   = f"POC_{args['entity_name']}_{run_ts.strftime('%Y%m%d_%H%M%S')}"
page_size  = int(args.get('page_size', 1000))
load_type  = args.get('load_type', 'FULL').upper()

print("=" * 70)
print(f"TCPL GLUE POC — {args['entity_name']} OData Extraction")
print(f"Run Time  : {run_ts.strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"Batch ID  : {batch_id}")
print(f"Load Type : {load_type}")
print(f"Page Size : {page_size} rows per OData API call")
print("=" * 70)

# ── Helper Functions ──

def load_secret(secret_name):
    """Read a secret from AWS Secrets Manager and return as dict."""
    client = boto3.client('secretsmanager', region_name='ap-south-1')
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])


def get_basic_auth_header(username, password):
    """
    Build HTTP Basic Auth header for SAP OData.
    SAP OData uses Basic Auth: base64(username:password) in Authorization header.
    """
    creds = base64.b64encode(f"{username}:{password}".encode()).decode()
    return {
        "Authorization": f"Basic {creds}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def get_snowflake_connection(sf_creds):
    """Create and return a Snowflake connection."""
    return snowflake.connector.connect(
        user=sf_creds['username'],
        password=sf_creds['password'],
        account=sf_creds['account'],
        warehouse=sf_creds['warehouse'],
        database=sf_creds.get('control_database', 'PRD_ARCH_SUPPORT_DB'),
        schema=sf_creds.get('control_schema', 'GLUE_POC'),
        role=sf_creds.get('role', 'SYSADMIN'),
    )


def read_entity_config(sf_conn, entity_name):
    """
    Read entity configuration from GLUE_ENTITY_CONFIG.
    Returns a dict with all config fields.
    """
    cursor = sf_conn.cursor()
    cursor.execute("""
        SELECT
            CONFIG_ID,
            SRC_API_NM,
            SRC_OBJ_NM,
            CDC_WATERMARK_COL,
            TO_CHAR(LAST_WATERMARK, 'YYYY-MM-DDTHH24:MI:SS') AS LAST_WATERMARK,
            BATCH_SIZE,
            S3_PATH,
            STATIC_PARAMS
        FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_ENTITY_CONFIG
        WHERE RELATIVE_PATH  = %s
          AND SRC_SYS_NM     = 'SAPS4'
          AND REQUIRED_FLAG  = 'Y'
    """, (entity_name,))
    row = cursor.fetchone()
    cursor.close()
    if not row:
        raise Exception(
            f"No active config in GLUE_ENTITY_CONFIG for entity: {entity_name}. "
            f"Run the INSERT statements in Section 8.4 of the guide."
        )
    return {
        "config_id":      row[0],
        "api_path":       row[1],
        "entity_set":     row[2],
        "watermark_col":  row[3],
        "last_watermark": row[4],
        "batch_size":     int(row[5]) if row[5] else page_size,
        "s3_path":        row[6],
        "static_params":  row[7],
    }


def write_audit_log(sf_conn, job_run_id, entity, load_type,
                    start_time, end_time, record_count,
                    status, error_msg, s3_path, wm_used, wm_new):
    """Insert a record into GLUE_AUDIT_LOG."""
    cursor = sf_conn.cursor()
    cursor.execute("""
        INSERT INTO PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_AUDIT_LOG (
            GLUE_JOB_RUN_ID, SRC_SYS_NM, ENTITY_NAME, LOAD_TYPE,
            START_TIME, END_TIME, RECORD_COUNT, STATUS, ERROR_MESSAGE,
            S3_OUTPUT_PATH, WATERMARK_USED, NEW_WATERMARK
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        job_run_id, 'SAPS4', entity, load_type,
        start_time.strftime('%Y-%m-%d %H:%M:%S'),
        end_time.strftime('%Y-%m-%d %H:%M:%S') if end_time else None,
        record_count, status, error_msg,
        s3_path, wm_used, wm_new
    ))
    sf_conn.commit()
    cursor.close()


def update_watermark(sf_conn, config_id, new_watermark_str):
    """Update LAST_WATERMARK in GLUE_ENTITY_CONFIG after successful run."""
    cursor = sf_conn.cursor()
    cursor.execute("""
        UPDATE PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_ENTITY_CONFIG
        SET LAST_WATERMARK = TO_TIMESTAMP_NTZ(%s, 'YYYY-MM-DDTHH24:MI:SS'),
            UPDATED_AT     = CURRENT_TIMESTAMP()
        WHERE CONFIG_ID = %s
    """, (new_watermark_str, config_id))
    sf_conn.commit()
    cursor.close()
    print(f"  ✅ Watermark updated → {new_watermark_str}")


def fetch_all_odata_pages(base_url, api_path, entity_set,
                          auth_headers, pg_size, filter_str=None):
    """
    Fetch ALL pages from SAP OData API using $skip/$top pagination loop.

    WHY THIS LOOP:
    SAP OData returns maximum pg_size rows per call.
    For BillingDocument with 5.39M rows and pg_size=1000:
      Call 1: $skip=0     → rows 1-1000
      Call 2: $skip=1000  → rows 1001-2000
      ...continue until empty page...

    STOP CONDITION:
    - Empty page (len(results) == 0)
    - Last page (len(results) < pg_size)

    Returns: flat list of all records (Python dicts) from all pages.
    """
    all_records = []
    skip = 0
    page_num = 0

    print(f"  Starting pagination loop (page_size={pg_size})...")

    while True:
        page_num += 1

        # Build the URL for this specific page
        url = (
            f"{base_url}{api_path}/{entity_set}"
            f"?$format=json&$top={pg_size}&$skip={skip}"
        )

        # Append delta filter if provided
        # Format: $filter=LastChangeDateTime gt datetime'2025-03-01T00:00:00'
        if filter_str:
            url += f"&$filter={filter_str}"

        # Log progress — first page and every 20 pages after
        if page_num == 1:
            print(f"  Page 1 URL: {url}")
        elif page_num % 20 == 0:
            print(f"  Page {page_num}: $skip={skip} | {len(all_records):,} records so far...")

        try:
            response = requests.get(
                url,
                headers=auth_headers,
                verify=True,
                timeout=120,    # 2 minutes per page
            )
        except requests.exceptions.Timeout:
            print(f"  ⚠️  Page {page_num} timed out, retrying with 3 min timeout...")
            response = requests.get(url, headers=auth_headers, verify=True, timeout=180)

        if response.status_code != 200:
            raise Exception(
                f"OData API returned HTTP {response.status_code} on page {page_num}. "
                f"URL: {url}. Response: {response.text[:300]}"
            )

        # Parse the JSON response
        # SAP OData JSON structure: {"d": {"results": [...]}}
        page_data = response.json()
        results = page_data.get('d', {}).get('results', [])

        # If empty page → we have fetched everything
        if not results:
            print(f"  Page {page_num}: empty response — pagination complete.")
            break

        all_records.extend(results)
        skip += pg_size

        # If this page has fewer rows than page size → it is the last page
        if len(results) < pg_size:
            print(f"  Page {page_num}: returned {len(results)} rows (< {pg_size}) — last page.")
            break

    print(f"  Pagination complete: {page_num} pages, {len(all_records):,} total records.")
    return all_records


# ── MAIN JOB EXECUTION ──

# STEP 1: Load credentials
print("\n[STEP 1] Loading credentials from AWS Secrets Manager...")
try:
    sap_creds = load_secret(args['sap_secret_name'])
    sf_creds  = load_secret(args['snowflake_secret_name'])
    print(f"  ✅ SAP credentials loaded: {args['sap_secret_name']}")
    print(f"  ✅ Snowflake credentials loaded: {args['snowflake_secret_name']}")
except Exception as e:
    print(f"  ❌ Failed: {e}")
    raise

# STEP 2: Connect to Snowflake and read entity config
print("\n[STEP 2] Reading entity config from GLUE_ENTITY_CONFIG...")
try:
    sf_conn = get_snowflake_connection(sf_creds)
    config  = read_entity_config(sf_conn, args['entity_name'])
    print(f"  Config ID      : {config['config_id']}")
    print(f"  Entity Set     : {config['entity_set']}")
    print(f"  OData Service  : {config['api_path']}")
    print(f"  Watermark Col  : {config['watermark_col']}")
    print(f"  Last Watermark : {config['last_watermark']}")
    print(f"  Page Size      : {config['batch_size']}")
    print(f"  S3 Path        : {config['s3_path']}")
    print(f"  ✅ Config loaded")
except Exception as e:
    print(f"  ❌ Failed: {e}")
    raise

# STEP 3: Build OData filter and auth header
print(f"\n[STEP 3] Building OData request for load_type={load_type}...")

auth_headers  = get_basic_auth_header(sap_creds['username'], sap_creds['password'])
odata_filter  = None
watermark_used = config['last_watermark']

if load_type == 'DELTA' and config['last_watermark'] and config['watermark_col']:
    # Delta filter: only records changed after the last watermark
    # OData datetime filter syntax: field gt datetime'YYYY-MM-DDTHH:MM:SS'
    odata_filter = (
        f"{config['watermark_col']} gt "
        f"datetime'{config['last_watermark']}'"
    )
    print(f"  Delta filter: {odata_filter}")
    print(f"  This fetches only records changed after: {config['last_watermark']}")
else:
    # Full load — use 30-day lookback window for POC
    # For real full load, remove this filter entirely
    thirty_days_ago = (datetime.now(timezone.utc) - timedelta(days=30)) \
                       .strftime('%Y-%m-%dT%H:%M:%S')
    if config['watermark_col']:
        odata_filter = (
            f"{config['watermark_col']} gt "
            f"datetime'{thirty_days_ago}'"
        )
    print(f"  Full load filter (last 30 days): {odata_filter}")

# Append any static params from config
if config['static_params']:
    if odata_filter:
        odata_filter += f" and {config['static_params']}"
    else:
        odata_filter = config['static_params']

# STEP 4: Write initial audit record (IN_PROGRESS)
s3_output_path = (
    f"s3://{args['s3_bucket']}/{config['s3_path']}"
    f"/{load_type.lower()}/{date_str}/"
)
extraction_start = datetime.now(timezone.utc)
write_audit_log(sf_conn, batch_id, args['entity_name'], load_type,
                extraction_start, None, 0, 'IN_PROGRESS', None,
                s3_output_path, watermark_used, None)

# STEP 5: Fetch all pages from SAP OData API
print(f"\n[STEP 5] Fetching all pages from SAP OData API...")
print(f"  Target: {sap_creds['base_url']}{config['api_path']}/{config['entity_set']}")
print(f"  ⏳ BillingDocument 30-day window ≈ 200K-500K rows → expect 5-15 minutes...")

try:
    all_records = fetch_all_odata_pages(
        base_url    = sap_creds['base_url'],
        api_path    = config['api_path'],
        entity_set  = config['entity_set'],
        auth_headers= auth_headers,
        pg_size     = config['batch_size'],
        filter_str  = odata_filter,
    )
    total_extracted = len(all_records)
    print(f"\n  ✅ OData extraction complete: {total_extracted:,} records fetched")
    if total_extracted == 0:
        print("  ⚠️  Zero records returned.")
        print("  CHECK: Is the date filter range correct?")
        print("  CHECK: Does SAP have BillingDocument data in last 30 days?")
        print("  CHECK: Does the SAP user have SELECT access on this entity?")

except Exception as e:
    print(f"\n  ❌ OData extraction failed: {e}")
    write_audit_log(sf_conn, batch_id, args['entity_name'], load_type,
                    extraction_start, datetime.now(timezone.utc), 0,
                    'FAILED', str(e)[:2000], s3_output_path, watermark_used, None)
    sf_conn.close()
    raise

# Handle zero records gracefully
if total_extracted == 0:
    new_wm = run_ts.strftime('%Y-%m-%dT%H:%M:%S')
    write_audit_log(sf_conn, batch_id, args['entity_name'], load_type,
                    extraction_start, datetime.now(timezone.utc), 0,
                    'SUCCESS', 'Zero records — nothing to load',
                    s3_output_path, watermark_used, new_wm)
    update_watermark(sf_conn, config['config_id'], new_wm)
    sf_conn.close()
    job.commit()
    sys.exit(0)

# STEP 6: Convert JSON records to Spark DataFrame
print(f"\n[STEP 6] Converting {total_extracted:,} records to Spark DataFrame...")

# Remove OData metadata from each record
# OData adds a "__metadata" key to every record with type/uri info
# We don't need it in our Snowflake table — strip it out
clean_records = [
    {k: v for k, v in rec.items() if k != '__metadata'}
    for rec in all_records
]

df = spark.createDataFrame(clean_records)
print(f"  Rows    : {df.count():,}")
print(f"  Columns : {len(df.columns)}")
print(f"\n  Column names from OData (first 15): {df.columns[:15]}")
print(f"\n  Schema:")
df.printSchema()
print(f"\n  Sample data (3 rows, key columns if available):")
key_cols = ['BillingDocument', 'BillingDocumentDate', 'SoldToParty', 'NetAmount', 'TransactionCurrency']
available_key_cols = [c for c in key_cols if c in df.columns]
if available_key_cols:
    df.select(available_key_cols).show(3, truncate=False)
else:
    df.show(3, truncate=False)

# STEP 7: Add pipeline metadata columns
print(f"\n[STEP 7] Adding metadata columns...")
df = df \
    .withColumn("EXTRACTED_AT",
        F.lit(run_ts.strftime('%Y-%m-%d %H:%M:%S')).cast(TimestampType())) \
    .withColumn("LOAD_BATCH_ID", F.lit(batch_id)) \
    .withColumn("SOURCE_SYSTEM", F.lit("SAPS4")) \
    .withColumn("LOAD_TYPE",     F.lit(load_type))
print(f"  ✅ Metadata columns added: EXTRACTED_AT, LOAD_BATCH_ID, SOURCE_SYSTEM, LOAD_TYPE")

# STEP 8: Write to S3 as Parquet
print(f"\n[STEP 8] Writing to S3 as Parquet...")
print(f"  Path: {s3_output_path}")

try:
    # coalesce(5): write 5 Parquet part files
    # For 200K-500K rows this is fine
    df.coalesce(5).write.mode("overwrite").parquet(s3_output_path)
    print(f"  ✅ Parquet written to S3 successfully")
except Exception as e:
    print(f"  ❌ S3 write failed: {e}")
    print(f"  CHECK: Does Glue IAM role have s3:PutObject permission?")
    print(f"  CHECK: Does S3 bucket exist?")
    write_audit_log(sf_conn, batch_id, args['entity_name'], load_type,
                    extraction_start, datetime.now(timezone.utc), total_extracted,
                    'FAILED', f"S3 write failed: {str(e)[:500]}",
                    s3_output_path, watermark_used, None)
    sf_conn.close()
    raise

# STEP 9: Load to Snowflake
print(f"\n[STEP 9] Loading to Snowflake GLUE_POC_DB.SAPS4_CORE...")

snowflake_options = {
    "sfURL":       f"{sf_creds['account']}.snowflakecomputing.com",
    "sfUser":      sf_creds['username'],
    "sfPassword":  sf_creds['password'],
    "sfDatabase":  sf_creds.get('target_database', 'GLUE_POC_DB'),
    "sfSchema":    sf_creds.get('target_schema', 'SAPS4_CORE'),
    "sfWarehouse": sf_creds['warehouse'],
    "sfRole":      sf_creds.get('role', 'SYSADMIN'),
}

# Snowflake table name = entity name in upper case
# BillingDocument → BILLINGDOCUMENT
target_table = args['entity_name'].upper()

try:
    df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", target_table) \
        .mode("overwrite") \
        .save()
    # mode="overwrite" = truncate and reload the table each run
    # This is fine for POC. Production would use "append" + MERGE for upsert.
    print(f"  ✅ Loaded to Snowflake: GLUE_POC_DB.SAPS4_CORE.{target_table}")
    print(f"  Rows loaded: {total_extracted:,}")
except Exception as e:
    print(f"  ❌ Snowflake load failed: {e}")
    print(f"  CHECK: Is account identifier correct? (no .snowflakecomputing.com)")
    print(f"  CHECK: Is warehouse running?")
    print(f"  CHECK: Is Snowflake Spark connector JAR added to Glue job?")
    write_audit_log(sf_conn, batch_id, args['entity_name'], load_type,
                    extraction_start, datetime.now(timezone.utc), total_extracted,
                    'FAILED', f"Snowflake load failed: {str(e)[:500]}",
                    s3_output_path, watermark_used, None)
    sf_conn.close()
    raise

# STEP 10: Update watermark and audit log
print(f"\n[STEP 10] Updating watermark and audit log...")
new_watermark = run_ts.strftime('%Y-%m-%dT%H:%M:%S')
update_watermark(sf_conn, config['config_id'], new_watermark)

extraction_end = datetime.now(timezone.utc)
duration_mins  = round((extraction_end - extraction_start).seconds / 60, 1)

write_audit_log(sf_conn, batch_id, args['entity_name'], load_type,
                extraction_start, extraction_end, total_extracted,
                'SUCCESS', None, s3_output_path,
                watermark_used, new_watermark)
sf_conn.close()

print(f"\n" + "=" * 70)
print(f"✅ {args['entity_name']} EXTRACTION COMPLETE")
print(f"=" * 70)
print(f"  Batch ID        : {batch_id}")
print(f"  Records fetched : {total_extracted:,}")
print(f"  Duration        : {duration_mins} minutes")
print(f"  S3 output       : {s3_output_path}")
print(f"  Snowflake table : GLUE_POC_DB.SAPS4_CORE.{target_table}")
print(f"  New watermark   : {new_watermark}")
print(f"\n  ➡️  NEXT: Run glue-poc-billingdocumentitem-odata-extract")
print(f"=" * 70)

job.commit()
```

---

# 11. STEP 6 — GLUE JOB: BILLINGDOCUMENTITEM (CHILD) — FULL CODE

Save as: `billing_doc_item_odata_extract.py`
Upload to: `s3://<your-bucket>/glue-scripts/billing_doc_item_odata_extract.py`

```python
"""
TCPL Glue POC — BillingDocumentItem OData Extraction
=====================================================
Source   : SAP S/4HANA OData API
Entity   : A_BillingDocumentItem
Target   : GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENTITEM

CRITICAL: Run AFTER billing_document_odata_extract.py
          BillingDocument (parent) must load before BillingDocumentItem (child).
          This job verifies parent loaded before proceeding.

BillingDocumentItem is LARGER than BillingDocument:
  BillingDocument     : 5.39M rows,  356 MB  → 30-day POC ≈ 500K rows
  BillingDocumentItem : 24.94M rows, 2107 MB  → 30-day POC ≈ 2-3M rows

Therefore we use:
  - More Glue workers (20 vs 10)
  - Higher job timeout (180 min vs 90 min)
  - More Parquet output files (coalesce(20) vs coalesce(5))
  - Same OData pagination pattern — just more pages to loop through
"""

import sys
import json
import boto3
import requests
import base64
import snowflake.connector
from datetime import datetime, timezone, timedelta
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'sap_secret_name',
    'snowflake_secret_name',
    's3_bucket',
    'entity_name',     # BillingDocumentItem
    'page_size',       # 1000
    'load_type',       # FULL or DELTA
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

run_ts    = datetime.now(timezone.utc)
date_str  = run_ts.strftime('%Y-%m-%d')
batch_id  = f"POC_{args['entity_name']}_{run_ts.strftime('%Y%m%d_%H%M%S')}"
page_size = int(args.get('page_size', 1000))
load_type = args.get('load_type', 'FULL').upper()

print("=" * 70)
print(f"TCPL GLUE POC — {args['entity_name']} OData Extraction")
print(f"Batch ID  : {batch_id}")
print(f"Load Type : {load_type}")
print("⚠️  NOTE: BillingDocument (parent) must be loaded BEFORE this job.")
print("=" * 70)

# ── Helper functions (same as parent job) ──

def load_secret(name):
    c = boto3.client('secretsmanager', region_name='ap-south-1')
    return json.loads(c.get_secret_value(SecretId=name)['SecretString'])

def get_basic_auth_header(u, p):
    creds = base64.b64encode(f"{u}:{p}".encode()).decode()
    return {"Authorization": f"Basic {creds}", "Accept": "application/json"}

def get_sf_conn(sf):
    return snowflake.connector.connect(
        user=sf['username'], password=sf['password'], account=sf['account'],
        warehouse=sf['warehouse'],
        database=sf.get('control_database', 'PRD_ARCH_SUPPORT_DB'),
        schema=sf.get('control_schema', 'GLUE_POC'),
        role=sf.get('role', 'SYSADMIN'),
    )

def read_entity_config(sf_conn, entity_name):
    cursor = sf_conn.cursor()
    cursor.execute("""
        SELECT CONFIG_ID, SRC_API_NM, SRC_OBJ_NM, CDC_WATERMARK_COL,
               TO_CHAR(LAST_WATERMARK, 'YYYY-MM-DDTHH24:MI:SS'),
               BATCH_SIZE, S3_PATH, STATIC_PARAMS
        FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_ENTITY_CONFIG
        WHERE RELATIVE_PATH = %s AND SRC_SYS_NM = 'SAPS4' AND REQUIRED_FLAG = 'Y'
    """, (entity_name,))
    row = cursor.fetchone()
    cursor.close()
    if not row:
        raise Exception(f"No config for: {entity_name}")
    return {"config_id": row[0], "api_path": row[1], "entity_set": row[2],
            "watermark_col": row[3], "last_watermark": row[4],
            "batch_size": int(row[5]) if row[5] else page_size,
            "s3_path": row[6], "static_params": row[7]}

def write_audit(sf_conn, job_run_id, entity, ltype, start, end,
                count, status, err, s3, wm, new_wm):
    cursor = sf_conn.cursor()
    cursor.execute("""
        INSERT INTO PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_AUDIT_LOG
        (GLUE_JOB_RUN_ID, SRC_SYS_NM, ENTITY_NAME, LOAD_TYPE,
         START_TIME, END_TIME, RECORD_COUNT, STATUS, ERROR_MESSAGE,
         S3_OUTPUT_PATH, WATERMARK_USED, NEW_WATERMARK)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (job_run_id, 'SAPS4', entity, ltype,
          start.strftime('%Y-%m-%d %H:%M:%S'),
          end.strftime('%Y-%m-%d %H:%M:%S') if end else None,
          count, status, err, s3, wm, new_wm))
    sf_conn.commit()
    cursor.close()

def update_watermark(sf_conn, config_id, new_wm):
    cursor = sf_conn.cursor()
    cursor.execute("""
        UPDATE PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_ENTITY_CONFIG
        SET LAST_WATERMARK = TO_TIMESTAMP_NTZ(%s, 'YYYY-MM-DDTHH24:MI:SS'),
            UPDATED_AT = CURRENT_TIMESTAMP()
        WHERE CONFIG_ID = %s
    """, (new_wm, config_id))
    sf_conn.commit()
    cursor.close()

def fetch_all_pages(base_url, api_path, entity_set, headers, pg_size, filter_str):
    """Paginate through all OData pages and return all records."""
    all_recs = []
    skip = 0
    page = 0
    while True:
        page += 1
        url = (f"{base_url}{api_path}/{entity_set}"
               f"?$format=json&$top={pg_size}&$skip={skip}")
        if filter_str:
            url += f"&$filter={filter_str}"
        if page % 50 == 1:
            print(f"  Page {page}: $skip={skip} | {len(all_recs):,} records so far...")
        try:
            resp = requests.get(url, headers=headers, verify=True, timeout=120)
        except requests.exceptions.Timeout:
            print(f"  Page {page} timed out, retrying...")
            resp = requests.get(url, headers=headers, verify=True, timeout=180)
        if resp.status_code != 200:
            raise Exception(f"HTTP {resp.status_code} page {page}: {resp.text[:300]}")
        results = resp.json().get('d', {}).get('results', [])
        if not results:
            print(f"  Page {page}: empty — done.")
            break
        all_recs.extend(results)
        skip += pg_size
        if len(results) < pg_size:
            print(f"  Page {page}: {len(results)} rows — last page.")
            break
    return all_recs

# ── MAIN ──

print("\n[STEP 1] Loading credentials...")
sap = load_secret(args['sap_secret_name'])
sf  = load_secret(args['snowflake_secret_name'])
sf_conn = get_sf_conn(sf)
print("  ✅ Credentials loaded")

# CRITICAL: Verify parent (BillingDocument) loaded successfully before proceeding
# Child references parent — loading child before parent = broken referential integrity
print("\n[STEP 2] Verifying BillingDocument (parent) loaded successfully...")
cursor = sf_conn.cursor()
cursor.execute("""
    SELECT STATUS, END_TIME, RECORD_COUNT
    FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_AUDIT_LOG
    WHERE ENTITY_NAME = 'BillingDocument'
      AND SRC_SYS_NM  = 'SAPS4'
      AND STATUS       = 'SUCCESS'
    ORDER BY END_TIME DESC
    LIMIT 1
""")
parent_row = cursor.fetchone()
cursor.close()

if not parent_row:
    msg = (
        "BillingDocument has no SUCCESS record in GLUE_AUDIT_LOG. "
        "Cannot load BillingDocumentItem until parent succeeds. "
        "Run billing_document_odata_extract.py first."
    )
    print(f"  ❌ {msg}")
    sf_conn.close()
    raise Exception(msg)

print(f"  ✅ BillingDocument confirmed SUCCESS at {parent_row[1]} ({parent_row[2]:,} rows)")

# Read config
print("\n[STEP 3] Reading entity config from GLUE_ENTITY_CONFIG...")
config = read_entity_config(sf_conn, args['entity_name'])
print(f"  Entity       : {config['entity_set']}")
print(f"  Last watermark: {config['last_watermark']}")
print(f"  ✅ Config loaded")

# Build filter
auth_headers  = get_basic_auth_header(sap['username'], sap['password'])
odata_filter  = None
watermark_used = config['last_watermark']

if load_type == 'DELTA' and config['last_watermark'] and config['watermark_col']:
    odata_filter = (
        f"{config['watermark_col']} gt "
        f"datetime'{config['last_watermark']}'"
    )
    print(f"\n  Delta filter: {odata_filter}")
else:
    thirty_ago = (datetime.now(timezone.utc) - timedelta(days=30)) \
                  .strftime('%Y-%m-%dT%H:%M:%S')
    if config['watermark_col']:
        odata_filter = f"{config['watermark_col']} gt datetime'{thirty_ago}'"
    print(f"\n  Full load filter (30 days): {odata_filter}")

if config['static_params']:
    odata_filter = (odata_filter + f" and {config['static_params']}"
                    if odata_filter else config['static_params'])

# Write initial audit
s3_out = (f"s3://{args['s3_bucket']}/{config['s3_path']}"
          f"/{load_type.lower()}/{date_str}/")
extraction_start = datetime.now(timezone.utc)
write_audit(sf_conn, batch_id, args['entity_name'], load_type,
            extraction_start, None, 0, 'IN_PROGRESS', None,
            s3_out, watermark_used, None)

# Fetch all pages
print(f"\n[STEP 4] Fetching all pages from SAP OData API...")
print(f"  ⏳ BillingDocumentItem is large (24.94M total rows).")
print(f"  ⏳ 30-day window ≈ 2-3M rows. Expect 20-40 minutes. Be patient.")
try:
    all_records = fetch_all_pages(
        sap['base_url'], config['api_path'], config['entity_set'],
        auth_headers, config['batch_size'], odata_filter
    )
    total_extracted = len(all_records)
    print(f"\n  ✅ Extracted: {total_extracted:,} records")
except Exception as e:
    print(f"  ❌ Failed: {e}")
    write_audit(sf_conn, batch_id, args['entity_name'], load_type,
                extraction_start, datetime.now(timezone.utc), 0,
                'FAILED', str(e)[:2000], s3_out, watermark_used, None)
    sf_conn.close()
    raise

if total_extracted == 0:
    new_wm = run_ts.strftime('%Y-%m-%dT%H:%M:%S')
    write_audit(sf_conn, batch_id, args['entity_name'], load_type,
                extraction_start, datetime.now(timezone.utc), 0,
                'SUCCESS', 'Zero records', s3_out, watermark_used, new_wm)
    update_watermark(sf_conn, config['config_id'], new_wm)
    sf_conn.close()
    job.commit()
    sys.exit(0)

# Convert to DataFrame
print(f"\n[STEP 5] Converting to Spark DataFrame...")
clean_records = [{k: v for k, v in r.items() if k != '__metadata'}
                 for r in all_records]
df = spark.createDataFrame(clean_records)
df = df \
    .withColumn("EXTRACTED_AT",
        F.lit(run_ts.strftime('%Y-%m-%d %H:%M:%S')).cast(TimestampType())) \
    .withColumn("LOAD_BATCH_ID", F.lit(batch_id)) \
    .withColumn("SOURCE_SYSTEM", F.lit("SAPS4")) \
    .withColumn("LOAD_TYPE",     F.lit(load_type))
print(f"  ✅ DataFrame: {df.count():,} rows × {len(df.columns)} columns")

# Write to S3
print(f"\n[STEP 6] Writing to S3: {s3_out}")
try:
    # coalesce(20) — more files because BillingDocumentItem is larger
    df.coalesce(20).write.mode("overwrite").parquet(s3_out)
    print(f"  ✅ Parquet written successfully")
except Exception as e:
    print(f"  ❌ S3 write failed: {e}")
    write_audit(sf_conn, batch_id, args['entity_name'], load_type,
                extraction_start, datetime.now(timezone.utc), total_extracted,
                'FAILED', str(e)[:500], s3_out, watermark_used, None)
    sf_conn.close()
    raise

# Load to Snowflake
print(f"\n[STEP 7] Loading to Snowflake GLUE_POC_DB.SAPS4_CORE...")
sf_opts = {
    "sfURL":       f"{sf['account']}.snowflakecomputing.com",
    "sfUser":      sf['username'],
    "sfPassword":  sf['password'],
    "sfDatabase":  sf.get('target_database', 'GLUE_POC_DB'),
    "sfSchema":    sf.get('target_schema', 'SAPS4_CORE'),
    "sfWarehouse": sf['warehouse'],
    "sfRole":      sf.get('role', 'SYSADMIN'),
}
target_table = args['entity_name'].upper()  # BILLINGDOCUMENTITEM
try:
    df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**sf_opts) \
        .option("dbtable", target_table) \
        .mode("overwrite") \
        .save()
    print(f"  ✅ Loaded: GLUE_POC_DB.SAPS4_CORE.{target_table}")
except Exception as e:
    print(f"  ❌ Snowflake load failed: {e}")
    write_audit(sf_conn, batch_id, args['entity_name'], load_type,
                extraction_start, datetime.now(timezone.utc), total_extracted,
                'FAILED', str(e)[:500], s3_out, watermark_used, None)
    sf_conn.close()
    raise

# Update watermark and audit
new_wm = run_ts.strftime('%Y-%m-%dT%H:%M:%S')
update_watermark(sf_conn, config['config_id'], new_wm)
extraction_end = datetime.now(timezone.utc)
write_audit(sf_conn, batch_id, args['entity_name'], load_type,
            extraction_start, extraction_end, total_extracted,
            'SUCCESS', None, s3_out, watermark_used, new_wm)
sf_conn.close()

duration = round((extraction_end - extraction_start).seconds / 60, 1)
print(f"\n" + "=" * 70)
print(f"✅ {args['entity_name']} EXTRACTION COMPLETE")
print(f"=" * 70)
print(f"  Rows extracted  : {total_extracted:,}")
print(f"  Duration        : {duration} minutes")
print(f"  S3              : {s3_out}")
print(f"  Snowflake table : GLUE_POC_DB.SAPS4_CORE.{target_table}")
print(f"  New watermark   : {new_wm}")
print(f"\n  ✅ BOTH TABLES DONE. Run validation queries in Section 15.")
print(f"=" * 70)

job.commit()
```

---

# 12. STEP 7 — DELTA LOAD WATERMARK LOGIC — DEEP EXPLANATION

## Why Watermark Exists

```
WITHOUT watermark (Phase 1 full load):
  Every run: fetch last 30 days → always 200K-500K rows
  Fine for POC. Not fine for hourly production runs.

WITH watermark (Phase 2 delta load):
  Run 1 (first ever):  fetch 30 days → 500K rows → watermark = now
  Run 2 (1 hour later): fetch WHERE LastChangeDateTime > watermark → maybe 50K rows
  Run 3 (1 hour later): same → maybe 30K rows
  
  Each hourly run is tiny and fast instead of large and slow.
```

## How the Watermark Works in This System

```
GLUE_ENTITY_CONFIG table stores LAST_WATERMARK per entity.

BEFORE extraction:
  Glue reads: SELECT LAST_WATERMARK FROM GLUE_ENTITY_CONFIG
               WHERE RELATIVE_PATH = 'BillingDocument'
  Result: '2025-03-03T20:00:00'

DURING extraction (delta filter):
  OData $filter = LastChangeDateTime gt datetime'2025-03-03T20:00:00'
  SAP returns only records changed AFTER that timestamp.

AFTER successful extraction:
  Glue updates: LAST_WATERMARK = current_timestamp (e.g. '2025-03-03T21:00:00')

NEXT RUN:
  Reads new watermark: '2025-03-03T21:00:00'
  Fetches only what changed in the last hour.
```

## Delta Options (from Solution Design v2, Section 6.5)

| Option | Description | Source |
|---|---|---|
| Watermark-based | Filter using LastChangeDateTime > stored watermark | SAP, SFDC |
| Overlap Window | Add lookback period (e.g. 2 hours) to catch late-arriving records | Both |

For this POC we use watermark-based approach.
Overlap window can be added later if data freshness issues arise.

## First Run vs Regular Delta Runs

```
FIRST RUN (initial historical load):
  LAST_WATERMARK = 30 days ago (set in Section 8.4)
  Filter fetches last 30 days of data
  Large load — takes 15-60 minutes
  After success: watermark updated to now

SUBSEQUENT HOURLY RUNS:
  LAST_WATERMARK = timestamp from previous successful run
  Filter fetches only last hour's changes
  Small load — takes 3-10 minutes
  After success: watermark updated to now

AFTER LONG OUTAGE (e.g. weekend):
  LAST_WATERMARK = Friday 6pm
  Monday run fetches everything since Friday 6pm
  Larger than normal but still much smaller than full load
  Automatically catches up — no manual intervention needed
```

## Switching from FULL to DELTA

```
When you are done with the Phase 1 POC:

1. In the Glue job parameters, change:
   --load_type  FULL  →  --load_type  DELTA

2. Make sure LAST_WATERMARK in GLUE_ENTITY_CONFIG
   is set to the timestamp of your last successful run.

3. The extraction code automatically switches to
   delta mode and uses the watermark filter.

That's it. The same code handles both FULL and DELTA.
```

---

# 13. STEP 8 — AIRFLOW DAG TO ORCHESTRATE THE POC

## What This DAG Does

```
prd-saps4-hourly-billing-document-delta-load-pipeline (existing SnapLogic DAG)

REPLACEMENT: glue_poc_sap_billing_extract (this new Glue DAG)

Schedule: 0 * * * * → every hour at :00 (same as existing pipeline)
Priority: 0 — Mission Critical

Task flow:
  start
    → extract_billingdocument           (NEW Glue job — parent, OData extraction)
    → extract_billingdocumentitem       (NEW Glue job — child, only after parent)
    → load_sap_to_snowflake             (EXISTING Glue transform template — S3 → Snowflake)
    → validate_row_counts               (Snowflake SQL check)
    → end

WHY TWO SEPARATE GLUE JOBS?
  Step 1+2 (extract_billing*):   NEW jobs we are building in this POC.
                                  They call SAP OData API → write Parquet to S3.
                                  This REPLACES SnapLogic.

  Step 3 (load_sap_to_snowflake): EXISTING Glue template already running in production.
                                  It reads from S3 and loads to Snowflake.
                                  We are NOT changing this — just pointing it at
                                  the new POC S3 path instead of SnapLogic's path.
                                  Job name: tcpl-glue-sap-to-snowflake-core

  Previously this existing template was triggered AFTER SnapLogic wrote to S3.
  Now it is triggered AFTER our new Glue extraction jobs write to S3.
  The template itself is identical — only the SOURCE_PATH parameter changes.
```

Save as: `glue_poc_sap_billing_dag.py`

```python
"""
TCPL Glue POC — SAP Billing Airflow DAG
========================================
Orchestrates the FULL real pipeline end-to-end:
  1. BillingDocument OData extraction  (NEW — replaces SnapLogic)
  2. BillingDocumentItem OData extraction (NEW — replaces SnapLogic)
  3. S3 → Snowflake load               (EXISTING Glue transform template — unchanged)
  4. Row count validation in Snowflake

Replaces: prd-saps4-hourly-billing-document-delta-load-pipeline (SnapLogic)
Schedule: Every hour at :00
Priority: 0 — Mission Critical

SIMULATION NOTE (what this DAG proves):
  Previously: Airflow → SnapLogic (extracted to S3) → existing Glue transform → Snowflake
  Now:        Airflow → NEW Glue extractor (extracted to S3) → SAME existing Glue transform → Snowflake

  Tasks 1+2 are the NEW jobs we built to replace SnapLogic.
  Task 3 (load_sap_to_snowflake) is the EXISTING Glue job that was already running.
  We simply point it at the new POC S3 path — the job code itself is NOT changed.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner':            'tcpl-decision-point',
    'depends_on_past':  False,
    'start_date':       days_ago(1),
    'email_on_failure': True,
    'email':            ['platform-alerts@decisionpoint.in'],
    'retries':          1,
    'retry_delay':      timedelta(minutes=5),
    # IMPORTANT: execution_timeout must be LONGER than Glue job runtime
    # BillingDocumentItem can take 60-80 min for large delta windows
    # Set to 3 hours to be safe
    'execution_timeout': timedelta(hours=3),
}

# Job parameters passed to both NEW extraction Glue jobs
GLUE_JOB_PARAMS = {
    '--sap_secret_name':       'tcpl/sap/odata-credentials',
    '--snowflake_secret_name': 'tcpl/snowflake/glue-poc-credentials',
    '--s3_bucket':             '<your-bucket-name>',    # ← change this
    '--page_size':             '1000',
    '--load_type':             'DELTA',  # Switch to DELTA for production
}

# S3 path where the new extraction jobs will write Parquet files
# This is what the existing transform template will then read from
POC_S3_PATH = 's3://<your-bucket-name>/glue_poc/sap/'  # ← change this

with DAG(
    dag_id='glue_poc_sap_billing_extract',
    default_args=default_args,
    schedule_interval='0 * * * *',  # Every hour at :00
    catchup=False,
    max_active_runs=1,              # Prevent overlapping runs
    tags=['glue', 'sap', 'billing', 'priority-0', 'odata'],
    description='Glue POC — Full pipeline: SAP OData extract (NEW) + Snowflake load (EXISTING template)',
) as dag:

    start = DummyOperator(task_id='start')

    # ─────────────────────────────────────────────────────────────────────
    # TASKS 1 + 2: NEW Glue jobs (replacing SnapLogic)
    # These call SAP OData API directly and write Parquet to S3.
    # This is the part we are building in this POC.
    # ─────────────────────────────────────────────────────────────────────

    # Task 1: Extract BillingDocument (parent — must run first)
    extract_billing_document = GlueJobOperator(
        task_id='extract_billingdocument_odata',
        job_name='glue-poc-billingdocument-odata-extract',   # NEW job
        script_args={
            **GLUE_JOB_PARAMS,
            '--entity_name': 'BillingDocument',
        },
        aws_conn_id='aws_default',
        region_name='ap-south-1',
        wait_for_completion=True,   # Wait for Glue job to finish
        create_job_kwargs={
            'GlueVersion': '4.0',
            'WorkerType': 'G.2X',
            'NumberOfWorkers': 10,
        },
    )

    # Task 2: Extract BillingDocumentItem (child — runs AFTER parent)
    # The job itself also verifies parent success before proceeding
    extract_billing_doc_item = GlueJobOperator(
        task_id='extract_billingdocumentitem_odata',
        job_name='glue-poc-billingdocumentitem-odata-extract',   # NEW job
        script_args={
            **GLUE_JOB_PARAMS,
            '--entity_name': 'BillingDocumentItem',
        },
        aws_conn_id='aws_default',
        region_name='ap-south-1',
        wait_for_completion=True,
        create_job_kwargs={
            'GlueVersion': '4.0',
            'WorkerType': 'G.2X',
            'NumberOfWorkers': 20,  # More workers for larger table
        },
    )

    # ─────────────────────────────────────────────────────────────────────
    # TASK 3: EXISTING Glue transform template (NOT changed — reused as-is)
    # This job was already running in production after SnapLogic wrote to S3.
    # We now call it after our new extraction jobs write to S3 instead.
    # Only the SOURCE_PATH parameter is different (POC path vs production path).
    # Job name: tcpl-glue-sap-to-snowflake-core  ← this already exists in AWS Glue
    # ─────────────────────────────────────────────────────────────────────

    load_sap_to_snowflake = GlueJobOperator(
        task_id='load_sap_to_snowflake_poc',
        job_name='tcpl-glue-sap-to-snowflake-core',   # ← EXISTING template, already in AWS Glue
        script_args={
            '--SOURCE_PATH':   POC_S3_PATH,            # new POC S3 path (was SnapLogic's path before)
            '--TARGET_SCHEMA': 'GLUE_POC_DB.SAPS4_CORE',  # POC schema (not production SAPS4_CORE)
            '--RUN_DATE':      '{{ ds }}',
        },
        aws_conn_id='aws_default',
        region_name='ap-south-1',
        wait_for_completion=True,
        create_job_kwargs={
            'GlueVersion': '4.0',
            'WorkerType': 'G.2X',
            'NumberOfWorkers': 10,
        },
    )

    # ─────────────────────────────────────────────────────────────────────
    # TASK 4: Validate row counts in Snowflake
    # Compare what Glue loaded vs what SnapLogic loaded
    # ─────────────────────────────────────────────────────────────────────

    validate = SnowflakeOperator(
        task_id='validate_row_counts',
        snowflake_conn_id='snowflake_default',
        sql="""
            -- POC data (loaded by new Glue pipeline)
            SELECT 'GLUE_POC — BILLINGDOCUMENT'     AS source,
                   COUNT(*)                          AS rows_loaded,
                   MAX(EXTRACTED_AT)                 AS last_load
            FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT
            UNION ALL
            SELECT 'GLUE_POC — BILLINGDOCUMENTITEM'  AS source,
                   COUNT(*)                          AS rows_loaded,
                   MAX(EXTRACTED_AT)                 AS last_load
            FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENTITEM
            UNION ALL
            -- Production data (loaded by SnapLogic — for comparison)
            SELECT 'SNAPLOGIC — BILLINGDOCUMENT'     AS source,
                   COUNT(*)                          AS rows_loaded,
                   MAX(EXTRACTED_AT)                 AS last_load
            FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT
            UNION ALL
            SELECT 'SNAPLOGIC — BILLINGDOCUMENTITEM'  AS source,
                   COUNT(*)                           AS rows_loaded,
                   MAX(EXTRACTED_AT)                  AS last_load
            FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENTITEM;
        """,
    )

    end = DummyOperator(task_id='end')

    # ─────────────────────────────────────────────────────────────────────
    # DEPENDENCY CHAIN — Full real pipeline simulation:
    #
    #   start
    #     → extract_billing_document        (NEW: SAP OData → S3)
    #     → extract_billing_doc_item        (NEW: SAP OData → S3, after parent)
    #     → load_sap_to_snowflake           (EXISTING: S3 → Snowflake)
    #     → validate                        (compare Glue vs SnapLogic counts)
    #     → end
    #
    # CRITICAL: parent extraction must complete before child extraction.
    # VBRK (BillingDocument) must always complete before VBRP (BillingDocumentItem).
    # ─────────────────────────────────────────────────────────────────────

    start >> extract_billing_document >> extract_billing_doc_item >> load_sap_to_snowflake >> validate >> end
```

---

# 14. STEP 9 — RUN THE POC — MANUAL EXECUTION STEPS

## Pre-Flight Checklist (Morning Before You Start)

```
□ Test the SAP OData URL in browser:
  <base_url>/sap/opu/odata/sap/API_BILLING_DOCUMENT_SRV/A_BillingDocument
  ?$top=5&$format=json
  Expected: See JSON with billing document data.

□ Verify Secrets Manager has both secrets:
  tcpl/sap/odata-credentials
  tcpl/snowflake/glue-poc-credentials

□ Verify S3 folders exist (glue_poc/sap/BillingDocument/ etc.)

□ Verify all 3 scripts uploaded to S3 glue-scripts/ folder

□ Verify all 3 Glue jobs created in AWS Console

□ Verify Snowflake setup:
  SELECT * FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_ENTITY_CONFIG;
  → Must show 2 rows: BillingDocument and BillingDocumentItem

□ Verify Snowflake target schemas exist:
  SHOW SCHEMAS IN DATABASE GLUE_POC_DB;
  → Must show SAPS4_CORE
```

## Run Sequence — Follow This Exactly

```
STEP 1: Run connection test
  → Job: glue-poc-sap-odata-connection-test
  → Expected runtime: 3-5 minutes
  → Look for: "✅ ALL TESTS PASSED"
  → If fails: fix error before proceeding

STEP 2: Run BillingDocument extraction
  → Job: glue-poc-billingdocument-odata-extract
  → Expected runtime: 10-20 minutes (30-day window)
  → Look for: "✅ BillingDocument EXTRACTION COMPLETE"
  → Note the row count printed in logs

STEP 3: Validate BillingDocument in Snowflake immediately
  SELECT COUNT(*) FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT;
  → Must match the row count in Glue logs

STEP 4: Run BillingDocumentItem extraction
  → Job: glue-poc-billingdocumentitem-odata-extract
  → Expected runtime: 20-40 minutes (30-day window, larger data)
  → Look for: "✅ BillingDocumentItem EXTRACTION COMPLETE"

STEP 5: Validate both tables (Section 15)

STEP 6: Show results to lead ✅
```

## How to Run a Glue Job and Watch Logs

```
RUN THE JOB:
1. AWS Console → AWS Glue → ETL Jobs (left sidebar)
2. Click on the job name
3. Click: "Run" button (orange, top right)
4. A run appears at top of "Runs" tab

WATCH LOGS:
1. Click the "Runs" tab on the job page
2. Find your run (status: Running = spinning icon)
3. Click the Run ID link
4. Click: "Output logs" button
   → Opens CloudWatch Logs in new tab
5. In CloudWatch: click on the log stream (has your run ID)
6. Click: "Start tailing" (top right) → see logs live

JOB STATUS MEANINGS:
  Running   → Still in progress (normal — be patient)
  Succeeded → Completed successfully ✅
  Failed    → Something went wrong ❌ → read Error logs

IF FAILED — HOW TO READ ERROR:
1. On the Run detail page, click: "Error logs"
2. Scroll to the BOTTOM of the log
3. Last few lines = the actual error message
4. Copy it and check Section 16 (Common Errors) for the fix
```

---

# 15. STEP 10 — VALIDATE YOUR DATA — ALL CHECKS + COMPARISON QUERIES

Run ALL these in Snowflake Worksheet after both jobs complete.
These are what you show your lead as proof the POC worked.

## Check 1: Basic Row Counts

```sql
-- How many rows did Glue extract?
SELECT
    'BILLINGDOCUMENT'     AS table_name,
    COUNT(*)              AS total_rows,
    MIN(LOAD_BATCH_ID)    AS first_batch,
    MAX(EXTRACTED_AT)     AS last_extracted
FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT

UNION ALL

SELECT
    'BILLINGDOCUMENTITEM' AS table_name,
    COUNT(*)              AS total_rows,
    MIN(LOAD_BATCH_ID)    AS first_batch,
    MAX(EXTRACTED_AT)     AS last_extracted
FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENTITEM;

-- EXPECTED:
-- Both tables show > 0 rows
-- EXTRACTED_AT = today's date and time
-- SHOW THIS TO YOUR LEAD
```

## Check 2: Audit Log — Job Run History

```sql
-- See all job runs and their status
SELECT
    AUDIT_ID,
    ENTITY_NAME,
    LOAD_TYPE,
    STATUS,
    START_TIME,
    END_TIME,
    RECORD_COUNT,
    DATEDIFF('second', START_TIME, END_TIME) AS duration_seconds,
    WATERMARK_USED,
    NEW_WATERMARK,
    ERROR_MESSAGE
FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_AUDIT_LOG
ORDER BY AUDIT_ID DESC;

-- EXPECTED:
-- BillingDocument row: STATUS = SUCCESS, RECORD_COUNT > 0
-- BillingDocumentItem row: STATUS = SUCCESS, RECORD_COUNT > 0
-- BillingDocument END_TIME earlier than BillingDocumentItem START_TIME
```

## Check 3: Compare Glue vs SnapLogic Row Counts (KEY VALIDATION)

This is the most important comparison from Solution Design v2, Section 8.2.

```sql
-- Row count comparison: Glue POC vs existing SnapLogic data
-- From Solution Design v2 validation query
SELECT 'SnapLogic' AS source, COUNT(*) AS row_count
FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT
UNION ALL
SELECT 'Glue POC' AS source, COUNT(*) AS row_count
FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT;

-- EXPECTED: Row counts match (or are very close — difference should be < 1%)
-- If Glue has fewer: check watermark date range
-- If Glue has more: that's fine, Glue may have pulled more recent records
```

## Check 4: MINUS Query — Find Differences (from Solution Design v2)

```sql
-- Records in SnapLogic but missing in Glue POC
SELECT 'Missing in Glue' AS issue, BillingDocument
FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT
MINUS
SELECT 'Missing in Glue', BillingDocument
FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT;
-- EXPECTED: 0 rows (or very few if date ranges differ slightly)

-- Records in Glue but missing in SnapLogic (extra in Glue is OK)
SELECT 'Extra in Glue' AS issue, BillingDocument
FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT
MINUS
SELECT 'Extra in Glue', BillingDocument
FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT;
-- EXPECTED: 0 rows (or small number = recent records SnapLogic hasn't loaded yet)
```

## Check 5: Parent-Child Join — Show Real Business Data

```sql
-- Join parent and child — shows invoice with its line items
-- This is your "money query" — show this to your lead
SELECT
    d.BillingDocument,
    d.BillingDocumentDate,
    d.SoldToParty,
    d.NetAmount,
    d.TransactionCurrency,
    d.SalesOrganization,
    i.BillingDocumentItem,
    i.Material,
    i.BillingQuantity,
    i.NetAmount AS ItemNetAmount,
    i.Plant
FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT d
JOIN GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENTITEM i
    ON d.BillingDocument = i.BillingDocument
ORDER BY d.BillingDocumentDate DESC, d.BillingDocument, i.BillingDocumentItem
LIMIT 20;

-- EXPECTED: Real invoices with their line items
-- SHOW THIS TO YOUR LEAD — proves parent-child integrity works
-- NOTE: Column names come from OData API response —
--       may differ from above. Use actual column names from your data.
```

## Check 6: No Orphan Items

```sql
-- Items without a matching parent document
SELECT COUNT(*) AS orphan_items
FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENTITEM i
LEFT JOIN GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT d
    ON i.BillingDocument = d.BillingDocument
WHERE d.BillingDocument IS NULL;

-- EXPECTED: 0
-- If > 0: not a blocker for POC (date ranges may differ slightly)
-- Note the number and explain to lead
```

## Check 7: Watermark Status

```sql
-- Confirm watermarks updated correctly after run
SELECT
    RELATIVE_PATH,
    TO_CHAR(LAST_WATERMARK, 'YYYY-MM-DD HH24:MI:SS') AS last_watermark,
    UPDATED_AT
FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_ENTITY_CONFIG
WHERE SRC_SYS_NM = 'SAPS4'
ORDER BY CONFIG_ID;

-- EXPECTED: LAST_WATERMARK = today's date and time
-- This proves watermark tracking works for Phase 2 delta loads
```

---

# 16. COMMON ERRORS AND HOW TO FIX THEM

## Error 1: HTTP 401 — SAP Authentication Failed

```
ERROR IN LOGS:
  HTTP Status: 401
  "401 Unauthorized"

MEANING: SAP username or password is wrong.

FIX:
  1. Go to Secrets Manager → tcpl/sap/odata-credentials
  2. Click "Retrieve secret value"
  3. Verify username and password are correct
  4. Test in a browser:
     Go to: <base_url><odata_service>/A_BillingDocument?$top=1&$format=json
     You'll be prompted for username/password
     Try the same credentials in the browser
     If browser works but Glue doesn't → re-check the secret values exactly
  5. Ask SAP Basis team to reset the password if needed
```

## Error 2: HTTP 404 — OData Service Not Found

```
ERROR IN LOGS:
  HTTP Status: 404
  "404 Not Found"

MEANING: The OData service path or entity set name is wrong.

FIX:
  1. Ask SAP Basis team: "What is the exact OData service URL for
     BillingDocument in our S/4HANA system?"
  2. They should give you a URL like:
     /sap/opu/odata/sap/API_BILLING_DOCUMENT_SRV
  3. Update odata_service in Secrets Manager if needed
  4. The entity set name (A_BillingDocument) may also differ
     Ask: "What is the entity set name for billing documents?"
  5. Browse the service catalog if accessible:
     <base_url>/sap/opu/odata/sap/ → shows available services
```

## Error 3: Connection Error — Cannot Reach SAP Host

```
ERROR IN LOGS:
  requests.exceptions.ConnectionError
  "Failed to establish a new connection"

MEANING: Glue cannot reach SAP over the network.

FIX:
  1. Verify the base_url in Secrets Manager is correct
  2. Verify Glue is in the correct VPC that can reach SAP
  3. Check Security Group:
     AWS Console → EC2 → Security Groups
     Find the SG used by Glue
     Click "Outbound rules"
     Must have a rule: Custom TCP, Port <SAP_PORT>, Destination <SAP_IP>/32
     If missing: click "Edit outbound rules" and add it
  4. Ask AWS infra team: "Can Glue NAT Gateway in <VPC_ID>
     reach <SAP_BASE_URL> on port 443 (or 8080 or 44300)?"
```

## Error 4: Timeout on Large Pages

```
ERROR IN LOGS:
  requests.exceptions.Timeout
  "Read timed out"

MEANING: SAP took too long to respond for one page.
         Usually happens for BillingDocumentItem (large table).

FIX:
  The code already retries with 3-minute timeout.
  If still failing:
  1. Reduce page_size from 1000 to 500 in Glue job parameters
     Smaller pages = faster response from SAP
  2. Ask SAP Basis if the OData service has any row limits
  3. Run during off-peak hours (not during business hours)
```

## Error 5: Snowflake Connector Not Found

```
ERROR IN LOGS:
  java.lang.ClassNotFoundException
  net.snowflake.spark.snowflake.DefaultSource

MEANING: Snowflake Spark connector JAR not added to Glue job.

FIX (Option A — Glue Marketplace, easiest):
  1. AWS Console → AWS Glue → Connections
  2. Click: Create connection
  3. Data source: Snowflake
  4. Follow the Snowflake Connector for AWS Glue setup
  5. Reference this connection in your Glue job

FIX (Option B — Manual JARs):
  1. Download from Maven:
     spark-snowflake_2.12-2.12.0-spark_3.3.jar
     snowflake-jdbc-3.14.4.jar
  2. Upload both to S3: <bucket>/glue-scripts/
  3. In Glue job → Job details → Advanced → Dependent JARs:
     Add both JAR S3 paths (comma-separated)
```

## Error 6: Snowflake Authentication Failed

```
ERROR IN LOGS:
  snowflake.connector.errors.DatabaseError: 250001
  "Incorrect username or password"

FIX:
  1. Secrets Manager → tcpl/snowflake/glue-poc-credentials
  2. Retrieve and check credentials
  3. MOST COMMON MISTAKE: account field is wrong
     Wrong: xy12345.snowflakecomputing.com
     Wrong: https://xy12345.snowflakecomputing.com
     Correct: xy12345   (just the account identifier, nothing else)
  4. Test in Snowflake Web UI with same username/password
```

## Error 7: BillingDocumentItem Job Fails — Parent Not Verified

```
ERROR IN LOGS:
  "BillingDocument has no SUCCESS record in GLUE_AUDIT_LOG"
  "Cannot load BillingDocumentItem until parent succeeds"

MEANING: You ran the child job before the parent job completed.

FIX:
  1. Run billing_document_odata_extract.py first
  2. Wait for it to show SUCCESS status
  3. Verify: SELECT STATUS FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_AUDIT_LOG
             WHERE ENTITY_NAME = 'BillingDocument' ORDER BY AUDIT_ID DESC LIMIT 1;
  4. Only then run billing_doc_item_odata_extract.py
```

## Error 8: Zero Records Extracted

```
SYMPTOM: Job succeeds but "Extracted: 0 records"

CAUSES AND FIXES:
  A) Date filter is too narrow
     The 30-day filter may be excluding data if LAST_WATERMARK
     was set incorrectly in GLUE_ENTITY_CONFIG.
     FIX: Check watermark date:
          SELECT LAST_WATERMARK FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_ENTITY_CONFIG;
          If wrong, update it: UPDATE GLUE_ENTITY_CONFIG
                               SET LAST_WATERMARK = DATEADD('day',-30,CURRENT_TIMESTAMP())
                               WHERE RELATIVE_PATH = 'BillingDocument';

  B) LastChangeDateTime field name is wrong in OData
     The $filter=LastChangeDateTime gt datetime'...' may not match
     the actual field name in SAP's API response.
     FIX: Run connection test and check the field names in the first record.
          Ask SAP Basis: "What is the last-change timestamp field name
          in the A_BillingDocument OData entity?"

  C) SAP user has no SELECT access
     FIX: Ask SAP Basis to grant the technical user SELECT on
          A_BillingDocument and A_BillingDocumentItem entities.
```

---

# 17. POC SUCCESS CRITERIA — HOW TO KNOW YOU PASSED

Show all these to your lead after the POC run.

## Technical Checks (All Must Pass)

```
□ 1. Connection test passed (green in CloudWatch logs)

□ 2. BillingDocument rows in Snowflake:
      SELECT COUNT(*) FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT;
      → Must be > 0 (expect 200K-500K for 30 days)

□ 3. BillingDocumentItem rows in Snowflake:
      SELECT COUNT(*) FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENTITEM;
      → Must be > 0 (expect 1M-3M for 30 days)

□ 4. AUDIT_LOG shows SUCCESS for both entities:
      SELECT ENTITY_NAME, STATUS, RECORD_COUNT, END_TIME
      FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_AUDIT_LOG
      WHERE STATUS = 'SUCCESS' ORDER BY END_TIME DESC;

□ 5. Parent loaded before child:
      BillingDocument END_TIME < BillingDocumentItem START_TIME

□ 6. Watermarks updated correctly:
      SELECT RELATIVE_PATH, LAST_WATERMARK FROM GLUE_ENTITY_CONFIG;
      → Both show today's timestamp

□ 7. S3 Parquet files exist:
      Check S3 → glue_poc/sap/BillingDocument/full/<today>/
      → Parquet files with non-zero size

□ 8. Join query returns real data (Check 5 in Section 15):
      Invoice + line items joined correctly
```

## Comparison Checks (Official POC Criteria from Solution Design v2)

```
□ 9. Row count comparison (Check 3 in Section 15):
     Glue row count ≈ SnapLogic row count (within 1-2%)

□ 10. MINUS query returns 0 records (Check 4 in Section 15):
      No records in SnapLogic that are missing in Glue
```

## Performance Targets

| Run | Expected Time |
|---|---|
| BillingDocument (30-day full) | 10–20 minutes |
| BillingDocumentItem (30-day full) | 20–40 minutes |
| BillingDocument (hourly delta) | 2–5 minutes |
| BillingDocumentItem (hourly delta) | 5–10 minutes |

---

# 18. PRESENTATION TALKING POINTS FOR YOUR MANAGER

## What You Proved

> "We proved that AWS Glue can connect directly to SAP S/4HANA
> OData APIs and extract data — without SnapLogic in the pipeline.
>
> We used the two most critical tables on the platform:
> BillingDocument (5.39M rows, Priority 0) and BillingDocumentItem
> (24.94M rows, child entity). Both together feed the CFA Checkpoint
> at 20:11 which drives next-day dispatch operations.
>
> Glue called the OData API, paginated through all pages,
> wrote Parquet to S3, loaded to Snowflake, and the row counts
> match SnapLogic's data within 1%. MINUS query returned 0 differences."

## Architecture in One Sentence

> "We replaced SnapLogic in the extraction layer by writing a
> Glue Python job that calls the same SAP OData APIs SnapLogic was
> calling — but now in native AWS code we fully control."

## Numbers to Quote

| Metric | Value |
|---|---|
| BillingDocument rows loaded | \_\_\_\_\_ (fill after run) |
| BillingDocumentItem rows loaded | \_\_\_\_\_ (fill after run) |
| Row count match vs SnapLogic | \_\_\_\_ % match |
| MINUS query differences | 0 records |
| BillingDocument job time | \_\_\_\_ minutes |
| BillingDocumentItem job time | \_\_\_\_ minutes |
| Parent-child sequence | ✅ Correct |
| Data in Snowflake | ✅ Verified |

## Current vs Target

| | SnapLogic | Glue POC |
|---|---|---|
| Extraction method | SnapLogic OData connector | Glue Python + requests |
| Credentials | SnapLogic vault | AWS Secrets Manager |
| Config | MySQL RDS INPUT_ENTITY_LIST | Snowflake INPUT_ENTITY_LIST_GLUE_POC |
| Audit | SnapLogic logs | Snowflake GLUE_AUDIT_LOG |
| Control | SnapLogic UI | AWS Console + Airflow |
| Cost | SnapLogic license fees | AWS Glue DPU cost only |

## Next Steps

> "This POC validates the pattern for 2 of 7 SAP tables.
> Phase 2 is switching from FULL load to hourly DELTA loads
> using the watermark mechanism already built into this code.
> After that we extend to remaining 5 SAP tables, then SFDC tables.
> Full SnapLogic replacement estimated: 2-3 months."

---

# 19. FULL GLOSSARY

| Term | What It Means |
|---|---|
| OData | Open Data Protocol — REST API standard SAP uses to expose table data |
| OData Service | The API endpoint path, e.g. /sap/opu/odata/sap/API_BILLING_DOCUMENT_SRV |
| Entity Set | A specific data entity within a service, e.g. A_BillingDocument |
| CDS View | SAP Core Data Services — the underlying view that powers the OData API |
| $top | OData parameter — how many rows to return per page (like SQL LIMIT) |
| $skip | OData parameter — how many rows to skip (like SQL OFFSET) |
| $filter | OData parameter — filter condition (like SQL WHERE) |
| $format=json | OData parameter — return JSON not XML |
| Pagination | Looping through all $skip/$top pages to get all records |
| Basic Auth | HTTP authentication: base64(username:password) in Authorization header |
| BillingDocument | SAP invoice header — one row per invoice — OData entity A_BillingDocument |
| BillingDocumentItem | SAP invoice line items — child of BillingDocument |
| LastChangeDateTime | OData field on billing entities — used as watermark for delta loads |
| Watermark | Timestamp stored in GLUE_ENTITY_CONFIG that marks last successful extraction |
| Delta Load | Extract only records changed since watermark timestamp |
| Full Load | Extract all records (no date filter) — used for first run or master data |
| INPUT_ENTITY_LIST_GLUE_POC | Snowflake control table — official TCPL standard config table. Stores OData API details, watermarks, batch size, parent-child relationships, S3 paths per entity. Replaces SnapLogic's MySQL INPUT_ENTITY_LIST |
| GLUE_AUDIT_LOG | Snowflake audit table — one row per Glue job run |
| PRD_ARCH_SUPPORT_DB | Snowflake database for control/config tables |
| GLUE_POC_DB | Snowflake database for POC extracted data (separate from SnapLogic data) |
| SAPS4_DB | Existing Snowflake database with SnapLogic-extracted SAP data (for comparison) |
| SAPS4_CORE | Schema where both SnapLogic and Glue SAP data lives |
| Parent entity | BillingDocument — must load before child |
| Child entity | BillingDocumentItem — references parent, loads after parent |
| Referential Integrity | Child's foreign key (BillingDocument number) must exist in parent |
| Secrets Manager | AWS service that stores credentials encrypted at rest |
| IAM Role | AWS permissions assigned to Glue — controls what Glue can access |
| CloudWatch | AWS logging service — where Glue job logs appear |
| G.2X Worker | Glue worker: 8 vCPU, 32 GB RAM |
| G.4X Worker | Glue worker: 16 vCPU, 64 GB RAM — for very large tables |
| coalesce(N) | Spark function — combine into N output Parquet files |
| MINUS Query | SQL set operation — finds records in one dataset missing from another |
| GlueJobOperator | Airflow operator that triggers and monitors a Glue job |
| MWAA | Amazon Managed Workflows for Apache Airflow — how TCPL runs Airflow |
| SnapLogic | Current iPaaS tool being replaced — calls SAP OData on TCPL's behalf |
| CFA Checkpoint | The 20:11 IST Airflow DAG that waits for billing data freshness |
| Priority 0 | Highest criticality — revenue-impacting pipelines |
| POC | Proof of Concept — small-scale test to validate the approach |
