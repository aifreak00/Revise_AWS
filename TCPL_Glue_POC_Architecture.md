# TCPL × AWS Glue POC — Full Architecture & End-to-End Guide

**Replacing SnapLogic with AWS Glue | SAP OData + SFDC Bulk API 2.0**
**Timeline: Feb 24 → Mar 15, 2025 | 12 Tables | 981M Records | 62 GB**

---

## Section 1 — Current State vs Target State

### Current State (using SnapLogic today)

```
SAP S/4HANA  ──┐
                ├──► SnapLogic ──► S3 ──► Glue Transform ──► Snowflake
Salesforce   ──┘    (extractor)          (already exists)
```

**Problem:** SnapLogic charges per pipeline/connector. At TCPL scale (250+ pipelines) = very high cost.

### Target State (what you are building in this POC)

```
SAP S/4HANA  ──┐
                ├──► AWS Glue ──► S3 ──► Glue Transform ──► Snowflake
Salesforce   ──┘   (extractor)          (already exists)
                    ^ YOU BUILD THIS
```

**Goal:** Replace SnapLogic with Glue for the extraction layer. Prove data accuracy = 100% match. If POC passes → migrate all 250+ SnapLogic pipelines.

---

## Section 2 — Full Architecture Diagram

### Block 1 — Your Laptop

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  YOUR LAPTOP                                                                │
│                                                                             │
│  VS Code                                                                    │
│  ├── glue_utils.py                    (shared helper functions)            │
│  ├── glue_poc_sfdc_extract.py         (SFDC extraction job)                │
│  ├── glue_poc_sap_extract.py          (SAP extraction job)                 │
│  ├── glue_poc_sfdc_connectivity_test.py                                    │
│  └── glue_poc_sap_connectivity_test.py                                     │
│                         │                                                   │
│                         │  aws s3 cp  (run once to upload)                 │
└─────────────────────────┼───────────────────────────────────────────────────┘
                          │
                          ▼
```

### Block 2 — S3 Scripts Folder

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  S3 — SCRIPTS FOLDER  (your code lives here permanently)                    │
│                                                                             │
│  s3://tcpl-datalake/scripts/glue_poc/                                      │
│  ├── glue_utils.py                                                         │
│  ├── glue_poc_sfdc_extract.py                                              │
│  ├── glue_poc_sap_extract.py                                               │
│  └── glue_poc_sfdc_connectivity_test.py                                    │
└─────────────────────────────────────────────────────────────────────────────┘
                          │
                          │  Glue reads .py from S3 on every run
                          ▼
```

### Block 3 — AWS Secrets Manager

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  AWS SECRETS MANAGER  (credentials — never hardcode these)                  │
│                                                                             │
│  tcpl/glue_poc/sap_odata   → { base_url, username, password }             │
│  tcpl/glue_poc/sfdc_oauth  → { client_id, client_secret,                  │
│                                 username, password, security_token }        │
│  tcpl/glue_poc/snowflake   → { account, user, password,                   │
│                                 warehouse, database, schema }               │
└─────────────────────────────────────────────────────────────────────────────┘
                          │
                          │  Glue reads secrets at runtime
                          ▼
```

### Block 4 — Snowflake Control Tables

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  SNOWFLAKE — CONTROL TABLES  (tracks config + watermarks + audit)           │
│  Database: PRD_ARCH_SUPPORT_DB  |  Schema: GLUE_POC                        │
│                                                                             │
│  GLUE_ENTITY_CONFIG                   GLUE_AUDIT_LOG                       │
│  ├── CONFIG_ID                        ├── AUDIT_ID                         │
│  ├── SRC_SYS_NM  (SFDC / SAPS4)      ├── ENTITY_NAME                      │
│  ├── SRC_OBJ_NM  (table name)         ├── LOAD_TYPE  (FULL/DELTA)          │
│  ├── LOAD_TYP    (F / D)              ├── RECORD_COUNT                     │
│  ├── LAST_WATERMARK  ← delta start    ├── DURATION_SECONDS                 │
│  ├── CDC_WATERMARK_COL                ├── S3_PATH                          │
│  ├── BATCH_SIZE                       ├── STATUS  (SUCCESS/FAILED)         │
│  └── S3_PATH  (output location)       └── ERROR_MESSAGE                    │
│                                                                             │
│  12 rows seeded — one per table (5 SFDC + 7 SAP)                          │
└─────────────────────────────────────────────────────────────────────────────┘
                          │
                          │  Airflow wakes up at 2AM IST daily
                          ▼
```

### Block 5 — Airflow DAG

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  AIRFLOW DAG  (tcpl_glue_poc_dag.py — orchestrates everything)              │
│                                                                             │
│  Schedule: 0 2 * * *  (2AM IST daily)                                      │
│                                                                             │
│  ┌──────────────────┐    ┌──────────────────┐                              │
│  │ sfdc_secondary_  │    │ sap_acdoca_      │                              │
│  │ sales_extract    │    │ finance_extract  │                              │
│  └────────┬─────────┘    └────────┬─────────┘                              │
│           │  (runs in parallel)    │                                        │
│           ▼                        ▼                                        │
│  ┌──────────────────┐    ┌──────────────────┐                              │
│  │ sfdc_visit_      │    │ sap_billing_     │                              │
│  │ details_extract  │    │ document_extract │                              │
│  └────────┬─────────┘    └────────┬─────────┘                              │
│           │      ... more tasks    │                                        │
│           ▼                        ▼                                        │
│  ┌──────────────────────────────────────────┐                              │
│  │         snowflake_load  (all tables)     │                              │
│  └──────────────────────┬───────────────────┘                              │
│                          ▼                                                  │
│  ┌──────────────────────────────────────────┐                              │
│  │         run_validation_queries           │                              │
│  └──────────────────────────────────────────┘                              │
│                                                                             │
│  GlueJobOperator submits each job → waits → SUCCESS or FAIL + alert email │
└─────────────────────────────────────────────────────────────────────────────┘
                          │
                          │  Airflow triggers Glue job with parameters
                          ▼
```

### Block 6 — AWS Glue Job Definitions

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  AWS GLUE JOB DEFINITIONS  (created once in AWS Console)                    │
│                                                                             │
│  Job 1: tcpl-glue-poc-sfdc-extract                                         │
│  ├── Script:   s3://.../glue_poc_sfdc_extract.py                           │
│  ├── Role:     TCPLGluePOCRole                                             │
│  ├── Workers:  10 × G.1X  (large tables like SecondarySalesLineItem)       │
│  └── Params:   --ENTITY_NAME, --LOAD_TYPE, --RUN_DATE                      │
│                                                                             │
│  Job 2: tcpl-glue-poc-sap-extract                                          │
│  ├── Script:   s3://.../glue_poc_sap_extract.py                            │
│  ├── Role:     TCPLGluePOCRole                                             │
│  ├── Workers:  10 × G.1X  (ACDOCA has 418M rows)                          │
│  └── Params:   --ENTITY_NAME, --LOAD_TYPE, --RUN_DATE                      │
└─────────────────────────────────────────────────────────────────────────────┘
                          │
                          │  Glue spins up Spark cluster (~90 sec)
                          ▼
```

### Block 7 — Spark Cluster (Transformation Happens Here)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  SPARK CLUSTER  (10 DPU workers — your code runs here in parallel)          │
│                                                                             │
│  FOR SFDC:                          FOR SAP:                                │
│  ─────────────────────              ─────────────────────────               │
│  1. Get OAuth token from SFDC       1. Basic Auth to SAP OData              │
│  2. Build SOQL query:               2. Build OData URL:                     │
│     SELECT * FROM Object               /BillingDocument                    │
│     WHERE LastModifiedDate             ?$filter=LastChangeDateTime          │
│     > {last_watermark}                 gt datetime'{watermark}'             │
│  3. Submit Bulk API 2.0 job         3. Paginate $top=5000 $skip=0,5000...  │
│  4. Poll until JobComplete          4. Collect all pages                    │
│  5. Retrieve CSV results            5. Handle OData v2/v4 response          │
│                                                                             │
│  TRANSFORMATION (in Spark memory — both sources):                          │
│  ├── Cast types  (string → double, timestamp, long)                        │
│  ├── Add metadata cols  (_extracted_at, _load_type, _source, _run_date)    │
│  ├── Drop nulls / duplicates                                               │
│  ├── Union all pages into one final DataFrame                              │
│  └── Write to S3 as Parquet (all workers write in parallel)                │
│                                                                             │
│  Logs stream live to CloudWatch: /aws-glue/jobs/output                     │
└─────────────────────────────────────────────────────────────────────────────┘
                          │
                          │  .write.parquet()
                          ▼
```

### Block 8 — S3 Output Folder

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  S3 — OUTPUT FOLDER  (extracted data lands here as Parquet files)           │
│                                                                             │
│  s3://tcpl-datalake/glue_poc/                                              │
│  ├── sfdc/                                                                 │
│  │   ├── SecondarySalesLineItem/                                           │
│  │   │   ├── full/2024-03-03/                                             │
│  │   │   │   ├── part-00000.snappy.parquet  ← DPU worker 1               │
│  │   │   │   ├── part-00001.snappy.parquet  ← DPU worker 2               │
│  │   │   │   └── ...                                                      │
│  │   │   └── delta/2024-03-04/                                            │
│  │   │       └── part-00000.snappy.parquet                               │
│  │   ├── VisitDetails/                                                     │
│  │   ├── Inventory/                                                        │
│  │   ├── Retailer/                                                         │
│  │   └── Account/                                                          │
│  └── sap/                                                                  │
│      ├── ACDOCA_FINANCE_DATA/                                              │
│      ├── BillingDocument/                                                  │
│      ├── BillingDocumentItem/                                              │
│      ├── BillingDocumentItemPrcgElmnt/                                     │
│      ├── SalesOrder/                                                       │
│      ├── SalesOrderItem/                                                   │
│      └── CustomerMaster/  (full load only — no delta)                     │
└─────────────────────────────────────────────────────────────────────────────┘
                          │
                          │  After S3 write → Glue updates Snowflake:
                          │  1. UPDATE GLUE_ENTITY_CONFIG SET LAST_WATERMARK
                          │  2. INSERT INTO GLUE_AUDIT_LOG (status, count)
                          │
                          │  Airflow triggers next task: Snowflake load
                          ▼
```

### Block 9 — Existing Glue Transform Job

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  EXISTING GLUE TRANSFORM JOB  (already at TCPL — you don't build this)     │
│                                                                             │
│  COPY INTO GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT                         │
│  FROM 's3://tcpl-datalake/glue_poc/sap/BillingDocument/delta/2024-03-04/' │
│  FILE_FORMAT = (TYPE = PARQUET)                                            │
│  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;                                  │
└─────────────────────────────────────────────────────────────────────────────┘
                          │
                          ▼
```

### Block 10 — Snowflake POC Schema

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  SNOWFLAKE — POC SCHEMA  (Glue data loaded here for comparison)             │
│  Database: GLUE_POC_DB                                                      │
│                                                                             │
│  GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT         ← from SAP via Glue       │
│  GLUE_POC_DB.SAPS4_CORE.ACDOCA_FINANCE_DATA                               │
│  GLUE_POC_DB.SFDC_CORE.SECONDARYSALESLINEITEM   ← from SFDC via Glue      │
│  ... all 12 tables                                                          │
└─────────────────────────────────────────────────────────────────────────────┘
                          │
                          │  Airflow triggers validation task
                          ▼
```

### Block 11 — Validation

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  VALIDATION — compare Glue data vs existing SnapLogic data                  │
│                                                                             │
│  SnapLogic data:  SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT    (existing)       │
│  Glue data:       GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT (new)            │
│                                                                             │
│  Test A — Row Count:                                                        │
│    SELECT COUNT(*) FROM SAPS4_DB...    → 5,390,000                         │
│    SELECT COUNT(*) FROM GLUE_POC_DB... → 5,390,000  ✅ MATCH               │
│                                                                             │
│  Test B — MINUS (records missing in Glue):                                 │
│    SELECT cols FROM SAPS4_DB...                                             │
│    MINUS                                                                    │
│    SELECT cols FROM GLUE_POC_DB...    → 0 rows  ✅                         │
│                                                                             │
│  Test C — MINUS (extra records in Glue):                                   │
│    SELECT cols FROM GLUE_POC_DB...                                          │
│    MINUS                                                                    │
│    SELECT cols FROM SAPS4_DB...       → 0 rows  ✅                         │
│                                                                             │
│  Test D — Checksum:                                                         │
│    MD5(LISTAGG(key_cols)) same in both → ✅                                │
│                                                                             │
│  Test E — Delta window:                                                     │
│    Records changed in date window match between both sources → ✅           │
└─────────────────────────────────────────────────────────────────────────────┘
                          │
                          ▼
```

### Block 12 — March 15 Milestone Review

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  MARCH 15 MILESTONE REVIEW                                                  │
│                                                                             │
│  Present to: Vishnu Rao (Business Sponsor) + Sam Joseph (Platform Owner)   │
│                                                                             │
│  ✅ All 12 tables match 100% → GO decision                                 │
│  ❌ Mismatches found → NO-GO + document root cause + fix plan               │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Section 3 — 12 Tables You Must Extract

### Salesforce (SFDC) — 5 tables | 194M rows | 21 GB

| Table | Rows | Size | Type | Delta Key |
|---|---|---|---|---|
| SecondarySalesLineItem | 122.6M | 13.1 GB | Delta | LastModifiedDate ← BIGGEST |
| VisitDetails | 63.4M | 6.6 GB | Delta | LastModifiedDate |
| Inventory | 5.2M | 701 MB | Delta | LastModifiedDate |
| Retailer | 2.4M | 355 MB | Delta | LastModifiedDate |
| Account | 49K | 6.8 MB | Delta | LastModifiedDate |

### SAP S/4HANA — 7 tables | 787M rows | 41 GB

| Table | Rows | Size | Type | Delta Key |
|---|---|---|---|---|
| ACDOCA_FINANCE_DATA | 418.25M | 27.6 GB | Delta | LastChangeDateTime ← BIGGEST |
| BillingDocumentItemPrcgElmnt | 311.17M | 7.7 GB | Delta | LastChangeDateTime |
| BillingDocumentItem | 24.94M | 2.1 GB | Delta | LastChangeDateTime |
| SalesOrderItem | 23.54M | 2.2 GB | Delta | LastChangeDateTime |
| BillingDocument | 5.39M | 347 MB | Delta | LastChangeDateTime |
| SalesOrder | 3.79M | 236 MB | Delta | LastChangeDateTime |
| CustomerMaster | 80K | 8.8 MB | **FULL** | always full — no delta |

> **Total: 12 tables | ~981 Million rows | ~62 GB**

---

## Section 4 — Week-by-Week What To Do

### Week 1: Feb 24 – Mar 1 | CONNECTIVITY

#### MON Feb 24 — Setup everything

- [ ] Create 3 Secrets Manager entries (SAP + SFDC + Snowflake creds)

```bash
aws secretsmanager create-secret \
  --name tcpl/glue_poc/sap_odata \
  --secret-string '{"base_url":"https://your-sap.tcpl.com/sap/opu/odata/sap/",
                    "username":"GLUE_TECH_USER","password":"xxx"}'

aws secretsmanager create-secret \
  --name tcpl/glue_poc/sfdc_oauth \
  --secret-string '{"client_id":"xxx","client_secret":"xxx",
                    "username":"glue@tcpl.com","password":"xxx",
                    "security_token":"xxx",
                    "login_url":"https://login.salesforce.com"}'

aws secretsmanager create-secret \
  --name tcpl/glue_poc/snowflake \
  --secret-string '{"account":"tcpl.ap-southeast-1","user":"GLUE_POC_USER",
                    "password":"xxx","warehouse":"TCPL_LOAD_WH",
                    "database":"PRD_ARCH_SUPPORT_DB","schema":"GLUE_POC",
                    "role":"TCPL_GLUE_POC_ROLE"}'
```

- [ ] Run Snowflake SQL — create control tables + seed all 12 entities:

```sql
USE DATABASE PRD_ARCH_SUPPORT_DB;
USE SCHEMA GLUE_POC;
CREATE OR REPLACE TABLE GLUE_ENTITY_CONFIG (...);
CREATE OR REPLACE TABLE GLUE_AUDIT_LOG (...);
INSERT INTO GLUE_ENTITY_CONFIG VALUES (...);  -- 12 rows
```

- [ ] Upload glue_utils.py to S3:

```bash
aws s3 cp glue_utils.py s3://tcpl-datalake/scripts/glue_poc/glue_utils.py
```

- [ ] Follow up on SAP firewall whitelisting (already in progress — escalate)

---

#### TUE Feb 25 — SFDC connectivity test

- [ ] Get SFDC Connected App credentials from TCPL SFDC Admin
- [ ] Upload connectivity test script:

```bash
aws s3 cp glue_poc_sfdc_connectivity_test.py \
  s3://tcpl-datalake/scripts/glue_poc/
```

- [ ] Create Glue job in Console:

```
Name:    tcpl-glue-poc-sfdc-connectivity-test
Script:  s3://tcpl-datalake/scripts/glue_poc/glue_poc_sfdc_connectivity_test.py
Role:    TCPLGluePOCRole
Workers: 2 × G.1X  (just a test — small)
```

- [ ] Click Run → watch CloudWatch logs. Expected output:

```
✅ OAuth PASSED — Instance: https://tcpl.my.salesforce.com
✅ REST API PASSED — 5 Account records returned
✅ BULK API PASSED — Job ID: 7505X...
```

- [ ] If `INVALID_LOGIN` error → ask SFDC admin for security token
- [ ] Document result ✅ or ❌ + what you saw

---

#### WED Feb 26 — SAP connectivity test ← TODAY

- [ ] Get SAP OData technical user credentials from TCPL SAP team
- [ ] Upload + create + run SAP connectivity test job (same as SFDC above)
- [ ] Expected output:

```
HTTP Status: 200
✅ SAP CONNECTIVITY TEST PASSED
Records returned: 5
Sample keys: ['BillingDocument', 'NetAmount', ...]
```

- [ ] If timeout → firewall not open yet → escalate, switch to SFDC first
- [ ] If 401 → wrong credentials
- [ ] If 404 → wrong OData service name

---

#### THU Feb 27 — Fix issues + decide which source goes first

- [ ] Fix any connectivity issues from Wed
- [ ] Decide: whichever source connected successfully → build that first
- [ ] Start writing the extraction job for the first source

---

#### FRI Feb 28 — Both connected, start extraction job

- [ ] Both connectivity tests passing ✅
- [ ] Document: HTTP status, auth method, API version (OData v2 or v4), quirks
- [ ] Upload final extraction scripts to S3:

```bash
aws s3 cp glue_poc_sfdc_extract.py s3://tcpl-datalake/scripts/glue_poc/
aws s3 cp glue_poc_sap_extract.py  s3://tcpl-datalake/scripts/glue_poc/
```

- [ ] Send Week 1 status update to manager

---

### Week 2: Mar 3 – Mar 8 | EXTRACTION (2 TABLES)

#### MON Mar 3 — Build + test Table 1 (small sample first)

- [ ] Create Glue job for Table 1 in Console:

```
Name:       tcpl-glue-poc-sfdc-extract
Script:     s3://.../glue_poc_sfdc_extract.py
Workers:    10 × G.1X
Extra libs: s3://.../glue_utils.py
Params:     --ENTITY_NAME  SecondarySalesLineItem__c
            --LOAD_TYPE    F
            --RUN_DATE     2024-03-03
```

- [ ] Run via CLI:

```bash
aws glue start-job-run \
  --job-name tcpl-glue-poc-sfdc-extract \
  --arguments '{"--ENTITY_NAME":"SecondarySalesLineItem__c",
                "--LOAD_TYPE":"F","--RUN_DATE":"2024-03-03"}'
```

- [ ] Watch CloudWatch — you should see:

```
Authenticating with Salesforce OAuth2...
Authenticated ✅
Submitting Bulk Query: SELECT Id, Name, ... FROM SecondarySalesLineItem__c
Job submitted → JobID: 7505X...
Job state: InProgress | Records: 1,234,567
...
Writing 122,600,000 rows → s3://tcpl-datalake/glue_poc/sfdc/.../full/2024-03-03/
Write complete ✅
Audit logged → SecondarySalesLineItem | SUCCESS | 122600000 rows
```

- [ ] Verify in S3:

```bash
aws s3 ls s3://tcpl-datalake/glue_poc/sfdc/SecondarySalesLineItem/full/2024-03-03/
# should see part-00000.snappy.parquet etc.
```

---

#### TUE Mar 4 — Full load Table 1 + verify

- [ ] Confirm full load S3 file count and total size looks right
- [ ] Check GLUE_AUDIT_LOG in Snowflake:

```sql
SELECT * FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_AUDIT_LOG
ORDER BY CREATED_AT DESC;
-- Should show: SecondarySalesLineItem | FULL | 122600000 | SUCCESS
```

---

#### WED Mar 5 — Build Table 2

- [ ] Same process for Table 2 (e.g., BillingDocument from SAP)
- [ ] Create Glue job: `tcpl-glue-poc-sap-extract`
- [ ] Run with `ENTITY_NAME = BillingDocument`, `LOAD_TYPE = F`

---

#### THU Mar 6 — Full load Table 2 + Delta test on Table 1

- [ ] Confirm Table 2 full load successful in S3 + audit log
- [ ] Run delta load for Table 1:

```bash
aws glue start-job-run \
  --job-name tcpl-glue-poc-sfdc-extract \
  --arguments '{"--ENTITY_NAME":"SecondarySalesLineItem__c",
                "--LOAD_TYPE":"D","--RUN_DATE":"2024-03-04"}'
```

- [ ] Check: only new/changed records since last watermark extracted
- [ ] Check: `LAST_WATERMARK` updated in `GLUE_ENTITY_CONFIG`

---

#### FRI Mar 7–8 — Fix issues + verify delta watermarks

- [ ] Both delta loads confirmed working ✅
- [ ] Audit log has clean entries for all runs ✅
- [ ] Send Week 2 status to manager

---

### Week 3: Mar 10 – Mar 15 | SNOWFLAKE LOAD + VALIDATION

#### MON Mar 10 — Load Table 1 to Snowflake POC schema

- [ ] Trigger existing Glue transform job pointing to new S3 path:

```sql
COPY INTO GLUE_POC_DB.SFDC_CORE.SECONDARYSALESLINEITEM
FROM 's3://tcpl-datalake/glue_poc/sfdc/SecondarySalesLineItem/full/2024-03-03/'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

- [ ] Verify row count in Snowflake:

```sql
SELECT COUNT(*) FROM GLUE_POC_DB.SFDC_CORE.SECONDARYSALESLINEITEM;
-- Expected: 122,600,000
```

---

#### TUE Mar 11 — Load Table 2 + row count validation

- [ ] Load BillingDocument to `GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT`
- [ ] Run Validation A (row count comparison):

```sql
SELECT
  'BillingDocument'                                               AS table_name,
  (SELECT COUNT(*) FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT)     AS snaplogic_count,
  (SELECT COUNT(*) FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT)  AS glue_count,
  snaplogic_count = glue_count                                    AS is_match;
-- Expected: is_match = TRUE ✅
```

---

#### WED Mar 12 — MINUS queries + checksum validation

- [ ] Validation B — records missing in Glue:

```sql
SELECT BILLINGDOCUMENT, BILLINGDOCUMENTITEM
FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT
MINUS
SELECT BILLINGDOCUMENT, BILLINGDOCUMENTITEM
FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT;
-- Expected: 0 rows ✅
```

- [ ] Validation C — extra records in Glue:

```sql
SELECT BILLINGDOCUMENT, BILLINGDOCUMENTITEM
FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT
MINUS
SELECT BILLINGDOCUMENT, BILLINGDOCUMENTITEM
FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT;
-- Expected: 0 rows ✅
```

- [ ] Validation D — Checksum:

```sql
SELECT 'SnapLogic', MD5(LISTAGG(BILLINGDOCUMENT||NETAMOUNT,',')
                        WITHIN GROUP (ORDER BY BILLINGDOCUMENT))
FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT
UNION ALL
SELECT 'Glue',      MD5(LISTAGG(BILLINGDOCUMENT||NETAMOUNT,',')
                        WITHIN GROUP (ORDER BY BILLINGDOCUMENT))
FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT;
-- Expected: both checksums identical ✅
```

- [ ] Validation E — Delta window check:

```sql
SELECT 'SnapLogic delta', COUNT(*)
FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT
WHERE LASTCHANGEDATETIME BETWEEN '2024-03-03' AND '2024-03-04'
UNION ALL
SELECT 'Glue delta', COUNT(*)
FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT
WHERE _LOAD_TYPE = 'DELTA' AND _EXTRACTED_AT::DATE = '2024-03-04';
-- Expected: counts match ✅
```

---

#### THU Mar 13 — Document everything

- [ ] Fill in validation report:

| Table | SnapLogic | Glue | Match? |
|---|---|---|---|
| BillingDocument | 5,390,000 | 5,390,000 | ✅ YES |
| SecondarySalesLineItem | 122.6M | 122.6M | ✅ YES |
| ... all 12 tables | | | |

- [ ] Note performance timings:

| Table | SnapLogic | Glue | Faster? |
|---|---|---|---|
| BillingDocument (5.4M) | 8 min | 6 min | ✅ 25% faster |
| SecondarySalesLineItem (122M) | 45 min | 38 min | ✅ 16% faster |

- [ ] Document any issues + how they were resolved

---

#### FRI Mar 14–15 — Milestone Review

- [ ] Complete Go/No-Go recommendation:
  - ✅ **GO** → 100% match, proceed with full migration (250+ pipelines)
  - ❌ **NO-GO** → list mismatches, root causes, proposed fixes
- [ ] Present to Vishnu Rao + Sam Joseph
- [ ] **MILESTONE COMPLETE 🎯**

---

## Section 5 — IAM Role Needed

`TCPLGluePOCRole` must have:

| Permission | Resource |
|---|---|
| `AWSGlueServiceRole` (AWS managed) | — |
| S3 Read/Write | `s3://tcpl-datalake/glue_poc/*` |
| S3 Read | `s3://tcpl-datalake/scripts/glue_poc/*` |
| Secrets Manager `GetSecretValue` | `tcpl/glue_poc/*` |
| CloudWatch Logs Write | `/aws-glue/*` |

---

## Section 6 — Common Issues + Fixes

| Issue | Symptom | Fix |
|---|---|---|
| SAP firewall not open | Connection timeout | Escalate daily to infra. Build SAP code meanwhile. Switch to SFDC first. |
| SFDC INVALID_LOGIN | OAuth 400 error | Add `security_token` to secret. Ask SFDC admin to set IP Relaxation on Connected App. |
| SAP pagination stops early | Only 5000 rows extracted even with more data | Check response for `nextLink`. Use `data.get("@odata.nextLink")` or `data.get("d",{}).get("__next")` |
| Glue OOM on 122M SFDC table | Job killed, no output | Write page-by-page to S3. Increase DPUs 10 → 20. |
| Watermark not updated | Data in S3 but `LAST_WATERMARK` unchanged | Wrap `update_watermark()` in `try-finally`. Manually fix: `UPDATE GLUE_ENTITY_CONFIG SET LAST_WATERMARK = '2024-03-04' WHERE SRC_OBJ_NM = 'BillingDocument'` |
| MINUS returns rows despite matching row counts | Row counts match but data differs | Cast both to same type. Check timestamp/numeric format. Use `CAST(col AS VARCHAR)` in MINUS. |

---

## Section 7 — Final Checklist for March 15

### Connectivity
- [ ] SAP OData connectivity test PASSED ✅
- [ ] SFDC Bulk API 2.0 connectivity test PASSED ✅
- [ ] All credentials in Secrets Manager ✅

### Control Tables
- [ ] `GLUE_ENTITY_CONFIG` created + seeded with 12 entities ✅
- [ ] `GLUE_AUDIT_LOG` created ✅

### Extraction
- [ ] SFDC extraction job built and tested ✅
- [ ] SAP extraction job built and tested ✅
- [ ] Full load working for at least 2 tables ✅
- [ ] Delta load working + watermark updates correctly ✅
- [ ] Output in S3 as Parquet ✅
- [ ] `GLUE_AUDIT_LOG` populated with run details ✅

### Snowflake Load
- [ ] Table 1 loaded to `GLUE_POC_DB` schema ✅
- [ ] Table 2 loaded to `GLUE_POC_DB` schema ✅

### Validation
- [ ] Row count: 100% match ✅
- [ ] MINUS queries: 0 records returned ✅
- [ ] Delta validation: correct records captured ✅
- [ ] Checksum: matching ✅

### Documentation
- [ ] Validation report written ✅
- [ ] Performance comparison documented ✅
- [ ] Issues + resolutions documented ✅
- [ ] Go/No-Go recommendation ready ✅

---

> You are replacing SnapLogic with AWS Glue for TCPL's data extraction layer.
> **12 tables. 981M records. 62 GB. 3 weeks. Let's go 💪**
