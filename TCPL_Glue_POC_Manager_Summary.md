# TCPL AWS Glue POC — What I've Understood & How We'll Do It

---

## 🧠 What I've Understood (If My Manager Asks)

### The Problem We're Solving

Right now, TCPL's data pipeline looks like this:

```
Airflow → SnapLogic → S3 → Glue (Transform) → Snowflake
```

SnapLogic is a third-party tool that costs money per pipeline/connector. At TCPL's scale — 250+ pipelines — this adds up significantly. The business wants to find out: **can AWS Glue replace SnapLogic for the extraction part?**

So the job of this POC is to prove that AWS Glue can pull data directly from SAP and Salesforce just as well as SnapLogic does — and if it can, TCPL will save cost and simplify the whole stack.

---

### What We're Actually Building

We're building the **extraction layer** — that's the part that connects to source systems and pulls data into S3. This is different from the Glue jobs that already exist at TCPL, which do transformation (S3 → Snowflake). **This is new work.**

The target architecture looks like this:

```
Airflow → AWS Glue (New — we're building this) → S3 → Glue Transform → Snowflake
```

---

### The Two Source Systems

**Salesforce (SFDC) — 5 tables, ~194 Million records, ~21 GB**
- We connect via Bulk API 2.0 (the right API for large-scale extracts)
- Key tables: SecondarySalesLineItem (122.6M rows — the biggest), VisitDetails, Inventory, Retailer, Account
- All are delta loads — we track changes using `LastModifiedDate`

**SAP S/4HANA — 7 tables, ~787 Million records, ~41 GB**
- We connect via OData API
- Key tables: ACDOCA Finance (418M rows — the largest table overall), BillingDocument, SalesOrder, SalesOrderItem, and others
- 6 are delta loads using `LastChangeDateTime`; CustomerMaster is always a full load

**Grand total: ~981 Million records, ~62 GB across 12 tables**

---

### How Delta / Incremental Loading Works

We maintain a **control table in Snowflake** called `GLUE_ENTITY_CONFIG`. This stores the last watermark (timestamp) for each table. When a Glue job runs:
1. It reads the last watermark from the control table
2. It fetches only records changed **after** that timestamp (with a 2-hour overlap buffer to catch late-arriving records)
3. After a successful run, it updates the watermark to "now"

This means each daily run only pulls what changed — not the full dataset every time.

---

### How We Validate Success

At the end, we compare data extracted by Glue vs what SnapLogic already extracted, using:
- **Row count comparison** — does Glue get the same number of records?
- **MINUS queries in Snowflake** — are there any records in SnapLogic's data that are missing from Glue's, or vice versa?
- **Checksum comparison** — do key column hashes match?

If all 12 tables match 100% → **Go decision** → full migration begins.
If there are gaps → **No-Go** → we document findings and investigate.

---

### The 3-Week Plan at a Glance

| Week | Dates | What We're Doing |
|------|-------|-----------------|
| Week 1 | Feb 24 – Mar 1 | Connectivity — get Glue talking to SAP and SFDC |
| Week 2 | Mar 3 – Mar 8 | Build extraction jobs, run full loads for 2 tables |
| Week 3 | Mar 10 – Mar 15 | Load to Snowflake, validate, write the report |

**Milestone: March 15** — present findings to Vishnu Rao (Business Sponsor) and Sam Joseph (Platform Owner).

---

## 🛠️ How We'll Do It (Rough Approach)

### Step 1 — Set Up Infrastructure (Day 1, before any code)

- Create **3 secrets in AWS Secrets Manager**: one for SAP credentials, one for Salesforce OAuth, one for Snowflake
- Set up the **IAM role** (`TCPLGluePOCRole`) with permissions for S3, Secrets Manager, and CloudWatch logs
- Define the **S3 folder structure** under `s3://tcpl-datalake/glue_poc/sfdc/` and `/sap/` — each table gets a `full/` and `delta/` subfolder by run date

---

### Step 2 — Create Control Tables in Snowflake

Run SQL to create two tables in `PRD_ARCH_SUPPORT_DB.GLUE_POC`:

- **`GLUE_ENTITY_CONFIG`** — stores config for all 12 entities (source system, API name, object name, load type, watermark column, last watermark timestamp, S3 path, batch size)
- **`GLUE_AUDIT_LOG`** — records every job run with start time, end time, record count, S3 path, status, and any error messages

Then seed `GLUE_ENTITY_CONFIG` with all 12 entities.

---

### Step 3 — Run Connectivity Tests First (Week 1)

Before writing any extraction code, we run lightweight test jobs just to confirm Glue can reach the source systems:

- **SAP test**: Fetch 5 records from `BillingDocument` via OData — confirms firewall is open, credentials work, and we understand the response format (OData v2 vs v4 matters for pagination)
- **SFDC test**: Get OAuth token, run a tiny SOQL query, submit a small Bulk API job — confirms Connected App setup is correct

> **Key blocker to watch**: SAP firewall whitelisting is In Progress. If it's not done by Wed Feb 26, we switch to SFDC first and build SAP code offline with mock data.

---

### Step 4 — Build the Shared Utility Module (`glue_utils.py`)

Before the extraction jobs, we write one shared utility file that both the SAP and SFDC jobs will import. It handles:
- Fetching secrets from Secrets Manager
- Getting/updating Snowflake watermarks
- Writing audit log entries
- Building the S3 output path (with full/delta/date structure)
- Writing Spark DataFrames to S3 as Parquet (Snappy compressed)

This avoids duplicating the same boilerplate code in every job.

---

### Step 5 — Build SFDC Extraction Job

The Salesforce job (`glue_poc_sfdc_extract.py`) does this:

1. Authenticates using OAuth2 username-password flow → gets access token
2. Dynamically fetches all fields for the object via the Describe API
3. Builds a SOQL query — for delta loads, adds a `WHERE LastModifiedDate > <watermark>` filter
4. Submits a **Bulk API 2.0** job (async — SFDC processes it in the background)
5. Polls every 15-30 seconds until the job is `JobComplete`
6. Downloads results page by page using the `Sforce-Locator` header for pagination
7. Converts records to a Spark DataFrame, adds metadata columns (`_extracted_at`, `_load_type`, `_source`, `_job_run_id`)
8. Writes to S3 as Parquet
9. Updates the watermark in Snowflake

The job takes `ENTITY_NAME`, `LOAD_TYPE`, and `RUN_DATE` as parameters so one job script handles all 5 SFDC tables.

---

### Step 6 — Build SAP Extraction Job

The SAP job (`glue_poc_sap_extract.py`) does this:

1. Authenticates using Basic Auth (username + password from Secrets Manager)
2. Builds an OData URL with `$filter`, `$top`, `$skip`, `$orderby`, `$format=json`
3. For delta loads, adds `LastChangeDateTime gt datetime'<watermark-2hrs>'` to the filter
4. Pages through results using `$skip/$top` — fetches 5000 records per page, increments `$skip`, stops when a page returns fewer records than `$top`
5. Handles rate limiting (HTTP 429 → sleep and retry)
6. Handles both OData v2 (`d.results`) and OData v4 (`value`) response formats
7. Writes pages to Spark DataFrames in batches of 50 pages to avoid OOM on very large tables (418M row ACDOCA)
8. Writes to S3 as Parquet chunks, then updates watermark

---

### Step 7 — Orchestrate with Airflow

We create one Airflow DAG (`tcpl_glue_poc_pipeline`) that:
- Runs daily at 2AM IST
- Triggers all 5 SFDC extraction tasks in parallel
- Triggers all 7 SAP extraction tasks in parallel
- After all extractions complete, runs the Snowflake load jobs (using existing TCPL Glue transform templates)
- Runs validation queries to check row counts

Each table gets its own Airflow task with the right `ENTITY_NAME` and DPU count (bigger tables get more DPUs — SecondarySalesLineItem gets 10, Account gets 2).

---

### Step 8 — Validate and Report (Week 3)

We run these validation checks in Snowflake and document results:

- **Validation A**: Row count comparison across all 12 tables (Glue vs SnapLogic)
- **Validation B**: `MINUS` query — records in SnapLogic missing from Glue (expected: 0 rows)
- **Validation C**: `MINUS` query — extra records in Glue not in SnapLogic (expected: 0 rows)
- **Validation D**: MD5 checksum on key columns — hashes must match
- **Validation E**: Delta window check — did the delta load capture the right records for the right time window?
- **Validation F**: Audit log health check — all jobs showing SUCCESS? Any errors?

We document: row counts, MINUS results, performance comparison (Glue duration vs SnapLogic duration), and write a **Go/No-Go recommendation** with evidence.

---

## 🚨 Known Risks and How We'll Handle Them

| Risk | Mitigation |
|------|-----------|
| SAP firewall not open by Wed Feb 26 | Switch to SFDC first, build SAP code offline with mock data |
| SFDC OAuth `INVALID_LOGIN` error | Verify security token in Secrets Manager; ask SFDC Admin to set IP Relaxation on Connected App |
| SAP OData pagination stopping early | Check for `@odata.nextLink` (v4) vs `d.__next` (v2) in response |
| OOM on large tables (122M+ rows) | Write in 50-page chunks, increase DPU count to 20 |
| Watermark not updating after success | Wrap in `try-finally` to guarantee the update call runs; check audit log for mismatch |
| MINUS query finds differences even with matching row counts | Check for numeric precision or timestamp format differences; add explicit `CAST` in validation queries |

---

## ✅ Definition of Done (March 15)

- [ ] Both SAP and SFDC connectivity tests passed and documented
- [ ] Control tables created and seeded in Snowflake
- [ ] Full load working for at least 2 tables (one SAP, one SFDC)
- [ ] Delta / incremental load working with correct watermark updates
- [ ] Data landed in S3 as Parquet, audit log populated
- [ ] Tables loaded into Snowflake POC schema
- [ ] All validation checks passed — 100% row count match, 0-row MINUS queries
- [ ] Validation report written with Go/No-Go recommendation

---

*Goal: Replace SnapLogic with AWS Glue for TCPL's data extraction layer. 12 tables. 981M records. 62 GB. 3 weeks.*
