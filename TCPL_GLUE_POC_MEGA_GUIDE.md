# 🚀 TCPL GLUE POC — MEGA IMPLEMENTATION GUIDE
## Complete End-to-End: Everything You Need to Build This From Scratch
### Deadline: March 15 | Author: Follow this file every single day

---

> **YOUR JOB IN ONE LINE:**
> Build Python scripts (Glue jobs) that connect to SAP and Salesforce, pull data, and dump it as Parquet files into S3.
> The transformation (S3 → Snowflake) is ALREADY DONE by someone else. You don't touch that part.

---

## 🗺️ THE BIG PICTURE

```
CURRENT STATE (today):
  Airflow → SnapLogic → S3 → [Existing Glue Template] → Snowflake
                 ↑
         This costs money. You are replacing it.

TARGET STATE (what you are building):
  Airflow → YOUR NEW Glue Jobs → S3 → [Existing Glue Template] → Snowflake
                 ↑
         This is what you build. Your job ends at S3.

YOUR FILES TO BUILD:
  ✅ glue_utils.py                    (shared helper — write once, used everywhere)
  ✅ glue_poc_sfdc_connectivity_test.py   (Week 1)
  ✅ glue_poc_sap_connectivity_test.py    (Week 1)
  ✅ glue_poc_sfdc_extract.py             (Week 2)
  ✅ glue_poc_sap_extract.py              (Week 2)
  ✅ tcpl_glue_poc_dag.py                 (Week 3)
  ✅ Snowflake SQL scripts                (Week 1 + Week 3)

NOT YOUR JOB:
  ❌ s3_to_snowflake transformation jobs — already exist, just reuse
```

---

## 📊 12 TABLES YOU WILL EXTRACT

### SFDC Tables (5) — via Salesforce Bulk API 2.0

| Table | Rows | Size | Load Type | Delta Key |
|---|---|---|---|---|
| SecondarySalesLineItem__c | 122.6M | 13.1 GB | Delta | LastModifiedDate |
| VisitDetails__c | 63.4M | 6.6 GB | Delta | LastModifiedDate |
| Inventory__c | 5.2M | 701 MB | Delta | LastModifiedDate |
| Retailer__c | 2.4M | 355 MB | Delta | LastModifiedDate |
| Account | 49K | 6.8 MB | Delta | LastModifiedDate |

### SAP Tables (7) — via SAP OData API

| Table | Rows | Size | Load Type | Delta Key |
|---|---|---|---|---|
| ACDOCA_FINANCE_DATA | 418.25M | 27.6 GB | Delta | LastChangeDateTime |
| BillingDocumentItemPrcgElmnt | 311.17M | 7.7 GB | Delta | LastChangeDateTime |
| BillingDocumentItem | 24.94M | 2.1 GB | Delta | LastChangeDateTime |
| BillingDocument | 5.39M | 347 MB | Delta | LastChangeDateTime |
| SalesOrderItem | 23.54M | 2.2 GB | Delta | LastChangeDateTime |
| SalesOrder | 3.79M | 236 MB | Delta | LastChangeDateTime |
| CustomerMaster | 80K | 8.8 MB | **FULL ONLY** | No delta |

**TOTAL: ~981 Million records, ~62 GB**

---

## 📅 3-WEEK PLAN AT A GLANCE

```
WEEK 1 (Feb 24 – Mar 1):   AWS setup + Snowflake tables + Connectivity tests
WEEK 2 (Mar 3 – Mar 8):    Build extraction jobs + Run full loads for 2 tables
WEEK 3 (Mar 10 – Mar 15):  Load to Snowflake + Validate + Present findings
```

---
---

# ═══════════════════════════════════════
# WEEK 1: SETUP + CONNECTIVITY
# ═══════════════════════════════════════

---

## 📌 DAY 1 (Mon Feb 24) — AWS Infrastructure Setup

### STEP 0A: S3 Folder Structure

> You don't need to create these manually. Glue will create them automatically when it writes data. Just confirm the bucket `tcpl-datalake` exists.

```
s3://tcpl-datalake/glue_poc/
├── sfdc/
│   ├── SecondarySalesLineItem/
│   │   ├── full/2024-03-03/part-00000.parquet
│   │   └── delta/2024-03-04/part-00000.parquet
│   ├── VisitDetails/
│   ├── Inventory/
│   ├── Retailer/
│   └── Account/
└── sap/
    ├── ACDOCA_FINANCE_DATA/
    ├── BillingDocumentItemPrcgElmnt/
    ├── BillingDocumentItem/
    ├── BillingDocument/
    ├── SalesOrderItem/
    ├── SalesOrder/
    └── CustomerMaster/
        └── full/2024-03-03/   ← CustomerMaster is always FULL load only
```

---

### STEP 0B: IAM Role for Glue Jobs

Go to AWS Console → IAM → Roles → Find or create `TCPLGluePOCRole`

Attach the managed policy: `AWSGlueServiceRole`

Also attach this inline policy:

```json
{
  "PolicyName": "TCPLGluePOCPolicy",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::tcpl-datalake",
        "arn:aws:s3:::tcpl-datalake/glue_poc/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["secretsmanager:GetSecretValue"],
      "Resource": [
        "arn:aws:secretsmanager:ap-south-1:*:secret:tcpl/glue_poc/sap*",
        "arn:aws:secretsmanager:ap-south-1:*:secret:tcpl/glue_poc/sfdc*",
        "arn:aws:secretsmanager:ap-south-1:*:secret:tcpl/glue_poc/snowflake*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"],
      "Resource": "arn:aws:logs:ap-south-1:*:log-group:/aws-glue/*"
    }
  ]
}
```

---

### STEP 0C: Store Credentials in AWS Secrets Manager

Go to: AWS Console → Secrets Manager → Store a new secret → Other type of secret

Create these 3 secrets. Fill in the actual values from TCPL teams.

**Secret 1 — Name:** `tcpl/glue_poc/sap_odata`
```json
{
    "base_url":  "https://your-sap-host.tcpl.com/sap/opu/odata/sap/",
    "username":  "GLUE_TECHNICAL_USER",
    "password":  "your-sap-password"
}
```

**Secret 2 — Name:** `tcpl/glue_poc/sfdc_oauth`
```json
{
    "client_id":      "your-sfdc-connected-app-client-id",
    "client_secret":  "your-sfdc-connected-app-secret",
    "username":       "glue-integration@tcpl.com",
    "password":       "your-sfdc-password",
    "security_token": "your-sfdc-security-token",
    "login_url":      "https://login.salesforce.com"
}
```

> ⚠️ **security_token** — Ask TCPL SFDC Admin. They get it from: SFDC → Settings → My Personal Information → Reset My Security Token

**Secret 3 — Name:** `tcpl/glue_poc/snowflake`
```json
{
    "account":   "tcpl.ap-southeast-1",
    "user":      "GLUE_POC_USER",
    "password":  "your-sf-password",
    "warehouse": "TCPL_LOAD_WH",
    "database":  "PRD_ARCH_SUPPORT_DB",
    "schema":    "GLUE_POC",
    "role":      "TCPL_GLUE_POC_ROLE"
}
```

---

### STEP 1: Create Snowflake Control Tables

Open Snowflake → Use database `PRD_ARCH_SUPPORT_DB` → schema `GLUE_POC` → Run ALL of this SQL:

```sql
-- ══════════════════════════════════════════════════════════════════════
-- STEP 1: Create Control Tables + Seed All 12 Entities
-- ══════════════════════════════════════════════════════════════════════

USE DATABASE PRD_ARCH_SUPPORT_DB;
USE SCHEMA GLUE_POC;
USE WAREHOUSE TCPL_LOAD_WH;

-- ── Table 1: GLUE_ENTITY_CONFIG ──────────────────────────────────────
-- Stores entity config + watermarks. Glue reads this before every run.
CREATE OR REPLACE TABLE GLUE_ENTITY_CONFIG (
    CONFIG_ID         NUMBER AUTOINCREMENT PRIMARY KEY,
    SRC_SYS_NM        VARCHAR(20)   NOT NULL,   -- SAPS4 or SFDC
    SRC_API_NM        VARCHAR(200)  NOT NULL,   -- API endpoint name
    SRC_OBJ_NM        VARCHAR(100)  NOT NULL,   -- Object/entity name
    RELATIVE_PATH     VARCHAR(100)  NOT NULL,   -- S3 sub-path
    LOAD_TYP          VARCHAR(1)    NOT NULL,   -- D=Delta, F=Full
    REQUIRED_FLAG     VARCHAR(1)    DEFAULT 'Y',-- Y=Active, N=Inactive
    CDC_WATERMARK_COL VARCHAR(100),             -- Column for delta tracking
    LAST_WATERMARK    TIMESTAMP_NTZ,            -- Last extracted timestamp
    BATCH_SIZE        NUMBER        DEFAULT 5000,
    STATIC_PARAMS     VARCHAR(500),             -- Extra OData filters
    S3_PATH           VARCHAR(200)  NOT NULL,   -- Target S3 path
    CREATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── Table 2: GLUE_AUDIT_LOG ──────────────────────────────────────────
-- Every Glue job run writes one row here — success or failure.
CREATE OR REPLACE TABLE GLUE_AUDIT_LOG (
    AUDIT_ID         NUMBER AUTOINCREMENT PRIMARY KEY,
    GLUE_JOB_RUN_ID  VARCHAR(100),
    SRC_SYS_NM       VARCHAR(20),
    ENTITY_NAME      VARCHAR(100),
    LOAD_TYPE        VARCHAR(10),              -- FULL or DELTA
    WATERMARK_FROM   TIMESTAMP_NTZ,
    WATERMARK_TO     TIMESTAMP_NTZ,
    START_TIME       TIMESTAMP_NTZ,
    END_TIME         TIMESTAMP_NTZ,
    DURATION_SECONDS NUMBER,
    RECORD_COUNT     NUMBER,
    S3_PATH          VARCHAR(500),
    STATUS           VARCHAR(20),              -- SUCCESS, FAILED, IN_PROGRESS
    ERROR_MESSAGE    VARCHAR(2000),
    CREATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── Seed Data: SFDC Tables (5) ────────────────────────────────────────
INSERT INTO GLUE_ENTITY_CONFIG
    (SRC_SYS_NM, SRC_API_NM, SRC_OBJ_NM, RELATIVE_PATH, LOAD_TYP,
     REQUIRED_FLAG, CDC_WATERMARK_COL, LAST_WATERMARK, BATCH_SIZE, S3_PATH)
VALUES
('SFDC', 'bulk_api_v2', 'SecondarySalesLineItem__c',
 'sfdc/SecondarySalesLineItem', 'D', 'Y',
 'LastModifiedDate', '2020-01-01 00:00:00', 50000,
 's3://tcpl-datalake/glue_poc/sfdc/SecondarySalesLineItem/'),

('SFDC', 'bulk_api_v2', 'VisitDetails__c',
 'sfdc/VisitDetails', 'D', 'Y',
 'LastModifiedDate', '2020-01-01 00:00:00', 50000,
 's3://tcpl-datalake/glue_poc/sfdc/VisitDetails/'),

('SFDC', 'bulk_api_v2', 'Inventory__c',
 'sfdc/Inventory', 'D', 'Y',
 'LastModifiedDate', '2020-01-01 00:00:00', 20000,
 's3://tcpl-datalake/glue_poc/sfdc/Inventory/'),

('SFDC', 'bulk_api_v2', 'Retailer__c',
 'sfdc/Retailer', 'D', 'Y',
 'LastModifiedDate', '2020-01-01 00:00:00', 10000,
 's3://tcpl-datalake/glue_poc/sfdc/Retailer/'),

('SFDC', 'bulk_api_v2', 'Account',
 'sfdc/Account', 'D', 'Y',
 'LastModifiedDate', '2020-01-01 00:00:00', 5000,
 's3://tcpl-datalake/glue_poc/sfdc/Account/');

-- ── Seed Data: SAP Tables (7) ─────────────────────────────────────────
INSERT INTO GLUE_ENTITY_CONFIG
    (SRC_SYS_NM, SRC_API_NM, SRC_OBJ_NM, RELATIVE_PATH, LOAD_TYP,
     REQUIRED_FLAG, CDC_WATERMARK_COL, LAST_WATERMARK, BATCH_SIZE,
     STATIC_PARAMS, S3_PATH)
VALUES
('SAPS4', 'ZGLUE_ACDOCA_SRV', 'ACDOCA_FINANCE_DATA',
 'sap/ACDOCA_FINANCE_DATA', 'D', 'Y',
 'LastChangeDateTime', '2020-01-01T00:00:00', 5000, NULL,
 's3://tcpl-datalake/glue_poc/sap/ACDOCA_FINANCE_DATA/'),

('SAPS4', 'API_BILLING_DOCUMENT_SRV', 'BillingDocumentItemPrcgElmnt',
 'sap/BillingDocumentItemPrcgElmnt', 'D', 'Y',
 'LastChangeDateTime', '2020-01-01T00:00:00', 5000, NULL,
 's3://tcpl-datalake/glue_poc/sap/BillingDocumentItemPrcgElmnt/'),

('SAPS4', 'API_BILLING_DOCUMENT_SRV', 'BillingDocumentItem',
 'sap/BillingDocumentItem', 'D', 'Y',
 'LastChangeDateTime', '2020-01-01T00:00:00', 5000, NULL,
 's3://tcpl-datalake/glue_poc/sap/BillingDocumentItem/'),

('SAPS4', 'API_BILLING_DOCUMENT_SRV', 'BillingDocument',
 'sap/BillingDocument', 'D', 'Y',
 'LastChangeDateTime', '2020-01-01T00:00:00', 5000, NULL,
 's3://tcpl-datalake/glue_poc/sap/BillingDocument/'),

('SAPS4', 'API_SALES_ORDER_SRV', 'SalesOrderItem',
 'sap/SalesOrderItem', 'D', 'Y',
 'LastChangeDateTime', '2020-01-01T00:00:00', 5000, NULL,
 's3://tcpl-datalake/glue_poc/sap/SalesOrderItem/'),

('SAPS4', 'API_SALES_ORDER_SRV', 'SalesOrder',
 'sap/SalesOrder', 'D', 'Y',
 'LastChangeDateTime', '2020-01-01T00:00:00', 5000,
 'SalesOrderType eq ''OR'' or SalesOrderType eq ''ZOR''',
 's3://tcpl-datalake/glue_poc/sap/SalesOrder/'),

-- CustomerMaster: FULL LOAD only, no watermark
('SAPS4', 'API_BUSINESS_PARTNER_SRV', 'CustomerMaster',
 'sap/CustomerMaster', 'F', 'Y',
 NULL, NULL, 5000, NULL,
 's3://tcpl-datalake/glue_poc/sap/CustomerMaster/');

-- ── Verify: should see 12 rows ────────────────────────────────────────
SELECT SRC_SYS_NM, SRC_OBJ_NM, LOAD_TYP, BATCH_SIZE, LAST_WATERMARK
FROM GLUE_ENTITY_CONFIG
ORDER BY SRC_SYS_NM, CONFIG_ID;
-- Expected: 12 rows — 5 SFDC + 7 SAP
```

---

### STEP 2: Upload `glue_utils.py` to S3

This is a **shared helper file** used by ALL Glue jobs. You write it once, upload once, and every job uses it.

**Where to upload:**
`s3://tcpl-datalake/scripts/glue_poc/glue_utils.py`

**In EVERY Glue job, add this parameter:**
`--extra-py-files` = `s3://tcpl-datalake/scripts/glue_poc/glue_utils.py`

### FILE: `glue_utils.py`

```python
"""
glue_utils.py
─────────────
Shared utilities used by ALL Glue extraction jobs.
Upload ONCE to: s3://tcpl-datalake/scripts/glue_poc/glue_utils.py
Add to every Glue job: --extra-py-files s3://tcpl-datalake/scripts/glue_poc/glue_utils.py
"""

import boto3
import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import snowflake.connector

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ══════════════════════════════════════════════════════════════════════
# SECRETS MANAGER
# ══════════════════════════════════════════════════════════════════════

def get_secret(secret_name: str, region: str = "ap-south-1") -> Dict[str, Any]:
    """Fetch secret JSON from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


# ══════════════════════════════════════════════════════════════════════
# SNOWFLAKE HELPERS
# ══════════════════════════════════════════════════════════════════════

def get_snowflake_conn(sf_creds: Dict) -> snowflake.connector.SnowflakeConnection:
    """Return a Snowflake connection from credentials dict."""
    return snowflake.connector.connect(
        account=sf_creds["account"],
        user=sf_creds["user"],
        password=sf_creds["password"],
        warehouse=sf_creds["warehouse"],
        database=sf_creds["database"],
        schema=sf_creds["schema"],
        role=sf_creds["role"],
        session_parameters={"QUERY_TAG": "GLUE_POC_EXTRACTION"},
    )


def get_entity_config(conn: snowflake.connector.SnowflakeConnection,
                      src_sys: str, entity_name: str) -> Dict:
    """Read entity config row from GLUE_ENTITY_CONFIG."""
    cursor = conn.cursor(snowflake.connector.DictCursor)
    cursor.execute("""
        SELECT CONFIG_ID, SRC_SYS_NM, SRC_API_NM, SRC_OBJ_NM,
               RELATIVE_PATH, LOAD_TYP, CDC_WATERMARK_COL,
               LAST_WATERMARK, BATCH_SIZE, STATIC_PARAMS, S3_PATH
        FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_ENTITY_CONFIG
        WHERE SRC_SYS_NM = %s
          AND SRC_OBJ_NM = %s
          AND REQUIRED_FLAG = 'Y'
    """, (src_sys, entity_name))
    row = cursor.fetchone()
    if not row:
        raise ValueError(f"No active config found for {src_sys}.{entity_name}")
    return dict(row)


def update_watermark(conn: snowflake.connector.SnowflakeConnection,
                     config_id: int, new_watermark: datetime) -> None:
    """Update LAST_WATERMARK after successful extraction."""
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_ENTITY_CONFIG
        SET LAST_WATERMARK = %s,
            UPDATED_AT     = CURRENT_TIMESTAMP()
        WHERE CONFIG_ID = %s
    """, (new_watermark.strftime("%Y-%m-%d %H:%M:%S"), config_id))
    conn.commit()
    logger.info(f"Watermark updated → ConfigID={config_id}, New={new_watermark}")


def log_audit(conn: snowflake.connector.SnowflakeConnection,
              job_run_id: str, src_sys: str, entity: str,
              load_type: str, wm_from, wm_to,
              start_time: datetime, end_time: datetime,
              record_count: int, s3_path: str, status: str,
              error_msg: Optional[str] = None) -> None:
    """Insert execution record into GLUE_AUDIT_LOG."""
    duration = int((end_time - start_time).total_seconds())
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_AUDIT_LOG (
            GLUE_JOB_RUN_ID, SRC_SYS_NM, ENTITY_NAME, LOAD_TYPE,
            WATERMARK_FROM, WATERMARK_TO, START_TIME, END_TIME,
            DURATION_SECONDS, RECORD_COUNT, S3_PATH, STATUS, ERROR_MESSAGE
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        job_run_id, src_sys, entity, load_type,
        wm_from.strftime("%Y-%m-%d %H:%M:%S") if wm_from else None,
        wm_to.strftime("%Y-%m-%d %H:%M:%S") if wm_to else None,
        start_time.strftime("%Y-%m-%d %H:%M:%S"),
        end_time.strftime("%Y-%m-%d %H:%M:%S"),
        duration, record_count, s3_path, status,
        error_msg[:2000] if error_msg else None
    ))
    conn.commit()
    logger.info(f"Audit logged → {entity} | {status} | {record_count} rows | {duration}s")


# ══════════════════════════════════════════════════════════════════════
# S3 HELPERS
# ══════════════════════════════════════════════════════════════════════

def get_s3_output_path(base_path: str, load_type: str, run_date: str) -> str:
    """
    Build S3 output path.
    Full:  s3://bucket/glue_poc/sap/BillingDocument/full/2024-03-03/
    Delta: s3://bucket/glue_poc/sap/BillingDocument/delta/2024-03-04/
    """
    folder = "full" if load_type == "F" else "delta"
    return f"{base_path.rstrip('/')}/{folder}/{run_date}/"


def write_parquet_to_s3(spark, df, s3_path: str, partition_cols=None) -> int:
    """Write Spark DataFrame to S3 as Parquet. Returns row count."""
    count = df.count()
    logger.info(f"Writing {count:,} rows → {s3_path}")
    writer = df.write.mode("overwrite").option("compression", "snappy")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(s3_path)
    logger.info(f"Write complete ✅ → {s3_path}")
    return count
```

---

## 📌 DAY 2 (Tue Feb 25) — SFDC Connectivity Test

### How to Create a Glue Job (Console Steps)

1. Go to: **AWS Console → Glue → ETL Jobs → Create Job**
2. Choose: **Script editor** → Spark
3. Name: `tcpl-glue-poc-sfdc-connectivity-test`
4. IAM Role: `TCPLGluePOCRole`
5. Glue version: `Glue 4.0 - Spark 3.3, Python 3`
6. Paste the script below into the editor
7. Under **Job parameters**, add:
   - Key: `--extra-py-files` → Value: `s3://tcpl-datalake/scripts/glue_poc/glue_utils.py`
   - Key: `--additional-python-modules` → Value: `snowflake-connector-python`
8. Save and click **Run**
9. Go to **Runs tab** → click the run → click **Output logs** → opens CloudWatch

### FILE: `glue_poc_sfdc_connectivity_test.py`

```python
"""
SFDC Connectivity Test
Run this FIRST in Week 1 (Task 5).
Tests: 1) OAuth token  2) REST API query  3) Bulk API 2.0 job submission
If all 3 tests pass → SFDC connectivity is confirmed ✅
"""

import sys, json, logging, requests
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from glue_utils import get_secret

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

sfdc_creds = get_secret("tcpl/glue_poc/sfdc_oauth")

# ── TEST 1: OAuth Token ───────────────────────────────────────────────
logger.info("=" * 60)
logger.info("TEST 1: Getting OAuth access token...")
token_response = requests.post(
    f"{sfdc_creds['login_url']}/services/oauth2/token",
    data={
        "grant_type":    "password",
        "client_id":     sfdc_creds["client_id"],
        "client_secret": sfdc_creds["client_secret"],
        "username":      sfdc_creds["username"],
        "password":      sfdc_creds["password"] + sfdc_creds.get("security_token", ""),
    },
    timeout=30
)

if token_response.status_code == 200:
    token_data = token_response.json()
    access_token = token_data["access_token"]
    instance_url = token_data["instance_url"]
    logger.info(f"✅ TEST 1 PASSED — OAuth OK. Instance: {instance_url}")
else:
    logger.error(f"❌ TEST 1 FAILED: {token_response.status_code} → {token_response.text}")
    raise RuntimeError("SFDC OAuth failed — check credentials in Secrets Manager")

headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

# ── TEST 2: REST API Query ────────────────────────────────────────────
logger.info("TEST 2: Testing REST API with small SOQL query...")
query_url = f"{instance_url}/services/data/v57.0/query?q=SELECT+Id,Name+FROM+Account+LIMIT+5"
query_resp = requests.get(query_url, headers=headers, timeout=30)

if query_resp.status_code == 200:
    records = query_resp.json().get("records", [])
    logger.info(f"✅ TEST 2 PASSED — REST API OK. {len(records)} Account records returned")
    logger.info(f"Sample record: {records[0] if records else 'No records'}")
else:
    logger.error(f"❌ TEST 2 FAILED: {query_resp.text[:500]}")

# ── TEST 3: Bulk API 2.0 Job Submission ──────────────────────────────
logger.info("TEST 3: Testing Bulk API 2.0 job submission...")
bulk_url = f"{instance_url}/services/data/v57.0/jobs/query"
bulk_payload = {
    "operation":   "query",
    "query":       "SELECT Id, Name FROM Account LIMIT 100",
    "contentType": "CSV",
}
bulk_resp = requests.post(bulk_url, headers=headers, json=bulk_payload, timeout=30)

if bulk_resp.status_code in [200, 201]:
    job_id = bulk_resp.json()["id"]
    logger.info(f"✅ TEST 3 PASSED — Bulk API OK. Job ID: {job_id}")
    # Abort the test job (cleanup)
    requests.patch(
        f"{bulk_url}/{job_id}",
        headers=headers,
        json={"state": "Aborted"},
        timeout=30
    )
    logger.info("Test job aborted (cleanup done)")
else:
    logger.error(f"❌ TEST 3 FAILED: {bulk_resp.text[:500]}")

logger.info("=" * 60)
logger.info("═══ SFDC CONNECTIVITY TEST COMPLETE ═══")
job.commit()
```

**Expected output in CloudWatch logs:**
```
✅ TEST 1 PASSED — OAuth OK. Instance: https://tcpl.my.salesforce.com
✅ TEST 2 PASSED — REST API OK. 5 Account records returned
✅ TEST 3 PASSED — Bulk API OK. Job ID: 750xxxxxxxxx
```

---

## 📌 DAY 3 (Wed Feb 26) — SAP Connectivity Test

> ⚠️ **BLOCKER WARNING:** SAP firewall whitelisting is IN PROGRESS. If this test fails with timeout or 503 → firewall is still not open. Escalate to TCPL Infra team daily. While waiting → keep building the extraction code (it will work once firewall opens).

Same steps as above to create Glue job. Name it: `tcpl-glue-poc-sap-connectivity-test`

### FILE: `glue_poc_sap_connectivity_test.py`

```python
"""
SAP Connectivity Test
Run in Week 1 (Task 3).
Tests basic network reach + authentication to SAP OData.
"""

import sys, json, logging, requests
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from glue_utils import get_secret

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

sap_creds = get_secret("tcpl/glue_poc/sap_odata")

# Test: Basic network + auth — fetch 5 records from BillingDocument
test_url = (f"{sap_creds['base_url']}/API_BILLING_DOCUMENT_SRV"
            f"/BillingDocument?$top=5&$format=json")

logger.info(f"Testing SAP connectivity: {test_url}")

response = requests.get(
    test_url,
    auth=(sap_creds["username"], sap_creds["password"]),
    headers={"Accept": "application/json", "sap-client": "100"},
    timeout=30
)

logger.info(f"HTTP Status: {response.status_code}")

if response.status_code == 200:
    data = response.json()
    # Handle both OData v2 and v4 response format
    records = (data.get("d", {}).get("results", []) or
               data.get("value", []))
    logger.info(f"✅ SAP CONNECTIVITY TEST PASSED")
    logger.info(f"Records returned: {len(records)}")
    if records:
        logger.info(f"Available fields: {list(records[0].keys())}")
elif response.status_code == 401:
    logger.error("❌ AUTHENTICATION FAILED — check username/password in Secrets Manager")
    raise RuntimeError("SAP auth failed")
elif response.status_code == 403:
    logger.error("❌ AUTHORIZATION FAILED — technical user lacks permission to this OData service")
    raise RuntimeError("SAP authorization failed")
elif response.status_code == 404:
    logger.error("❌ OData endpoint not found — check API service name in the URL")
    raise RuntimeError("SAP endpoint 404")
elif response.status_code in [502, 503, 504]:
    logger.error("❌ NETWORK ERROR — SAP host unreachable. Firewall whitelisting likely still pending.")
    raise RuntimeError("SAP network unreachable")
else:
    logger.error(f"❌ Unexpected status: {response.status_code} → {response.text[:500]}")
    response.raise_for_status()

job.commit()
```

**Expected output:**
```
✅ SAP CONNECTIVITY TEST PASSED
Records returned: 5
Available fields: ['BillingDocument', 'BillingDocumentDate', 'NetAmountInCoCodeCrcy', ...]
```

---

## 📌 DAY 4 (Thu Feb 27)

- Fix any connectivity issues from Day 2 and Day 3
- Decide which source to start with in Week 2 (whichever connected first)
- SAP firewall still not open? → Start with SFDC first

## 📌 DAY 5 (Fri Feb 28)

- Both connectivity tests should be passing
- Document: response format, field names, any API quirks
- Send Week 1 status to your manager

---
---

# ═══════════════════════════════════════
# WEEK 2: EXTRACTION JOBS
# ═══════════════════════════════════════

---

## STEP 3: Build SFDC Extraction Job

### How to Create the Glue Job

1. AWS Console → Glue → Create Job
2. **Name:** `tcpl-glue-poc-sfdc-extract`
3. IAM Role: `TCPLGluePOCRole`
4. Type: Spark, Glue 4.0, Python 3
5. Paste the script below
6. Job parameters (add all of these):
   - `--extra-py-files` = `s3://tcpl-datalake/scripts/glue_poc/glue_utils.py`
   - `--additional-python-modules` = `snowflake-connector-python`
   - `--ENTITY_NAME` = (leave blank, set per run)
   - `--LOAD_TYPE` = `D`
   - `--RUN_DATE` = (leave blank, Airflow sets this)
7. DPU: `10` for large tables (SecondarySalesLineItem), `4` for small ones

### How to Test It (Manual Run)

Go to: Glue → Jobs → `tcpl-glue-poc-sfdc-extract` → Run with parameters:
```
--ENTITY_NAME = Account      ← Start with the smallest table (49K rows)
--LOAD_TYPE   = F            ← Full load for first test
--RUN_DATE    = 2024-03-03
```

After it passes, test delta:
```
--ENTITY_NAME = Account
--LOAD_TYPE   = D            ← Delta load test
--RUN_DATE    = 2024-03-04
```

### FILE: `glue_poc_sfdc_extract.py`

```python
"""
AWS Glue Job: tcpl-glue-poc-sfdc-extract
──────────────────────────────────────────
Extracts data from Salesforce using Bulk API 2.0.
Supports: Full Load (F) and Delta/Incremental Load (D)

Job Parameters:
  --ENTITY_NAME : SFDC object name (e.g. SecondarySalesLineItem__c)
  --LOAD_TYPE   : F=Full, D=Delta (overrides control table if provided)
  --RUN_DATE    : YYYY-MM-DD (defaults to today if not set)

S3 Output:
  Full:  s3://tcpl-datalake/glue_poc/sfdc/{entity}/full/{date}/
  Delta: s3://tcpl-datalake/glue_poc/sfdc/{entity}/delta/{date}/
"""

import sys
import time
import requests
import json
import io
import csv
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from glue_utils import (
    get_secret, get_snowflake_conn, get_entity_config,
    update_watermark, log_audit, get_s3_output_path, write_parquet_to_s3
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ── Init Glue ─────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENTITY_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

JOB_RUN_ID  = args.get("JOB_RUN_ID", "local-test")
ENTITY_NAME = args["ENTITY_NAME"]
RUN_DATE    = args.get("RUN_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

# ── Get Secrets & Config ──────────────────────────────────────────────
sfdc_creds = get_secret("tcpl/glue_poc/sfdc_oauth")
sf_creds   = get_secret("tcpl/glue_poc/snowflake")
sf_conn    = get_snowflake_conn(sf_creds)
config     = get_entity_config(sf_conn, "SFDC", ENTITY_NAME)

LOAD_TYPE  = args.get("LOAD_TYPE", config["LOAD_TYP"])   # F or D
BATCH_SIZE = config["BATCH_SIZE"]
S3_BASE    = config["S3_PATH"]
S3_PATH    = get_s3_output_path(S3_BASE, LOAD_TYPE, RUN_DATE)

start_time = datetime.now(timezone.utc)

logger.info(f"""
═══════════════════════════════════════════════════
SFDC EXTRACTION JOB STARTING
Entity:    {ENTITY_NAME}
Load Type: {'FULL' if LOAD_TYPE == 'F' else 'DELTA'}
Run Date:  {RUN_DATE}
S3 Output: {S3_PATH}
═══════════════════════════════════════════════════
""")


# ══════════════════════════════════════════════════════════════════════
# SALESFORCE BULK API 2.0 CLIENT
# ══════════════════════════════════════════════════════════════════════

class SFDCBulkAPI:
    """Salesforce Bulk API 2.0 client."""

    def __init__(self, creds: Dict):
        self.login_url      = creds.get("login_url", "https://login.salesforce.com")
        self.client_id      = creds["client_id"]
        self.client_secret  = creds["client_secret"]
        self.username       = creds["username"]
        self.password       = creds["password"]
        self.security_token = creds.get("security_token", "")
        self.access_token   = None
        self.instance_url   = None
        self.api_version    = "v57.0"

    def authenticate(self) -> None:
        """OAuth2 Username-Password flow to get access token."""
        logger.info("Authenticating with Salesforce OAuth2...")
        response = requests.post(
            f"{self.login_url}/services/oauth2/token",
            data={
                "grant_type":    "password",
                "client_id":     self.client_id,
                "client_secret": self.client_secret,
                "username":      self.username,
                "password":      self.password + self.security_token,
            },
            timeout=30
        )
        response.raise_for_status()
        token_data = response.json()
        self.access_token = token_data["access_token"]
        self.instance_url = token_data["instance_url"]
        logger.info(f"Authenticated ✅ → Instance: {self.instance_url}")

    @property
    def headers(self) -> Dict:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type":  "application/json",
        }

    def submit_bulk_query(self, soql: str) -> str:
        """Submit Bulk Query job. Returns jobId."""
        logger.info(f"Submitting Bulk Query: {soql[:200]}...")
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/query"
        payload = {
            "operation":       "queryAll",   # includes soft-deleted records
            "query":           soql,
            "contentType":     "CSV",
            "columnDelimiter": "COMMA",
            "lineEnding":      "LF",
        }
        response = requests.post(url, headers=self.headers, json=payload, timeout=60)
        response.raise_for_status()
        job_id = response.json()["id"]
        logger.info(f"Bulk Query job submitted → JobID: {job_id}")
        return job_id

    def poll_job_status(self, job_id: str,
                        poll_interval: int = 30,
                        max_wait: int = 7200) -> Dict:
        """Poll until job is JobComplete or Failed."""
        url = (f"{self.instance_url}/services/data/{self.api_version}"
               f"/jobs/query/{job_id}")
        elapsed = 0
        while elapsed < max_wait:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            status_data = response.json()
            state = status_data.get("state", "")
            processed = status_data.get("numberRecordsProcessed", 0)
            logger.info(f"Job {job_id} → State: {state} | Records: {processed:,} | Elapsed: {elapsed}s")

            if state == "JobComplete":
                return status_data
            elif state in ("Failed", "Aborted"):
                raise RuntimeError(f"Bulk job {job_id} failed with state: {state}")

            time.sleep(poll_interval)
            elapsed += poll_interval

        raise TimeoutError(f"Bulk job {job_id} did not complete within {max_wait}s")

    def get_job_results(self, job_id: str) -> List[Dict]:
        """Retrieve all result pages from completed Bulk job."""
        all_records = []
        locator = None
        page = 0

        while True:
            page += 1
            url = (f"{self.instance_url}/services/data/{self.api_version}"
                   f"/jobs/query/{job_id}/results"
                   f"?maxRecords={BATCH_SIZE}")
            if locator:
                url += f"&locator={locator}"

            response = requests.get(url, headers={
                **self.headers, "Accept": "text/csv"
            }, timeout=120)
            response.raise_for_status()

            reader = csv.DictReader(io.StringIO(response.text))
            page_records = list(reader)
            all_records.extend(page_records)
            logger.info(f"Page {page}: {len(page_records):,} records (total: {len(all_records):,})")

            locator = response.headers.get("Sforce-Locator")
            if not locator or locator == "null":
                break

        return all_records

    def build_soql(self, obj_name: str, watermark_col: str,
                   last_watermark, load_type: str) -> str:
        """Build SOQL query with or without delta filter."""
        # Get all fields dynamically via describe API
        desc_url = (f"{self.instance_url}/services/data/{self.api_version}"
                    f"/sobjects/{obj_name}/describe")
        response = requests.get(desc_url, headers=self.headers, timeout=30)
        response.raise_for_status()
        fields = [f["name"] for f in response.json()["fields"]]
        field_str = ", ".join(fields)

        soql = f"SELECT {field_str} FROM {obj_name}"

        if load_type == "D" and last_watermark:
            # Add 2-hour overlap window to catch late-arriving records
            wm_dt = datetime.fromisoformat(str(last_watermark).replace(" ", "T"))
            wm_with_overlap = (wm_dt - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
            soql += f" WHERE {watermark_col} > {wm_with_overlap}"

        logger.info(f"Built SOQL: {soql[:300]}")
        return soql


# ══════════════════════════════════════════════════════════════════════
# MAIN EXTRACTION LOGIC
# ══════════════════════════════════════════════════════════════════════

total_records = 0
wm_to = datetime.now(timezone.utc)
status = "FAILED"
error_msg = None

try:
    # Step 1: Authenticate
    sfdc = SFDCBulkAPI(sfdc_creds)
    sfdc.authenticate()

    # Step 2: Build SOQL
    soql = sfdc.build_soql(
        obj_name      = ENTITY_NAME,
        watermark_col = config.get("CDC_WATERMARK_COL", "LastModifiedDate"),
        last_watermark= config.get("LAST_WATERMARK"),
        load_type     = LOAD_TYPE,
    )

    # Step 3: Submit Bulk Query job
    job_id = sfdc.submit_bulk_query(soql)

    # Step 4: Poll until complete
    job_status = sfdc.poll_job_status(job_id, poll_interval=15)
    logger.info(f"Job complete. Total records: {job_status['numberRecordsProcessed']:,}")

    # Step 5: Retrieve results
    records = sfdc.get_job_results(job_id)
    total_records = len(records)
    logger.info(f"Retrieved {total_records:,} records from SFDC")

    if total_records == 0 and LOAD_TYPE == "D":
        logger.info("No new/changed records since last watermark. Exiting cleanly.")
        status = "SUCCESS"
    else:
        if records:
            # Step 6: Convert to Spark DataFrame and write Parquet
            rdd = spark.sparkContext.parallelize(records, numSlices=200)
            df = spark.read.json(rdd.map(lambda r: json.dumps(r)))

            # Add metadata columns
            df = (df
                  .withColumn("_extracted_at", F.lit(wm_to.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
                  .withColumn("_load_type",    F.lit("FULL" if LOAD_TYPE == "F" else "DELTA"))
                  .withColumn("_source",       F.lit("SFDC"))
                  .withColumn("_job_run_id",   F.lit(JOB_RUN_ID))
                  .withColumn("_run_date",     F.lit(RUN_DATE))
            )

            logger.info("Schema:")
            df.printSchema()
            df.show(3, truncate=False)

            total_records = write_parquet_to_s3(spark, df, S3_PATH)

        # Step 7: Update watermark (only for delta loads)
        if LOAD_TYPE == "D":
            update_watermark(sf_conn, config["CONFIG_ID"], wm_to)

        status = "SUCCESS"
        logger.info(f"✅ SFDC extraction complete: {total_records:,} records → {S3_PATH}")

except Exception as e:
    error_msg = str(e)
    logger.error(f"❌ SFDC extraction FAILED: {error_msg}")
    raise

finally:
    # Always log to audit table — whether success or failure
    end_time = datetime.now(timezone.utc)
    log_audit(
        conn=sf_conn, job_run_id=JOB_RUN_ID,
        src_sys="SFDC", entity=ENTITY_NAME,
        load_type="FULL" if LOAD_TYPE == "F" else "DELTA",
        wm_from=config.get("LAST_WATERMARK"),
        wm_to=wm_to, start_time=start_time, end_time=end_time,
        record_count=total_records, s3_path=S3_PATH,
        status=status, error_msg=error_msg,
    )
    sf_conn.close()
    job.commit()
```

---

## STEP 4: Build SAP Extraction Job

### How to Create the Glue Job

1. AWS Console → Glue → Create Job
2. **Name:** `tcpl-glue-poc-sap-extract`
3. IAM Role: `TCPLGluePOCRole`
4. Type: Spark, Glue 4.0, Python 3
5. Paste the script below
6. Job parameters:
   - `--extra-py-files` = `s3://tcpl-datalake/scripts/glue_poc/glue_utils.py`
   - `--additional-python-modules` = `snowflake-connector-python`
   - `--ENTITY_NAME` = (set per run)
   - `--LOAD_TYPE` = `D`
   - `--RUN_DATE` = (set per run)
7. DPU settings per table:
   - `ACDOCA_FINANCE_DATA` → 20 DPU (418M rows, largest)
   - `BillingDocumentItemPrcgElmnt` → 16 DPU
   - `BillingDocumentItem`, `SalesOrderItem` → 8 DPU
   - `BillingDocument`, `SalesOrder` → 4 DPU
   - `CustomerMaster` → 2 DPU

### How to Test It

```
First test (small table):
  --ENTITY_NAME = BillingDocument
  --LOAD_TYPE   = F
  --RUN_DATE    = 2024-03-03

Second test (delta):
  --ENTITY_NAME = BillingDocument
  --LOAD_TYPE   = D
  --RUN_DATE    = 2024-03-04
```

### FILE: `glue_poc_sap_extract.py`

```python
"""
AWS Glue Job: tcpl-glue-poc-sap-extract
─────────────────────────────────────────
Extracts data from SAP S/4HANA using OData APIs.
Handles: Full Load (F), Delta Load (D), $skip/$top pagination.
Supports: Static filters (e.g. SalesOrderType), 2-hour overlap window.

Job Parameters:
  --ENTITY_NAME : SAP entity name (e.g. BillingDocument)
  --LOAD_TYPE   : F=Full, D=Delta
  --RUN_DATE    : YYYY-MM-DD
"""

import sys
import requests
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Generator
from urllib.parse import quote

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from glue_utils import (
    get_secret, get_snowflake_conn, get_entity_config,
    update_watermark, log_audit, get_s3_output_path, write_parquet_to_s3
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ── Init Glue ─────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENTITY_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

JOB_RUN_ID  = args.get("JOB_RUN_ID", "local-test")
ENTITY_NAME = args["ENTITY_NAME"]
RUN_DATE    = args.get("RUN_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

# ── Get Secrets & Config ──────────────────────────────────────────────
sap_creds = get_secret("tcpl/glue_poc/sap_odata")
sf_creds  = get_secret("tcpl/glue_poc/snowflake")
sf_conn   = get_snowflake_conn(sf_creds)
config    = get_entity_config(sf_conn, "SAPS4", ENTITY_NAME)

LOAD_TYPE  = args.get("LOAD_TYPE", config["LOAD_TYP"])
BATCH_SIZE = config.get("BATCH_SIZE", 5000)
S3_BASE    = config["S3_PATH"]
S3_PATH    = get_s3_output_path(S3_BASE, LOAD_TYPE, RUN_DATE)
API_NAME   = config["SRC_API_NM"]

start_time = datetime.now(timezone.utc)

logger.info(f"""
═══════════════════════════════════════════════════
SAP ODATA EXTRACTION JOB STARTING
Entity:    {ENTITY_NAME}
API:       {API_NAME}
Load Type: {'FULL' if LOAD_TYPE == 'F' else 'DELTA'}
Run Date:  {RUN_DATE}
S3 Output: {S3_PATH}
═══════════════════════════════════════════════════
""")


# ══════════════════════════════════════════════════════════════════════
# SAP ODATA CLIENT
# ══════════════════════════════════════════════════════════════════════

class SAPODataClient:
    """SAP OData API client with pagination and CSRF token handling."""

    def __init__(self, creds: Dict, api_name: str):
        self.base_url   = creds["base_url"].rstrip("/")
        self.username   = creds["username"]
        self.password   = creds["password"]
        self.api_name   = api_name
        self.session    = requests.Session()
        self.session.auth = (self.username, self.password)
        self.session.headers.update({
            "Accept":       "application/json",
            "Content-Type": "application/json",
            "sap-client":   "100",
        })

    def test_connectivity(self, entity: str) -> bool:
        """Quick connectivity test — fetch 1 record."""
        try:
            url = f"{self.base_url}/{self.api_name}/{entity}?$top=1&$format=json"
            response = self.session.get(url, timeout=30)
            if response.status_code == 200:
                logger.info(f"✅ SAP connectivity OK → {entity}")
                return True
            else:
                logger.error(f"❌ SAP connectivity failed: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"❌ SAP connectivity exception: {e}")
            return False

    def build_odata_url(self, entity: str, watermark_col: Optional[str],
                        last_watermark, load_type: str,
                        static_params: Optional[str], skip: int) -> str:
        """Build OData URL with filters, pagination."""
        base = f"{self.base_url}/{self.api_name}/{entity}"
        params = [f"$top={BATCH_SIZE}", f"$skip={skip}", "$format=json"]

        filters = []
        if load_type == "D" and watermark_col and last_watermark:
            wm_str = str(last_watermark).replace(" ", "T")
            # 2-hour overlap window for late-arriving records
            wm_dt = datetime.fromisoformat(wm_str)
            wm_with_overlap = (wm_dt - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S")
            filters.append(f"{watermark_col} gt datetime'{wm_with_overlap}'")

        if static_params:
            filters.append(f"({static_params})")

        if filters:
            params.append(f"$filter={quote(' and '.join(filters))}")

        if watermark_col:
            params.append(f"$orderby={watermark_col} asc")

        return f"{base}?{'&'.join(params)}"

    def extract_all_pages(self, entity: str, watermark_col: Optional[str],
                          last_watermark, load_type: str,
                          static_params: Optional[str]) -> Generator[List[Dict], None, None]:
        """
        Generator: yields one page of records at a time.
        Uses $skip/$top pagination.
        """
        skip = 0
        page = 0
        total_fetched = 0

        while True:
            page += 1
            url = self.build_odata_url(
                entity, watermark_col, last_watermark,
                load_type, static_params, skip
            )
            logger.info(f"Page {page}: GET {url[:200]}")

            response = self.session.get(url, timeout=120)

            if response.status_code == 429:   # Rate limit hit
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Rate limited. Waiting {retry_after}s...")
                import time; time.sleep(retry_after)
                continue

            response.raise_for_status()
            data = response.json()

            # Handle both OData v2 and v4 response format
            records = (data.get("d", {}).get("results", []) or   # OData v2
                       data.get("value", []))                      # OData v4

            if not records:
                logger.info(f"No more records at page {page}. Total: {total_fetched:,}")
                break

            total_fetched += len(records)
            logger.info(f"Page {page}: {len(records):,} records (total: {total_fetched:,})")
            yield records

            if len(records) < BATCH_SIZE:
                logger.info(f"Last page (got {len(records)} < batch {BATCH_SIZE}). Done.")
                break

            skip += BATCH_SIZE


# ══════════════════════════════════════════════════════════════════════
# MAIN EXTRACTION LOGIC
# ══════════════════════════════════════════════════════════════════════

total_records = 0
wm_to = datetime.now(timezone.utc)
wm_from = None
status = "FAILED"
error_msg = None

try:
    client = SAPODataClient(sap_creds, API_NAME)

    # Fail fast if can't reach SAP
    if not client.test_connectivity(ENTITY_NAME):
        raise ConnectionError(f"Cannot connect to SAP OData: {API_NAME}/{ENTITY_NAME}")

    wm_from = config.get("LAST_WATERMARK")
    logger.info(f"Extracting: entity={ENTITY_NAME}, "
                f"load={'FULL' if LOAD_TYPE=='F' else 'DELTA'}, "
                f"from={wm_from}")

    # Stream pages → write in chunks of 50 to avoid OOM on large tables
    all_spark_dfs = []
    batch_num = 0

    for page_records in client.extract_all_pages(
        entity        = ENTITY_NAME,
        watermark_col = config.get("CDC_WATERMARK_COL"),
        last_watermark= wm_from,
        load_type     = LOAD_TYPE,
        static_params = config.get("STATIC_PARAMS"),
    ):
        batch_num += 1
        rdd = spark.sparkContext.parallelize(page_records, numSlices=4)
        page_df = spark.read.json(rdd.map(lambda r: json.dumps(r)))
        all_spark_dfs.append(page_df)

        # Flush every 50 pages to avoid OOM (important for ACDOCA 418M rows)
        if batch_num % 50 == 0:
            combined = all_spark_dfs[0]
            for df in all_spark_dfs[1:]:
                combined = combined.unionByName(df, allowMissingColumns=True)
            chunk_path = f"{S3_PATH}chunk_{batch_num // 50}/"
            count = write_parquet_to_s3(spark, combined, chunk_path)
            total_records += count
            all_spark_dfs = []   # free memory
            logger.info(f"Chunk {batch_num//50} written: {count:,} rows")

    # Write remaining pages
    if all_spark_dfs:
        combined = all_spark_dfs[0]
        for df in all_spark_dfs[1:]:
            combined = combined.unionByName(df, allowMissingColumns=True)

        # Add metadata columns
        combined = (combined
            .withColumn("_extracted_at", F.lit(wm_to.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
            .withColumn("_load_type",    F.lit("FULL" if LOAD_TYPE == "F" else "DELTA"))
            .withColumn("_source",       F.lit("SAPS4"))
            .withColumn("_job_run_id",   F.lit(JOB_RUN_ID))
            .withColumn("_run_date",     F.lit(RUN_DATE))
        )
        count = write_parquet_to_s3(spark, combined, f"{S3_PATH}chunk_final/")
        total_records += count

    # Update watermark only for delta loads with records found
    if LOAD_TYPE == "D" and total_records > 0:
        update_watermark(sf_conn, config["CONFIG_ID"], wm_to)

    status = "SUCCESS"
    logger.info(f"✅ SAP extraction complete: {total_records:,} rows → {S3_PATH}")

except Exception as e:
    error_msg = str(e)
    logger.error(f"❌ SAP extraction FAILED: {error_msg}")
    raise

finally:
    end_time = datetime.now(timezone.utc)
    log_audit(
        conn=sf_conn, job_run_id=JOB_RUN_ID,
        src_sys="SAPS4", entity=ENTITY_NAME,
        load_type="FULL" if LOAD_TYPE == "F" else "DELTA",
        wm_from=wm_from, wm_to=wm_to,
        start_time=start_time, end_time=end_time,
        record_count=total_records, s3_path=S3_PATH,
        status=status, error_msg=error_msg,
    )
    sf_conn.close()
    job.commit()
```

---

## 📌 WEEK 2 DAY BY DAY

```
MON Mar 3:  Build SFDC extraction job → test with Account (49K rows, smallest)
TUE Mar 4:  Run FULL LOAD for Table 1 → monitor CloudWatch logs + check S3
WED Mar 5:  Build Table 2 extraction job (SAP BillingDocument)
THU Mar 6:  Run FULL LOAD Table 2 + run DELTA LOAD Table 1
            Verify: only new/changed records in delta output
            Check GLUE_AUDIT_LOG has entries for each run
FRI Mar 7:  Fix any issues. Re-run to confirm watermark updates correctly.
            Send Week 2 status to manager.
```

---
---

# ═══════════════════════════════════════
# WEEK 3: SNOWFLAKE LOAD + VALIDATION
# ═══════════════════════════════════════

---

## STEP 5: Airflow DAG — Orchestrate Everything

Deploy this DAG to your Airflow (MWAA) instance. It orchestrates the full pipeline.

### FILE: `tcpl_glue_poc_dag.py`

```python
"""
Airflow DAG: tcpl_glue_poc_pipeline
─────────────────────────────────────
Orchestrates the full Glue POC pipeline.
Schedule: Daily at 2AM IST.

Flow:
  All SFDC extract jobs (parallel) → Load SFDC to Snowflake
  All SAP extract jobs (parallel)  → Load SAP to Snowflake
  Both loads done                  → Run validation
"""

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner":            "decision_point_analytics",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": True,
    "email":            ["data-ops@decisionpoint.ai"],
}

with DAG(
    dag_id="tcpl_glue_poc_pipeline",
    default_args=default_args,
    description="AWS Glue POC — SAP + SFDC extraction to Snowflake",
    schedule_interval="0 2 * * *",   # 2AM IST daily
    start_date=days_ago(1),
    catchup=False,
    tags=["tcpl","glue_poc","extraction"],
) as dag:

    run_date = "{{ ds }}"

    # ── SFDC EXTRACTION TASKS (all run in parallel) ───────────────────

    sfdc_secondary_sales = GlueJobOperator(
        task_id="sfdc_extract_secondary_sales_line_item",
        job_name="tcpl-glue-poc-sfdc-extract",
        script_args={"--ENTITY_NAME": "SecondarySalesLineItem__c",
                     "--LOAD_TYPE": "D", "--RUN_DATE": run_date},
        aws_conn_id="aws_tcpl", region_name="ap-south-1",
        wait_for_completion=True, num_of_dpus=10,
    )

    sfdc_visit_details = GlueJobOperator(
        task_id="sfdc_extract_visit_details",
        job_name="tcpl-glue-poc-sfdc-extract",
        script_args={"--ENTITY_NAME": "VisitDetails__c",
                     "--LOAD_TYPE": "D", "--RUN_DATE": run_date},
        aws_conn_id="aws_tcpl", num_of_dpus=8,
    )

    sfdc_inventory = GlueJobOperator(
        task_id="sfdc_extract_inventory",
        job_name="tcpl-glue-poc-sfdc-extract",
        script_args={"--ENTITY_NAME": "Inventory__c",
                     "--LOAD_TYPE": "D", "--RUN_DATE": run_date},
        aws_conn_id="aws_tcpl", num_of_dpus=4,
    )

    sfdc_retailer = GlueJobOperator(
        task_id="sfdc_extract_retailer",
        job_name="tcpl-glue-poc-sfdc-extract",
        script_args={"--ENTITY_NAME": "Retailer__c",
                     "--LOAD_TYPE": "D", "--RUN_DATE": run_date},
        aws_conn_id="aws_tcpl", num_of_dpus=4,
    )

    sfdc_account = GlueJobOperator(
        task_id="sfdc_extract_account",
        job_name="tcpl-glue-poc-sfdc-extract",
        script_args={"--ENTITY_NAME": "Account",
                     "--LOAD_TYPE": "D", "--RUN_DATE": run_date},
        aws_conn_id="aws_tcpl", num_of_dpus=2,
    )

    # ── SAP EXTRACTION TASKS (all run in parallel) ────────────────────

    sap_acdoca = GlueJobOperator(
        task_id="sap_extract_acdoca_finance_data",
        job_name="tcpl-glue-poc-sap-extract",
        script_args={"--ENTITY_NAME": "ACDOCA_FINANCE_DATA",
                     "--LOAD_TYPE": "D", "--RUN_DATE": run_date},
        aws_conn_id="aws_tcpl", num_of_dpus=20,
    )

    sap_billing_prcg = GlueJobOperator(
        task_id="sap_extract_billing_doc_item_prcg_elmnt",
        job_name="tcpl-glue-poc-sap-extract",
        script_args={"--ENTITY_NAME": "BillingDocumentItemPrcgElmnt",
                     "--LOAD_TYPE": "D", "--RUN_DATE": run_date},
        aws_conn_id="aws_tcpl", num_of_dpus=16,
    )

    sap_billing_doc_item = GlueJobOperator(
        task_id="sap_extract_billing_doc_item",
        job_name="tcpl-glue-poc-sap-extract",
        script_args={"--ENTITY_NAME": "BillingDocumentItem",
                     "--LOAD_TYPE": "D", "--RUN_DATE": run_date},
        aws_conn_id="aws_tcpl", num_of_dpus=8,
    )

    sap_billing_doc = GlueJobOperator(
        task_id="sap_extract_billing_document",
        job_name="tcpl-glue-poc-sap-extract",
        script_args={"--ENTITY_NAME": "BillingDocument",
                     "--LOAD_TYPE": "D", "--RUN_DATE": run_date},
        aws_conn_id="aws_tcpl", num_of_dpus=4,
    )

    sap_so_item = GlueJobOperator(
        task_id="sap_extract_sales_order_item",
        job_name="tcpl-glue-poc-sap-extract",
        script_args={"--ENTITY_NAME": "SalesOrderItem",
                     "--LOAD_TYPE": "D", "--RUN_DATE": run_date},
        aws_conn_id="aws_tcpl", num_of_dpus=8,
    )

    sap_so = GlueJobOperator(
        task_id="sap_extract_sales_order",
        job_name="tcpl-glue-poc-sap-extract",
        script_args={"--ENTITY_NAME": "SalesOrder",
                     "--LOAD_TYPE": "D", "--RUN_DATE": run_date},
        aws_conn_id="aws_tcpl", num_of_dpus=4,
    )

    sap_customer_master = GlueJobOperator(
        task_id="sap_extract_customer_master",
        job_name="tcpl-glue-poc-sap-extract",
        script_args={"--ENTITY_NAME": "CustomerMaster",
                     "--LOAD_TYPE": "F",   # Always full load — no delta
                     "--RUN_DATE": run_date},
        aws_conn_id="aws_tcpl", num_of_dpus=2,
    )

    # ── SNOWFLAKE LOAD TASKS (uses EXISTING Glue transform templates) ──
    # These are the "existing Glue templates" — NOT written by you.
    # You just point them at the new S3 paths.

    load_sfdc_to_snowflake = GlueJobOperator(
        task_id="load_sfdc_to_snowflake_poc",
        job_name="tcpl-glue-sfdc-to-snowflake-core",   # ← EXISTING job, already built
        script_args={
            "--SOURCE_PATH":   "s3://tcpl-datalake/glue_poc/sfdc/",
            "--TARGET_SCHEMA": "GLUE_POC_DB.SFDC_CORE",
            "--RUN_DATE":      run_date,
        },
        aws_conn_id="aws_tcpl", num_of_dpus=8,
    )

    load_sap_to_snowflake = GlueJobOperator(
        task_id="load_sap_to_snowflake_poc",
        job_name="tcpl-glue-sap-to-snowflake-core",    # ← EXISTING job, already built
        script_args={
            "--SOURCE_PATH":   "s3://tcpl-datalake/glue_poc/sap/",
            "--TARGET_SCHEMA": "GLUE_POC_DB.SAPS4_CORE",
            "--RUN_DATE":      run_date,
        },
        aws_conn_id="aws_tcpl", num_of_dpus=10,
    )

    # ── VALIDATION TASK ──────────────────────────────────────────────

    run_validation = SnowflakeOperator(
        task_id="run_data_validation_queries",
        sql="CALL GLUE_POC_DB.PROCEDURES.RUN_FULL_VALIDATION('{{ ds }}')",
        snowflake_conn_id="snowflake_tcpl",
        warehouse="TCPL_ANALYTICS_WH",
        database="GLUE_POC_DB",
    )

    # ── DEPENDENCY GRAPH ─────────────────────────────────────────────
    # SFDC and SAP run in parallel.
    # Loads only start after all extractions finish.
    # Validation runs after both loads are done.

    sfdc_tasks = [sfdc_secondary_sales, sfdc_visit_details,
                  sfdc_inventory, sfdc_retailer, sfdc_account]
    sap_tasks  = [sap_acdoca, sap_billing_prcg, sap_billing_doc_item,
                  sap_billing_doc, sap_so_item, sap_so, sap_customer_master]

    sfdc_tasks >> load_sfdc_to_snowflake
    sap_tasks  >> load_sap_to_snowflake

    [load_sfdc_to_snowflake, load_sap_to_snowflake] >> run_validation
```

---

## STEP 6: Snowflake Validation Setup (Run Before Task 14)

```sql
-- ══════════════════════════════════════════════════════════════════════
-- STEP 6: Validation Setup
-- Run this in Snowflake before doing Task 14 (data validation)
-- ══════════════════════════════════════════════════════════════════════

-- Create schemas for validation results
CREATE SCHEMA IF NOT EXISTS GLUE_POC_DB.VALIDATION;
CREATE SCHEMA IF NOT EXISTS GLUE_POC_DB.PROCEDURES;

-- Validation results table — stores all comparison results
CREATE TABLE IF NOT EXISTS GLUE_POC_DB.VALIDATION.RESULTS (
    VALIDATION_DATE  DATE,
    TABLE_NAME       VARCHAR(100),
    VALIDATION_TYPE  VARCHAR(50),
    SNAPLOGIC_COUNT  NUMBER,
    GLUE_COUNT       NUMBER,
    DIFFERENCE       NUMBER,
    MATCH_PCT        DECIMAL(8,4),
    STATUS           VARCHAR(20),      -- PASS or FAIL
    NOTES            VARCHAR(500),
    CREATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Stored procedure — Airflow calls this after Snowflake load is done
CREATE OR REPLACE PROCEDURE GLUE_POC_DB.PROCEDURES.RUN_FULL_VALIDATION(RUN_DATE DATE)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- BillingDocument row count comparison
    INSERT INTO GLUE_POC_DB.VALIDATION.RESULTS
    SELECT
        :RUN_DATE, 'BillingDocument', 'ROW_COUNT',
        (SELECT COUNT(*) FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT),
        (SELECT COUNT(*) FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT),
        ABS((SELECT COUNT(*) FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT) -
            (SELECT COUNT(*) FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT)),
        ROUND((SELECT COUNT(*) FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT) /
              NULLIF((SELECT COUNT(*) FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT), 0) * 100, 4),
        CASE WHEN ABS((SELECT COUNT(*) FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT) -
                      (SELECT COUNT(*) FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT)) = 0
             THEN 'PASS' ELSE 'FAIL' END,
        NULL;

    -- SecondarySalesLineItem row count comparison
    INSERT INTO GLUE_POC_DB.VALIDATION.RESULTS
    SELECT
        :RUN_DATE, 'SecondarySalesLineItem', 'ROW_COUNT',
        (SELECT COUNT(*) FROM SFDC_DB.SFDC_CORE.SECONDARYSALESLINEITEM),
        (SELECT COUNT(*) FROM GLUE_POC_DB.SFDC_CORE.SECONDARYSALESLINEITEM),
        ABS((SELECT COUNT(*) FROM GLUE_POC_DB.SFDC_CORE.SECONDARYSALESLINEITEM) -
            (SELECT COUNT(*) FROM SFDC_DB.SFDC_CORE.SECONDARYSALESLINEITEM)),
        ROUND((SELECT COUNT(*) FROM GLUE_POC_DB.SFDC_CORE.SECONDARYSALESLINEITEM) /
              NULLIF((SELECT COUNT(*) FROM SFDC_DB.SFDC_CORE.SECONDARYSALESLINEITEM), 0) * 100, 4),
        CASE WHEN ABS((SELECT COUNT(*) FROM GLUE_POC_DB.SFDC_CORE.SECONDARYSALESLINEITEM) -
                      (SELECT COUNT(*) FROM SFDC_DB.SFDC_CORE.SECONDARYSALESLINEITEM)) = 0
             THEN 'PASS' ELSE 'FAIL' END,
        NULL;

    RETURN 'Validation complete for ' || :RUN_DATE;
END;
$$;
```

---

## STEP 7: Run Validation Queries (Task 14)

Run each of these manually in Snowflake during Task 14.

```sql
-- ══════════════════════════════════════════════════════════════════════
-- VALIDATION A: Row Count Comparison
-- Expected: is_match = TRUE for all rows
-- ══════════════════════════════════════════════════════════════════════
SELECT
    'BillingDocument' AS table_name,
    (SELECT COUNT(*) FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT)    AS snaplogic_count,
    (SELECT COUNT(*) FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT) AS glue_count,
    (SELECT COUNT(*) FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT) =
    (SELECT COUNT(*) FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT)    AS is_match
UNION ALL SELECT
    'BillingDocumentItem',
    (SELECT COUNT(*) FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENTITEM),
    (SELECT COUNT(*) FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENTITEM),
    (SELECT COUNT(*) FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENTITEM) =
    (SELECT COUNT(*) FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENTITEM)
UNION ALL SELECT
    'SecondarySalesLineItem',
    (SELECT COUNT(*) FROM SFDC_DB.SFDC_CORE.SECONDARYSALESLINEITEM),
    (SELECT COUNT(*) FROM GLUE_POC_DB.SFDC_CORE.SECONDARYSALESLINEITEM),
    (SELECT COUNT(*) FROM GLUE_POC_DB.SFDC_CORE.SECONDARYSALESLINEITEM) =
    (SELECT COUNT(*) FROM SFDC_DB.SFDC_CORE.SECONDARYSALESLINEITEM)
UNION ALL SELECT
    'Account',
    (SELECT COUNT(*) FROM SFDC_DB.SFDC_CORE.ACCOUNT),
    (SELECT COUNT(*) FROM GLUE_POC_DB.SFDC_CORE.ACCOUNT),
    (SELECT COUNT(*) FROM GLUE_POC_DB.SFDC_CORE.ACCOUNT) =
    (SELECT COUNT(*) FROM SFDC_DB.SFDC_CORE.ACCOUNT)
ORDER BY table_name;


-- ══════════════════════════════════════════════════════════════════════
-- VALIDATION B: MINUS Query — Records MISSING from Glue (should be 0)
-- ══════════════════════════════════════════════════════════════════════
SELECT 'Missing in Glue' AS issue,
       BILLINGDOCUMENT, BILLINGDOCUMENTITEM
FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT
MINUS
SELECT 'Missing in Glue',
       BILLINGDOCUMENT, BILLINGDOCUMENTITEM
FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT;
-- ✅ Expected: 0 rows


-- ══════════════════════════════════════════════════════════════════════
-- VALIDATION C: MINUS Query — EXTRA records in Glue not in SnapLogic
-- ══════════════════════════════════════════════════════════════════════
SELECT 'Extra in Glue' AS issue,
       BILLINGDOCUMENT, BILLINGDOCUMENTITEM
FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT
MINUS
SELECT 'Extra in Glue',
       BILLINGDOCUMENT, BILLINGDOCUMENTITEM
FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT;
-- ✅ Expected: 0 rows


-- ══════════════════════════════════════════════════════════════════════
-- VALIDATION D: Checksum — Compare key column hash
-- ══════════════════════════════════════════════════════════════════════
SELECT
    'SnapLogic' AS source,
    MD5(LISTAGG(BILLINGDOCUMENT || NETAMOUNT, ',')
        WITHIN GROUP (ORDER BY BILLINGDOCUMENT)) AS checksum
FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT
UNION ALL
SELECT
    'Glue',
    MD5(LISTAGG(BILLINGDOCUMENT || NETAMOUNT, ',')
        WITHIN GROUP (ORDER BY BILLINGDOCUMENT))
FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT;
-- ✅ Expected: Both checksums identical


-- ══════════════════════════════════════════════════════════════════════
-- VALIDATION E: Delta progression check
-- Verify delta load captured all records in the time window
-- ══════════════════════════════════════════════════════════════════════
SELECT
    'SnapLogic delta window' AS source,
    COUNT(*) AS delta_records
FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT
WHERE LASTCHANGEDATETIME > '2024-03-03T00:00:00'
  AND LASTCHANGEDATETIME <= '2024-03-04T00:00:00'
UNION ALL
SELECT
    'Glue delta window',
    COUNT(*)
FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT
WHERE _EXTRACTED_AT::DATE = '2024-03-04'
  AND _LOAD_TYPE = 'DELTA';
-- ✅ Expected: Both counts identical


-- ══════════════════════════════════════════════════════════════════════
-- VALIDATION F: Audit Log Health Check
-- See all Glue job runs — status, row counts, timing
-- ══════════════════════════════════════════════════════════════════════
SELECT
    SRC_SYS_NM,
    ENTITY_NAME,
    LOAD_TYPE,
    STATUS,
    RECORD_COUNT,
    DURATION_SECONDS,
    ROUND(RECORD_COUNT / NULLIF(DURATION_SECONDS, 0)) AS records_per_sec,
    ERROR_MESSAGE
FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_AUDIT_LOG
WHERE START_TIME >= CURRENT_DATE
ORDER BY START_TIME DESC;
```

---

## 📌 WEEK 3 DAY BY DAY

```
MON Mar 10: Load Table 1 to Snowflake GLUE_POC_DB using existing transform template
            Verify row count in Snowflake matches S3

TUE Mar 11: Load Table 2 to Snowflake
            Run VALIDATION A (row count comparison)

WED Mar 12: Run VALIDATION B + C (MINUS queries — should return 0 rows)
            Run VALIDATION D (checksum)
            Run VALIDATION E (delta window check)

THU Mar 13: Document all results
            Note any failures + root causes
            Record performance timings (Glue vs SnapLogic)

FRI Mar 14-15: Complete Validation Report (template below)
               Write Go/No-Go recommendation
               Present to Vishnu Rao + Sam Joseph
               ← THIS IS YOUR DEADLINE ✅
```

---
---

# ═══════════════════════════════════════
# TROUBLESHOOTING — COMMON ISSUES
# ═══════════════════════════════════════

---

## Issue 1: SAP Firewall Not Open Yet

**Symptom:** Connection timeout or "Connection refused" from Glue job

**What to do:**
- Cannot fix without infra team opening the firewall. Escalate daily.
- In the meantime: build all SAP extraction code completely (it will work once firewall opens)
- Switch to SFDC first — more likely ready faster
- Use mock data locally if needed to test your logic

---

## Issue 2: SFDC "INVALID_LOGIN" Error

**Symptom:** OAuth returns `400 INVALID_LOGIN`

**Cause A:** Wrong username/password in Secrets Manager → re-verify with TCPL SFDC Admin

**Cause B:** Security token missing → Add `security_token` to the Secrets Manager entry. Required when connecting from non-whitelisted IP (like AWS Glue).

**Cause C:** Connected App IP Relaxation not set → SFDC Admin must go to: Setup → Connected Apps → Edit → IP Relaxation = "Relax IP restrictions"

---

## Issue 3: SAP OData Pagination Stops Early

**Symptom:** Glue extracts 5000 rows then stops (table has more records)

**Cause:** OData v4 uses `@odata.nextLink`, OData v2 uses `__next`

**Fix:** The code already handles both. If still stuck, add this explicit check:
```python
next_link = (data.get("@odata.nextLink") or
             data.get("d", {}).get("__next"))
if next_link:
    url = next_link  # Use the server-provided next URL directly
```

---

## Issue 4: Parquet Write Fails / OOM for Large SFDC Tables

**Symptom:** Glue job killed after collecting all 122M SecondarySalesLineItem rows

**Cause:** All records held in memory before writing

**Fix:** Write each page as append instead of collecting all first:
```python
df.write.mode("append").option("compression", "snappy").parquet(s3_path)
```
Also increase DPU: 10 → 20 for SecondarySalesLineItem.

---

## Issue 5: Snowflake Watermark Not Updated After Success

**Symptom:** Job shows SUCCESS, data is in S3, but `LAST_WATERMARK` didn't update

**Fix:** Manually update it in Snowflake:
```sql
UPDATE PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_ENTITY_CONFIG
SET LAST_WATERMARK = '2024-03-04 00:00:00',
    UPDATED_AT = CURRENT_TIMESTAMP()
WHERE SRC_OBJ_NM = 'BillingDocument';
```

---

## Issue 6: MINUS Query Returns Records Despite Matching Row Counts

**Symptom:** Row counts are equal but MINUS still finds differences

**Cause:** Numeric precision (SFDC returns `1.0`, SAP returns `1`) or timestamp format

**Fix:** CAST both sides to same type in MINUS query:
```sql
SELECT CAST(NETAMOUNT AS DECIMAL(18,2)) FROM ...
MINUS
SELECT CAST(NETAMOUNT AS DECIMAL(18,2)) FROM ...
```

---
---

# ═══════════════════════════════════════
# MARCH 15 VALIDATION REPORT TEMPLATE
# ═══════════════════════════════════════

Fill this in and present to Vishnu Rao + Sam Joseph on March 15.

```
══════════════════════════════════════════════════════════════════════
GLUE POC VALIDATION REPORT
Date: March 15, 2024 | Author: [Your Name]
══════════════════════════════════════════════════════════════════════

1. ROW COUNT COMPARISON RESULTS
────────────────────────────────
┌─────────────────────────────────┬────────────┬────────────┬────────┐
│ TABLE                           │ SNAPLOGIC  │ GLUE       │ MATCH? │
├─────────────────────────────────┼────────────┼────────────┼────────┤
│ BillingDocument                 │            │            │        │
│ BillingDocumentItem             │            │            │        │
│ BillingDocumentItemPrcgElmnt    │            │            │        │
│ SalesOrder                      │            │            │        │
│ SalesOrderItem                  │            │            │        │
│ ACDOCA_FINANCE_DATA             │            │            │        │
│ CustomerMaster                  │            │            │        │
│ SecondarySalesLineItem          │            │            │        │
│ VisitDetails                    │            │            │        │
│ Inventory                       │            │            │        │
│ Retailer                        │            │            │        │
│ Account                         │            │            │        │
└─────────────────────────────────┴────────────┴────────────┴────────┘
RESULT: [X]/12 tables match 100%

2. MINUS QUERY RESULTS
───────────────────────
BillingDocument MINUS: [X] rows returned (expected: 0)
SecondarySalesLineItem MINUS: [X] rows returned (expected: 0)

3. DELTA LOAD VALIDATION
──────────────────────────
BillingDocument delta window [date → date]:
  SnapLogic: [X] records
  Glue:      [X] records | MATCH: Y/N

SecondarySalesLineItem delta:
  SnapLogic: [X] records
  Glue:      [X] records | MATCH: Y/N

4. PERFORMANCE COMPARISON
──────────────────────────
┌─────────────────────────────────┬────────────┬────────────┬────────┐
│ TABLE                           │ SNAPLOGIC  │ GLUE       │ FASTER?│
├─────────────────────────────────┼────────────┼────────────┼────────┤
│ BillingDocument                 │            │            │        │
│ SecondarySalesLineItem          │            │            │        │
└─────────────────────────────────┴────────────┴────────────┴────────┘

5. RECOMMENDATION: ✅ GO / ❌ NO-GO
──────────────────────────────────────
Decision: [GO / NO-GO]

Rationale:
  [ ] Data accuracy: [X]/12 tables match
  [ ] MINUS queries: [X] records returned (0 = pass)
  [ ] Delta loads: correct records captured
  [ ] Performance: [faster/slower] than SnapLogic

Issues found:
  [List any issues + root causes + resolutions]

Next steps if GO:
  1. Migrate remaining SnapLogic pipelines to Glue (8 tables per sprint)
  2. Run parallel operations for 2 weeks during cutover
  3. Decommission SnapLogic after validation sign-off
══════════════════════════════════════════════════════════════════════
```

---
---

# ═══════════════════════════════════════
# FINAL CHECKLIST — MARCH 15
# ═══════════════════════════════════════

Use this every day to track progress:

```
WEEK 1 — CONNECTIVITY + SETUP
  □ Secrets Manager: 3 secrets created (SAP, SFDC, Snowflake)
  □ glue_utils.py uploaded to s3://tcpl-datalake/scripts/glue_poc/
  □ GLUE_ENTITY_CONFIG table created + seeded with 12 entities
  □ GLUE_AUDIT_LOG table created
  □ SFDC connectivity test job PASSED (all 3 tests ✅)
  □ SAP connectivity test job PASSED ✅
  □ Both passing → document credentials, response format, field names

WEEK 2 — EXTRACTION
  □ SFDC extraction job (tcpl-glue-poc-sfdc-extract) created in Glue
  □ SAP extraction job (tcpl-glue-poc-sap-extract) created in Glue
  □ Table 1: Full load tested with small entity (Account or BillingDocument)
  □ Table 1: Full load runs successfully end to end
  □ Table 2: Full load done
  □ Delta load working — watermark updates correctly after each run
  □ S3 has Parquet files under correct paths (full/ and delta/)
  □ GLUE_AUDIT_LOG has entries for each run with correct status

WEEK 3 — LOAD + VALIDATE + PRESENT
  □ Table 1 loaded to Snowflake GLUE_POC_DB (using existing template)
  □ Table 2 loaded to Snowflake GLUE_POC_DB (using existing template)
  □ Validation A (row counts): all match ✅
  □ Validation B+C (MINUS queries): 0 rows returned ✅
  □ Validation D (checksum): matching ✅
  □ Validation E (delta window): counts match ✅
  □ Performance timings recorded (Glue vs SnapLogic)
  □ Validation Report document completed
  □ Go/No-Go recommendation written with supporting data
  □ Presented to Vishnu Rao + Sam Joseph → March 15 ✅ DONE
```

---

## 📁 YOUR FILES SUMMARY — What to Create and Where

| File | What it does | Where to put it |
|---|---|---|
| `glue_utils.py` | Shared helpers (secrets, Snowflake, S3) | `s3://tcpl-datalake/scripts/glue_poc/` |
| `glue_poc_sfdc_connectivity_test.py` | Tests SFDC OAuth + Bulk API | Upload to Glue job |
| `glue_poc_sap_connectivity_test.py` | Tests SAP OData connection | Upload to Glue job |
| `glue_poc_sfdc_extract.py` | Extracts all SFDC tables | Upload to Glue job |
| `glue_poc_sap_extract.py` | Extracts all SAP tables | Upload to Glue job |
| `tcpl_glue_poc_dag.py` | Airflow DAG to run everything | Deploy to Airflow/MWAA |
| Snowflake SQL (Step 1) | Creates control tables | Run in Snowflake |
| Snowflake SQL (Step 6) | Creates validation tables + procedure | Run in Snowflake |

---

*12 tables. 981 Million records. 62 GB. 3 weeks. You've got this bro 💪*
*Keep this file open every single day. Follow it step by step. Don't skip anything.*
