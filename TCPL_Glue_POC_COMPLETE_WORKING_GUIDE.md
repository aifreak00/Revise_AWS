# 🚀 TCPL Glue POC — Complete End-to-End Working Guide
## Everything You Need to Build This, Step by Step

---

> **What you are doing:** Replacing SnapLogic with AWS Glue as the data extraction layer for TCPL.
> **Current flow:** Airflow → SnapLogic → S3 → Glue Transform → Snowflake
> **New flow you're building:** Airflow → AWS Glue (extractor) → S3 → Glue Transform → Snowflake
> **Deadline:** March 15, 2025 — Milestone Review with Vishnu Rao (Business Sponsor) + Sam Joseph (Platform Owner)

---

## 📊 12 TABLES YOU WILL EXTRACT

### SFDC Tables (5) — via Bulk API 2.0
| Table | Rows | Size | Load Type | Delta Key |
|---|---|---|---|---|
| SecondarySalesLineItem__c | 122.6M | 13.1 GB | Delta | LastModifiedDate |
| VisitDetails__c | 63.4M | 6.6 GB | Delta | LastModifiedDate |
| Inventory__c | 5.2M | 701 MB | Delta | LastModifiedDate |
| Retailer__c | 2.4M | 355 MB | Delta | LastModifiedDate |
| Account | 49K | 6.8 MB | Delta | LastModifiedDate |

### SAP Tables (7) — via OData API
| Table | Rows | Size | Load Type | Delta Key |
|---|---|---|---|---|
| ACDOCA_FINANCE_DATA | 418.25M | 27.6 GB | Delta | LastChangeDateTime |
| BillingDocumentItemPrcgElmnt | 311.17M | 7.7 GB | Delta | LastChangeDateTime |
| BillingDocumentItem | 24.94M | 2.1 GB | Delta | LastChangeDateTime |
| BillingDocument | 5.39M | 347 MB | Delta | LastChangeDateTime |
| SalesOrderItem | 23.54M | 2.2 GB | Delta | LastChangeDateTime |
| SalesOrder | 3.79M | 236 MB | Delta | LastChangeDateTime |
| CustomerMaster | 80K | 8.8 MB | **FULL** | No delta — always full |

**Total: ~981M records, ~62 GB across 12 tables**

---

## 📅 3-WEEK PLAN AT A GLANCE

```
WEEK 1 (Feb 24 – Mar 1):  CONNECTIVITY — get SAP + SFDC talking to Glue
WEEK 2 (Mar 3 – Mar 8):   EXTRACTION — build jobs, run full loads for 2 tables
WEEK 3 (Mar 10 – Mar 15): SNOWFLAKE LOAD + VALIDATION — load, compare, report
```

---

---

# ══════════════════════════════════
# WEEK 1: CONNECTIVITY
# ══════════════════════════════════

---

## ✅ DAY 1 (Mon Feb 24) — AWS Setup + Snowflake Control Tables

### STEP 0A: S3 Folder Structure

The landing zones in S3. These don't need to be created manually — Glue will create them on write. Just make sure the bucket exists.

```
s3://tcpl-datalake/glue_poc/
├── sfdc/
│   ├── SecondarySalesLineItem/
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
```

Each table will write to:
- Full load:  `s3://tcpl-datalake/glue_poc/sfdc/Account/full/2024-03-03/part-00000.parquet`
- Delta load: `s3://tcpl-datalake/glue_poc/sfdc/Account/delta/2024-03-04/part-00000.parquet`

---

### STEP 0B: IAM Role for Glue Jobs

Create/confirm the role `TCPLGluePOCRole` exists with this inline policy attached:

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

Also attach the managed policy: `AWSGlueServiceRole`

---

### STEP 0C: Store Credentials in AWS Secrets Manager

Go to AWS Console → Secrets Manager → Create 3 secrets.

**Secret 1 name:** `tcpl/glue_poc/sap_odata`
```json
{
    "base_url":  "https://your-sap-host.tcpl.com/sap/opu/odata/sap/",
    "username":  "GLUE_TECHNICAL_USER",
    "password":  "your-sap-password"
}
```

**Secret 2 name:** `tcpl/glue_poc/sfdc_oauth`
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
> **Note on security_token:** Required when connecting from non-whitelisted IP. Get it from TCPL SFDC Admin → User Settings → Reset My Security Token.

**Secret 3 name:** `tcpl/glue_poc/snowflake`
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

### STEP 1: Snowflake Control Tables (Run this in Snowflake)

Open Snowflake → select database `PRD_ARCH_SUPPORT_DB` → schema `GLUE_POC` → run the full SQL below.

```sql
-- ══════════════════════════════════════════════════════════════════════
-- STEP 1: Create Control Tables + Seed Data
-- Database: PRD_ARCH_SUPPORT_DB | Schema: GLUE_POC
-- ══════════════════════════════════════════════════════════════════════

USE DATABASE PRD_ARCH_SUPPORT_DB;
USE SCHEMA GLUE_POC;
USE WAREHOUSE TCPL_LOAD_WH;

-- ── Table 1: GLUE_ENTITY_CONFIG ──────────────────────────────────────
-- Stores entity config + watermarks for all 12 tables
CREATE OR REPLACE TABLE GLUE_ENTITY_CONFIG (
    CONFIG_ID         NUMBER AUTOINCREMENT PRIMARY KEY,
    SRC_SYS_NM        VARCHAR(20)    NOT NULL,   -- SAPS4 or SFDC
    SRC_API_NM        VARCHAR(200)   NOT NULL,   -- API endpoint name
    SRC_OBJ_NM        VARCHAR(100)   NOT NULL,   -- Object/entity name
    RELATIVE_PATH     VARCHAR(100)   NOT NULL,   -- S3 sub-path
    LOAD_TYP          VARCHAR(1)     NOT NULL,   -- D=Delta, F=Full
    REQUIRED_FLAG     VARCHAR(1)     DEFAULT 'Y',-- Y=Active, N=Inactive
    CDC_WATERMARK_COL VARCHAR(100),              -- Column for delta tracking
    LAST_WATERMARK    TIMESTAMP_NTZ,             -- Last extracted timestamp
    BATCH_SIZE        NUMBER         DEFAULT 5000,
    STATIC_PARAMS     VARCHAR(500),              -- Extra OData filters
    S3_PATH           VARCHAR(200)   NOT NULL,   -- Target S3 path
    CREATED_AT        TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT        TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

-- ── Table 2: GLUE_AUDIT_LOG ──────────────────────────────────────────
-- Tracks every Glue job run
CREATE OR REPLACE TABLE GLUE_AUDIT_LOG (
    AUDIT_ID         NUMBER AUTOINCREMENT PRIMARY KEY,
    GLUE_JOB_RUN_ID  VARCHAR(100),
    SRC_SYS_NM       VARCHAR(20),
    ENTITY_NAME      VARCHAR(100),
    LOAD_TYPE        VARCHAR(10),               -- FULL or DELTA
    WATERMARK_FROM   TIMESTAMP_NTZ,
    WATERMARK_TO     TIMESTAMP_NTZ,
    START_TIME       TIMESTAMP_NTZ,
    END_TIME         TIMESTAMP_NTZ,
    DURATION_SECONDS NUMBER,
    RECORD_COUNT     NUMBER,
    S3_PATH          VARCHAR(500),
    STATUS           VARCHAR(20),               -- SUCCESS, FAILED, IN_PROGRESS
    ERROR_MESSAGE    VARCHAR(2000),
    CREATED_AT       TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

-- ── Seed Data: All 12 Tables ─────────────────────────────────────────

-- === SFDC TABLES (5) ===
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

-- === SAP TABLES (7) ===
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

-- CustomerMaster: FULL LOAD, no watermark
('SAPS4', 'API_BUSINESS_PARTNER_SRV', 'CustomerMaster',
 'sap/CustomerMaster', 'F', 'Y',
 NULL, NULL, 5000, NULL,
 's3://tcpl-datalake/glue_poc/sap/CustomerMaster/');

-- ── Verify ───────────────────────────────────────────────────────────
SELECT SRC_SYS_NM, SRC_OBJ_NM, LOAD_TYP, BATCH_SIZE, LAST_WATERMARK
FROM GLUE_ENTITY_CONFIG
ORDER BY SRC_SYS_NM, CONFIG_ID;
-- Expected: 12 rows — 5 SFDC + 7 SAP
```

---

## STEP 2: Upload `glue_utils.py` to S3

This shared utility module is used by ALL Glue jobs. Upload it once.

**Upload location:** `s3://tcpl-datalake/scripts/glue_poc/glue_utils.py`

**In every Glue job, add this job parameter:**
`--extra-py-files` = `s3://tcpl-datalake/scripts/glue_poc/glue_utils.py`

### FILE: `glue_utils.py`

```python
"""
glue_utils.py
─────────────
Shared utilities used by all SAP and SFDC Glue extraction jobs.
Upload to: s3://tcpl-datalake/scripts/glue_poc/glue_utils.py
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
    """Update LAST_WATERMARK in control table after successful extraction."""
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

## ✅ DAY 2 (Tue Feb 25) — SFDC Connectivity Test

### How to Create a Glue Job for Connectivity Test

1. Go to AWS Console → Glue → Jobs → Create Job
2. Name: `tcpl-glue-poc-sfdc-connectivity-test`
3. IAM Role: `TCPLGluePOCRole`
4. Type: Spark, Glue version 4.0
5. Script: upload the file below
6. Job parameters:
   - `--extra-py-files` = `s3://tcpl-datalake/scripts/glue_poc/glue_utils.py`
   - `--additional-python-modules` = `snowflake-connector-python`
7. Run it. Check CloudWatch logs.

### FILE: `glue_poc_sfdc_connectivity_test.py`

```python
"""
SFDC Connectivity Test — Run this FIRST in Week 1, Task 5.
Tests: 1) OAuth token  2) REST API query  3) Bulk API 2.0 job submission
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
logger.info("Test 1: Getting OAuth access token...")
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
logger.info("Test 2: Testing REST API with small SOQL query...")
query_url = f"{instance_url}/services/data/v57.0/query?q=SELECT+Id,Name+FROM+Account+LIMIT+5"
query_resp = requests.get(query_url, headers=headers, timeout=30)

if query_resp.status_code == 200:
    records = query_resp.json().get("records", [])
    logger.info(f"✅ TEST 2 PASSED — REST API OK. {len(records)} Account records returned")
    logger.info(f"Sample record: {records[0] if records else 'No records'}")
else:
    logger.error(f"❌ TEST 2 FAILED: {query_resp.text[:500]}")

# ── TEST 3: Bulk API 2.0 Job Submission ──────────────────────────────
logger.info("Test 3: Testing Bulk API 2.0 job submission...")
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
    logger.info("Test job aborted (cleanup)")
else:
    logger.error(f"❌ TEST 3 FAILED: {bulk_resp.text[:500]}")

logger.info("═══ SFDC CONNECTIVITY TEST COMPLETE ═══")
job.commit()
```

**Expected output in CloudWatch:**
```
✅ TEST 1 PASSED — OAuth OK. Instance: https://tcpl.my.salesforce.com
✅ TEST 2 PASSED — REST API OK. 5 Account records returned
✅ TEST 3 PASSED — Bulk API OK. Job ID: 750xxxxxxxxx
```

---

## ✅ DAY 3 (Wed Feb 26) — SAP Connectivity Test

Same steps as SFDC — create Glue job with script below.
**Name:** `tcpl-glue-poc-sap-connectivity-test`

> ⚠️ **Week 1 Blocker Warning:** SAP firewall whitelisting is IN PROGRESS. If this job fails with a timeout or 503, it means the firewall isn't open yet. Escalate daily. While waiting, keep building the extraction code (it will work once the firewall is open).

### FILE: `glue_poc_sap_connectivity_test.py`

```python
"""
SAP Connectivity Test — Run this in Week 1, Task 3.
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

# Test 1: Basic network + auth — fetch 5 records from BillingDocument
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
logger.info(f"Response Headers: {dict(response.headers)}")

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

---

---

# ══════════════════════════════════
# WEEK 2: EXTRACTION JOBS
# ══════════════════════════════════

---

## STEP 3: SFDC Extraction Job

### How to Create the Glue Job

1. AWS Console → Glue → Jobs → Create Job
2. **Name:** `tcpl-glue-poc-sfdc-extract`
3. IAM Role: `TCPLGluePOCRole`
4. Type: Spark, Glue 4.0, Python 3
5. Script: upload `glue_poc_sfdc_extract.py`
6. Job parameters (set as defaults, can override per run):
   - `--extra-py-files` = `s3://tcpl-datalake/scripts/glue_poc/glue_utils.py`
   - `--additional-python-modules` = `snowflake-connector-python`
   - `--ENTITY_NAME` = (leave blank — set per-run)
   - `--LOAD_TYPE` = `D`
   - `--RUN_DATE` = (leave blank — Airflow will set this)
7. DPU: set 10 for large tables (SecondarySalesLineItem), 4 for small ones

### How to Run It (Manual Test)

```
Job name: tcpl-glue-poc-sfdc-extract
Parameters to pass when triggering:
  --ENTITY_NAME = Account
  --LOAD_TYPE   = F
  --RUN_DATE    = 2024-03-03
```

Start with `Account` (smallest, 49K rows) for the first test run. Then scale up.

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
# SALESFORCE BULK API 2.0 CLIENT CLASS
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
            # Add 2-hour overlap window for late-arriving records
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

            logger.info(f"Schema:\n")
            df.printSchema()
            df.show(3, truncate=False)

            total_records = write_parquet_to_s3(spark, df, S3_PATH)

        # Step 7: Update watermark
        if LOAD_TYPE == "D":
            update_watermark(sf_conn, config["CONFIG_ID"], wm_to)

        status = "SUCCESS"
        logger.info(f"✅ SFDC extraction complete: {total_records:,} records → {S3_PATH}")

except Exception as e:
    error_msg = str(e)
    logger.error(f"❌ SFDC extraction FAILED: {error_msg}")
    raise

finally:
    # Always log to audit table regardless of success/failure
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

## STEP 4: SAP Extraction Job

### How to Create the Glue Job

1. AWS Console → Glue → Jobs → Create Job
2. **Name:** `tcpl-glue-poc-sap-extract`
3. IAM Role: `TCPLGluePOCRole`
4. Type: Spark, Glue 4.0, Python 3
5. Script: upload `glue_poc_sap_extract.py`
6. Job parameters:
   - `--extra-py-files` = `s3://tcpl-datalake/scripts/glue_poc/glue_utils.py`
   - `--additional-python-modules` = `snowflake-connector-python`
   - `--ENTITY_NAME` = (set per run)
   - `--LOAD_TYPE` = `D`
   - `--RUN_DATE` = (set per run)
7. DPU: 20 for ACDOCA (418M rows), 16 for BillingDocItemPrcg, 4-8 for others

### How to Test It

```
First test run (small table):
  --ENTITY_NAME = BillingDocument
  --LOAD_TYPE   = F
  --RUN_DATE    = 2024-03-03

Second run (delta test):
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
  --LOAD_TYPE   : F=Full, D=Delta (overrides control table if provided)
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
SAP_BASE   = sap_creds["base_url"]
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
# SAP ODATA CLIENT CLASS
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
        self.csrf_token = None

    def fetch_csrf_token(self) -> None:
        """SAP OData requires CSRF token for modifying operations. Fetch it here."""
        url = f"{self.base_url}/{self.api_name}/"
        response = self.session.get(
            url,
            headers={"x-csrf-token": "Fetch"},
            timeout=30
        )
        self.csrf_token = response.headers.get("x-csrf-token")
        self.session.headers.update({"x-csrf-token": self.csrf_token})
        logger.info(f"CSRF token fetched ✅")

    def test_connectivity(self, entity: str) -> bool:
        """Test connectivity by fetching 1 record. Fail fast if can't connect."""
        url = f"{self.base_url}/{self.api_name}/{entity}?$top=1&$format=json"
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            logger.info(f"SAP connectivity test PASSED ✅ → {entity}")
            return True
        except Exception as e:
            logger.error(f"SAP connectivity test FAILED ❌ → {e}")
            return False

    def build_odata_url(self, entity: str, watermark_col: Optional[str],
                        last_watermark, load_type: str,
                        static_params: Optional[str],
                        skip: int = 0) -> str:
        """Build OData URL with filters, pagination, and format."""
        base = f"{self.base_url}/{self.api_name}/{entity}"
        params = [f"$format=json", f"$top={BATCH_SIZE}", f"$skip={skip}"]

        filters = []

        # Delta filter — add 2-hour overlap window for late-arriving records
        if load_type == "D" and watermark_col and last_watermark:
            wm_dt = datetime.fromisoformat(str(last_watermark).replace(" ", "T"))
            wm_with_overlap = wm_dt - timedelta(hours=2)
            wm_str = wm_with_overlap.strftime("%Y-%m-%dT%H:%M:%SZ")
            filters.append(f"{watermark_col} gt datetime'{wm_str}'")

        # Static params (e.g. SalesOrderType filter for SalesOrder entity)
        if static_params:
            filters.append(f"({static_params})")

        if filters:
            params.append(f"$filter={quote(' and '.join(filters))}")

        # Order by watermark for consistent pagination
        if watermark_col:
            params.append(f"$orderby={watermark_col} asc")

        return f"{base}?{'&'.join(params)}"

    def extract_all_pages(self, entity: str, watermark_col: Optional[str],
                          last_watermark, load_type: str,
                          static_params: Optional[str]) -> Generator[List[Dict], None, None]:
        """
        Generator: yields pages of records using $skip/$top pagination.
        Each yield is a list of record dicts for one page.
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
                logger.info(f"No more records at page {page}. Total fetched: {total_fetched:,}")
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

    # Stream pages → Spark DataFrames → write in chunks of 50 pages
    # (Avoid OOM on very large tables like ACDOCA 418M rows)
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

        # Flush every 50 pages to avoid OOM
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

    # Update watermark only for Delta loads with records found
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

---

# ══════════════════════════════════
# WEEK 3: SNOWFLAKE LOAD + VALIDATION
# ══════════════════════════════════

---

## STEP 5: Airflow DAG — Orchestrate Everything

Deploy this DAG to your Airflow instance. It will:
1. Run all 5 SFDC extraction jobs in parallel
2. Run all 7 SAP extraction jobs in parallel
3. Load SFDC data to Snowflake POC schema
4. Load SAP data to Snowflake POC schema
5. Run validation stored procedure

### FILE: `tcpl_glue_poc_dag.py`

```python
"""
Airflow DAG: tcpl_glue_poc_pipeline
─────────────────────────────────────
Orchestrates the full Glue POC pipeline.
Schedule: Daily at 2AM IST (matches production Airflow pattern).
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

    # ── SFDC EXTRACTION TASKS (run in parallel) ──────────────────────

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

    # ── SAP EXTRACTION TASKS (run in parallel) ───────────────────────

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
                     "--LOAD_TYPE": "F",   # ALWAYS full load
                     "--RUN_DATE": run_date},
        aws_conn_id="aws_tcpl", num_of_dpus=2,
    )

    # ── SNOWFLAKE LOAD TASKS (uses existing Glue transform templates) ─

    load_sfdc_to_snowflake = GlueJobOperator(
        task_id="load_sfdc_to_snowflake_poc",
        job_name="tcpl-glue-sfdc-to-snowflake-core",   # EXISTING template
        script_args={
            "--SOURCE_PATH":   "s3://tcpl-datalake/glue_poc/sfdc/",
            "--TARGET_SCHEMA": "GLUE_POC_DB.SFDC_CORE",
            "--RUN_DATE":      run_date,
        },
        aws_conn_id="aws_tcpl", num_of_dpus=8,
    )

    load_sap_to_snowflake = GlueJobOperator(
        task_id="load_sap_to_snowflake_poc",
        job_name="tcpl-glue-sap-to-snowflake-core",    # EXISTING template
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
    # All SFDC jobs → SFDC load → validation
    # All SAP jobs  → SAP load  → validation

    sfdc_tasks = [sfdc_secondary_sales, sfdc_visit_details,
                  sfdc_inventory, sfdc_retailer, sfdc_account]
    sap_tasks  = [sap_acdoca, sap_billing_prcg, sap_billing_doc_item,
                  sap_billing_doc, sap_so_item, sap_so, sap_customer_master]

    sfdc_tasks >> load_sfdc_to_snowflake
    sap_tasks  >> load_sap_to_snowflake

    [load_sfdc_to_snowflake, load_sap_to_snowflake] >> run_validation
```

---

## STEP 6: Validation Stored Procedure + Queries (Run in Snowflake)

### Create the Validation Results Table + Stored Procedure

```sql
-- ══════════════════════════════════════════════════════════════════════
-- STEP 6: Validation Setup
-- Run in Snowflake before doing Task 14
-- ══════════════════════════════════════════════════════════════════════

-- Create schema for validation
CREATE SCHEMA IF NOT EXISTS GLUE_POC_DB.VALIDATION;
CREATE SCHEMA IF NOT EXISTS GLUE_POC_DB.PROCEDURES;

-- Validation results table
CREATE TABLE IF NOT EXISTS GLUE_POC_DB.VALIDATION.RESULTS (
    VALIDATION_DATE  DATE,
    TABLE_NAME       VARCHAR(100),
    VALIDATION_TYPE  VARCHAR(50),
    SNAPLOGIC_COUNT  NUMBER,
    GLUE_COUNT       NUMBER,
    DIFFERENCE       NUMBER,
    MATCH_PCT        DECIMAL(8,4),
    STATUS           VARCHAR(20),
    NOTES            VARCHAR(500),
    CREATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Stored procedure — called by Airflow after Snowflake load
CREATE OR REPLACE PROCEDURE GLUE_POC_DB.PROCEDURES.RUN_FULL_VALIDATION(RUN_DATE DATE)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- BillingDocument row count
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

    -- SecondarySalesLineItem row count
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

### Manual Validation Queries (Task 14 — Run These Yourself)

```sql
-- ══════════════════════════════════════════════════════════════════════
-- VALIDATION A: Row Count Comparison for Both POC Tables
-- Expected: is_match = TRUE for all rows
-- ══════════════════════════════════════════════════════════════════════
SELECT
    'BillingDocument' AS table_name,
    (SELECT COUNT(*) FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT)           AS snaplogic_count,
    (SELECT COUNT(*) FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT)        AS glue_count,
    (SELECT COUNT(*) FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT) =
    (SELECT COUNT(*) FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT)           AS is_match
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
-- Verify delta load captured the right records in the time window
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
-- See all Glue job runs — status, row counts, duration
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

---

# ══════════════════════════════════
# TROUBLESHOOTING — COMMON ISSUES
# ══════════════════════════════════

---

## Issue 1: SAP Firewall Not Open Yet

**Symptom:** Connection timeout or "Connection refused" from Glue job

**What to do:**
- Cannot fix without infra team opening the firewall. Escalate daily.
- In the meantime: build the SAP extraction code completely (already done above)
- Switch to SFDC first (more likely ready faster)
- Use mock data to test logic offline if needed

---

## Issue 2: SFDC "INVALID_LOGIN" Error

**Symptom:** OAuth returns `400 INVALID_LOGIN`

**Cause A:** Wrong username/password in Secrets Manager → Re-verify with TCPL SFDC Admin

**Cause B:** Security token needed → Add `security_token` to the Secrets Manager entry. It's required when connecting from a non-whitelisted IP (like AWS Glue).

**Cause C:** Connected App IP Relaxation not set → SFDC Admin must go to: Setup → Connected App → Edit → IP Relaxation = "Relax IP restrictions"

---

## Issue 3: SAP OData Pagination Stops Early

**Symptom:** Glue extracts 5000 rows then stops (even though table has more)

**Cause:** Response missing a `__next` or `nextLink` token, or server-side page limit

**Fix:** The code already handles both OData v2 and v4 pagination:
```python
# OData v2 uses "d.results" + "__next"
# OData v4 uses "value" + "@odata.nextLink"
records = (data.get("d", {}).get("results", []) or
           data.get("value", []))
```
If pagination still stops early, add this check for the `nextLink` token:
```python
next_link = (data.get("@odata.nextLink") or
             data.get("d", {}).get("__next"))
if next_link:
    url = next_link  # Use the server-provided next URL directly
```

---

## Issue 4: Parquet Write Fails / OOM for Large SFDC Tables

**Symptom:** Glue job killed after collecting all 122M SecondarySalesLineItem rows

**Cause:** Collecting all records in memory before writing

**Fix:** The SAP job already handles this with the 50-page chunk pattern. Apply same to SFDC for large tables — write each page batch as append:
```python
# In SFDC job, replace "collect all then write" with:
df.write.mode("append").option("compression", "snappy").parquet(s3_path)
```
Also increase DPU: 10 → 20 for SecondarySalesLineItem.

---

## Issue 5: Snowflake Watermark Not Updated After Success

**Symptom:** Job shows SUCCESS in audit log, data is in S3, but `LAST_WATERMARK` didn't update

**Cause:** Snowflake connection closed or commit failed before update

**Fix:** The `update_watermark()` call is inside the `try` block (before `finally`). If it still fails, manually update:
```sql
UPDATE PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_ENTITY_CONFIG
SET LAST_WATERMARK = '2024-03-04 00:00:00',
    UPDATED_AT = CURRENT_TIMESTAMP()
WHERE SRC_OBJ_NM = 'BillingDocument';
```

---

## Issue 6: MINUS Query Returns Records Despite Matching Row Counts

**Symptom:** Row counts are identical but MINUS finds differences

**Cause A:** Same records, different data values (data mismatch — investigate immediately)

**Cause B:** Numeric precision: SFDC returns `1.0`, SAP returns `1`

**Cause C:** Timestamp format difference

**Fix:** CAST both sides to same type in MINUS query:
```sql
-- Add CAST to the MINUS query for sensitive columns:
SELECT CAST(NETAMOUNT AS DECIMAL(18,2)) FROM ...
MINUS
SELECT CAST(NETAMOUNT AS DECIMAL(18,2)) FROM ...
```

---

---

# ══════════════════════════════════
# MARCH 15 VALIDATION REPORT TEMPLATE
# ══════════════════════════════════

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
  [ ] Cost reduction: estimated [X]% vs SnapLogic

Issues found:
  [List any issues + root causes + resolutions]

Next steps if GO:
  1. Migrate remaining SnapLogic pipelines to Glue (8 tables per sprint)
  2. Run parallel operations for 2 weeks during cutover
  3. Decommission SnapLogic after validation sign-off
```

---

---

# ══════════════════════════════════
# FINAL CHECKLIST — MARCH 15 REVIEW
# ══════════════════════════════════

```
WEEK 1 — CONNECTIVITY
  □ Secrets Manager: 3 secrets created (SAP, SFDC, Snowflake)
  □ glue_utils.py uploaded to S3 scripts folder
  □ GLUE_ENTITY_CONFIG created + seeded with 12 entities
  □ GLUE_AUDIT_LOG created
  □ SFDC connectivity test job PASSED (all 3 tests)
  □ SAP connectivity test job PASSED
  □ Both passing → document credentials, response format, field names

WEEK 2 — EXTRACTION
  □ SFDC extraction job (tcpl-glue-poc-sfdc-extract) created in Glue
  □ SAP extraction job (tcpl-glue-poc-sap-extract) created in Glue
  □ Table 1: Full load tested with small entity (Account/BillingDocument)
  □ Table 1: Full load runs successfully for large entity
  □ Table 2: Full load done
  □ Delta load working — watermark updates correctly after each run
  □ S3 has Parquet files under correct paths (full/ and delta/)
  □ GLUE_AUDIT_LOG has entries for each run with correct status

WEEK 3 — LOAD & VALIDATE
  □ Table 1 loaded to Snowflake GLUE_POC_DB
  □ Table 2 loaded to Snowflake GLUE_POC_DB
  □ Validation A (row counts): all match
  □ Validation B+C (MINUS queries): 0 rows returned
  □ Validation D (checksum): matching
  □ Validation E (delta window): counts match
  □ Performance timings recorded
  □ Validation Report document completed
  □ Go/No-Go recommendation written with supporting data
  □ Presented to Vishnu Rao + Sam Joseph → March 15 ✅
```

---

*Keep this file open every day. Follow it step by step. You've got this 💪*
*12 tables. 981M records. 62 GB. 3 weeks.*
