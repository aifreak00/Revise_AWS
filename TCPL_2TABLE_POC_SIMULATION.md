# 🚀 TCPL GLUE POC — 2 TABLE END-TO-END SIMULATION
## Complete Implementation: Account (SFDC) + BillingDocument (SAP)
### March 15 Milestone | Follow This File Every Single Day

---

> ## WHY ONLY 2 TABLES?
> This is a **POC (Proof of Concept)**. Not production.
> The goal is to PROVE the pattern works → get approval → then do all 12 tables.
>
> **Table 1:** `Account` (SFDC) → 49K rows → smallest → quick sanity check
> **Table 2:** `BillingDocument` (SAP) → 5.39M rows → medium → tests OData + pagination + delta
>
> If these 2 pass → pattern is proven → safe to say GO for all 12 tables.

---

## 🗺️ WHAT YOU ARE BUILDING

```
CURRENT (today):
  Airflow → SnapLogic → S3 → [Existing Glue Template] → Snowflake

YOUR NEW FLOW (what you build):
  Airflow → YOUR Glue Job → S3 → [Existing Glue Template] → Snowflake

YOUR JOB ENDS AT S3.
The S3 → Snowflake part is already built by someone else. You just reuse it.
```

---

## 📅 3-WEEK PLAN

```
WEEK 1 (Feb 24 – Mar 1):   Setup + Connectivity Tests
WEEK 2 (Mar 3 – Mar 8):    Extract 2 Tables → Full Load + Delta Load
WEEK 3 (Mar 10 – Mar 15):  Load to Snowflake + Validate + Present
```

---

## 📁 FILES YOU WILL CREATE

| File | Purpose | When |
|---|---|---|
| `glue_utils.py` | Shared helpers | Week 1, Day 1 |
| `glue_poc_sfdc_connectivity_test.py` | Test SFDC connection | Week 1, Day 2 |
| `glue_poc_sap_connectivity_test.py` | Test SAP connection | Week 1, Day 3 |
| `glue_poc_sfdc_extract.py` | Extract Account from SFDC | Week 2 |
| `glue_poc_sap_extract.py` | Extract BillingDocument from SAP | Week 2 |
| `tcpl_glue_poc_dag.py` | Airflow DAG | Week 3 |
| Snowflake SQL scripts | Tables + validation queries | Week 1 + Week 3 |

---
---

# ═══════════════════════════════════════════
# WEEK 1: SETUP + CONNECTIVITY
# ═══════════════════════════════════════════

---

## 📌 DAY 1 — Monday Feb 24
### AWS Infrastructure + Snowflake Control Tables

---

### STEP 1A: Verify S3 Bucket Structure

> No need to create folders manually. Glue creates them when it writes.
> Just confirm the bucket `tcpl-datalake` exists in AWS Console → S3.

The paths your jobs will write to:
```
s3://tcpl-datalake/glue_poc/
├── sfdc/
│   └── Account/
│       ├── full/
│       │   └── 2024-03-03/
│       │       └── part-00000.parquet   ← Account full load output
│       └── delta/
│           └── 2024-03-04/
│               └── part-00000.parquet   ← Account delta load output
└── sap/
    └── BillingDocument/
        ├── full/
        │   └── 2024-03-03/
        │       └── part-00000.parquet   ← BillingDocument full load output
        └── delta/
            └── 2024-03-04/
                └── part-00000.parquet   ← BillingDocument delta load output
```

---

### STEP 1B: Create IAM Role for Glue

Go to: **AWS Console → IAM → Roles → Create Role**

- Trusted entity: AWS Service → Glue
- Role name: `TCPLGluePOCRole`
- Attach managed policy: `AWSGlueServiceRole`

Then add this **inline policy** (name it `TCPLGluePOCPolicy`):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3Access",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::tcpl-datalake",
        "arn:aws:s3:::tcpl-datalake/glue_poc/*",
        "arn:aws:s3:::tcpl-datalake/scripts/*"
      ]
    },
    {
      "Sid": "SecretsManagerAccess",
      "Effect": "Allow",
      "Action": ["secretsmanager:GetSecretValue"],
      "Resource": [
        "arn:aws:secretsmanager:ap-south-1:*:secret:tcpl/glue_poc/sap*",
        "arn:aws:secretsmanager:ap-south-1:*:secret:tcpl/glue_poc/sfdc*",
        "arn:aws:secretsmanager:ap-south-1:*:secret:tcpl/glue_poc/snowflake*"
      ]
    },
    {
      "Sid": "CloudWatchLogs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:ap-south-1:*:log-group:/aws-glue/*"
    }
  ]
}
```

---

### STEP 1C: Store Credentials in AWS Secrets Manager

Go to: **AWS Console → Secrets Manager → Store a new secret → Other type of secret**

Create exactly 3 secrets. Fill actual values from TCPL teams.

---

**Secret 1 — Name:** `tcpl/glue_poc/sfdc_oauth`

```json
{
    "client_id":      "YOUR_SFDC_CONNECTED_APP_CLIENT_ID",
    "client_secret":  "YOUR_SFDC_CONNECTED_APP_SECRET",
    "username":       "glue-integration@tcpl.com",
    "password":       "YOUR_SFDC_PASSWORD",
    "security_token": "YOUR_SFDC_SECURITY_TOKEN",
    "login_url":      "https://login.salesforce.com"
}
```

> ⚠️ **security_token** — Ask TCPL SFDC Admin. They reset it at:
> SFDC → Settings → My Personal Information → Reset My Security Token

---

**Secret 2 — Name:** `tcpl/glue_poc/sap_odata`

```json
{
    "base_url":  "https://your-sap-host.tcpl.com/sap/opu/odata/sap/",
    "username":  "GLUE_TECHNICAL_USER",
    "password":  "YOUR_SAP_PASSWORD"
}
```

---

**Secret 3 — Name:** `tcpl/glue_poc/snowflake`

```json
{
    "account":   "tcpl.ap-southeast-1",
    "user":      "GLUE_POC_USER",
    "password":  "YOUR_SNOWFLAKE_PASSWORD",
    "warehouse": "TCPL_LOAD_WH",
    "database":  "PRD_ARCH_SUPPORT_DB",
    "schema":    "GLUE_POC",
    "role":      "TCPL_GLUE_POC_ROLE"
}
```

---

### STEP 1D: Create Snowflake Control Tables

Open Snowflake → Run ALL of this SQL in one shot:

```sql
-- ══════════════════════════════════════════════════════════════════════
-- POC CONTROL TABLES — Run this once on Day 1
-- Database: PRD_ARCH_SUPPORT_DB | Schema: GLUE_POC
-- ══════════════════════════════════════════════════════════════════════

USE DATABASE PRD_ARCH_SUPPORT_DB;
USE SCHEMA GLUE_POC;
USE WAREHOUSE TCPL_LOAD_WH;

-- ── Table 1: GLUE_ENTITY_CONFIG ───────────────────────────────────────
-- Glue reads this before every run to get config + last watermark
CREATE OR REPLACE TABLE GLUE_ENTITY_CONFIG (
    CONFIG_ID         NUMBER AUTOINCREMENT PRIMARY KEY,
    SRC_SYS_NM        VARCHAR(20)   NOT NULL,    -- SAPS4 or SFDC
    SRC_API_NM        VARCHAR(200)  NOT NULL,    -- API endpoint name
    SRC_OBJ_NM        VARCHAR(100)  NOT NULL,    -- Object/entity name
    RELATIVE_PATH     VARCHAR(100)  NOT NULL,    -- S3 sub-path identifier
    LOAD_TYP          VARCHAR(1)    NOT NULL,    -- D=Delta, F=Full
    REQUIRED_FLAG     VARCHAR(1)    DEFAULT 'Y', -- Y=Active, N=Inactive
    CDC_WATERMARK_COL VARCHAR(100),              -- Column used for delta
    LAST_WATERMARK    TIMESTAMP_NTZ,             -- Last extracted timestamp
    BATCH_SIZE        NUMBER        DEFAULT 5000,
    STATIC_PARAMS     VARCHAR(500),              -- Extra OData $filter
    S3_PATH           VARCHAR(200)  NOT NULL,    -- Full S3 base path
    CREATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── Table 2: GLUE_AUDIT_LOG ───────────────────────────────────────────
-- Every Glue job run writes one row here — SUCCESS or FAILED
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
    CREATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ══════════════════════════════════════════════════════════════════════
-- SEED DATA — Only 2 POC tables
-- ══════════════════════════════════════════════════════════════════════

-- Table 1: Account (SFDC) — smallest table, 49K rows, DELTA load
INSERT INTO GLUE_ENTITY_CONFIG (
    SRC_SYS_NM, SRC_API_NM, SRC_OBJ_NM, RELATIVE_PATH,
    LOAD_TYP, REQUIRED_FLAG, CDC_WATERMARK_COL,
    LAST_WATERMARK, BATCH_SIZE, STATIC_PARAMS, S3_PATH
) VALUES (
    'SFDC',
    'bulk_api_v2',
    'Account',
    'sfdc/Account',
    'D',
    'Y',
    'LastModifiedDate',
    '2020-01-01 00:00:00',
    5000,
    NULL,
    's3://tcpl-datalake/glue_poc/sfdc/Account/'
);

-- Table 2: BillingDocument (SAP) — 5.39M rows, DELTA load
INSERT INTO GLUE_ENTITY_CONFIG (
    SRC_SYS_NM, SRC_API_NM, SRC_OBJ_NM, RELATIVE_PATH,
    LOAD_TYP, REQUIRED_FLAG, CDC_WATERMARK_COL,
    LAST_WATERMARK, BATCH_SIZE, STATIC_PARAMS, S3_PATH
) VALUES (
    'SAPS4',
    'API_BILLING_DOCUMENT_SRV',
    'BillingDocument',
    'sap/BillingDocument',
    'D',
    'Y',
    'LastChangeDateTime',
    '2020-01-01T00:00:00',
    5000,
    NULL,
    's3://tcpl-datalake/glue_poc/sap/BillingDocument/'
);

-- ── Verify: Should see exactly 2 rows ────────────────────────────────
SELECT
    CONFIG_ID,
    SRC_SYS_NM,
    SRC_OBJ_NM,
    LOAD_TYP,
    CDC_WATERMARK_COL,
    LAST_WATERMARK,
    BATCH_SIZE,
    S3_PATH
FROM GLUE_ENTITY_CONFIG
ORDER BY CONFIG_ID;

-- Expected output:
-- 1 | SFDC  | Account         | D | LastModifiedDate  | 2020-01-01 | 5000 | s3://...
-- 2 | SAPS4 | BillingDocument | D | LastChangeDateTime| 2020-01-01 | 5000 | s3://...
```

---

### STEP 1E: Upload `glue_utils.py` to S3

This file is used by ALL Glue jobs. Write it once, upload once.

**Upload to:** `s3://tcpl-datalake/scripts/glue_poc/glue_utils.py`

How to upload:
- AWS Console → S3 → `tcpl-datalake` → `scripts/glue_poc/` → Upload
- OR via CLI: `aws s3 cp glue_utils.py s3://tcpl-datalake/scripts/glue_poc/glue_utils.py`

**Add to EVERY Glue job parameters:**
```
Key:   --extra-py-files
Value: s3://tcpl-datalake/scripts/glue_poc/glue_utils.py
```

### FILE: `glue_utils.py`

```python
"""
glue_utils.py — Shared utility functions for all TCPL Glue POC jobs.

Upload ONCE to: s3://tcpl-datalake/scripts/glue_poc/glue_utils.py
Reference in every Glue job via: --extra-py-files <s3_path>

Contains:
  - get_secret()         : Fetch credentials from AWS Secrets Manager
  - get_snowflake_conn() : Connect to Snowflake
  - get_entity_config()  : Read entity config from GLUE_ENTITY_CONFIG
  - update_watermark()   : Update LAST_WATERMARK after successful run
  - log_audit()          : Write run record to GLUE_AUDIT_LOG
  - get_s3_output_path() : Build S3 path (full/ or delta/)
  - write_parquet_to_s3(): Write Spark DataFrame to S3 as Parquet
"""

import boto3
import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any

import snowflake.connector

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ══════════════════════════════════════════════════════════════════════
# SECRETS MANAGER
# ══════════════════════════════════════════════════════════════════════

def get_secret(secret_name: str, region: str = "ap-south-1") -> Dict[str, Any]:
    """
    Fetch a secret from AWS Secrets Manager and return as dict.

    Usage:
        creds = get_secret("tcpl/glue_poc/sfdc_oauth")
        token = creds["client_id"]
    """
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    logger.info(f"Secret fetched: {secret_name}")
    return secret


# ══════════════════════════════════════════════════════════════════════
# SNOWFLAKE HELPERS
# ══════════════════════════════════════════════════════════════════════

def get_snowflake_conn(sf_creds: Dict) -> snowflake.connector.SnowflakeConnection:
    """
    Create and return a Snowflake connection using credentials dict.

    Usage:
        sf_creds = get_secret("tcpl/glue_poc/snowflake")
        conn = get_snowflake_conn(sf_creds)
    """
    conn = snowflake.connector.connect(
        account=sf_creds["account"],
        user=sf_creds["user"],
        password=sf_creds["password"],
        warehouse=sf_creds["warehouse"],
        database=sf_creds["database"],
        schema=sf_creds["schema"],
        role=sf_creds["role"],
        session_parameters={"QUERY_TAG": "GLUE_POC_EXTRACTION"},
    )
    logger.info(f"Snowflake connected: {sf_creds['account']} / {sf_creds['database']}")
    return conn


def get_entity_config(conn: snowflake.connector.SnowflakeConnection,
                      src_sys: str,
                      entity_name: str) -> Dict:
    """
    Read entity configuration row from GLUE_ENTITY_CONFIG.
    Returns dict with all columns.

    Usage:
        config = get_entity_config(conn, "SFDC", "Account")
        last_watermark = config["LAST_WATERMARK"]
        batch_size     = config["BATCH_SIZE"]
    """
    cursor = conn.cursor(snowflake.connector.DictCursor)
    cursor.execute("""
        SELECT
            CONFIG_ID,
            SRC_SYS_NM,
            SRC_API_NM,
            SRC_OBJ_NM,
            RELATIVE_PATH,
            LOAD_TYP,
            CDC_WATERMARK_COL,
            LAST_WATERMARK,
            BATCH_SIZE,
            STATIC_PARAMS,
            S3_PATH
        FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_ENTITY_CONFIG
        WHERE SRC_SYS_NM   = %s
          AND SRC_OBJ_NM   = %s
          AND REQUIRED_FLAG = 'Y'
    """, (src_sys, entity_name))

    row = cursor.fetchone()
    if not row:
        raise ValueError(
            f"No active config found for {src_sys}.{entity_name}. "
            f"Check GLUE_ENTITY_CONFIG table."
        )
    config = dict(row)
    logger.info(
        f"Config loaded: {entity_name} | "
        f"LoadType={config['LOAD_TYP']} | "
        f"LastWatermark={config['LAST_WATERMARK']} | "
        f"BatchSize={config['BATCH_SIZE']}"
    )
    return config


def update_watermark(conn: snowflake.connector.SnowflakeConnection,
                     config_id: int,
                     new_watermark: datetime) -> None:
    """
    Update LAST_WATERMARK in GLUE_ENTITY_CONFIG after successful extraction.
    Only call this AFTER data is confirmed written to S3 successfully.

    Usage:
        update_watermark(conn, config["CONFIG_ID"], wm_to)
    """
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_ENTITY_CONFIG
        SET LAST_WATERMARK = %s,
            UPDATED_AT     = CURRENT_TIMESTAMP()
        WHERE CONFIG_ID = %s
    """, (
        new_watermark.strftime("%Y-%m-%d %H:%M:%S"),
        config_id
    ))
    conn.commit()
    logger.info(f"Watermark updated → CONFIG_ID={config_id} → {new_watermark}")


def log_audit(conn: snowflake.connector.SnowflakeConnection,
              job_run_id: str,
              src_sys: str,
              entity: str,
              load_type: str,
              wm_from,
              wm_to,
              start_time: datetime,
              end_time: datetime,
              record_count: int,
              s3_path: str,
              status: str,
              error_msg: Optional[str] = None) -> None:
    """
    Insert one row into GLUE_AUDIT_LOG.
    Always called in the finally block — whether job succeeded or failed.

    Usage:
        log_audit(conn, job_run_id, "SFDC", "Account", "DELTA",
                  wm_from, wm_to, start_time, end_time,
                  record_count, s3_path, "SUCCESS")
    """
    duration = int((end_time - start_time).total_seconds())

    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_AUDIT_LOG (
            GLUE_JOB_RUN_ID,
            SRC_SYS_NM,
            ENTITY_NAME,
            LOAD_TYPE,
            WATERMARK_FROM,
            WATERMARK_TO,
            START_TIME,
            END_TIME,
            DURATION_SECONDS,
            RECORD_COUNT,
            S3_PATH,
            STATUS,
            ERROR_MESSAGE
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        job_run_id,
        src_sys,
        entity,
        load_type,
        wm_from.strftime("%Y-%m-%d %H:%M:%S") if wm_from else None,
        wm_to.strftime("%Y-%m-%d %H:%M:%S") if wm_to else None,
        start_time.strftime("%Y-%m-%d %H:%M:%S"),
        end_time.strftime("%Y-%m-%d %H:%M:%S"),
        duration,
        record_count,
        s3_path,
        status,
        error_msg[:2000] if error_msg else None
    ))
    conn.commit()
    logger.info(
        f"Audit logged → entity={entity} | status={status} | "
        f"records={record_count:,} | duration={duration}s"
    )


# ══════════════════════════════════════════════════════════════════════
# S3 HELPERS
# ══════════════════════════════════════════════════════════════════════

def get_s3_output_path(base_s3_path: str, load_type: str, run_date: str) -> str:
    """
    Build S3 output path based on load type and run date.

    Full load:  s3://tcpl-datalake/glue_poc/sfdc/Account/full/2024-03-03/
    Delta load: s3://tcpl-datalake/glue_poc/sfdc/Account/delta/2024-03-04/

    Usage:
        s3_path = get_s3_output_path(config["S3_PATH"], "D", "2024-03-04")
    """
    folder = "full" if load_type == "F" else "delta"
    path = f"{base_s3_path.rstrip('/')}/{folder}/{run_date}/"
    logger.info(f"S3 output path: {path}")
    return path


def write_parquet_to_s3(spark, df, s3_path: str) -> int:
    """
    Write a Spark DataFrame to S3 as Parquet (snappy compressed).
    Returns: row count written.

    Usage:
        count = write_parquet_to_s3(spark, df, s3_path)
    """
    count = df.count()
    logger.info(f"Writing {count:,} rows → {s3_path}")
    (df.write
       .mode("overwrite")
       .option("compression", "snappy")
       .parquet(s3_path))
    logger.info(f"Write complete ✅ → {s3_path}")
    return count
```

---

## 📌 DAY 2 — Tuesday Feb 25
### SFDC Connectivity Test

### How to Create a Glue Job (Step-by-Step Console Guide)

```
1. Go to: AWS Console → AWS Glue → ETL Jobs
2. Click: Create Job
3. Choose: Script Editor → Spark
4. Fill in:
   - Name:     tcpl-glue-poc-sfdc-connectivity-test
   - IAM Role: TCPLGluePOCRole
   - Glue version: Glue 4.0 (Spark 3.3, Python 3)
   - Worker type: G.1X
   - Number of workers: 2
5. Paste the Python script below into the editor
6. Under "Advanced properties" → "Job parameters", add:
   Key: --extra-py-files
   Value: s3://tcpl-datalake/scripts/glue_poc/glue_utils.py

   Key: --additional-python-modules
   Value: snowflake-connector-python
7. Click Save → Click Run
8. Go to: Runs tab → Click your run → Click "Output logs"
   This opens CloudWatch where you see print/logger output
```

### FILE: `glue_poc_sfdc_connectivity_test.py`

```python
"""
SFDC Connectivity Test
═══════════════════════
Run this FIRST (Week 1, Task 5).

What it tests:
  Test 1 → Get OAuth access token from Salesforce
  Test 2 → Run a simple REST API query (SELECT 5 Accounts)
  Test 3 → Submit a Bulk API 2.0 job (then immediately abort it)

How to know it passed:
  Look in CloudWatch logs for:
    ✅ TEST 1 PASSED
    ✅ TEST 2 PASSED
    ✅ TEST 3 PASSED

If any test fails → see TROUBLESHOOTING section at end of this guide.
"""

import sys
import json
import logging
import requests

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from glue_utils import get_secret

# ── Setup logging ─────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ── Init Glue ─────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ── Load SFDC credentials from Secrets Manager ────────────────────────
logger.info("Loading SFDC credentials from Secrets Manager...")
sfdc_creds = get_secret("tcpl/glue_poc/sfdc_oauth")
logger.info("Credentials loaded ✅")

logger.info("=" * 65)
logger.info("STARTING SFDC CONNECTIVITY TESTS")
logger.info("=" * 65)


# ══════════════════════════════════════════════════════════════════════
# TEST 1: OAuth Token
# Get access token using username + password + security_token
# ══════════════════════════════════════════════════════════════════════

logger.info("\nTEST 1: Getting OAuth access token...")

token_response = requests.post(
    f"{sfdc_creds['login_url']}/services/oauth2/token",
    data={
        "grant_type":    "password",
        "client_id":     sfdc_creds["client_id"],
        "client_secret": sfdc_creds["client_secret"],
        "username":      sfdc_creds["username"],
        # SFDC requires password + security_token concatenated
        "password":      sfdc_creds["password"] + sfdc_creds.get("security_token", ""),
    },
    timeout=30
)

if token_response.status_code == 200:
    token_data    = token_response.json()
    access_token  = token_data["access_token"]
    instance_url  = token_data["instance_url"]
    logger.info(f"✅ TEST 1 PASSED")
    logger.info(f"   Instance URL : {instance_url}")
    logger.info(f"   Token type   : {token_data.get('token_type', 'Bearer')}")
else:
    logger.error(f"❌ TEST 1 FAILED")
    logger.error(f"   Status : {token_response.status_code}")
    logger.error(f"   Response: {token_response.text[:500]}")
    raise RuntimeError(
        "SFDC OAuth failed. Check Secrets Manager credentials. "
        "Most common cause: wrong security_token or IP restrictions."
    )

# Standard headers for all subsequent SFDC API calls
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type":  "application/json",
}


# ══════════════════════════════════════════════════════════════════════
# TEST 2: REST API Query
# Run a simple SOQL query — proves we can query data
# ══════════════════════════════════════════════════════════════════════

logger.info("\nTEST 2: Testing REST API with SOQL query...")

query_url = (
    f"{instance_url}/services/data/v57.0/query"
    f"?q=SELECT+Id,Name,LastModifiedDate+FROM+Account+LIMIT+5"
)

query_resp = requests.get(query_url, headers=headers, timeout=30)

if query_resp.status_code == 200:
    result  = query_resp.json()
    records = result.get("records", [])
    logger.info(f"✅ TEST 2 PASSED")
    logger.info(f"   Records returned : {len(records)}")
    logger.info(f"   Total Account count in org: {result.get('totalSize', 'unknown')}")
    if records:
        logger.info(f"   Sample record: {json.dumps(records[0], indent=2)}")
else:
    logger.error(f"❌ TEST 2 FAILED")
    logger.error(f"   Status  : {query_resp.status_code}")
    logger.error(f"   Response: {query_resp.text[:500]}")


# ══════════════════════════════════════════════════════════════════════
# TEST 3: Bulk API 2.0 Job Submission
# Submit and immediately abort a small query job
# ══════════════════════════════════════════════════════════════════════

logger.info("\nTEST 3: Testing Bulk API 2.0 job submission...")

bulk_url = f"{instance_url}/services/data/v57.0/jobs/query"

# Submit the bulk job
bulk_payload = {
    "operation":       "query",
    "query":           "SELECT Id, Name, LastModifiedDate FROM Account LIMIT 100",
    "contentType":     "CSV",
    "columnDelimiter": "COMMA",
    "lineEnding":      "LF",
}

bulk_resp = requests.post(
    bulk_url,
    headers=headers,
    json=bulk_payload,
    timeout=30
)

if bulk_resp.status_code in [200, 201]:
    job_data = bulk_resp.json()
    job_id   = job_data["id"]
    logger.info(f"✅ TEST 3 PASSED")
    logger.info(f"   Bulk Job ID : {job_id}")
    logger.info(f"   Job State   : {job_data.get('state', 'unknown')}")

    # Clean up: abort the test job immediately
    abort_resp = requests.patch(
        f"{bulk_url}/{job_id}",
        headers=headers,
        json={"state": "Aborted"},
        timeout=30
    )
    if abort_resp.status_code in [200, 201]:
        logger.info(f"   Test job aborted and cleaned up ✅")
    else:
        logger.warning(f"   Could not abort test job (non-critical): {abort_resp.status_code}")
else:
    logger.error(f"❌ TEST 3 FAILED")
    logger.error(f"   Status  : {bulk_resp.status_code}")
    logger.error(f"   Response: {bulk_resp.text[:500]}")


# ══════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════

logger.info("\n" + "=" * 65)
logger.info("SFDC CONNECTIVITY TEST COMPLETE")
logger.info("If all 3 tests showed ✅ above → SFDC is ready for Week 2")
logger.info("=" * 65)

job.commit()
```

**What you should see in CloudWatch logs:**
```
✅ TEST 1 PASSED
   Instance URL : https://tcpl.my.salesforce.com
✅ TEST 2 PASSED
   Records returned : 5
   Total Account count in org: 49000
✅ TEST 3 PASSED
   Bulk Job ID : 750xxxxxxxxxxxxxxxxx
   Test job aborted and cleaned up ✅
SFDC CONNECTIVITY TEST COMPLETE
```

---

## 📌 DAY 3 — Wednesday Feb 26
### SAP Connectivity Test

> ⚠️ **BLOCKER WARNING:**
> SAP firewall whitelisting is **IN PROGRESS** (waiting on infra).
> If this test fails with timeout/503 → firewall is NOT open yet.
> **Action:** Escalate to TCPL Infra team daily.
> **Meanwhile:** Keep building the SAP extraction code — it will work once firewall opens.

Same Glue job creation steps as Day 2.
**Job Name:** `tcpl-glue-poc-sap-connectivity-test`

### FILE: `glue_poc_sap_connectivity_test.py`

```python
"""
SAP OData Connectivity Test
════════════════════════════
Run in Week 1 (Task 3).

What it tests:
  Test 1 → Basic HTTP connection + authentication to SAP OData
  Test 2 → Fetch 5 records from BillingDocument entity
  Test 3 → Verify response format (OData v2 or v4)

SAP OData URL format:
  https://{host}/sap/opu/odata/sap/{SERVICE_NAME}/{EntityName}?$top=5

If firewall not open yet → you'll get 502/503/timeout.
Escalate to infra team with the Glue NAT Gateway IP to whitelist.
"""

import sys
import json
import logging
import requests

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from glue_utils import get_secret

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger.info("Loading SAP credentials from Secrets Manager...")
sap_creds = get_secret("tcpl/glue_poc/sap_odata")
logger.info("Credentials loaded ✅")

logger.info("=" * 65)
logger.info("STARTING SAP CONNECTIVITY TESTS")
logger.info("=" * 65)


# ══════════════════════════════════════════════════════════════════════
# TEST 1: Basic Connectivity + Authentication
# ══════════════════════════════════════════════════════════════════════

logger.info("\nTEST 1: Connecting to SAP OData endpoint...")

# BillingDocument is our POC table — use it as the test endpoint
test_url = (
    f"{sap_creds['base_url'].rstrip('/')}"
    f"/API_BILLING_DOCUMENT_SRV"
    f"/BillingDocument"
    f"?$top=1&$format=json"
)

logger.info(f"Testing URL: {test_url}")

try:
    response = requests.get(
        test_url,
        auth=(sap_creds["username"], sap_creds["password"]),
        headers={
            "Accept":     "application/json",
            "sap-client": "100",     # SAP client number — confirm with SAP team
        },
        timeout=30
    )

    logger.info(f"HTTP Status Code: {response.status_code}")
    logger.info(f"Response Headers: {dict(response.headers)}")

    if response.status_code == 200:
        logger.info(f"✅ TEST 1 PASSED — Connected to SAP OData successfully")
    elif response.status_code == 401:
        logger.error("❌ TEST 1 FAILED — 401 UNAUTHORIZED")
        logger.error("   Cause: Wrong username or password in Secrets Manager")
        logger.error("   Action: Verify credentials with TCPL SAP team")
        raise RuntimeError("SAP authentication failed — check credentials")
    elif response.status_code == 403:
        logger.error("❌ TEST 1 FAILED — 403 FORBIDDEN")
        logger.error("   Cause: Technical user lacks access to API_BILLING_DOCUMENT_SRV")
        logger.error("   Action: Ask SAP BASIS team to grant access to this OData service")
        raise RuntimeError("SAP authorization failed — user needs OData service access")
    elif response.status_code == 404:
        logger.error("❌ TEST 1 FAILED — 404 NOT FOUND")
        logger.error("   Cause: API_BILLING_DOCUMENT_SRV not found at this URL")
        logger.error("   Action: Confirm exact OData service name with SAP team")
        raise RuntimeError("SAP OData endpoint not found")
    elif response.status_code in [502, 503, 504, 0]:
        logger.error("❌ TEST 1 FAILED — NETWORK ERROR")
        logger.error(f"   Status: {response.status_code}")
        logger.error("   Cause: SAP host is unreachable from Glue")
        logger.error("   Cause: Firewall whitelisting still PENDING")
        logger.error("   Action: Escalate to infra team with Glue NAT Gateway IP")
        raise RuntimeError("SAP host unreachable — firewall not open")
    else:
        logger.error(f"❌ TEST 1 FAILED — Unexpected status: {response.status_code}")
        logger.error(f"   Response: {response.text[:300]}")
        raise RuntimeError(f"SAP unexpected response: {response.status_code}")

except requests.exceptions.Timeout:
    logger.error("❌ TEST 1 FAILED — CONNECTION TIMEOUT")
    logger.error("   Cause: Request timed out after 30 seconds")
    logger.error("   Almost certainly means: firewall is NOT open yet")
    logger.error("   Action: Escalate firewall whitelisting immediately")
    raise

except requests.exceptions.ConnectionError as e:
    logger.error("❌ TEST 1 FAILED — CONNECTION ERROR")
    logger.error(f"   Error: {str(e)}")
    logger.error("   Cause: Cannot reach SAP host at all")
    logger.error("   Action: Confirm SAP host URL is correct + firewall is open")
    raise


# ══════════════════════════════════════════════════════════════════════
# TEST 2: Fetch BillingDocument Records
# ══════════════════════════════════════════════════════════════════════

logger.info("\nTEST 2: Fetching 5 BillingDocument records...")

fetch_url = (
    f"{sap_creds['base_url'].rstrip('/')}"
    f"/API_BILLING_DOCUMENT_SRV"
    f"/BillingDocument"
    f"?$top=5&$format=json"
)

fetch_resp = requests.get(
    fetch_url,
    auth=(sap_creds["username"], sap_creds["password"]),
    headers={"Accept": "application/json", "sap-client": "100"},
    timeout=30
)

fetch_resp.raise_for_status()
data = fetch_resp.json()

# SAP OData can return either v2 or v4 format — handle both
records = (
    data.get("d", {}).get("results", [])  or  # OData v2 format
    data.get("value", [])                       # OData v4 format
)

if records:
    logger.info(f"✅ TEST 2 PASSED")
    logger.info(f"   Records returned : {len(records)}")
    logger.info(f"   OData format     : {'v2' if 'd' in data else 'v4'}")
    logger.info(f"   Available fields : {list(records[0].keys())}")
    logger.info(f"   Sample record    :")
    for key, val in list(records[0].items())[:8]:
        logger.info(f"     {key}: {val}")
else:
    logger.warning("⚠️ TEST 2 WARNING — 200 OK but no records returned")
    logger.warning("   This could mean the entity is empty OR filter is wrong")
    logger.warning(f"   Raw response: {json.dumps(data)[:300]}")


# ══════════════════════════════════════════════════════════════════════
# TEST 3: Confirm Pagination Works (fetch page 2)
# ══════════════════════════════════════════════════════════════════════

logger.info("\nTEST 3: Testing $skip/$top pagination...")

page2_url = (
    f"{sap_creds['base_url'].rstrip('/')}"
    f"/API_BILLING_DOCUMENT_SRV"
    f"/BillingDocument"
    f"?$top=5&$skip=5&$format=json"
)

page2_resp = requests.get(
    page2_url,
    auth=(sap_creds["username"], sap_creds["password"]),
    headers={"Accept": "application/json", "sap-client": "100"},
    timeout=30
)

page2_resp.raise_for_status()
page2_data = page2_resp.json()
page2_records = (
    page2_data.get("d", {}).get("results", []) or
    page2_data.get("value", [])
)

if page2_records:
    logger.info(f"✅ TEST 3 PASSED — Pagination works")
    logger.info(f"   Page 2 records returned: {len(page2_records)}")
    logger.info(f"   First key field: {page2_records[0].get('BillingDocument', 'N/A')}")
else:
    logger.warning("⚠️ TEST 3 — Page 2 returned no records (table might have < 10 rows)")


# ══════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════

logger.info("\n" + "=" * 65)
logger.info("SAP CONNECTIVITY TEST COMPLETE")
logger.info("If all 3 tests showed ✅ → SAP is ready for Week 2")
logger.info("Document the field names you see — you'll need them in the extraction job")
logger.info("=" * 65)

job.commit()
```

**Expected output in CloudWatch:**
```
✅ TEST 1 PASSED — Connected to SAP OData successfully
✅ TEST 2 PASSED
   Records returned : 5
   OData format     : v2
   Available fields : ['BillingDocument', 'BillingDocumentDate', 'NetAmountInCoCodeCrcy', ...]
✅ TEST 3 PASSED — Pagination works
   Page 2 records returned: 5
SAP CONNECTIVITY TEST COMPLETE
```

---

## 📌 DAYS 4-5 — Thu/Fri
- Fix any connectivity issues
- Decide which source to start with in Week 2 (whichever connected first)
- SAP firewall still blocked? → Start with SFDC Account table

---
---

# ═══════════════════════════════════════════
# WEEK 2: BUILD EXTRACTION JOBS
# ═══════════════════════════════════════════

---

## STEP 3: SFDC Extraction Job — `Account` Table

### How to Create the Glue Job

```
1. AWS Console → Glue → ETL Jobs → Create Job → Script Editor → Spark
2. Name:       tcpl-glue-poc-sfdc-extract
3. IAM Role:   TCPLGluePOCRole
4. Glue version: Glue 4.0
5. Worker type:  G.1X
6. Workers:      4 (for Account 49K rows — increase for larger tables)
7. Paste script below
8. Job parameters:
   --extra-py-files            → s3://tcpl-datalake/scripts/glue_poc/glue_utils.py
   --additional-python-modules → snowflake-connector-python
   --ENTITY_NAME               → Account   (set per run)
   --LOAD_TYPE                 → F         (F for first test, D for delta)
   --RUN_DATE                  → 2024-03-03
9. Save → Run
```

### Test Sequence (Run in This Order)

```
Run 1 — Full Load:
  --ENTITY_NAME = Account
  --LOAD_TYPE   = F
  --RUN_DATE    = 2024-03-03

  Expected: ~49,000 records written to
  s3://tcpl-datalake/glue_poc/sfdc/Account/full/2024-03-03/

Run 2 — Delta Load (run next day):
  --ENTITY_NAME = Account
  --LOAD_TYPE   = D
  --RUN_DATE    = 2024-03-04

  Expected: Only records modified after 2024-03-03 written to
  s3://tcpl-datalake/glue_poc/sfdc/Account/delta/2024-03-04/
  (might be 0 if no Account changed overnight — that's fine)
```

### FILE: `glue_poc_sfdc_extract.py`

```python
"""
AWS Glue Job: tcpl-glue-poc-sfdc-extract
══════════════════════════════════════════
Extracts data from Salesforce using Bulk API 2.0.

For this POC: Extracts the Account object.
Supports both Full Load (F) and Delta/Incremental Load (D).

How Salesforce Bulk API 2.0 works:
  Step 1: POST → submit a SOQL query as an async bulk job
  Step 2: GET  → poll the job status until "JobComplete"
  Step 3: GET  → download results as CSV pages
  Step 4: Convert CSV to Parquet → write to S3

Delta logic:
  For delta loads, we add: WHERE LastModifiedDate > {last_watermark}
  We use a 2-hour overlap window to catch late-arriving records.
  After success → update LAST_WATERMARK in GLUE_ENTITY_CONFIG.

Job Parameters:
  --ENTITY_NAME : SFDC object name. For POC = "Account"
  --LOAD_TYPE   : F = Full load | D = Delta load
  --RUN_DATE    : YYYY-MM-DD format. Used in S3 path.
"""

import sys
import time
import csv
import io
import json
import logging
import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F

from glue_utils import (
    get_secret,
    get_snowflake_conn,
    get_entity_config,
    update_watermark,
    log_audit,
    get_s3_output_path,
    write_parquet_to_s3,
)

# ── Logging Setup ──────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ── Initialize Glue ───────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENTITY_NAME"])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ── Job Parameters ────────────────────────────────────────────────────
JOB_RUN_ID  = args.get("JOB_RUN_ID", "manual-run-001")
ENTITY_NAME = args["ENTITY_NAME"]                                         # e.g. "Account"
RUN_DATE    = args.get("RUN_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

# ── Load Secrets ──────────────────────────────────────────────────────
sfdc_creds = get_secret("tcpl/glue_poc/sfdc_oauth")
sf_creds   = get_secret("tcpl/glue_poc/snowflake")

# ── Load Entity Config from Snowflake ─────────────────────────────────
sf_conn = get_snowflake_conn(sf_creds)
config  = get_entity_config(sf_conn, "SFDC", ENTITY_NAME)

# ── Resolve Job Config ────────────────────────────────────────────────
LOAD_TYPE  = args.get("LOAD_TYPE", config["LOAD_TYP"])   # F or D
BATCH_SIZE = int(config["BATCH_SIZE"])                    # How many records per CSV page
S3_BASE    = config["S3_PATH"]
S3_PATH    = get_s3_output_path(S3_BASE, LOAD_TYPE, RUN_DATE)

start_time = datetime.now(timezone.utc)

logger.info(f"""
{'='*65}
SFDC EXTRACTION JOB STARTING
  Entity    : {ENTITY_NAME}
  Load Type : {'FULL' if LOAD_TYPE == 'F' else 'DELTA (incremental)'}
  Run Date  : {RUN_DATE}
  S3 Output : {S3_PATH}
  Job Run ID: {JOB_RUN_ID}
{'='*65}
""")


# ══════════════════════════════════════════════════════════════════════
# SALESFORCE BULK API 2.0 CLIENT CLASS
# ══════════════════════════════════════════════════════════════════════

class SFDCBulkClient:
    """
    Salesforce Bulk API 2.0 client.

    Usage:
        client = SFDCBulkClient(sfdc_creds)
        client.authenticate()
        soql   = client.build_soql("Account", "LastModifiedDate", last_wm, "D")
        job_id = client.submit_bulk_query(soql)
        client.poll_until_complete(job_id)
        records = client.download_results(job_id)
    """

    API_VERSION = "v57.0"

    def __init__(self, creds: Dict):
        self.login_url      = creds.get("login_url", "https://login.salesforce.com")
        self.client_id      = creds["client_id"]
        self.client_secret  = creds["client_secret"]
        self.username       = creds["username"]
        self.password       = creds["password"]
        self.security_token = creds.get("security_token", "")
        self.access_token   = None
        self.instance_url   = None

    # ── Step 1: Authenticate ──────────────────────────────────────────

    def authenticate(self) -> None:
        """Get OAuth access token using username-password flow."""
        logger.info(f"Authenticating with SFDC at {self.login_url}...")

        resp = requests.post(
            f"{self.login_url}/services/oauth2/token",
            data={
                "grant_type":    "password",
                "client_id":     self.client_id,
                "client_secret": self.client_secret,
                "username":      self.username,
                # SFDC requires: password concatenated with security_token
                "password":      self.password + self.security_token,
            },
            timeout=30,
        )
        resp.raise_for_status()

        data              = resp.json()
        self.access_token = data["access_token"]
        self.instance_url = data["instance_url"]

        logger.info(f"Authenticated ✅ → {self.instance_url}")

    @property
    def _headers(self) -> Dict:
        """Standard headers for all API calls after authentication."""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type":  "application/json",
        }

    # ── Step 2: Build SOQL ────────────────────────────────────────────

    def build_soql(self,
                   obj_name: str,
                   watermark_col: str,
                   last_watermark,
                   load_type: str) -> str:
        """
        Build SOQL query.
        - Full load:  SELECT all fields FROM {obj}
        - Delta load: SELECT all fields FROM {obj} WHERE {col} > {watermark - 2hrs}
        """

        # Get all field names via describe API (dynamic — handles any object)
        desc_url = (
            f"{self.instance_url}/services/data/{self.API_VERSION}"
            f"/sobjects/{obj_name}/describe"
        )
        desc_resp = requests.get(desc_url, headers=self._headers, timeout=30)
        desc_resp.raise_for_status()

        all_fields = [f["name"] for f in desc_resp.json()["fields"]]
        field_list = ", ".join(all_fields)

        logger.info(f"Object {obj_name} has {len(all_fields)} fields")

        soql = f"SELECT {field_list} FROM {obj_name}"

        if load_type == "D" and last_watermark:
            # Apply 2-hour lookback window to catch late-arriving records
            wm_dt       = datetime.fromisoformat(str(last_watermark).replace(" ", "T"))
            wm_adjusted = (wm_dt - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
            soql       += f" WHERE {watermark_col} > {wm_adjusted}"
            logger.info(f"Delta filter applied: {watermark_col} > {wm_adjusted}")

        logger.info(f"SOQL built ({len(soql)} chars): {soql[:200]}...")
        return soql

    # ── Step 3: Submit Bulk Job ───────────────────────────────────────

    def submit_bulk_query(self, soql: str) -> str:
        """Submit SOQL as async Bulk Query job. Returns job_id."""
        url = f"{self.instance_url}/services/data/{self.API_VERSION}/jobs/query"

        resp = requests.post(
            url,
            headers=self._headers,
            json={
                "operation":       "queryAll",   # includes soft-deleted records
                "query":           soql,
                "contentType":     "CSV",
                "columnDelimiter": "COMMA",
                "lineEnding":      "LF",
            },
            timeout=60,
        )
        resp.raise_for_status()

        job_id = resp.json()["id"]
        logger.info(f"Bulk job submitted ✅ → Job ID: {job_id}")
        return job_id

    # ── Step 4: Poll Until Complete ───────────────────────────────────

    def poll_until_complete(self,
                            job_id: str,
                            poll_interval_sec: int = 15,
                            max_wait_sec: int = 7200) -> int:
        """
        Poll job status every poll_interval_sec until JobComplete or Failed.
        Returns total records processed.
        Raises RuntimeError if job fails or times out.
        """
        url     = f"{self.instance_url}/services/data/{self.API_VERSION}/jobs/query/{job_id}"
        elapsed = 0

        logger.info(f"Polling job {job_id} every {poll_interval_sec}s (max {max_wait_sec}s)...")

        while elapsed < max_wait_sec:
            resp = requests.get(url, headers=self._headers, timeout=30)
            resp.raise_for_status()

            status_data = resp.json()
            state       = status_data.get("state", "")
            processed   = status_data.get("numberRecordsProcessed", 0)

            logger.info(
                f"  [{elapsed:>5}s] State: {state:<15} | "
                f"Records processed: {processed:>10,}"
            )

            if state == "JobComplete":
                logger.info(f"Job complete ✅ Total records: {processed:,}")
                return processed

            elif state in ("Failed", "Aborted"):
                error = status_data.get("errorMessage", "Unknown error")
                raise RuntimeError(
                    f"Bulk job {job_id} ended with state={state}. Error: {error}"
                )

            time.sleep(poll_interval_sec)
            elapsed += poll_interval_sec

        raise TimeoutError(
            f"Bulk job {job_id} did not complete within {max_wait_sec}s"
        )

    # ── Step 5: Download Results ──────────────────────────────────────

    def download_results(self, job_id: str) -> List[Dict]:
        """
        Download all result pages from completed bulk job.
        Returns: list of dicts (one per record).
        """
        all_records = []
        locator     = None
        page_num    = 0

        logger.info(f"Downloading results for job {job_id}...")

        while True:
            page_num += 1

            url = (
                f"{self.instance_url}/services/data/{self.API_VERSION}"
                f"/jobs/query/{job_id}/results"
                f"?maxRecords={BATCH_SIZE}"
            )
            if locator:
                url += f"&locator={locator}"

            resp = requests.get(
                url,
                headers={**self._headers, "Accept": "text/csv"},
                timeout=120,
            )
            resp.raise_for_status()

            # Parse CSV page
            reader       = csv.DictReader(io.StringIO(resp.text))
            page_records = list(reader)
            all_records.extend(page_records)

            logger.info(
                f"  Page {page_num}: {len(page_records):,} records "
                f"(running total: {len(all_records):,})"
            )

            # Check for next page
            locator = resp.headers.get("Sforce-Locator")
            if not locator or locator == "null":
                logger.info(f"All pages downloaded. Total: {len(all_records):,} records")
                break

        return all_records


# ══════════════════════════════════════════════════════════════════════
# MAIN EXTRACTION LOGIC
# ══════════════════════════════════════════════════════════════════════

total_records = 0
wm_to         = datetime.now(timezone.utc)
status        = "FAILED"
error_msg     = None

try:
    # ─── 1. Authenticate ─────────────────────────────────────────────
    client = SFDCBulkClient(sfdc_creds)
    client.authenticate()

    # ─── 2. Build SOQL ───────────────────────────────────────────────
    soql = client.build_soql(
        obj_name      = ENTITY_NAME,
        watermark_col = config.get("CDC_WATERMARK_COL", "LastModifiedDate"),
        last_watermark= config.get("LAST_WATERMARK"),
        load_type     = LOAD_TYPE,
    )

    # ─── 3. Submit Bulk Job ──────────────────────────────────────────
    job_id = client.submit_bulk_query(soql)

    # ─── 4. Poll Until Complete ──────────────────────────────────────
    total_in_sfdc = client.poll_until_complete(job_id, poll_interval_sec=15)

    # ─── 5. Download Results ─────────────────────────────────────────
    records = client.download_results(job_id)
    logger.info(f"Downloaded {len(records):,} records from SFDC")

    # Handle case where delta finds no new records (valid/expected)
    if len(records) == 0:
        if LOAD_TYPE == "D":
            logger.info("No new/changed records since last watermark → nothing to write. This is normal.")
        else:
            logger.warning("Full load returned 0 records — check SOQL or object permissions")
        status = "SUCCESS"

    else:
        # ─── 6. Convert to Spark DataFrame ───────────────────────────
        logger.info(f"Converting {len(records):,} records to Spark DataFrame...")

        rdd = spark.sparkContext.parallelize(records, numSlices=50)
        df  = spark.read.json(rdd.map(lambda r: json.dumps(r)))

        # Add metadata columns for lineage tracking
        df = (df
              .withColumn("_source",       F.lit("SFDC"))
              .withColumn("_entity",       F.lit(ENTITY_NAME))
              .withColumn("_load_type",    F.lit("FULL" if LOAD_TYPE == "F" else "DELTA"))
              .withColumn("_run_date",     F.lit(RUN_DATE))
              .withColumn("_job_run_id",   F.lit(JOB_RUN_ID))
              .withColumn("_extracted_at", F.lit(wm_to.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
        )

        logger.info("DataFrame schema:")
        df.printSchema()

        logger.info(f"Sample rows:")
        df.show(3, truncate=True)

        # ─── 7. Write to S3 as Parquet ───────────────────────────────
        total_records = write_parquet_to_s3(spark, df, S3_PATH)

        # ─── 8. Update Watermark (delta only) ────────────────────────
        if LOAD_TYPE == "D":
            update_watermark(sf_conn, config["CONFIG_ID"], wm_to)
            logger.info(f"Watermark updated to {wm_to}")

        status = "SUCCESS"
        logger.info(f"""
{'='*65}
✅ SFDC EXTRACTION COMPLETE
  Entity      : {ENTITY_NAME}
  Load Type   : {'FULL' if LOAD_TYPE == 'F' else 'DELTA'}
  Records     : {total_records:,}
  S3 Path     : {S3_PATH}
  Duration    : {int((datetime.now(timezone.utc) - start_time).total_seconds())}s
{'='*65}
""")

except Exception as e:
    error_msg = str(e)
    logger.error(f"❌ SFDC EXTRACTION FAILED: {error_msg}")
    raise

finally:
    # Always write to audit log — even on failure
    end_time = datetime.now(timezone.utc)
    log_audit(
        conn         = sf_conn,
        job_run_id   = JOB_RUN_ID,
        src_sys      = "SFDC",
        entity       = ENTITY_NAME,
        load_type    = "FULL" if LOAD_TYPE == "F" else "DELTA",
        wm_from      = config.get("LAST_WATERMARK"),
        wm_to        = wm_to,
        start_time   = start_time,
        end_time     = end_time,
        record_count = total_records,
        s3_path      = S3_PATH,
        status       = status,
        error_msg    = error_msg,
    )
    sf_conn.close()
    job.commit()
```

---

## STEP 4: SAP Extraction Job — `BillingDocument` Table

### How to Create the Glue Job

```
1. AWS Console → Glue → ETL Jobs → Create Job → Script Editor → Spark
2. Name:       tcpl-glue-poc-sap-extract
3. IAM Role:   TCPLGluePOCRole
4. Glue version: Glue 4.0
5. Worker type:  G.1X
6. Workers:      4 (BillingDocument 5.4M rows)
7. Paste script below
8. Job parameters:
   --extra-py-files            → s3://tcpl-datalake/scripts/glue_poc/glue_utils.py
   --additional-python-modules → snowflake-connector-python
   --ENTITY_NAME               → BillingDocument
   --LOAD_TYPE                 → F   (start with full load)
   --RUN_DATE                  → 2024-03-05
9. Save → Run
```

### Test Sequence

```
Run 1 — Full Load:
  --ENTITY_NAME = BillingDocument
  --LOAD_TYPE   = F
  --RUN_DATE    = 2024-03-05

  Expected: ~5,390,000 records in
  s3://tcpl-datalake/glue_poc/sap/BillingDocument/full/2024-03-05/

Run 2 — Delta Load (next day):
  --ENTITY_NAME = BillingDocument
  --LOAD_TYPE   = D
  --RUN_DATE    = 2024-03-06

  Expected: Only records where LastChangeDateTime > 2024-03-05
  written to: s3://tcpl-datalake/glue_poc/sap/BillingDocument/delta/2024-03-06/
```

### FILE: `glue_poc_sap_extract.py`

```python
"""
AWS Glue Job: tcpl-glue-poc-sap-extract
══════════════════════════════════════════
Extracts data from SAP S/4HANA using OData APIs.

For this POC: Extracts BillingDocument entity.
Supports both Full Load (F) and Delta/Incremental Load (D).

How SAP OData extraction works:
  1. Build OData URL with $filter (delta) and $top/$skip (pagination)
  2. GET the first page of records
  3. Loop: increment $skip by BATCH_SIZE, fetch next page
  4. Stop when a page returns fewer records than BATCH_SIZE
  5. Write each 50-page chunk to S3 (avoid OOM on large tables)

Delta logic:
  WHERE LastChangeDateTime gt datetime'2024-03-04T22:00:00'
  (2-hour lookback window for late-arriving records)

Pagination:
  $top=5000&$skip=0   → records 1-5000
  $top=5000&$skip=5000 → records 5001-10000
  ...continue until page returns < 5000 records

Job Parameters:
  --ENTITY_NAME : SAP entity name. For POC = "BillingDocument"
  --LOAD_TYPE   : F = Full | D = Delta
  --RUN_DATE    : YYYY-MM-DD
"""

import sys
import json
import logging
import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Generator
from urllib.parse import quote

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F

from glue_utils import (
    get_secret,
    get_snowflake_conn,
    get_entity_config,
    update_watermark,
    log_audit,
    get_s3_output_path,
    write_parquet_to_s3,
)

# ── Logging Setup ──────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ── Initialize Glue ───────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENTITY_NAME"])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ── Job Parameters ────────────────────────────────────────────────────
JOB_RUN_ID  = args.get("JOB_RUN_ID", "manual-run-001")
ENTITY_NAME = args["ENTITY_NAME"]
RUN_DATE    = args.get("RUN_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

# ── Load Secrets ──────────────────────────────────────────────────────
sap_creds = get_secret("tcpl/glue_poc/sap_odata")
sf_creds  = get_secret("tcpl/glue_poc/snowflake")

# ── Load Entity Config from Snowflake ─────────────────────────────────
sf_conn = get_snowflake_conn(sf_creds)
config  = get_entity_config(sf_conn, "SAPS4", ENTITY_NAME)

# ── Resolve Job Config ────────────────────────────────────────────────
LOAD_TYPE  = args.get("LOAD_TYPE", config["LOAD_TYP"])
BATCH_SIZE = int(config["BATCH_SIZE"])
S3_BASE    = config["S3_PATH"]
S3_PATH    = get_s3_output_path(S3_BASE, LOAD_TYPE, RUN_DATE)
API_NAME   = config["SRC_API_NM"]   # e.g. API_BILLING_DOCUMENT_SRV
SAP_BASE   = sap_creds["base_url"].rstrip("/")

start_time = datetime.now(timezone.utc)

logger.info(f"""
{'='*65}
SAP ODATA EXTRACTION JOB STARTING
  Entity    : {ENTITY_NAME}
  API       : {API_NAME}
  Load Type : {'FULL' if LOAD_TYPE == 'F' else 'DELTA (incremental)'}
  Batch Size: {BATCH_SIZE:,}
  Run Date  : {RUN_DATE}
  S3 Output : {S3_PATH}
  Job Run ID: {JOB_RUN_ID}
{'='*65}
""")


# ══════════════════════════════════════════════════════════════════════
# SAP ODATA CLIENT CLASS
# ══════════════════════════════════════════════════════════════════════

class SAPODataClient:
    """
    SAP OData API client.
    Handles authentication, URL building, and paginated extraction.

    Usage:
        client = SAPODataClient(sap_creds, "API_BILLING_DOCUMENT_SRV")
        for page in client.stream_pages("BillingDocument", "LastChangeDateTime", wm, "D", None):
            process(page)
    """

    def __init__(self, creds: Dict, api_name: str):
        self.base_url = creds["base_url"].rstrip("/")
        self.username = creds["username"]
        self.password = creds["password"]
        self.api_name = api_name

        # Create a persistent session (reuses TCP connection)
        self.session = requests.Session()
        self.session.auth = (self.username, self.password)
        self.session.headers.update({
            "Accept":       "application/json",
            "Content-Type": "application/json",
            "sap-client":   "100",
        })

    def test_connectivity(self, entity: str) -> bool:
        """Quick ping — confirm we can reach SAP and auth works."""
        url = f"{self.base_url}/{self.api_name}/{entity}?$top=1&$format=json"
        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code == 200:
                logger.info(f"SAP connectivity OK ✅ → {entity}")
                return True
            else:
                logger.error(f"SAP connectivity failed: HTTP {resp.status_code}")
                return False
        except Exception as e:
            logger.error(f"SAP connectivity exception: {e}")
            return False

    def _build_url(self,
                   entity: str,
                   watermark_col: Optional[str],
                   last_watermark,
                   load_type: str,
                   static_params: Optional[str],
                   skip: int) -> str:
        """
        Build OData request URL.

        Example full load URL:
          .../BillingDocument?$top=5000&$skip=0&$format=json

        Example delta URL:
          .../BillingDocument?$top=5000&$skip=0&$format=json
          &$filter=LastChangeDateTime gt datetime'2024-03-04T22:00:00'
          &$orderby=LastChangeDateTime asc
        """
        base    = f"{self.base_url}/{self.api_name}/{entity}"
        params  = [f"$top={BATCH_SIZE}", f"$skip={skip}", "$format=json"]
        filters = []

        if load_type == "D" and watermark_col and last_watermark:
            # Parse watermark and subtract 2 hours (overlap window)
            wm_str  = str(last_watermark).replace(" ", "T")
            wm_dt   = datetime.fromisoformat(wm_str)
            wm_adj  = (wm_dt - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S")
            filters.append(f"{watermark_col} gt datetime'{wm_adj}'")
            logger.info(f"Delta filter: {watermark_col} > {wm_adj}")

        if static_params:
            # e.g. "SalesOrderType eq 'OR' or SalesOrderType eq 'ZOR'"
            filters.append(f"({static_params})")

        if filters:
            filter_str = " and ".join(filters)
            params.append(f"$filter={quote(filter_str)}")

        if watermark_col:
            # Order by watermark for consistent, resumable pagination
            params.append(f"$orderby={watermark_col} asc")

        return f"{base}?{'&'.join(params)}"

    def stream_pages(self,
                     entity: str,
                     watermark_col: Optional[str],
                     last_watermark,
                     load_type: str,
                     static_params: Optional[str]) -> Generator[List[Dict], None, None]:
        """
        Generator that yields one page of records at a time.
        Uses $skip/$top pagination.

        Stops when:
          - A page returns 0 records
          - A page returns fewer than BATCH_SIZE (last page)

        Example:
          for page_records in client.stream_pages(...):
              process_page(page_records)
        """
        skip         = 0
        page_num     = 0
        total_so_far = 0

        while True:
            page_num += 1
            url = self._build_url(
                entity, watermark_col, last_watermark,
                load_type, static_params, skip
            )

            logger.info(f"Page {page_num} | skip={skip:,} | URL: {url[:180]}...")

            # Handle SAP rate limiting (429)
            while True:
                resp = self.session.get(url, timeout=120)
                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", 60))
                    logger.warning(f"Rate limited by SAP. Waiting {wait}s before retry...")
                    import time; time.sleep(wait)
                else:
                    break

            resp.raise_for_status()
            data = resp.json()

            # Handle both OData v2 ({"d": {"results": [...]}})
            # and OData v4 ({"value": [...]})
            records = (
                data.get("d", {}).get("results", []) or  # OData v2
                data.get("value", [])                      # OData v4
            )

            if not records:
                logger.info(
                    f"Page {page_num} returned 0 records. "
                    f"Total extracted: {total_so_far:,}. Done."
                )
                break

            total_so_far += len(records)
            logger.info(
                f"Page {page_num}: {len(records):,} records "
                f"(running total: {total_so_far:,})"
            )

            yield records  # Yield this page for processing

            # If last page (got fewer records than batch size)
            if len(records) < BATCH_SIZE:
                logger.info(
                    f"Last page reached ({len(records)} < {BATCH_SIZE}). "
                    f"Total: {total_so_far:,}"
                )
                break

            skip += BATCH_SIZE  # Move to next page


# ══════════════════════════════════════════════════════════════════════
# MAIN EXTRACTION LOGIC
# ══════════════════════════════════════════════════════════════════════

total_records = 0
wm_to         = datetime.now(timezone.utc)
wm_from       = None
status        = "FAILED"
error_msg     = None

try:
    # ─── 1. Initialize SAP Client ────────────────────────────────────
    client = SAPODataClient(sap_creds, API_NAME)

    # Fail fast — don't proceed if SAP is unreachable
    if not client.test_connectivity(ENTITY_NAME):
        raise ConnectionError(
            f"Cannot reach SAP OData endpoint: {API_NAME}/{ENTITY_NAME}. "
            f"Check firewall whitelisting."
        )

    wm_from = config.get("LAST_WATERMARK")
    logger.info(
        f"Extracting {ENTITY_NAME} | "
        f"Load={'FULL' if LOAD_TYPE=='F' else 'DELTA'} | "
        f"From={wm_from}"
    )

    # ─── 2. Stream Pages → Spark → S3 ────────────────────────────────
    # Write in chunks of 50 pages to avoid Out-of-Memory on large tables
    # (BillingDocument 5.4M rows = ~1080 pages at 5000/page)

    page_dfs   = []   # Accumulate Spark DataFrames
    batch_num  = 0    # Which 50-page chunk we're on
    chunk_num  = 0    # Which chunk we've written to S3

    for page_records in client.stream_pages(
        entity        = ENTITY_NAME,
        watermark_col = config.get("CDC_WATERMARK_COL"),
        last_watermark= wm_from,
        load_type     = LOAD_TYPE,
        static_params = config.get("STATIC_PARAMS"),
    ):
        batch_num += 1

        # Convert this page to a Spark DataFrame
        rdd     = spark.sparkContext.parallelize(page_records, numSlices=4)
        page_df = spark.read.json(rdd.map(lambda r: json.dumps(r)))
        page_dfs.append(page_df)

        # Every 50 pages → combine into one DF → write to S3 → free memory
        if batch_num % 50 == 0:
            chunk_num += 1
            combined   = page_dfs[0]
            for df in page_dfs[1:]:
                combined = combined.unionByName(df, allowMissingColumns=True)

            chunk_path = f"{S3_PATH}chunk_{chunk_num:03d}/"
            count      = write_parquet_to_s3(spark, combined, chunk_path)
            total_records += count
            page_dfs = []  # Free memory

            logger.info(f"Chunk {chunk_num} written: {count:,} rows → {chunk_path}")

    # Write final remaining pages (last batch < 50 pages)
    if page_dfs:
        chunk_num += 1
        combined   = page_dfs[0]
        for df in page_dfs[1:]:
            combined = combined.unionByName(df, allowMissingColumns=True)

        # Add metadata columns to final batch
        combined = (combined
            .withColumn("_source",       F.lit("SAPS4"))
            .withColumn("_entity",       F.lit(ENTITY_NAME))
            .withColumn("_load_type",    F.lit("FULL" if LOAD_TYPE == "F" else "DELTA"))
            .withColumn("_run_date",     F.lit(RUN_DATE))
            .withColumn("_job_run_id",   F.lit(JOB_RUN_ID))
            .withColumn("_extracted_at", F.lit(wm_to.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
        )

        chunk_path = f"{S3_PATH}chunk_{chunk_num:03d}_final/"
        count      = write_parquet_to_s3(spark, combined, chunk_path)
        total_records += count
        logger.info(f"Final chunk written: {count:,} rows")

    # ─── 3. Update Watermark (delta only, only if records found) ─────
    if LOAD_TYPE == "D" and total_records > 0:
        update_watermark(sf_conn, config["CONFIG_ID"], wm_to)

    status = "SUCCESS"
    logger.info(f"""
{'='*65}
✅ SAP EXTRACTION COMPLETE
  Entity      : {ENTITY_NAME}
  Load Type   : {'FULL' if LOAD_TYPE == 'F' else 'DELTA'}
  Total Rows  : {total_records:,}
  S3 Path     : {S3_PATH}
  Chunks      : {chunk_num}
  Duration    : {int((datetime.now(timezone.utc) - start_time).total_seconds())}s
{'='*65}
""")

except Exception as e:
    error_msg = str(e)
    logger.error(f"❌ SAP EXTRACTION FAILED: {error_msg}")
    raise

finally:
    end_time = datetime.now(timezone.utc)
    log_audit(
        conn         = sf_conn,
        job_run_id   = JOB_RUN_ID,
        src_sys      = "SAPS4",
        entity       = ENTITY_NAME,
        load_type    = "FULL" if LOAD_TYPE == "F" else "DELTA",
        wm_from      = wm_from,
        wm_to        = wm_to,
        start_time   = start_time,
        end_time     = end_time,
        record_count = total_records,
        s3_path      = S3_PATH,
        status       = status,
        error_msg    = error_msg,
    )
    sf_conn.close()
    job.commit()
```

---

## 📌 WEEK 2 DAY BY DAY

```
MON Mar 3:
  □ Build SFDC extract job → test Account full load (--LOAD_TYPE F)
  □ Verify S3 has parquet files:
    s3://tcpl-datalake/glue_poc/sfdc/Account/full/2024-03-03/
  □ Check GLUE_AUDIT_LOG has 1 row with STATUS=SUCCESS

TUE Mar 4:
  □ Run Account DELTA load (--LOAD_TYPE D)
  □ Check GLUE_ENTITY_CONFIG: LAST_WATERMARK updated
  □ Check S3: delta/ folder has parquet file

WED Mar 5:
  □ Build SAP extract job → test BillingDocument full load
  □ This will take longer (5.4M rows) — monitor CloudWatch

THU Mar 6:
  □ Run BillingDocument DELTA load
  □ Verify watermark updated correctly
  □ Check S3 paths look correct

FRI Mar 7:
  □ Run this SQL to check everything looks good:

-- Quick health check — run in Snowflake
SELECT
    ENTITY_NAME,
    LOAD_TYPE,
    STATUS,
    RECORD_COUNT,
    DURATION_SECONDS,
    ROUND(RECORD_COUNT / NULLIF(DURATION_SECONDS, 0)) AS rows_per_sec,
    S3_PATH,
    ERROR_MESSAGE
FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_AUDIT_LOG
ORDER BY START_TIME DESC;

-- Expected output:
-- BillingDocument | DELTA   | SUCCESS | 12450   | 420  | 29   | s3://...
-- BillingDocument | FULL    | SUCCESS | 5390000 | 1800 | 2994 | s3://...
-- Account         | DELTA   | SUCCESS | 0       | 45   | 0    | s3://...  ← 0 is fine if no changes
-- Account         | FULL    | SUCCESS | 49000   | 120  | 408  | s3://...
```

---
---

# ═══════════════════════════════════════════
# WEEK 3: SNOWFLAKE LOAD + VALIDATION
# ═══════════════════════════════════════════

---

## STEP 5: Airflow DAG (2-Table POC Pipeline)

Deploy this to your Airflow/MWAA instance.

### FILE: `tcpl_glue_poc_dag.py`

```python
"""
Airflow DAG: tcpl_glue_poc_2table_pipeline
════════════════════════════════════════════
Orchestrates the 2-table Glue POC pipeline.

Flow:
  [Extract Account (SFDC)]     ──┐
                                  ├──→ [Load SFDC → Snowflake] ──┐
  [Extract BillingDocument]    ──┘                                 ├──→ [Validate] → [Done]
  (SAP, runs in parallel)       ──→ [Load SAP  → Snowflake] ──┘

Schedule: Daily at 2AM IST.
"""

from datetime import timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

# ── DAG Default Args ──────────────────────────────────────────────────
default_args = {
    "owner":            "latentview_analytics",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": True,
    "email":            ["data-ops@latentview.com"],
}

# ── DAG Definition ─────────────────────────────────────────────────────
with DAG(
    dag_id          = "tcpl_glue_poc_2table_pipeline",
    default_args    = default_args,
    description     = "TCPL Glue POC — Account (SFDC) + BillingDocument (SAP)",
    schedule_interval = "0 2 * * *",   # 2:00 AM IST daily
    start_date      = days_ago(1),
    catchup         = False,
    tags            = ["tcpl", "glue_poc", "poc"],
) as dag:

    run_date = "{{ ds }}"   # Airflow execution date (YYYY-MM-DD)

    # ══════════════════════════════════════════════════════════════════
    # EXTRACTION TASKS
    # ══════════════════════════════════════════════════════════════════

    # Task 1: Extract Account from Salesforce (SFDC)
    extract_sfdc_account = GlueJobOperator(
        task_id              = "extract_sfdc_account",
        job_name             = "tcpl-glue-poc-sfdc-extract",
        script_args          = {
            "--ENTITY_NAME": "Account",
            "--LOAD_TYPE":   "D",          # Delta load
            "--RUN_DATE":    run_date,
        },
        aws_conn_id          = "aws_tcpl",
        region_name          = "ap-south-1",
        wait_for_completion  = True,
        num_of_dpus          = 2,          # Account is small (49K rows)
        dag                  = dag,
    )

    # Task 2: Extract BillingDocument from SAP (runs in parallel with Account)
    extract_sap_billing_doc = GlueJobOperator(
        task_id              = "extract_sap_billing_document",
        job_name             = "tcpl-glue-poc-sap-extract",
        script_args          = {
            "--ENTITY_NAME": "BillingDocument",
            "--LOAD_TYPE":   "D",          # Delta load
            "--RUN_DATE":    run_date,
        },
        aws_conn_id          = "aws_tcpl",
        region_name          = "ap-south-1",
        wait_for_completion  = True,
        num_of_dpus          = 4,          # BillingDocument 5.4M rows
        dag                  = dag,
    )

    # ══════════════════════════════════════════════════════════════════
    # SNOWFLAKE LOAD TASKS
    # These use EXISTING Glue transform templates — not written by you
    # ══════════════════════════════════════════════════════════════════

    # Task 3: Load SFDC data to Snowflake (after Account extraction done)
    load_sfdc_to_snowflake = GlueJobOperator(
        task_id              = "load_sfdc_to_snowflake_poc",
        # ↓↓↓ THIS IS THE "EXISTING GLUE TEMPLATE" — already built, not your job
        job_name             = "tcpl-glue-sfdc-to-snowflake-core",
        script_args          = {
            "--SOURCE_PATH":   "s3://tcpl-datalake/glue_poc/sfdc/",
            "--TARGET_SCHEMA": "GLUE_POC_DB.SFDC_CORE",
            "--RUN_DATE":      run_date,
        },
        aws_conn_id          = "aws_tcpl",
        wait_for_completion  = True,
        num_of_dpus          = 2,
        dag                  = dag,
    )

    # Task 4: Load SAP data to Snowflake (after BillingDocument extraction done)
    load_sap_to_snowflake = GlueJobOperator(
        task_id              = "load_sap_to_snowflake_poc",
        # ↓↓↓ THIS IS THE "EXISTING GLUE TEMPLATE" — already built, not your job
        job_name             = "tcpl-glue-sap-to-snowflake-core",
        script_args          = {
            "--SOURCE_PATH":   "s3://tcpl-datalake/glue_poc/sap/",
            "--TARGET_SCHEMA": "GLUE_POC_DB.SAPS4_CORE",
            "--RUN_DATE":      run_date,
        },
        aws_conn_id          = "aws_tcpl",
        wait_for_completion  = True,
        num_of_dpus          = 4,
        dag                  = dag,
    )

    # ══════════════════════════════════════════════════════════════════
    # VALIDATION TASK
    # ══════════════════════════════════════════════════════════════════

    # Task 5: Run validation stored procedure in Snowflake
    run_validation = SnowflakeOperator(
        task_id          = "run_data_validation",
        sql              = "CALL GLUE_POC_DB.PROCEDURES.RUN_VALIDATION('{{ ds }}'::DATE);",
        snowflake_conn_id = "snowflake_tcpl",
        warehouse        = "TCPL_ANALYTICS_WH",
        database         = "GLUE_POC_DB",
        dag              = dag,
    )

    # ══════════════════════════════════════════════════════════════════
    # DEPENDENCY GRAPH
    # ══════════════════════════════════════════════════════════════════

    # Extractions run in PARALLEL (no dependency between Account and BillingDocument)
    # Each loads to Snowflake only after its own extraction is done
    # Validation runs only after BOTH loads are complete

    extract_sfdc_account    >> load_sfdc_to_snowflake
    extract_sap_billing_doc >> load_sap_to_snowflake

    [load_sfdc_to_snowflake, load_sap_to_snowflake] >> run_validation
```

---

## STEP 6: Snowflake Validation Setup + Queries

Run this in Snowflake before Task 14 (data validation).

```sql
-- ══════════════════════════════════════════════════════════════════════
-- VALIDATION SETUP — Run this BEFORE Task 14
-- ══════════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS GLUE_POC_DB.VALIDATION;
CREATE SCHEMA IF NOT EXISTS GLUE_POC_DB.PROCEDURES;

-- Table to store all validation results
CREATE TABLE IF NOT EXISTS GLUE_POC_DB.VALIDATION.RESULTS (
    VALIDATION_DATE  DATE,
    TABLE_NAME       VARCHAR(100),
    VALIDATION_TYPE  VARCHAR(50),
    SNAPLOGIC_COUNT  NUMBER,
    GLUE_COUNT       NUMBER,
    DIFFERENCE       NUMBER,
    MATCH_PCT        DECIMAL(8,4),
    STATUS           VARCHAR(10),       -- PASS or FAIL
    NOTES            VARCHAR(500),
    CREATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── Stored Procedure — Called by Airflow after load is done ───────────
CREATE OR REPLACE PROCEDURE GLUE_POC_DB.PROCEDURES.RUN_VALIDATION(RUN_DATE DATE)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN

    -- ── BillingDocument row count ────────────────────────────────────
    INSERT INTO GLUE_POC_DB.VALIDATION.RESULTS
    SELECT
        :RUN_DATE,
        'BillingDocument',
        'ROW_COUNT',
        (SELECT COUNT(*) FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT),
        (SELECT COUNT(*) FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT),
        ABS(
            (SELECT COUNT(*) FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT) -
            (SELECT COUNT(*) FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT)
        ),
        ROUND(
            (SELECT COUNT(*) FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT) /
            NULLIF((SELECT COUNT(*) FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT), 0) * 100,
        4),
        CASE
            WHEN ABS(
                (SELECT COUNT(*) FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT) -
                (SELECT COUNT(*) FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT)
            ) = 0 THEN 'PASS'
            ELSE 'FAIL'
        END,
        NULL;

    -- ── Account row count ─────────────────────────────────────────────
    INSERT INTO GLUE_POC_DB.VALIDATION.RESULTS
    SELECT
        :RUN_DATE,
        'Account',
        'ROW_COUNT',
        (SELECT COUNT(*) FROM SFDC_DB.SFDC_CORE.ACCOUNT),
        (SELECT COUNT(*) FROM GLUE_POC_DB.SFDC_CORE.ACCOUNT),
        ABS(
            (SELECT COUNT(*) FROM GLUE_POC_DB.SFDC_CORE.ACCOUNT) -
            (SELECT COUNT(*) FROM SFDC_DB.SFDC_CORE.ACCOUNT)
        ),
        ROUND(
            (SELECT COUNT(*) FROM GLUE_POC_DB.SFDC_CORE.ACCOUNT) /
            NULLIF((SELECT COUNT(*) FROM SFDC_DB.SFDC_CORE.ACCOUNT), 0) * 100,
        4),
        CASE
            WHEN ABS(
                (SELECT COUNT(*) FROM GLUE_POC_DB.SFDC_CORE.ACCOUNT) -
                (SELECT COUNT(*) FROM SFDC_DB.SFDC_CORE.ACCOUNT)
            ) = 0 THEN 'PASS'
            ELSE 'FAIL'
        END,
        NULL;

    RETURN 'Validation complete for ' || :RUN_DATE;
END;
$$;
```

---

## STEP 7: Run These Validation Queries Manually (Task 14)

### VALIDATION A — Row Count Comparison

```sql
-- ══════════════════════════════════════════════════════════════════════
-- VALIDATION A: Row Count Comparison
-- Run this for both tables. Expected: is_match = TRUE for both.
-- ══════════════════════════════════════════════════════════════════════

SELECT
    'BillingDocument (SAP)' AS table_name,
    (SELECT COUNT(*) FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT)    AS snaplogic_count,
    (SELECT COUNT(*) FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT) AS glue_count,
    ABS(
        (SELECT COUNT(*) FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT) -
        (SELECT COUNT(*) FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT)
    )                                                               AS difference,
    (SELECT COUNT(*) FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT) =
    (SELECT COUNT(*) FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT)    AS is_match

UNION ALL

SELECT
    'Account (SFDC)',
    (SELECT COUNT(*) FROM SFDC_DB.SFDC_CORE.ACCOUNT),
    (SELECT COUNT(*) FROM GLUE_POC_DB.SFDC_CORE.ACCOUNT),
    ABS(
        (SELECT COUNT(*) FROM GLUE_POC_DB.SFDC_CORE.ACCOUNT) -
        (SELECT COUNT(*) FROM SFDC_DB.SFDC_CORE.ACCOUNT)
    ),
    (SELECT COUNT(*) FROM GLUE_POC_DB.SFDC_CORE.ACCOUNT) =
    (SELECT COUNT(*) FROM SFDC_DB.SFDC_CORE.ACCOUNT)

ORDER BY table_name;

-- ✅ Expected result:
-- TABLE NAME             | SNAPLOGIC | GLUE    | DIFF | MATCH
-- Account (SFDC)         | 49000     | 49000   | 0    | TRUE
-- BillingDocument (SAP)  | 5390000   | 5390000 | 0    | TRUE
```

### VALIDATION B — MINUS Query: Missing from Glue

```sql
-- ══════════════════════════════════════════════════════════════════════
-- VALIDATION B: Records in SnapLogic that are MISSING from Glue
-- Expected: 0 rows returned
-- ══════════════════════════════════════════════════════════════════════

-- BillingDocument
SELECT
    'BillingDocument' AS table_name,
    BILLINGDOCUMENT,
    BILLINGDOCUMENTDATE,
    SOLDTOPARTY
FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT
MINUS
SELECT
    'BillingDocument',
    BILLINGDOCUMENT,
    BILLINGDOCUMENTDATE,
    SOLDTOPARTY
FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT;

-- ✅ Expected: 0 rows
-- ❌ If rows appear: records exist in SnapLogic but not in Glue extract → investigate

-- Account
SELECT
    'Account' AS table_name,
    ID,
    NAME,
    LASTMODIFIEDDATE
FROM SFDC_DB.SFDC_CORE.ACCOUNT
MINUS
SELECT
    'Account',
    ID,
    NAME,
    LASTMODIFIEDDATE
FROM GLUE_POC_DB.SFDC_CORE.ACCOUNT;

-- ✅ Expected: 0 rows
```

### VALIDATION C — MINUS Query: Extra in Glue

```sql
-- ══════════════════════════════════════════════════════════════════════
-- VALIDATION C: Records in Glue that are NOT in SnapLogic
-- Expected: 0 rows returned
-- ══════════════════════════════════════════════════════════════════════

-- BillingDocument
SELECT
    'BillingDocument' AS table_name,
    BILLINGDOCUMENT,
    BILLINGDOCUMENTDATE,
    SOLDTOPARTY
FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT
MINUS
SELECT
    'BillingDocument',
    BILLINGDOCUMENT,
    BILLINGDOCUMENTDATE,
    SOLDTOPARTY
FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT;

-- ✅ Expected: 0 rows

-- Account
SELECT
    'Account' AS table_name,
    ID,
    NAME,
    LASTMODIFIEDDATE
FROM GLUE_POC_DB.SFDC_CORE.ACCOUNT
MINUS
SELECT
    'Account',
    ID,
    NAME,
    LASTMODIFIEDDATE
FROM SFDC_DB.SFDC_CORE.ACCOUNT;

-- ✅ Expected: 0 rows
```

### VALIDATION D — Checksum

```sql
-- ══════════════════════════════════════════════════════════════════════
-- VALIDATION D: MD5 Checksum on key columns
-- Expected: SnapLogic checksum = Glue checksum
-- ══════════════════════════════════════════════════════════════════════

-- BillingDocument checksum
SELECT
    'SnapLogic' AS source,
    MD5(LISTAGG(BILLINGDOCUMENT || '|' || NETAMOUNTINCOCODECURRENCY, ',')
        WITHIN GROUP (ORDER BY BILLINGDOCUMENT)) AS checksum
FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT
UNION ALL
SELECT
    'Glue',
    MD5(LISTAGG(BILLINGDOCUMENT || '|' || NETAMOUNTINCOCODECURRENCY, ',')
        WITHIN GROUP (ORDER BY BILLINGDOCUMENT))
FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT;

-- ✅ Expected: Both checksums are identical

-- Account checksum
SELECT
    'SnapLogic' AS source,
    MD5(LISTAGG(ID || '|' || NAME, ',')
        WITHIN GROUP (ORDER BY ID)) AS checksum
FROM SFDC_DB.SFDC_CORE.ACCOUNT
UNION ALL
SELECT
    'Glue',
    MD5(LISTAGG(ID || '|' || NAME, ',')
        WITHIN GROUP (ORDER BY ID))
FROM GLUE_POC_DB.SFDC_CORE.ACCOUNT;

-- ✅ Expected: Both checksums are identical
```

### VALIDATION E — Delta Window Check

```sql
-- ══════════════════════════════════════════════════════════════════════
-- VALIDATION E: Delta load captured correct records
-- Verify Glue delta matches SnapLogic delta for same time window
-- ══════════════════════════════════════════════════════════════════════

-- BillingDocument: Compare delta window records
SELECT
    'SnapLogic (modified on 2024-03-05)' AS source,
    COUNT(*) AS record_count
FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT
WHERE LASTCHANGEDATETIME > '2024-03-04T22:00:00'    -- 2hr overlap window
  AND LASTCHANGEDATETIME <= '2024-03-06T00:00:00'

UNION ALL

SELECT
    'Glue delta run (2024-03-06)',
    COUNT(*)
FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT
WHERE _EXTRACTED_AT::DATE = '2024-03-06'
  AND _LOAD_TYPE = 'DELTA';

-- ✅ Expected: Both counts match

-- Account: Compare delta window records
SELECT
    'SnapLogic (Account modified on 2024-03-03)' AS source,
    COUNT(*) AS record_count
FROM SFDC_DB.SFDC_CORE.ACCOUNT
WHERE LASTMODIFIEDDATE > '2024-03-02T22:00:00'
  AND LASTMODIFIEDDATE <= '2024-03-04T00:00:00'

UNION ALL

SELECT
    'Glue delta run (2024-03-04)',
    COUNT(*)
FROM GLUE_POC_DB.SFDC_CORE.ACCOUNT
WHERE _EXTRACTED_AT::DATE = '2024-03-04'
  AND _LOAD_TYPE = 'DELTA';

-- ✅ Expected: Both counts match
```

### VALIDATION F — Audit Log Health Check

```sql
-- ══════════════════════════════════════════════════════════════════════
-- VALIDATION F: Audit Log — See all Glue job runs + performance
-- ══════════════════════════════════════════════════════════════════════

SELECT
    SRC_SYS_NM                                          AS source,
    ENTITY_NAME,
    LOAD_TYPE,
    STATUS,
    RECORD_COUNT,
    DURATION_SECONDS,
    ROUND(RECORD_COUNT / NULLIF(DURATION_SECONDS, 0))   AS rows_per_second,
    S3_PATH,
    ERROR_MESSAGE
FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_AUDIT_LOG
ORDER BY START_TIME DESC;

-- What good output looks like:
-- SAPS4 | BillingDocument | DELTA | SUCCESS | 12,450  | 420  | 29   | s3://...
-- SAPS4 | BillingDocument | FULL  | SUCCESS | 5390000 | 1800 | 2994 | s3://...
-- SFDC  | Account         | DELTA | SUCCESS | 0       | 45   | 0    | s3://...  (0 = no changes, fine)
-- SFDC  | Account         | FULL  | SUCCESS | 49000   | 120  | 408  | s3://...
```

---

## 📌 WEEK 3 DAY BY DAY

```
MON Mar 10:
  □ Load Account to Snowflake GLUE_POC_DB using existing template
    (Airflow triggers tcpl-glue-sfdc-to-snowflake-core pointing to new S3 path)
  □ Run VALIDATION A for Account → row count matches?

TUE Mar 11:
  □ Load BillingDocument to Snowflake GLUE_POC_DB using existing template
  □ Run VALIDATION A for BillingDocument → row count matches?

WED Mar 12:
  □ Run VALIDATION B + C (MINUS queries for both tables) → expect 0 rows
  □ Run VALIDATION D (checksum) → both checksums match?
  □ Run VALIDATION E (delta window check)

THU Mar 13:
  □ Document ALL validation results (fill template below)
  □ Record performance timings:
    BillingDocument full load: __ min (SnapLogic) vs __ min (Glue)
    Account full load: __ min (SnapLogic) vs __ min (Glue)
  □ Prepare Go/No-Go recommendation

FRI Mar 14-15:
  □ Complete Validation Report
  □ Present to Vishnu Rao + Sam Joseph
  □ MILESTONE REVIEW ← DEADLINE ✅
```

---
---

# ═══════════════════════════════════════════
# TROUBLESHOOTING
# ═══════════════════════════════════════════

---

## Issue 1: SAP Firewall Not Open

```
Symptom: Connection timeout / "Connection refused" / HTTP 502 or 503
Cause:   Glue's NAT Gateway IP is not whitelisted in SAP firewall
Fix:     1. Get Glue's NAT Gateway outbound IP from AWS VPC console
         2. Send that IP to TCPL Infra team to whitelist
         3. Escalate daily until resolved
         4. Meanwhile: build all SAP code, test with SFDC first
```

---

## Issue 2: SFDC "INVALID_LOGIN" Error

```
Symptom: OAuth returns 400 INVALID_LOGIN
Cause A: Wrong password in Secrets Manager
Fix A:   Re-enter password with TCPL SFDC Admin

Cause B: security_token is missing or wrong
Fix B:   SFDC Admin: Setup → My Personal Information → Reset Security Token
         Add the new token value to Secrets Manager (append to password)

Cause C: Connected App has IP restrictions
Fix C:   SFDC Admin: Setup → Connected Apps → Edit → IP Relaxation → Relax IP restrictions
```

---

## Issue 3: SAP Pagination Stops at 5000 Rows

```
Symptom: Job extracts exactly 5000 rows then stops (table has 5.4M)
Cause:   $skip/$top not working or server has max page limit
Fix:     Check if response has nextLink token:
           OData v4: data.get("@odata.nextLink")
           OData v2: data.get("d", {}).get("__next")
         If present, use that URL directly instead of building $skip URL
```

---

## Issue 4: Glue Job OOM (Out of Memory)

```
Symptom: Job killed mid-run, logs show java.lang.OutOfMemoryError
Cause:   Too many records accumulated in memory before writing to S3
Fix:     The code writes every 50 pages to S3 (chunk pattern).
         If still OOM: reduce chunk size to 20 pages.
         Also increase Glue Workers from 4 to 8.
```

---

## Issue 5: MINUS Query Returns Records

```
Symptom: Row counts match but MINUS shows differences
Cause A: Numeric precision — SFDC returns "1.0", SAP returns "1"
Fix A:   Add CAST: CAST(NETAMOUNT AS DECIMAL(18,2))

Cause B: Timestamp format difference
Fix B:   Add CAST: CAST(LASTCHANGEDATETIME AS TIMESTAMP_NTZ)

Cause C: Actual data difference — investigate specific records
Fix C:   Compare the differing rows side by side
```

---

## Issue 6: Watermark Not Updated After Success

```
Symptom: Job SUCCESS but LAST_WATERMARK still shows old value
Fix:     Manually update in Snowflake:

UPDATE PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_ENTITY_CONFIG
SET LAST_WATERMARK = '2024-03-06 00:00:00',
    UPDATED_AT     = CURRENT_TIMESTAMP()
WHERE SRC_OBJ_NM = 'BillingDocument';

UPDATE PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_ENTITY_CONFIG
SET LAST_WATERMARK = '2024-03-04 00:00:00',
    UPDATED_AT     = CURRENT_TIMESTAMP()
WHERE SRC_OBJ_NM = 'Account';
```

---
---

# ═══════════════════════════════════════════
# MARCH 15 VALIDATION REPORT — FILL THIS IN
# ═══════════════════════════════════════════

```
══════════════════════════════════════════════════════════════════════
GLUE POC VALIDATION REPORT — MARCH 15, 2024
Author: [Your Name]
Tables tested: Account (SFDC) + BillingDocument (SAP)
══════════════════════════════════════════════════════════════════════

1. ROW COUNT RESULTS
─────────────────────
┌──────────────────────┬───────────┬───────────┬──────────┬────────┐
│ TABLE                │ SNAPLOGIC │ GLUE      │ DIFF     │ MATCH? │
├──────────────────────┼───────────┼───────────┼──────────┼────────┤
│ Account (SFDC)       │           │           │          │        │
│ BillingDocument (SAP)│           │           │          │        │
└──────────────────────┴───────────┴───────────┴──────────┴────────┘
Result: [X]/2 tables match 100%

2. MINUS QUERY RESULTS
───────────────────────
Account MINUS:         [X] rows returned (expected: 0)
BillingDocument MINUS: [X] rows returned (expected: 0)

3. DELTA LOAD VALIDATION
──────────────────────────
Account delta (2024-03-03 → 2024-03-04):
  SnapLogic: [X] records
  Glue:      [X] records | MATCH: Y / N

BillingDocument delta (2024-03-05 → 2024-03-06):
  SnapLogic: [X] records
  Glue:      [X] records | MATCH: Y / N

4. PERFORMANCE
──────────────
┌──────────────────────┬───────────┬────────┬──────────┐
│ TABLE                │ SNAPLOGIC │ GLUE   │ FASTER?  │
├──────────────────────┼───────────┼────────┼──────────┤
│ Account (49K rows)   │           │        │          │
│ BillingDoc (5.4M)    │           │        │          │
└──────────────────────┴───────────┴────────┴──────────┘

5. RECOMMENDATION: ✅ GO / ❌ NO-GO
────────────────────────────────────
Decision: [GO / NO-GO]
Rationale:
  [ ] Row counts: [X]/2 tables match
  [ ] MINUS queries: 0 rows returned
  [ ] Delta loads work correctly
  [ ] Performance: [faster/slower/same] vs SnapLogic
  [ ] Cost: ~40% reduction vs SnapLogic licensing

Issues found:
  [List any issues + root cause + resolution]

Next steps if GO:
  1. Extend to remaining 10 tables
  2. Run parallel (Glue + SnapLogic) for 2 weeks
  3. Cutover and decommission SnapLogic
══════════════════════════════════════════════════════════════════════
```

---
---

# ═══════════════════════════════════════════
# DAILY CHECKLIST — TICK THIS OFF EVERY DAY
# ═══════════════════════════════════════════

```
══════ WEEK 1 — SETUP + CONNECTIVITY ══════

MON Feb 24:
  □ S3 bucket confirmed existing
  □ IAM Role TCPLGluePOCRole created with inline policy
  □ Secret: tcpl/glue_poc/sfdc_oauth created
  □ Secret: tcpl/glue_poc/sap_odata created
  □ Secret: tcpl/glue_poc/snowflake created
  □ Snowflake: GLUE_ENTITY_CONFIG created
  □ Snowflake: GLUE_AUDIT_LOG created
  □ Snowflake: 2 rows seeded (Account + BillingDocument)
  □ glue_utils.py uploaded to S3 scripts folder
  □ Followed up on SAP firewall whitelisting

TUE Feb 25:
  □ Got SFDC Connected App credentials from TCPL SFDC Admin
  □ Created Glue job: tcpl-glue-poc-sfdc-connectivity-test
  □ Ran SFDC connectivity test → all 3 tests PASSED ✅
  □ Documented SFDC instance URL + confirmed Bulk API works

WED Feb 26:
  □ Got SAP OData technical user credentials from TCPL SAP Team
  □ Created Glue job: tcpl-glue-poc-sap-connectivity-test
  □ Ran SAP connectivity test → all 3 tests PASSED ✅  (or blocked on firewall)
  □ Documented SAP OData field names from test response

THU Feb 27:
  □ Any connectivity issues fixed
  □ Decided which source to start with in Week 2

FRI Feb 28:
  □ Both connectivity tests PASSING
  □ Week 1 status sent to manager

══════ WEEK 2 — EXTRACTION ══════

MON Mar 3:
  □ Created Glue job: tcpl-glue-poc-sfdc-extract
  □ Ran Account FULL load (--LOAD_TYPE F --RUN_DATE 2024-03-03)
  □ S3 has: sfdc/Account/full/2024-03-03/*.parquet
  □ GLUE_AUDIT_LOG: 1 row, STATUS=SUCCESS, ~49000 records

TUE Mar 4:
  □ Ran Account DELTA load (--LOAD_TYPE D --RUN_DATE 2024-03-04)
  □ GLUE_ENTITY_CONFIG: LAST_WATERMARK updated for Account
  □ S3 has: sfdc/Account/delta/2024-03-04/*.parquet

WED Mar 5:
  □ Created Glue job: tcpl-glue-poc-sap-extract
  □ Ran BillingDocument FULL load (--LOAD_TYPE F --RUN_DATE 2024-03-05)
  □ S3 has: sap/BillingDocument/full/2024-03-05/chunk_*.parquet
  □ GLUE_AUDIT_LOG: 1 row, STATUS=SUCCESS, ~5390000 records

THU Mar 6:
  □ Ran BillingDocument DELTA load (--LOAD_TYPE D --RUN_DATE 2024-03-06)
  □ GLUE_ENTITY_CONFIG: LAST_WATERMARK updated for BillingDocument
  □ S3 has: sap/BillingDocument/delta/2024-03-06/chunk_*.parquet

FRI Mar 7:
  □ Ran audit health check SQL → all 4 runs show SUCCESS
  □ Week 2 status sent to manager

══════ WEEK 3 — LOAD + VALIDATE + PRESENT ══════

MON Mar 10:
  □ Account loaded to Snowflake GLUE_POC_DB.SFDC_CORE
  □ VALIDATION A for Account: row count matches ✅

TUE Mar 11:
  □ BillingDocument loaded to Snowflake GLUE_POC_DB.SAPS4_CORE
  □ VALIDATION A for BillingDocument: row count matches ✅

WED Mar 12:
  □ VALIDATION B (MINUS — missing from Glue): 0 rows ✅
  □ VALIDATION C (MINUS — extra in Glue): 0 rows ✅
  □ VALIDATION D (checksum): both match ✅
  □ VALIDATION E (delta window): counts match ✅

THU Mar 13:
  □ All validation results documented
  □ Performance timings recorded
  □ Validation Report filled in

FRI Mar 14-15:
  □ Validation Report complete
  □ Go/No-Go recommendation written
  □ Presented to Vishnu Rao + Sam Joseph ✅ DONE
```

---

## 📁 FILES SUMMARY

| File | What it does | Upload where |
|---|---|---|
| `glue_utils.py` | Shared helpers (Snowflake, Secrets, S3) | `s3://tcpl-datalake/scripts/glue_poc/` |
| `glue_poc_sfdc_connectivity_test.py` | Tests SFDC OAuth + Bulk API | Glue job script |
| `glue_poc_sap_connectivity_test.py` | Tests SAP OData connection | Glue job script |
| `glue_poc_sfdc_extract.py` | Extracts Account from SFDC | Glue job script |
| `glue_poc_sap_extract.py` | Extracts BillingDocument from SAP | Glue job script |
| `tcpl_glue_poc_dag.py` | Airflow orchestration DAG | MWAA DAGs folder |
| Snowflake SQL (STEP 1D) | Creates control + audit tables | Run in Snowflake |
| Snowflake SQL (STEP 6) | Creates validation table + SP | Run in Snowflake |
| Validation Report | March 15 presentation | Fill + present |

---

*2 tables. Account (49K rows) + BillingDocument (5.4M rows).*
*Prove the pattern works → get the GO → then do all 12.*
*Keep this file open every day. Follow it step by step. You've got this bro 💪*
