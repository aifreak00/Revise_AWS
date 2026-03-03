# 🎯 TCPL GLUE POC — SAP 2-TABLE RECIPE
## BillingDocument (Parent) + BillingDocumentItem (Child)
### Your Complete Step-by-Step Execution Guide — Follow This Like a Recipe

---

> **WHAT YOU ARE DOING TODAY:**
> Replace SnapLogic's SAP data pull for 2 tables with an AWS Glue Python job.
> You extract data from SAP via OData API → write Parquet files to S3.
> That's it. S3 to Snowflake is already handled by the existing template.

---

## 🗺️ THE PICTURE IN ONE DIAGRAM

```
TODAY (SnapLogic):
  Airflow DAG (prd-saps4-hourly-billing-documentdelta-load-pipeline)
       ↓
  SnapLogic Pipeline → reads SAP OData API
       ↓
  S3 (Parquet)
       ↓
  Existing Glue Template → Snowflake

AFTER YOUR POC (What you build):
  Airflow DAG (same DAG, or new poc DAG)
       ↓
  YOUR NEW Glue Job → reads SAP OData API
       ↓
  S3 (Parquet)   ←  YOUR JOB ENDS HERE ✅
       ↓
  Existing Glue Template → Snowflake (untouched, already works)
```

---

## 📊 YOUR 2 TABLES — Know Them Cold

| # | Table Name | Rows | Size | Type | Role | Delta Key | Schedule (from DAG) |
|---|---|---|---|---|---|---|---|
| 1 | `BillingDocument` | 5.39M | ~356 MB | Delta | **Parent** | `LastChangeDateTime` | Hourly `:00` |
| 2 | `BillingDocumentItem` | 24.94M | ~2.1 GB | Delta | **Child** | `LastChangeDateTime` | Hourly `:00` |

**Why Parent-Child matters:**
- `BillingDocument` = the invoice header. One row = one invoice.
- `BillingDocumentItem` = line items inside that invoice. One invoice can have many items.
- They join on `BillingDocument` field (the key).
- In your POC: you extract them **separately** but in the **right order** — Parent first, then Child.
- You do NOT need to join them in your Glue job. Just extract both correctly.

---

## 🔑 SAP OData API — What You Need to Know

### The SAP OData endpoints you will call:

```
Parent:
  GET https://<SAP_HOST>/sap/opu/odata/sap/API_BILLING_DOCUMENT_SRV/A_BillingDocument
  
Child:
  GET https://<SAP_HOST>/sap/opu/odata/sap/API_BILLING_DOCUMENT_SRV/A_BillingDocumentItem
```

### Key OData Query Parameters you will use:

| Parameter | What it does | Example |
|---|---|---|
| `$filter` | Delta filter — only fetch changed records | `LastChangeDateTime gt datetime'2024-03-03T00:00:00'` |
| `$top` | Page size — fetch N records at a time | `$top=5000` |
| `$skiptoken` | Pagination — SAP gives you this for next page | Used automatically |
| `$format=json` | Get JSON response | Always include this |
| `$select` | Get only specific fields (optional, use for performance) | `$select=BillingDocument,NetAmount` |

### What Delta Loading means for your job:
```
First Run (Full Load):
  filter = LastChangeDateTime > '2000-01-01T00:00:00'  (effectively gets everything)
  
Subsequent Runs (Delta):
  filter = LastChangeDateTime > '<last_watermark_from_snowflake>'
  
After each successful run:
  Update LAST_WATERMARK in Snowflake GLUE_ENTITY_CONFIG table
  Set it to = max(LastChangeDateTime) from the data you just fetched
```

---

---

# ═══════════════════════════════════════
# PHASE 1: AWS SETUP (Do this first — takes ~30 mins)
# ═══════════════════════════════════════

---

## STEP 1: Verify S3 Bucket Exists

**Where to go:** AWS Console → S3

1. Open https://console.aws.amazon.com
2. Search for "S3" in top search bar → click S3
3. In the bucket list, find `tcpl-datalake`
4. If it exists → ✅ move on
5. If it does NOT exist → stop and ask your infra team. Do not create it yourself.

**The folder paths your job will write to (Glue creates these automatically):**
```
s3://tcpl-datalake/glue_poc/sap/BillingDocument/full/YYYY-MM-DD/
s3://tcpl-datalake/glue_poc/sap/BillingDocument/delta/YYYY-MM-DD/
s3://tcpl-datalake/glue_poc/sap/BillingDocumentItem/full/YYYY-MM-DD/
s3://tcpl-datalake/glue_poc/sap/BillingDocumentItem/delta/YYYY-MM-DD/
```
> You do NOT need to create these manually. Your Glue code will create them when it runs.

---

## STEP 2: Check / Create IAM Role for Glue

**Where to go:** AWS Console → IAM → Roles

1. In top search bar type "IAM" → click IAM
2. Click **Roles** on the left sidebar
3. In the search box, type `TCPLGluePOCRole`
4. **If the role exists:** Click on it. Make sure it has these attached policies:
   - `AWSGlueServiceRole` (AWS managed)
   - An inline policy for S3 + Secrets Manager (see below)
5. **If the role does NOT exist:** Click **Create Role**
   - Trusted entity type: `AWS Service`
   - Use case: `Glue`
   - Click Next → Search and attach: `AWSGlueServiceRole`
   - Click Next → Name it: `TCPLGluePOCRole`
   - Click **Create Role**

**Now add the inline policy** (click the role → Permissions tab → Add permissions → Create inline policy → JSON tab):

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
        "arn:aws:s3:::tcpl-datalake/*"
      ]
    },
    {
      "Sid": "SecretsManagerAccess",
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue",
        "secretsmanager:DescribeSecret"
      ],
      "Resource": [
        "arn:aws:secretsmanager:ap-south-1:*:secret:tcpl/glue_poc/*"
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
- Name this policy: `TCPLGluePOCInlinePolicy`
- Click **Create policy**

---

## STEP 3: Store SAP Credentials in Secrets Manager

**Where to go:** AWS Console → Secrets Manager

1. In top search bar type "Secrets Manager" → click it
2. Click **Store a new secret** (orange button)
3. Secret type: **Other type of secret**
4. Click **Key/value** tab → enter each key-value pair below:

**Secret name:** `tcpl/glue_poc/sap_odata`

| Key | Value (get from SAP Basis/TCPL team) |
|---|---|
| `base_url` | `https://your-sap-host.tcpl.com/sap/opu/odata/sap/` |
| `username` | `GLUE_TECHNICAL_USER` (or whatever SAP user is created for Glue) |
| `password` | The password for that SAP user |

5. Click **Next** → leave rotation off → **Next**
6. Name: `tcpl/glue_poc/sap_odata`
7. Click **Store**

> ⚠️ **IMPORTANT:** The SAP user must have READ access to `API_BILLING_DOCUMENT_SRV` service. Ask your SAP Basis team to confirm this. If they haven't done it yet, tell them exactly: *"I need READ authorization to OData service API_BILLING_DOCUMENT_SRV for user GLUE_TECHNICAL_USER."*

**Also store Snowflake credentials** (you need this to read/write watermarks):

**Secret name:** `tcpl/glue_poc/snowflake`

| Key | Value |
|---|---|
| `account` | `tcpl.ap-southeast-1` (confirm with your Snowflake admin) |
| `user` | `GLUE_POC_USER` |
| `password` | The password |
| `warehouse` | `TCPL_LOAD_WH` |
| `database` | `PRD_ARCH_SUPPORT_DB` |
| `schema` | `GLUE_POC` |
| `role` | `TCPL_GLUE_POC_ROLE` |

---

## STEP 4: Upload Python Helper File to S3

**You need to upload `glue_utils.py` to S3 so your Glue jobs can import it.**

**Where to go:** AWS Console → S3 → tcpl-datalake

1. Go to S3 → click `tcpl-datalake`
2. Navigate into or create the path: `scripts/glue_poc/`
   - Click **Create folder** → name it `scripts` → Create
   - Click into `scripts` → Create folder → `glue_poc` → Create
3. Click **Upload** → **Add files** → select `glue_utils.py` from your computer
4. Click **Upload**

**The S3 path for this file:** `s3://tcpl-datalake/scripts/glue_poc/glue_utils.py`

---

---

# ═══════════════════════════════════════
# PHASE 2: SNOWFLAKE SETUP (Run these SQLs)
# ═══════════════════════════════════════

---

## STEP 5: Create Control Tables in Snowflake

**Where to go:** Snowflake UI → Worksheets

Open a new worksheet. Set context at the top:
- Role: `TCPL_GLUE_POC_ROLE`  
- Warehouse: `TCPL_LOAD_WH`
- Database: `PRD_ARCH_SUPPORT_DB`
- Schema: `GLUE_POC`

Run this SQL **exactly as written**:

```sql
-- ============================================================
-- SETUP: Create GLUE_POC schema if it doesn't exist
-- ============================================================
USE ROLE TCPL_GLUE_POC_ROLE;
USE WAREHOUSE TCPL_LOAD_WH;
USE DATABASE PRD_ARCH_SUPPORT_DB;

CREATE SCHEMA IF NOT EXISTS GLUE_POC;
USE SCHEMA GLUE_POC;

-- ============================================================
-- TABLE 1: GLUE_ENTITY_CONFIG
-- This is the control table. Glue reads this before every run.
-- It stores the watermark (last extract time) for each entity.
-- ============================================================
CREATE OR REPLACE TABLE GLUE_ENTITY_CONFIG (
    CONFIG_ID         NUMBER AUTOINCREMENT PRIMARY KEY,
    SRC_SYS_NM        VARCHAR(20)   NOT NULL,
    SRC_API_NM        VARCHAR(200)  NOT NULL,
    SRC_OBJ_NM        VARCHAR(100)  NOT NULL,
    RELATIVE_PATH     VARCHAR(100)  NOT NULL,
    LOAD_TYP          VARCHAR(1)    NOT NULL,   -- 'D' = Delta, 'F' = Full
    REQUIRED_FLAG     VARCHAR(1)    DEFAULT 'Y',-- 'Y' = active, 'N' = skip
    CDC_WATERMARK_COL VARCHAR(100),
    LAST_WATERMARK    TIMESTAMP_NTZ,            -- ← THIS IS CRITICAL
    BATCH_SIZE        NUMBER        DEFAULT 5000,
    STATIC_PARAMS     VARCHAR(500),
    S3_PATH           VARCHAR(200)  NOT NULL,
    CREATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- TABLE 2: GLUE_AUDIT_LOG
-- Every Glue job run writes a row here. Used for monitoring.
-- ============================================================
CREATE OR REPLACE TABLE GLUE_AUDIT_LOG (
    LOG_ID            NUMBER AUTOINCREMENT PRIMARY KEY,
    CONFIG_ID         NUMBER,
    SRC_OBJ_NM        VARCHAR(100),
    RUN_DATE          DATE,
    START_TIME        TIMESTAMP_NTZ,
    END_TIME          TIMESTAMP_NTZ,
    DURATION_SECS     NUMBER,
    STATUS            VARCHAR(20),   -- 'RUNNING', 'SUCCESS', 'FAILED'
    ROWS_EXTRACTED    NUMBER DEFAULT 0,
    FILES_WRITTEN     NUMBER DEFAULT 0,
    WATERMARK_START   TIMESTAMP_NTZ,
    WATERMARK_END     TIMESTAMP_NTZ,
    ERROR_MSG         VARCHAR(4000),
    CREATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- SEED DATA: Register the 2 tables for your POC
-- Run this AFTER creating the tables above
-- ============================================================

-- Entity 1: BillingDocument (Parent)
INSERT INTO GLUE_ENTITY_CONFIG (
    SRC_SYS_NM, SRC_API_NM, SRC_OBJ_NM, RELATIVE_PATH,
    LOAD_TYP, REQUIRED_FLAG, CDC_WATERMARK_COL, LAST_WATERMARK,
    BATCH_SIZE, S3_PATH
) VALUES (
    'SAPS4',
    'API_BILLING_DOCUMENT_SRV/A_BillingDocument',
    'BillingDocument',
    'sap/BillingDocument',
    'D',
    'Y',
    'LastChangeDateTime',
    '2024-01-01 00:00:00',   -- ← Starting watermark. Change this if needed.
    5000,
    's3://tcpl-datalake/glue_poc/sap/BillingDocument'
);

-- Entity 2: BillingDocumentItem (Child)
INSERT INTO GLUE_ENTITY_CONFIG (
    SRC_SYS_NM, SRC_API_NM, SRC_OBJ_NM, RELATIVE_PATH,
    LOAD_TYP, REQUIRED_FLAG, CDC_WATERMARK_COL, LAST_WATERMARK,
    BATCH_SIZE, S3_PATH
) VALUES (
    'SAPS4',
    'API_BILLING_DOCUMENT_SRV/A_BillingDocumentItem',
    'BillingDocumentItem',
    'sap/BillingDocumentItem',
    'D',
    'Y',
    'LastChangeDateTime',
    '2024-01-01 00:00:00',   -- ← Starting watermark. Change this if needed.
    5000,
    's3://tcpl-datalake/glue_poc/sap/BillingDocumentItem'
);

-- Verify the inserts:
SELECT * FROM GLUE_ENTITY_CONFIG;
-- You should see 2 rows: BillingDocument and BillingDocumentItem
```

---

---

# ═══════════════════════════════════════
# PHASE 3: PYTHON CODE — Write These Files
# ═══════════════════════════════════════

---

## FILE 1: `glue_utils.py` — Shared Helper Library

> This file is imported by all your Glue jobs. Write it once. Upload to S3.
> It handles: getting secrets, connecting to Snowflake, reading config, writing audit logs.

```python
# ============================================================
# glue_utils.py
# Shared utility functions for all TCPL Glue POC jobs
# Upload to: s3://tcpl-datalake/scripts/glue_poc/glue_utils.py
# ============================================================

import json
import boto3
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ──────────────────────────────────────────────────────────────
# FUNCTION 1: get_secret
# Fetches a secret from AWS Secrets Manager and returns it as dict
# ──────────────────────────────────────────────────────────────
def get_secret(secret_name: str, region_name: str = "ap-south-1") -> dict:
    """
    Fetch secret from AWS Secrets Manager.
    
    Args:
        secret_name: The name of the secret (e.g. 'tcpl/glue_poc/sap_odata')
        region_name: AWS region (default ap-south-1 for India)
    
    Returns:
        dict with the secret key-value pairs
    
    Example:
        creds = get_secret('tcpl/glue_poc/sap_odata')
        username = creds['username']
        password = creds['password']
    """
    client = boto3.client("secretsmanager", region_name=region_name)
    
    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret_string = response["SecretString"]
        return json.loads(secret_string)
    except Exception as e:
        logger.error(f"Failed to get secret '{secret_name}': {e}")
        raise


# ──────────────────────────────────────────────────────────────
# FUNCTION 2: get_snowflake_connection
# Returns a live Snowflake connection object
# ──────────────────────────────────────────────────────────────
def get_snowflake_connection():
    """
    Get a Snowflake connection using credentials from Secrets Manager.
    
    Returns:
        snowflake.connector.connection object
    
    Usage:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        conn.close()
    """
    import snowflake.connector
    
    creds = get_secret("tcpl/glue_poc/snowflake")
    
    conn = snowflake.connector.connect(
        account=creds["account"],
        user=creds["user"],
        password=creds["password"],
        warehouse=creds["warehouse"],
        database=creds["database"],
        schema=creds["schema"],
        role=creds["role"]
    )
    
    logger.info(f"Snowflake connected: {creds['database']}.{creds['schema']}")
    return conn


# ──────────────────────────────────────────────────────────────
# FUNCTION 3: get_entity_config
# Reads the entity config (including watermark) from Snowflake
# ──────────────────────────────────────────────────────────────
def get_entity_config(entity_name: str) -> dict:
    """
    Fetch the config row for a given entity from GLUE_ENTITY_CONFIG.
    
    Args:
        entity_name: The SRC_OBJ_NM value (e.g. 'BillingDocument')
    
    Returns:
        dict with all config columns
    
    Example return:
        {
            'config_id': 1,
            'src_api_nm': 'API_BILLING_DOCUMENT_SRV/A_BillingDocument',
            'load_typ': 'D',
            'cdc_watermark_col': 'LastChangeDateTime',
            'last_watermark': datetime(2024, 3, 3, 0, 0, 0),
            'batch_size': 5000,
            's3_path': 's3://tcpl-datalake/glue_poc/sap/BillingDocument',
            ...
        }
    """
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        query = """
            SELECT 
                CONFIG_ID, SRC_SYS_NM, SRC_API_NM, SRC_OBJ_NM,
                RELATIVE_PATH, LOAD_TYP, REQUIRED_FLAG,
                CDC_WATERMARK_COL, LAST_WATERMARK,
                BATCH_SIZE, STATIC_PARAMS, S3_PATH
            FROM GLUE_ENTITY_CONFIG
            WHERE SRC_OBJ_NM = %s
              AND REQUIRED_FLAG = 'Y'
        """
        cursor.execute(query, (entity_name,))
        row = cursor.fetchone()
        
        if not row:
            raise ValueError(f"No active config found for entity: {entity_name}")
        
        columns = [desc[0].lower() for desc in cursor.description]
        config = dict(zip(columns, row))
        
        logger.info(f"Config loaded for {entity_name}: watermark={config.get('last_watermark')}")
        return config
        
    finally:
        cursor.close()
        conn.close()


# ──────────────────────────────────────────────────────────────
# FUNCTION 4: update_watermark
# After a successful run, update the watermark in Snowflake
# ──────────────────────────────────────────────────────────────
def update_watermark(config_id: int, new_watermark: datetime):
    """
    Update LAST_WATERMARK in GLUE_ENTITY_CONFIG after successful extraction.
    
    Args:
        config_id: The CONFIG_ID value from GLUE_ENTITY_CONFIG
        new_watermark: The new watermark (max LastChangeDateTime from fetched data)
    
    IMPORTANT: Only call this after data is successfully written to S3.
    Never update watermark if extraction failed midway.
    """
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        query = """
            UPDATE GLUE_ENTITY_CONFIG
            SET LAST_WATERMARK = %s,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE CONFIG_ID = %s
        """
        cursor.execute(query, (new_watermark, config_id))
        conn.commit()
        logger.info(f"Watermark updated for config_id={config_id} to {new_watermark}")
    finally:
        cursor.close()
        conn.close()


# ──────────────────────────────────────────────────────────────
# FUNCTION 5: write_audit_log
# Write a row to GLUE_AUDIT_LOG for every job run
# ──────────────────────────────────────────────────────────────
def write_audit_log(
    config_id: int,
    src_obj_nm: str,
    start_time: datetime,
    end_time: datetime,
    status: str,
    rows_extracted: int = 0,
    files_written: int = 0,
    watermark_start=None,
    watermark_end=None,
    error_msg: str = None
):
    """
    Write an audit log entry to GLUE_AUDIT_LOG.
    
    Args:
        config_id: From GLUE_ENTITY_CONFIG
        src_obj_nm: Entity name (e.g. 'BillingDocument')
        start_time: When the job started
        end_time: When the job ended
        status: 'SUCCESS' or 'FAILED'
        rows_extracted: How many rows were fetched from SAP
        files_written: How many Parquet files were written to S3
        watermark_start: The watermark before this run
        watermark_end: The watermark after this run (= max LastChangeDateTime)
        error_msg: Error message if status is FAILED
    """
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        duration = int((end_time - start_time).total_seconds())
        run_date = start_time.date()
        
        query = """
            INSERT INTO GLUE_AUDIT_LOG (
                CONFIG_ID, SRC_OBJ_NM, RUN_DATE, START_TIME, END_TIME,
                DURATION_SECS, STATUS, ROWS_EXTRACTED, FILES_WRITTEN,
                WATERMARK_START, WATERMARK_END, ERROR_MSG
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (
            config_id, src_obj_nm, run_date, start_time, end_time,
            duration, status, rows_extracted, files_written,
            watermark_start, watermark_end, error_msg
        ))
        conn.commit()
        logger.info(f"Audit log written: {src_obj_nm} | {status} | {rows_extracted} rows")
    finally:
        cursor.close()
        conn.close()
```

---

## FILE 2: `glue_poc_sap_connectivity_test.py` — Test SAP Connection First

> Run this BEFORE the extraction job. It confirms SAP is reachable from Glue.

```python
# ============================================================
# glue_poc_sap_connectivity_test.py
# Tests SAP OData connectivity from AWS Glue
# Run this first. If it passes, proceed to extraction.
# ============================================================

import sys
import json
import requests
import logging
from awsglue.utils import getResolvedOptions

# Import our shared utils
sys.path.insert(0, '/tmp/')
import glue_utils

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_sap_connectivity():
    """
    Three tests:
    1. Can we get SAP credentials from Secrets Manager? 
    2. Can we reach the SAP host? (Basic auth HTTP call)
    3. Can we call the BillingDocument OData endpoint and get data?
    """
    
    print("=" * 60)
    print("TEST 1: Fetch SAP credentials from Secrets Manager")
    print("=" * 60)
    
    try:
        creds = glue_utils.get_secret("tcpl/glue_poc/sap_odata")
        base_url = creds["base_url"]
        username = creds["username"]
        password = creds["password"]
        print(f"✅ Credentials fetched successfully")
        print(f"   Base URL: {base_url}")
        print(f"   Username: {username}")
        print(f"   Password: {'*' * len(password)}")
    except Exception as e:
        print(f"❌ FAILED: {e}")
        raise
    
    print()
    print("=" * 60)
    print("TEST 2: HTTP connectivity to SAP host")
    print("=" * 60)
    
    # Build a minimal test URL — just fetch 1 record
    test_url = (
        f"{base_url}API_BILLING_DOCUMENT_SRV/A_BillingDocument"
        f"?$top=1&$format=json"
    )
    
    print(f"   Calling: {test_url}")
    
    try:
        response = requests.get(
            test_url,
            auth=(username, password),
            timeout=30,
            headers={"Accept": "application/json"}
        )
        
        print(f"   HTTP Status: {response.status_code}")
        
        if response.status_code == 200:
            print(f"✅ HTTP connection successful")
        elif response.status_code == 401:
            print(f"❌ FAILED: 401 Unauthorized")
            print(f"   → Check username/password in Secrets Manager")
            print(f"   → Check SAP user has access to API_BILLING_DOCUMENT_SRV")
            raise Exception("401 Unauthorized")
        elif response.status_code == 403:
            print(f"❌ FAILED: 403 Forbidden")
            print(f"   → SAP user lacks authorization to this OData service")
            print(f"   → Contact SAP Basis team: user needs S_SERVICE auth for API_BILLING_DOCUMENT_SRV")
            raise Exception("403 Forbidden")
        else:
            print(f"❌ FAILED: Unexpected status {response.status_code}")
            print(f"   Response body: {response.text[:500]}")
            raise Exception(f"HTTP {response.status_code}")
            
    except requests.exceptions.ConnectionError as e:
        print(f"❌ FAILED: Cannot connect to SAP host")
        print(f"   Error: {e}")
        print(f"   → Check if VPC/firewall allows Glue to reach SAP")
        print(f"   → Raise with infra team: open port from Glue subnet to SAP host")
        raise
    except requests.exceptions.Timeout:
        print(f"❌ FAILED: Connection timed out after 30 seconds")
        print(f"   → SAP host is unreachable. Firewall issue. Escalate to infra.")
        raise
    
    print()
    print("=" * 60)
    print("TEST 3: Parse OData response for BillingDocument")
    print("=" * 60)
    
    try:
        data = response.json()
        
        # OData v2 response format: {"d": {"results": [...]}}
        # OData v4 response format: {"value": [...]}
        if "d" in data:
            records = data["d"]["results"]
            odata_version = "v2"
        elif "value" in data:
            records = data["value"]
            odata_version = "v4"
        else:
            print(f"❌ Unexpected response format: {list(data.keys())}")
            raise Exception("Unknown OData format")
        
        print(f"✅ OData {odata_version} response parsed successfully")
        print(f"   Records returned: {len(records)}")
        
        if records:
            first_record = records[0]
            print(f"   Sample record keys ({len(first_record)} fields):")
            for k, v in list(first_record.items())[:10]:
                print(f"     {k}: {v}")
            
            # Check for LastChangeDateTime field
            if "LastChangeDateTime" in first_record:
                print(f"✅ Delta key field 'LastChangeDateTime' is present")
            else:
                print(f"⚠️  WARNING: 'LastChangeDateTime' not found in response!")
                print(f"   Available fields: {list(first_record.keys())}")
        
        print()
        print("=" * 60)
        print("TEST 4: BillingDocumentItem endpoint")
        print("=" * 60)
        
        item_url = (
            f"{base_url}API_BILLING_DOCUMENT_SRV/A_BillingDocumentItem"
            f"?$top=1&$format=json"
        )
        print(f"   Calling: {item_url}")
        
        item_response = requests.get(
            item_url,
            auth=(username, password),
            timeout=30,
            headers={"Accept": "application/json"}
        )
        
        if item_response.status_code == 200:
            item_data = item_response.json()
            if "d" in item_data:
                item_records = item_data["d"]["results"]
            else:
                item_records = item_data.get("value", [])
            
            print(f"✅ BillingDocumentItem endpoint accessible")
            print(f"   Records returned: {len(item_records)}")
            if item_records:
                print(f"   Fields: {list(item_records[0].keys())[:10]}")
        else:
            print(f"❌ BillingDocumentItem: HTTP {item_response.status_code}")
            
    except Exception as e:
        print(f"❌ Parse failed: {e}")
        raise
    
    print()
    print("=" * 60)
    print("✅ ALL CONNECTIVITY TESTS PASSED")
    print("   You are ready to run the extraction job.")
    print("=" * 60)


# Entry point
test_sap_connectivity()
```

---

## FILE 3: `glue_poc_sap_extract.py` — The Main Extraction Job

> This is the core job. It reads config from Snowflake, calls SAP OData API with pagination,
> writes Parquet to S3, and updates the watermark. This handles both BillingDocument and BillingDocumentItem.

```python
# ============================================================
# glue_poc_sap_extract.py
# Main SAP extraction Glue job for BillingDocument + BillingDocumentItem
#
# How to run:
#   Pass --entity_name BillingDocument     (to run parent)
#   Pass --entity_name BillingDocumentItem (to run child)
#
# The job reads config from Snowflake, calls SAP OData API with
# delta filter + pagination, writes Parquet to S3, updates watermark.
# ============================================================

import sys
import json
import logging
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import io
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions

sys.path.insert(0, '/tmp/')
import glue_utils

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────

# Read --entity_name argument from Glue job parameters
# When you create the Glue job, you pass: --entity_name BillingDocument
args = getResolvedOptions(sys.argv, ['entity_name'])
ENTITY_NAME = args['entity_name']

# Supported entities for this job
SUPPORTED_ENTITIES = ['BillingDocument', 'BillingDocumentItem']

if ENTITY_NAME not in SUPPORTED_ENTITIES:
    raise ValueError(
        f"Invalid entity_name: '{ENTITY_NAME}'. "
        f"Must be one of: {SUPPORTED_ENTITIES}"
    )

logger.info(f"Starting extraction for entity: {ENTITY_NAME}")


# ──────────────────────────────────────────────────────────────
# CLASS: SAPODataExtractor
# Handles all SAP API interaction + pagination
# ──────────────────────────────────────────────────────────────

class SAPODataExtractor:
    """
    Extracts data from SAP OData API with full pagination support.
    
    Supports both:
    - OData v2: response in {"d": {"results": [...], "__next": "..."}}
    - OData v4: response in {"value": [...], "@odata.nextLink": "..."}
    """
    
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip("/") + "/"
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json"
        })
        logger.info(f"SAPODataExtractor initialized with base_url={self.base_url}")
    
    def build_delta_filter(self, watermark_col: str, last_watermark: datetime) -> str:
        """
        Build the OData $filter for delta loading.
        
        For OData v2:
            LastChangeDateTime gt datetime'2024-03-03T00:00:00'
        
        For OData v4 (ISO 8601):
            LastChangeDateTime gt 2024-03-03T00:00:00Z
        
        We build the OData v2 format here. If your SAP is v4, 
        the code will handle it automatically via the nextLink format.
        """
        # Format: datetime'YYYY-MM-DDTHH:MM:SS'
        watermark_str = last_watermark.strftime("%Y-%m-%dT%H:%M:%S")
        return f"{watermark_col} gt datetime'{watermark_str}'"
    
    def fetch_page(self, url: str) -> tuple:
        """
        Fetch a single page from the OData endpoint.
        
        Returns:
            (records: list of dicts, next_url: str or None)
        """
        try:
            response = self.session.get(url, timeout=120)
            response.raise_for_status()
        except requests.exceptions.Timeout:
            logger.error(f"Timeout fetching: {url}")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error {response.status_code} fetching {url}: {response.text[:200]}")
            raise
        
        data = response.json()
        
        # Parse OData v2 vs v4
        if "d" in data:
            # OData v2
            results = data["d"].get("results", [])
            next_url = data["d"].get("__next")  # pagination link
        elif "value" in data:
            # OData v4
            results = data["value"]
            next_url = data.get("@odata.nextLink")
        else:
            logger.warning(f"Unexpected OData response format: {list(data.keys())}")
            results = []
            next_url = None
        
        return results, next_url
    
    def extract_all(
        self, 
        api_endpoint: str,
        watermark_col: str,
        last_watermark: datetime,
        batch_size: int = 5000
    ):
        """
        Extract ALL records with delta filter, paginating through all pages.
        
        This is a GENERATOR — it yields one page of records at a time.
        This means you write each page to S3 as you go, without holding
        all 24M rows in memory at once.
        
        Args:
            api_endpoint: The OData entity path (e.g. 'API_BILLING_DOCUMENT_SRV/A_BillingDocument')
            watermark_col: Delta column name (e.g. 'LastChangeDateTime')
            last_watermark: The last extracted timestamp from Snowflake
            batch_size: Records per page (default 5000)
        
        Yields:
            list of record dicts (one page at a time)
        
        Usage:
            for page_records in extractor.extract_all(...):
                # write page to S3
                # update max watermark seen
        """
        
        # Build the initial URL with filter + top + format
        delta_filter = self.build_delta_filter(watermark_col, last_watermark)
        
        initial_url = (
            f"{self.base_url}{api_endpoint}"
            f"?$filter={requests.utils.quote(delta_filter)}"
            f"&$top={batch_size}"
            f"&$format=json"
            f"&$orderby={watermark_col} asc"
        )
        
        logger.info(f"Starting extraction from: {api_endpoint}")
        logger.info(f"Delta filter: {delta_filter}")
        logger.info(f"Page size: {batch_size}")
        
        current_url = initial_url
        page_number = 0
        total_records = 0
        
        while current_url:
            page_number += 1
            logger.info(f"Fetching page {page_number} | Total so far: {total_records}")
            
            records, next_url = self.fetch_page(current_url)
            
            if not records:
                logger.info(f"Empty page received. Extraction complete.")
                break
            
            total_records += len(records)
            logger.info(f"Page {page_number}: {len(records)} records | Total: {total_records}")
            
            yield records
            
            current_url = next_url
        
        logger.info(f"Extraction complete. Total pages: {page_number} | Total records: {total_records}")


# ──────────────────────────────────────────────────────────────
# FUNCTION: write_parquet_to_s3
# Converts a list of records to Parquet and uploads to S3
# ──────────────────────────────────────────────────────────────

def write_parquet_to_s3(records: list, s3_path: str, file_index: int) -> str:
    """
    Convert records to Parquet and write to S3.
    
    Args:
        records: List of dicts (one page of SAP data)
        s3_path: The S3 prefix (e.g. 's3://tcpl-datalake/glue_poc/sap/BillingDocument/delta/2024-03-03')
        file_index: Page number (used to create unique filenames)
    
    Returns:
        Full S3 path of the written file
    
    File naming: part-00001.parquet, part-00002.parquet, etc.
    """
    
    # Build DataFrame from records
    df = pd.DataFrame(records)
    
    # Convert to PyArrow Table for Parquet writing
    table = pa.Table.from_pandas(df)
    
    # Build the full S3 key
    # e.g. glue_poc/sap/BillingDocument/delta/2024-03-03/part-00001.parquet
    s3_prefix = s3_path.replace("s3://", "")
    bucket = s3_prefix.split("/")[0]
    key_prefix = "/".join(s3_prefix.split("/")[1:])
    
    file_key = f"{key_prefix}/part-{str(file_index).zfill(5)}.parquet"
    
    # Write Parquet to in-memory buffer
    buffer = io.BytesIO()
    pq.write_table(
        table,
        buffer,
        compression="snappy",   # Snappy = fast compression, widely supported
        row_group_size=100000   # 100K rows per row group (good for Snowflake reads)
    )
    buffer.seek(0)
    
    # Upload to S3
    s3_client = boto3.client("s3", region_name="ap-south-1")
    s3_client.put_object(
        Bucket=bucket,
        Key=file_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )
    
    full_s3_path = f"s3://{bucket}/{file_key}"
    logger.info(f"Written to S3: {full_s3_path} ({len(records)} rows)")
    return full_s3_path


# ──────────────────────────────────────────────────────────────
# MAIN EXTRACTION LOGIC
# ──────────────────────────────────────────────────────────────

def main():
    
    start_time = datetime.utcnow()
    logger.info(f"Job started at {start_time} UTC")
    logger.info(f"Entity: {ENTITY_NAME}")
    
    # ── Step 1: Get entity config from Snowflake ──────────────
    logger.info("Step 1: Loading entity config from Snowflake...")
    config = glue_utils.get_entity_config(ENTITY_NAME)
    
    config_id       = config["config_id"]
    api_endpoint    = config["src_api_nm"]       # e.g. 'API_BILLING_DOCUMENT_SRV/A_BillingDocument'
    watermark_col   = config["cdc_watermark_col"]  # 'LastChangeDateTime'
    last_watermark  = config["last_watermark"]     # datetime from Snowflake
    batch_size      = int(config["batch_size"])    # 5000
    s3_base_path    = config["s3_path"]            # 's3://tcpl-datalake/glue_poc/sap/BillingDocument'
    load_type       = config["load_typ"]           # 'D' for delta
    
    logger.info(f"Config loaded:")
    logger.info(f"  API endpoint: {api_endpoint}")
    logger.info(f"  Watermark column: {watermark_col}")
    logger.info(f"  Last watermark: {last_watermark}")
    logger.info(f"  Batch size: {batch_size}")
    logger.info(f"  S3 base path: {s3_base_path}")
    
    # ── Step 2: Build S3 output path ─────────────────────────
    # Pattern: {s3_base_path}/{load_type_folder}/{date}/
    run_date_str = start_time.strftime("%Y-%m-%d")
    
    if load_type == "D":
        load_folder = "delta"
    else:
        load_folder = "full"
    
    s3_output_path = f"{s3_base_path}/{load_folder}/{run_date_str}"
    logger.info(f"S3 output path: {s3_output_path}")
    
    # ── Step 3: Get SAP credentials ───────────────────────────
    logger.info("Step 3: Fetching SAP credentials from Secrets Manager...")
    sap_creds = glue_utils.get_secret("tcpl/glue_poc/sap_odata")
    
    base_url = sap_creds["base_url"]
    username = sap_creds["username"]
    password = sap_creds["password"]
    
    # ── Step 4: Initialize extractor ─────────────────────────
    extractor = SAPODataExtractor(base_url, username, password)
    
    # ── Step 5: Extract + Write (page by page) ────────────────
    logger.info("Step 5: Starting extraction and writing to S3...")
    
    total_rows = 0
    files_written = 0
    max_watermark = last_watermark  # Track the max LastChangeDateTime seen
    
    try:
        for page_records in extractor.extract_all(
            api_endpoint=api_endpoint,
            watermark_col=watermark_col,
            last_watermark=last_watermark,
            batch_size=batch_size
        ):
            files_written += 1
            total_rows += len(page_records)
            
            # Write this page to S3 immediately (don't accumulate in memory)
            write_parquet_to_s3(
                records=page_records,
                s3_path=s3_output_path,
                file_index=files_written
            )
            
            # Track the maximum LastChangeDateTime in this page
            # This becomes the new watermark after the job completes
            for record in page_records:
                record_wm = record.get(watermark_col)
                if record_wm:
                    # SAP OData v2 returns datetime as: "/Date(1709596800000)/"
                    # SAP OData v4 returns: "2024-03-05T00:00:00Z"
                    # Handle both:
                    if isinstance(record_wm, str):
                        if record_wm.startswith("/Date("):
                            # Parse OData v2 date: /Date(milliseconds)/
                            ms = int(record_wm.replace("/Date(", "").replace(")/", "").split("+")[0])
                            record_dt = datetime.utcfromtimestamp(ms / 1000)
                        else:
                            # Parse ISO 8601
                            record_dt = datetime.fromisoformat(record_wm.replace("Z", "+00:00")).replace(tzinfo=None)
                        
                        if record_dt > max_watermark:
                            max_watermark = record_dt
            
            logger.info(f"Progress: {total_rows} rows | {files_written} files | max watermark: {max_watermark}")
        
        # ── Step 6: Update watermark in Snowflake ────────────
        if total_rows > 0:
            logger.info(f"Step 6: Updating watermark to {max_watermark}...")
            glue_utils.update_watermark(config_id, max_watermark)
        else:
            logger.info("Step 6: No new records found. Watermark not updated.")
        
        # ── Step 7: Write SUCCESS audit log ──────────────────
        end_time = datetime.utcnow()
        glue_utils.write_audit_log(
            config_id=config_id,
            src_obj_nm=ENTITY_NAME,
            start_time=start_time,
            end_time=end_time,
            status="SUCCESS",
            rows_extracted=total_rows,
            files_written=files_written,
            watermark_start=last_watermark,
            watermark_end=max_watermark if total_rows > 0 else last_watermark
        )
        
        logger.info(f"✅ JOB COMPLETE: {ENTITY_NAME}")
        logger.info(f"   Total rows extracted: {total_rows:,}")
        logger.info(f"   Parquet files written: {files_written}")
        logger.info(f"   S3 path: {s3_output_path}")
        logger.info(f"   Duration: {(end_time - start_time).total_seconds():.1f}s")
        
    except Exception as e:
        # ── Write FAILED audit log ────────────────────────────
        end_time = datetime.utcnow()
        error_msg = str(e)[:3000]
        
        glue_utils.write_audit_log(
            config_id=config_id,
            src_obj_nm=ENTITY_NAME,
            start_time=start_time,
            end_time=end_time,
            status="FAILED",
            rows_extracted=total_rows,
            files_written=files_written,
            watermark_start=last_watermark,
            error_msg=error_msg
        )
        
        logger.error(f"❌ JOB FAILED: {ENTITY_NAME}")
        logger.error(f"   Error: {error_msg}")
        logger.error(f"   Rows extracted before failure: {total_rows:,}")
        raise  # Re-raise so Glue marks the job as FAILED


# Entry point
if __name__ == "__main__":
    main()
```

---

---

# ═══════════════════════════════════════
# PHASE 4: CREATE GLUE JOBS IN AWS CONSOLE
# ═══════════════════════════════════════

> You create separate Glue jobs for:
> 1. The connectivity test
> 2. BillingDocument extraction  
> 3. BillingDocumentItem extraction

---

## STEP 6: Upload Python Files to S3

Before creating Glue jobs, upload your Python scripts to S3.

**Where to go:** AWS Console → S3 → tcpl-datalake → scripts/glue_poc/

Upload these 3 files:
1. `glue_utils.py`
2. `glue_poc_sap_connectivity_test.py`
3. `glue_poc_sap_extract.py`

After upload, you should have:
```
s3://tcpl-datalake/scripts/glue_poc/glue_utils.py
s3://tcpl-datalake/scripts/glue_poc/glue_poc_sap_connectivity_test.py
s3://tcpl-datalake/scripts/glue_poc/glue_poc_sap_extract.py
```

---

## STEP 7: Create Glue Job — Connectivity Test

**Where to go:** AWS Console → AWS Glue → ETL Jobs

1. Click **Create job**
2. Choose: **Script editor** → **Python Shell** → click **Create**
3. In the script editor, paste the contents of `glue_poc_sap_connectivity_test.py`

**Job Details (click "Job details" tab):**

| Setting | Value |
|---|---|
| **Name** | `tcpl-glue-poc-sap-connectivity-test` |
| **IAM Role** | `TCPLGluePOCRole` |
| **Type** | Python Shell |
| **Python version** | Python 3.9 |
| **Max capacity** | 0.0625 DPU (smallest — this is just a test) |
| **Job timeout** | 5 minutes |
| **Max retries** | 0 |

**Libraries — Extra Python libraries (Optional, under Advanced properties):**

In "Python library path", add:
```
s3://tcpl-datalake/scripts/glue_poc/glue_utils.py
```

4. Click **Save**

**Run it:**
- Click **Run** (top right)
- Go to **Run details** tab → wait for it to finish
- Click **Logs** → look for the ✅ messages
- If it fails → click **Error logs** → read the error

---

## STEP 8: Create Glue Job — BillingDocument Extraction

**Where to go:** AWS Console → AWS Glue → ETL Jobs → Create job

1. Choose: **Script editor** → **Spark** (not Python Shell — Spark gives more power for large data)
2. Paste contents of `glue_poc_sap_extract.py`

**Job Details:**

| Setting | Value |
|---|---|
| **Name** | `tcpl-glue-poc-sap-billing-document` |
| **IAM Role** | `TCPLGluePOCRole` |
| **Type** | Spark |
| **Glue version** | Glue 4.0 |
| **Language** | Python 3 |
| **Worker type** | G.1X (4 vCPU, 16 GB) |
| **Number of workers** | 2 (sufficient for 5.39M rows) |
| **Job timeout** | 60 minutes |
| **Max retries** | 1 |

**Job Parameters (click "Job parameters" under Advanced properties):**

| Key | Value |
|---|---|
| `--entity_name` | `BillingDocument` |
| `--extra-py-files` | `s3://tcpl-datalake/scripts/glue_poc/glue_utils.py` |
| `--additional-python-modules` | `snowflake-connector-python==3.5.0,pyarrow==12.0.0,pandas==2.0.0` |

3. Click **Save**

---

## STEP 9: Create Glue Job — BillingDocumentItem Extraction

Same as above, but with different settings for the larger child table.

**Job Details:**

| Setting | Value |
|---|---|
| **Name** | `tcpl-glue-poc-sap-billing-document-item` |
| **IAM Role** | `TCPLGluePOCRole` |
| **Type** | Spark |
| **Glue version** | Glue 4.0 |
| **Worker type** | G.1X |
| **Number of workers** | **4** (more workers for 24.94M rows — larger child table) |
| **Job timeout** | 120 minutes |
| **Max retries** | 1 |

**Job Parameters:**

| Key | Value |
|---|---|
| `--entity_name` | `BillingDocumentItem` |
| `--extra-py-files` | `s3://tcpl-datalake/scripts/glue_poc/glue_utils.py` |
| `--additional-python-modules` | `snowflake-connector-python==3.5.0,pyarrow==12.0.0,pandas==2.0.0` |

---

---

# ═══════════════════════════════════════
# PHASE 5: RUN THE JOBS — Exact Order
# ═══════════════════════════════════════

> Always run in this order. Never run BillingDocumentItem before BillingDocument.

---

## RUN ORDER (Critical — Follow This Exactly)

```
Step 1: Run connectivity test first
         ↓ wait for it to PASS
         
Step 2: Run BillingDocument (Parent) — Full Load first
         ↓ wait for SUCCESS in audit log
         
Step 3: Run BillingDocumentItem (Child) — Full Load
         ↓ wait for SUCCESS in audit log
         
Step 4: Check S3 — verify Parquet files are there
         ↓
         
Step 5: Validate in Snowflake — row counts vs SnapLogic
         ↓
         
Step 6: Run delta test — change watermark, re-run, verify only new records come
```

---

## HOW TO RUN A JOB:

**Where to go:** AWS Console → AWS Glue → ETL Jobs → click job name → Run

- You'll see the run appear in the **Runs** tab
- Status will show: `Running` → `Succeeded` or `Failed`
- Click on the run → scroll down → click **CloudWatch Logs** → click the log stream
- You will see all the `logger.info()` messages from the code

---

## HOW TO MONITOR WHILE RUNNING:

**Where to go:** AWS Console → AWS Glue → ETL Jobs → click the job → Runs tab

You'll see:
```
Run ID: jr_abc123def456
Status: Running ⏳
Duration: 4:32 and counting
DPU hours used: 0.15
```

Meanwhile check Snowflake for audit log updates:
```sql
SELECT * FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_AUDIT_LOG
ORDER BY CREATED_AT DESC
LIMIT 10;
```

---

---

# ═══════════════════════════════════════
# PHASE 6: AIRFLOW DAG — Wire it Up
# ═══════════════════════════════════════

> After extraction jobs work, create an Airflow DAG so it runs on schedule.
> This replaces the existing SnapLogic-based DAGs:
>   - prd-saps4-hourly-billing-documentdelta-load-pipeline (Priority 0 — Mission Critical!)

---

## FILE 4: `tcpl_glue_poc_billing_dag.py` — Airflow DAG

```python
# ============================================================
# tcpl_glue_poc_billing_dag.py
# Airflow DAG for POC: BillingDocument + BillingDocumentItem
#
# Schedule: Hourly (matching the existing SnapLogic DAG schedule)
# Priority: This is a Mission Critical pipeline (Priority 0)
#
# Deploy to: Your MWAA S3 bucket dags/ folder
# ============================================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ── Default arguments ─────────────────────────────────────────
default_args = {
    "owner": "tcpl-data-engineering",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ────────────────────────────────────────────
with DAG(
    dag_id="tcpl_glue_poc_sap_billing_pipeline",
    default_args=default_args,
    description="POC: SAP BillingDocument + BillingDocumentItem via Glue (replacing SnapLogic)",
    schedule_interval="0 * * * *",  # Hourly, at :00 — matching existing DAG
    catchup=False,
    max_active_runs=1,              # Never run 2 instances at the same time
    tags=["poc", "sap", "billing", "priority-0"],
) as dag:

    # ── Task 1: Extract BillingDocument (Parent) ───────────────
    extract_billing_document = GlueJobOperator(
        task_id="extract_billing_document",
        job_name="tcpl-glue-poc-sap-billing-document",
        script_args={
            "--entity_name": "BillingDocument"
        },
        aws_conn_id="aws_default",
        region_name="ap-south-1",
        wait_for_completion=True,     # Wait for Glue job to finish before moving on
        verbose=True,
    )
    
    # ── Task 2: Extract BillingDocumentItem (Child) ───────────
    # This task runs AFTER the parent completes successfully
    extract_billing_document_item = GlueJobOperator(
        task_id="extract_billing_document_item",
        job_name="tcpl-glue-poc-sap-billing-document-item",
        script_args={
            "--entity_name": "BillingDocumentItem"
        },
        aws_conn_id="aws_default",
        region_name="ap-south-1",
        wait_for_completion=True,
        verbose=True,
    )
    
    # ── Task 3: Log completion ─────────────────────────────────
    def log_pipeline_complete(**context):
        print("=" * 60)
        print("✅ TCPL Glue POC SAP Billing Pipeline Complete")
        print(f"   Run time: {context['execution_date']}")
        print(f"   BillingDocument: extracted")
        print(f"   BillingDocumentItem: extracted")
        print(f"   Check S3: s3://tcpl-datalake/glue_poc/sap/")
        print("=" * 60)
    
    pipeline_complete = PythonOperator(
        task_id="pipeline_complete",
        python_callable=log_pipeline_complete,
        provide_context=True,
    )
    
    # ── Task Dependencies (Order!) ────────────────────────────
    # Parent first → then Child → then log
    extract_billing_document >> extract_billing_document_item >> pipeline_complete
```

**How to deploy this DAG to MWAA:**
1. Go to AWS Console → MWAA → your environment → look for the S3 bucket name
2. Go to S3 → find that MWAA bucket → go to `dags/` folder
3. Upload `tcpl_glue_poc_billing_dag.py`
4. Wait ~2-3 minutes → go to MWAA Airflow UI → you'll see the new DAG

---

---

# ═══════════════════════════════════════
# PHASE 7: VALIDATION — Prove It Works
# ═══════════════════════════════════════

> This is how you show your POC is successful.
> You compare your Glue output against what SnapLogic currently has.

---

## STEP 10: Create Validation Tables in Snowflake

Run this SQL in Snowflake:

```sql
USE DATABASE PRD_ARCH_SUPPORT_DB;
USE SCHEMA GLUE_POC;

-- ──────────────────────────────────────────────────────────────
-- External stage pointing to YOUR Glue output in S3
-- This lets Snowflake read the Parquet files you wrote
-- ──────────────────────────────────────────────────────────────
CREATE OR REPLACE STAGE GLUE_POC_SAP_STAGE
    URL = 's3://tcpl-datalake/glue_poc/sap/'
    CREDENTIALS = (AWS_ROLE = 'arn:aws:iam::<YOUR_ACCOUNT_ID>:role/TCPLGluePOCRole')
    FILE_FORMAT = (TYPE = 'PARQUET');

-- ──────────────────────────────────────────────────────────────
-- Validation Table for BillingDocument
-- ──────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE GLUE_POC_BILLING_DOCUMENT AS
SELECT 
    $1:BillingDocument::VARCHAR           AS BILLING_DOCUMENT,
    $1:BillingDocumentType::VARCHAR        AS BILLING_DOCUMENT_TYPE,
    $1:SalesOrganization::VARCHAR          AS SALES_ORGANIZATION,
    $1:BillingDocumentDate::DATE           AS BILLING_DOCUMENT_DATE,
    $1:NetAmount::DECIMAL(18,2)            AS NET_AMOUNT,
    $1:TransactionCurrency::VARCHAR        AS TRANSACTION_CURRENCY,
    $1:SoldToParty::VARCHAR                AS SOLD_TO_PARTY,
    $1:LastChangeDateTime::TIMESTAMP_NTZ   AS LAST_CHANGE_DATE_TIME,
    METADATA$FILENAME                      AS SOURCE_FILE,
    CURRENT_TIMESTAMP()                    AS LOADED_AT
FROM @GLUE_POC_SAP_STAGE/BillingDocument/
    (FILE_FORMAT => 'PARQUET');

-- ──────────────────────────────────────────────────────────────
-- Validation Table for BillingDocumentItem
-- ──────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE GLUE_POC_BILLING_DOCUMENT_ITEM AS
SELECT 
    $1:BillingDocument::VARCHAR           AS BILLING_DOCUMENT,
    $1:BillingDocumentItem::VARCHAR        AS BILLING_DOCUMENT_ITEM,
    $1:Material::VARCHAR                   AS MATERIAL,
    $1:Plant::VARCHAR                      AS PLANT,
    $1:BillingQuantity::DECIMAL(18,3)      AS BILLING_QUANTITY,
    $1:NetAmount::DECIMAL(18,2)            AS NET_AMOUNT,
    $1:LastChangeDateTime::TIMESTAMP_NTZ   AS LAST_CHANGE_DATE_TIME,
    METADATA$FILENAME                      AS SOURCE_FILE,
    CURRENT_TIMESTAMP()                    AS LOADED_AT
FROM @GLUE_POC_SAP_STAGE/BillingDocumentItem/
    (FILE_FORMAT => 'PARQUET');
```

> ⚠️ **Note:** The field names above (`BillingDocument`, `NetAmount` etc.) must exactly match
> what SAP returns in the OData response. Run the connectivity test first and look at the
> actual field names in the test output. Adjust the Snowflake SQL if field names differ.

---

## STEP 11: Validation Queries — Run These to Prove POC Success

```sql
-- ══════════════════════════════════════════
-- VALIDATION A: Row Count Check
-- Compare your Glue output vs existing SnapLogic-loaded table
-- ══════════════════════════════════════════

-- Your Glue output row count:
SELECT 'GLUE_OUTPUT' AS SOURCE, COUNT(*) AS ROW_COUNT 
FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_POC_BILLING_DOCUMENT

UNION ALL

-- Existing SnapLogic-loaded table row count (adjust schema/table name):
SELECT 'SNAPLOGIC_OUTPUT' AS SOURCE, COUNT(*) AS ROW_COUNT 
FROM <EXISTING_SNOWFLAKE_SCHEMA>.BILLING_DOCUMENT;  -- Ask team for the actual table name

-- ══════════════════════════════════════════
-- VALIDATION B: MINUS Query (Data Match Check)
-- If this returns 0 rows → data is identical ✅
-- If rows returned → differences found ❌
-- ══════════════════════════════════════════

-- Records in Glue but NOT in SnapLogic:
SELECT BILLING_DOCUMENT, NET_AMOUNT, LAST_CHANGE_DATE_TIME
FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_POC_BILLING_DOCUMENT

MINUS

SELECT BILLING_DOCUMENT, NET_AMOUNT, LAST_CHANGE_DATE_TIME
FROM <EXISTING_SNOWFLAKE_SCHEMA>.BILLING_DOCUMENT
LIMIT 100;

-- Records in SnapLogic but NOT in Glue:
SELECT BILLING_DOCUMENT, NET_AMOUNT, LAST_CHANGE_DATE_TIME
FROM <EXISTING_SNOWFLAKE_SCHEMA>.BILLING_DOCUMENT

MINUS

SELECT BILLING_DOCUMENT, NET_AMOUNT, LAST_CHANGE_DATE_TIME
FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_POC_BILLING_DOCUMENT
LIMIT 100;

-- ══════════════════════════════════════════
-- VALIDATION C: Delta Load Verification
-- Run a second extraction with a narrow watermark
-- Then check only those records came through
-- ══════════════════════════════════════════

-- First: manually set a narrow watermark for testing:
UPDATE PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_ENTITY_CONFIG
SET LAST_WATERMARK = '2024-03-01 00:00:00'
WHERE SRC_OBJ_NM = 'BillingDocument';

-- Then re-run the Glue job.
-- Then check: did only records changed after 2024-03-01 come through?
SELECT 
    COUNT(*) AS RECORDS_IN_DELTA_WINDOW,
    MIN(LAST_CHANGE_DATE_TIME) AS EARLIEST,
    MAX(LAST_CHANGE_DATE_TIME) AS LATEST
FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_POC_BILLING_DOCUMENT
WHERE LAST_CHANGE_DATE_TIME > '2024-03-01 00:00:00';
-- EARLIEST should be just after 2024-03-01 00:00:00

-- ══════════════════════════════════════════
-- VALIDATION D: Audit Log Check
-- Verify job ran successfully and watermark updated
-- ══════════════════════════════════════════

SELECT 
    SRC_OBJ_NM,
    RUN_DATE,
    START_TIME,
    END_TIME,
    DURATION_SECS,
    STATUS,
    ROWS_EXTRACTED,
    FILES_WRITTEN,
    WATERMARK_START,
    WATERMARK_END
FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_AUDIT_LOG
ORDER BY CREATED_AT DESC
LIMIT 10;

-- ══════════════════════════════════════════
-- VALIDATION E: Parent-Child Join Check
-- Verify BillingDocumentItem rows link back to BillingDocument
-- ══════════════════════════════════════════

-- How many items per document (should be > 1 for most documents):
SELECT 
    BD.BILLING_DOCUMENT,
    BD.NET_AMOUNT AS HEADER_AMOUNT,
    COUNT(BDI.BILLING_DOCUMENT_ITEM) AS ITEM_COUNT,
    SUM(BDI.NET_AMOUNT) AS ITEMS_TOTAL_AMOUNT
FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_POC_BILLING_DOCUMENT BD
LEFT JOIN PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_POC_BILLING_DOCUMENT_ITEM BDI
    ON BD.BILLING_DOCUMENT = BDI.BILLING_DOCUMENT
GROUP BY 1, 2
ORDER BY ITEM_COUNT DESC
LIMIT 20;

-- Check for orphan items (items without a parent — should be 0):
SELECT COUNT(*) AS ORPHAN_ITEMS
FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_POC_BILLING_DOCUMENT_ITEM BDI
WHERE NOT EXISTS (
    SELECT 1 
    FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_POC_BILLING_DOCUMENT BD
    WHERE BD.BILLING_DOCUMENT = BDI.BILLING_DOCUMENT
);
-- Expected: 0
```

---

---

# ═══════════════════════════════════════
# TROUBLESHOOTING GUIDE
# ═══════════════════════════════════════

---

## Problem 1: Glue job can't reach SAP — Connection Refused or Timeout

**Symptom in logs:**
```
requests.exceptions.ConnectionError: HTTPSConnectionPool(host='your-sap-host.tcpl.com')
Max retries exceeded with url: /sap/opu/odata/...
```

**What this means:** The firewall between AWS Glue and your SAP server is not open.

**What to do:**
1. Raise a ticket with your infra/network team
2. Tell them exactly: *"Need firewall rule to allow outbound HTTPS (port 443) from AWS VPC subnet [subnet-XXXXX in ap-south-1] to SAP host [IP or hostname] on port 443 (or 8443)"*
3. While waiting: build and test everything else. The code is correct — it just can't reach SAP yet.

---

## Problem 2: 401 Unauthorized from SAP

**Symptom in logs:**
```
HTTP 401: Unauthorized
```

**Causes and fixes:**

| Cause | Fix |
|---|---|
| Wrong username or password in Secrets Manager | Go to Secrets Manager → find `tcpl/glue_poc/sap_odata` → Edit → correct the values |
| SAP user doesn't have OData authorization | Ask SAP Basis team: *"Please add S_SERVICE authorization for API_BILLING_DOCUMENT_SRV to user GLUE_TECHNICAL_USER"* |
| SAP user is locked | Ask SAP Basis to unlock the user |

---

## Problem 3: OData returns empty results but table has records

**Symptom:** Glue runs successfully, 0 rows extracted, but we know table has 5.39M records

**Most likely cause:** The `LAST_WATERMARK` in Snowflake is set too far in the future.

**Fix:**
```sql
-- Check current watermark:
SELECT SRC_OBJ_NM, LAST_WATERMARK 
FROM PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_ENTITY_CONFIG;

-- Reset to a date you know has data:
UPDATE PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_ENTITY_CONFIG
SET LAST_WATERMARK = '2020-01-01 00:00:00'
WHERE SRC_OBJ_NM = 'BillingDocument';
```

---

## Problem 4: /Date(1709596800000)/ in SAP response — what is this?

This is the OData v2 date format. It means milliseconds since epoch (Jan 1, 1970 UTC).

The code already handles this. But to manually verify:
```python
import datetime
ms = 1709596800000
dt = datetime.datetime.utcfromtimestamp(ms / 1000)
print(dt)  # 2024-03-05 00:00:00
```

---

## Problem 5: Glue job runs but GLUE_AUDIT_LOG has no entries

**Cause:** Glue job can't connect to Snowflake.

**Check:**
1. Is the Snowflake secret (`tcpl/glue_poc/snowflake`) correct?
2. Does `TCPLGluePOCRole` have permission to read from Secrets Manager?
3. Is Snowflake accessible from the AWS region (ap-south-1)?

**Quick fix — manually write a test audit row:**
```sql
INSERT INTO PRD_ARCH_SUPPORT_DB.GLUE_POC.GLUE_AUDIT_LOG
(CONFIG_ID, SRC_OBJ_NM, RUN_DATE, START_TIME, END_TIME, DURATION_SECS, STATUS)
VALUES (1, 'TEST', CURRENT_DATE(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 0, 'TEST');
```
If this works, Snowflake is fine — the issue is in the Glue→Snowflake connection.

---

## Problem 6: Job fails with "No module named 'snowflake'"

**Cause:** Snowflake connector not installed in Glue environment.

**Fix:** In Glue job parameters, make sure this is set:
```
Key:   --additional-python-modules
Value: snowflake-connector-python==3.5.0,pyarrow==12.0.0,pandas==2.0.0
```
Glue will install these automatically at job start.

---

---

# ═══════════════════════════════════════
# DAY-BY-DAY EXECUTION PLAN FOR TOMORROW
# ═══════════════════════════════════════

---

## Morning (9am – 12pm): Setup

| Time | Task | Where |
|---|---|---|
| 9:00 | Verify S3 bucket exists | AWS Console → S3 |
| 9:15 | Create/verify IAM Role | AWS Console → IAM → Roles |
| 9:30 | Create SAP secret in Secrets Manager | AWS Console → Secrets Manager |
| 9:45 | Create Snowflake secret in Secrets Manager | AWS Console → Secrets Manager |
| 10:00 | Run Snowflake SQL — create tables + seed 2 rows | Snowflake Worksheets |
| 10:30 | Write `glue_utils.py` | VS Code / any editor |
| 11:00 | Write `glue_poc_sap_connectivity_test.py` | VS Code |
| 11:30 | Upload both files to S3 | AWS Console → S3 |

## Mid-Morning (12pm – 1pm): Connectivity Test

| Time | Task | Where |
|---|---|---|
| 12:00 | Create connectivity test Glue job | AWS Console → Glue |
| 12:15 | Run connectivity test | Glue → Run |
| 12:15 | Watch logs in real time | Glue → Runs → CloudWatch Logs |
| 12:45 | Fix any issues found in test | Depends on error |

## Afternoon (1pm – 4pm): Build + Run Extraction

| Time | Task | Where |
|---|---|---|
| 1:00 | Write `glue_poc_sap_extract.py` | VS Code |
| 1:30 | Upload to S3 | AWS Console → S3 |
| 1:45 | Create BillingDocument Glue job | AWS Console → Glue |
| 2:00 | Run BillingDocument job | Glue → Run |
| 2:00 | Monitor run | Glue → Runs → CloudWatch Logs |
| 2:30 | Check S3 for Parquet files | AWS Console → S3 |
| 2:45 | Check Snowflake audit log | Snowflake → GLUE_AUDIT_LOG |
| 3:00 | Create BillingDocumentItem Glue job | AWS Console → Glue |
| 3:15 | Run BillingDocumentItem job | Glue → Run |
| 3:15 | Monitor run (bigger job, ~45-60 mins) | Glue → Runs → CloudWatch Logs |

## Late Afternoon (4pm – 6pm): Validate

| Time | Task | Where |
|---|---|---|
| 4:30 | Check S3 for BillingDocumentItem Parquet files | AWS Console → S3 |
| 4:45 | Run Snowflake validation queries | Snowflake Worksheets |
| 5:00 | Row count comparison (Glue vs SnapLogic) | Snowflake |
| 5:15 | MINUS queries for data match | Snowflake |
| 5:30 | Parent-Child join check | Snowflake |
| 5:45 | Document results | Any doc |

---

---

# ═══════════════════════════════════════
# CHECKLIST — End of POC Day
# ═══════════════════════════════════════

Check off each item before calling it done:

```
SETUP
  □ S3 bucket 'tcpl-datalake' confirmed to exist
  □ IAM Role 'TCPLGluePOCRole' created with correct permissions
  □ Secret 'tcpl/glue_poc/sap_odata' created in Secrets Manager
  □ Secret 'tcpl/glue_poc/snowflake' created in Secrets Manager
  □ GLUE_ENTITY_CONFIG table created in Snowflake
  □ GLUE_AUDIT_LOG table created in Snowflake
  □ BillingDocument config row inserted (REQUIRED_FLAG = 'Y')
  □ BillingDocumentItem config row inserted (REQUIRED_FLAG = 'Y')
  □ glue_utils.py uploaded to s3://tcpl-datalake/scripts/glue_poc/
  □ glue_poc_sap_connectivity_test.py uploaded to S3

CONNECTIVITY TEST
  □ Connectivity test Glue job created
  □ Connectivity test PASSED (status = Succeeded in Glue console)
  □ Logs show ✅ for all 4 tests
  □ SAP OData field names recorded (for Snowflake table creation)

EXTRACTION — BillingDocument (Parent)
  □ Glue job 'tcpl-glue-poc-sap-billing-document' created
  □ --entity_name = BillingDocument set in job parameters
  □ Job run SUCCEEDED
  □ GLUE_AUDIT_LOG has a SUCCESS row for BillingDocument
  □ LAST_WATERMARK updated in GLUE_ENTITY_CONFIG
  □ S3 has Parquet files at: s3://tcpl-datalake/glue_poc/sap/BillingDocument/

EXTRACTION — BillingDocumentItem (Child)
  □ Glue job 'tcpl-glue-poc-sap-billing-document-item' created
  □ --entity_name = BillingDocumentItem set in job parameters
  □ Job run SUCCEEDED (ran AFTER parent completed)
  □ GLUE_AUDIT_LOG has a SUCCESS row for BillingDocumentItem
  □ LAST_WATERMARK updated in GLUE_ENTITY_CONFIG
  □ S3 has Parquet files at: s3://tcpl-datalake/glue_poc/sap/BillingDocumentItem/

VALIDATION
  □ Validation A: Row counts match SnapLogic ✅
  □ Validation B: MINUS query returns 0 rows ✅
  □ Validation C: Delta load only fetches new records ✅
  □ Validation D: Audit log shows correct timings
  □ Validation E: Parent-Child join — 0 orphan items ✅
  □ Job duration recorded (for Glue vs SnapLogic comparison)
```

---

## 🎯 DEFINITION OF SUCCESS FOR YOUR POC

Your POC is a **GO** if:
1. ✅ BillingDocument: row count matches SnapLogic (within 0.1%)
2. ✅ BillingDocumentItem: row count matches SnapLogic (within 0.1%)
3. ✅ MINUS queries return 0 rows (data is identical)
4. ✅ Delta load correctly captures only changed records
5. ✅ Parent extracted before child (correct order preserved)
6. ✅ Both jobs completed without errors

---

*This is your bread and butter. Follow it step by step. Every step is here. You've got this. 💪*

*Remember: Your job ends at S3. Extract data from SAP → write Parquet to S3. That's it. The rest is already built.*
