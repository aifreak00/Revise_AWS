# 🚀 AWS Glue POC — Full End-to-End Simulation
## TCPL × Decision Point Analytics (LatentView)
### Replacing SnapLogic with AWS Glue | SAP OData + SFDC Bulk API 2.0

---

> 📋 **Based on:** `Glue_POC_Solution_Design_v2.docx` + `Glue_POC_March15_Plan_v2.xlsx`
> 🎯 **Your task:** Build this POC. This file = complete simulation of everything you need to build.
> 📅 **Timeline:** Feb 24 → Mar 15 (3 weeks, 15 activities)
> 📦 **Scope:** 12 tables (5 SFDC + 7 SAP), ~981M records, ~62 GB

---

## 🗺️ THE BIG PICTURE — What You Are Actually Building

```
CURRENT STATE (what exists today):
────────────────────────────────────
  Airflow → SnapLogic → S3 → Glue Transform → Snowflake
  (SnapLogic is the extractor — costs money, external tool)

TARGET STATE (what you're building — the POC):
────────────────────────────────────────────────
  Airflow → AWS Glue (extractor) → S3 → Glue Transform → Snowflake
  (Replace SnapLogic with Glue for extraction — cheaper, native AWS)

WHY THIS MATTERS:
──────────────────
  SnapLogic charges per pipeline/connector → expensive at TCPL scale
  AWS Glue = serverless PySpark → pay per DPU-hour only
  If POC succeeds → Go decision → migrate all 250+ SnapLogic pipelines

YOUR JOB IN THIS POC:
──────────────────────
  1. Connect Glue to SAP (via OData API)
  2. Connect Glue to Salesforce (via Bulk API 2.0)
  3. Extract 12 tables (full load + delta/incremental)
  4. Land data in S3 as Parquet
  5. Load to Snowflake POC schema
  6. Compare Glue data vs existing SnapLogic data
  7. Report: match 100%? → Go. Doesn't match? → No-Go + findings.

THE KEY DIFFERENCE FROM EXISTING GLUE JOBS:
─────────────────────────────────────────────
  Existing Glue jobs at TCPL = TRANSFORMATION (S3 → Snowflake)
  Your new Glue jobs         = EXTRACTION (SAP/SFDC → S3)
  This is NEW work. You're building the extraction layer from scratch.
```

---

## 📊 TABLES YOU WILL EXTRACT

### SFDC Tables (Bulk API 2.0)

```
┌────┬──────────────────────┬────────────┬─────────┬──────┬───────────────────────┐
│ #  │ TABLE                │ ROWS       │ SIZE    │ TYPE │ KEY FIELD             │
├────┼──────────────────────┼────────────┼─────────┼──────┼───────────────────────┤
│ 1  │ Secondary Sales      │ 122.6M     │ 13.1 GB │ Delta│ LastModifiedDate      │
│    │ Line Item            │            │         │      │ HIGHEST VOLUME        │
├────┼──────────────────────┼────────────┼─────────┼──────┼───────────────────────┤
│ 2  │ Visit Details        │ 63.4M      │ 6.6 GB  │ Delta│ LastModifiedDate      │
├────┼──────────────────────┼────────────┼─────────┼──────┼───────────────────────┤
│ 3  │ Inventory            │ 5.2M       │ 701 MB  │ Delta│ LastModifiedDate      │
├────┼──────────────────────┼────────────┼─────────┼──────┼───────────────────────┤
│ 4  │ Retailer             │ 2.4M       │ 355 MB  │ Delta│ LastModifiedDate      │
├────┼──────────────────────┼────────────┼─────────┼──────┼───────────────────────┤
│ 5  │ Account              │ 49K        │ 6.8 MB  │ Delta│ LastModifiedDate      │
└────┴──────────────────────┴────────────┴─────────┴──────┴───────────────────────┘
SFDC TOTAL: ~194 Million records, ~21 GB

### SAP S/4HANA Tables (OData API)

┌────┬─────────────────────────────────┬───────────┬─────────┬──────┬───────────────────────┐
│ #  │ TABLE                           │ ROWS      │ SIZE    │ TYPE │ KEY FIELD             │
├────┼─────────────────────────────────┼───────────┼─────────┼──────┼───────────────────────┤
│ 1  │ ACDOCA_FINANCE_DATA             │ 418.25M   │ 27.6 GB │ Delta│ LastChangeDateTime    │
│    │                                 │           │         │      │ LARGEST TABLE         │
├────┼─────────────────────────────────┼───────────┼─────────┼──────┼───────────────────────┤
│ 2  │ BillingDocumentItemPrcgElmnt    │ 311.17M   │ 7.7 GB  │ Delta│ LastChangeDateTime    │
├────┼─────────────────────────────────┼───────────┼─────────┼──────┼───────────────────────┤
│ 3  │ BillingDocumentItem             │ 24.94M    │ 2.1 GB  │ Delta│ LastChangeDateTime    │
├────┼─────────────────────────────────┼───────────┼─────────┼──────┼───────────────────────┤
│ 4  │ BillingDocument                 │ 5.39M     │ 347 MB  │ Delta│ LastChangeDateTime    │
├────┼─────────────────────────────────┼───────────┼─────────┼──────┼───────────────────────┤
│ 5  │ SalesOrderItem                  │ 23.54M    │ 2.2 GB  │ Delta│ LastChangeDateTime    │
├────┼─────────────────────────────────┼───────────┼─────────┼──────┼───────────────────────┤
│ 6  │ SalesOrder                      │ 3.79M     │ 236 MB  │ Delta│ LastChangeDateTime    │
├────┼─────────────────────────────────┼───────────┼─────────┼──────┼───────────────────────┤
│ 7  │ CustomerMaster                  │ 80K       │ 8.8 MB  │ FULL │ No delta — always full│
└────┴─────────────────────────────────┴───────────┴─────────┴──────┴───────────────────────┘
SAP TOTAL: ~787 Million records, ~41 GB

GRAND TOTAL: ~981 Million records, ~62 GB across 12 tables
```

---

## 📅 3-WEEK PLAN — MARCH 15 MILESTONE

```
WEEK 1: Feb 24 – Mar 1 — CONNECTIVITY
──────────────────────────────────────
  Task 1: SAP firewall whitelisting (IN PROGRESS — waiting on infra)
  Task 2: SAP OData technical user credentials (Pending)
  Task 3: Test SAP connectivity from Glue (Pending)
  Task 4: SFDC Connected App credentials (Pending)
  Task 5: Test SFDC connectivity from Glue (Pending)
  Task 6: Decide primary source (SAP or SFDC first) (Pending)

WEEK 2: Mar 3 – Mar 8 — EXTRACTION (2 TABLES)
──────────────────────────────────────────────
  Task 7:  Create control table in Snowflake (Pending)
  Task 8:  Build Glue extraction job — Table 1 (Pending)
  Task 9:  Full load — Table 1 to S3 (Pending)
  Task 10: Build Glue extraction job — Table 2 (Pending)
  Task 11: Full load — Table 2 to S3 (Pending)

WEEK 3: Mar 10 – Mar 15 — SNOWFLAKE LOAD & VALIDATION
──────────────────────────────────────────────────────
  Task 12: Load Table 1 to Snowflake (existing Glue template) (Pending)
  Task 13: Load Table 2 to Snowflake (existing Glue template) (Pending)
  Task 14: Data validation (row counts, MINUS queries) (Pending)
  Task 15: Document results + findings → Milestone review (Pending)
```

---

## 🏗️ STEP 0: AWS INFRASTRUCTURE SETUP

### S3 Paths for POC

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
    │   ├── full/2024-03-03/
    │   └── delta/2024-03-04/
    ├── BillingDocument/
    ├── BillingDocumentItem/
    ├── BillingDocumentItemPrcgElmnt/
    ├── SalesOrder/
    ├── SalesOrderItem/
    └── CustomerMaster/
        └── full/2024-03-03/    ← CustomerMaster always full load
```

### IAM Role for Glue POC Jobs

```json
{
  "RoleName": "TCPLGluePOCRole",
  "Policies": [
    "AWSGlueServiceRole",
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
            "arn:aws:secretsmanager:ap-south-1:*:secret:tcpl/glue_poc/sfdc*"
          ]
        },
        {
          "Effect": "Allow",
          "Action": ["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"],
          "Resource": "arn:aws:logs:ap-south-1:*:log-group:/aws-glue/*"
        }
      ]
    }
  ]
}
```

### Secrets Manager — Store Credentials

```python
# Run this ONCE to store credentials (not in any Glue job)
# tcpl/glue_poc/sap_odata
{
    "base_url":  "https://your-sap-host.tcpl.com/sap/opu/odata/sap/",
    "username":  "GLUE_TECHNICAL_USER",
    "password":  "your-sap-password"
}

# tcpl/glue_poc/sfdc_oauth
{
    "client_id":     "your-sfdc-connected-app-client-id",
    "client_secret": "your-sfdc-connected-app-secret",
    "username":      "glue-integration@tcpl.com",
    "password":      "your-sfdc-password",
    "security_token":"your-sfdc-security-token",
    "login_url":     "https://login.salesforce.com"
}

# tcpl/glue_poc/snowflake
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

## 🗄️ STEP 1: SNOWFLAKE CONTROL TABLES (Task 7)

```sql
-- ══════════════════════════════════════════════════════════════════════
-- RUN THIS IN SNOWFLAKE FIRST — Week 2, Task 7
-- Database: PRD_ARCH_SUPPORT_DB | Schema: GLUE_POC
-- ══════════════════════════════════════════════════════════════════════

USE DATABASE PRD_ARCH_SUPPORT_DB;
USE SCHEMA GLUE_POC;
USE WAREHOUSE TCPL_LOAD_WH;

-- ─────────────────────────────────────────────────────────────────────
-- TABLE 1: GLUE_ENTITY_CONFIG
-- Stores entity config + watermarks (replaces SnapLogic INPUT_ENTITY_LIST)
-- ─────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE GLUE_ENTITY_CONFIG (
    CONFIG_ID        NUMBER AUTOINCREMENT PRIMARY KEY,
    SRC_SYS_NM       VARCHAR(20)    NOT NULL,   -- SAPS4 or SFDC
    SRC_API_NM       VARCHAR(200)   NOT NULL,   -- API endpoint name
    SRC_OBJ_NM       VARCHAR(100)   NOT NULL,   -- Object/entity name
    RELATIVE_PATH    VARCHAR(100)   NOT NULL,   -- S3 sub-path
    LOAD_TYP         VARCHAR(1)     NOT NULL,   -- D=Delta, F=Full
    REQUIRED_FLAG    VARCHAR(1)     DEFAULT 'Y',-- Y=Active, N=Inactive
    CDC_WATERMARK_COL VARCHAR(100),             -- Column for delta tracking
    LAST_WATERMARK   TIMESTAMP_NTZ,             -- Last extracted timestamp
    BATCH_SIZE       NUMBER         DEFAULT 5000,
    STATIC_PARAMS    VARCHAR(500),              -- Extra OData filters
    S3_PATH          VARCHAR(200)   NOT NULL,   -- Target S3 path
    CREATED_AT       TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT       TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

-- ─────────────────────────────────────────────────────────────────────
-- TABLE 2: GLUE_AUDIT_LOG
-- Tracks every Glue job run — what ran, when, how many rows, status
-- ─────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE GLUE_AUDIT_LOG (
    AUDIT_ID         NUMBER AUTOINCREMENT PRIMARY KEY,
    GLUE_JOB_RUN_ID  VARCHAR(100),
    SRC_SYS_NM       VARCHAR(20),
    ENTITY_NAME      VARCHAR(100),
    LOAD_TYPE        VARCHAR(10),               -- FULL or DELTA
    WATERMARK_FROM   TIMESTAMP_NTZ,             -- Start of window
    WATERMARK_TO     TIMESTAMP_NTZ,             -- End of window
    START_TIME       TIMESTAMP_NTZ,
    END_TIME         TIMESTAMP_NTZ,
    DURATION_SECONDS NUMBER,
    RECORD_COUNT     NUMBER,
    S3_PATH          VARCHAR(500),
    STATUS           VARCHAR(20),               -- SUCCESS, FAILED, IN_PROGRESS
    ERROR_MESSAGE    VARCHAR(2000),
    CREATED_AT       TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

-- ─────────────────────────────────────────────────────────────────────
-- SEED DATA: Insert config for all 12 POC tables
-- ─────────────────────────────────────────────────────────────────────

-- === SFDC TABLES (5) ===
INSERT INTO GLUE_ENTITY_CONFIG
    (SRC_SYS_NM, SRC_API_NM, SRC_OBJ_NM, RELATIVE_PATH, LOAD_TYP,
     REQUIRED_FLAG, CDC_WATERMARK_COL, LAST_WATERMARK, BATCH_SIZE, S3_PATH)
VALUES
-- SecondarySalesLineItem — 122.6M rows, highest volume
('SFDC', 'bulk_api_v2', 'SecondarySalesLineItem__c',
 'sfdc/SecondarySalesLineItem', 'D', 'Y',
 'LastModifiedDate', '2020-01-01 00:00:00', 50000,
 's3://tcpl-datalake/glue_poc/sfdc/SecondarySalesLineItem/'),

-- VisitDetails — 63.4M rows
('SFDC', 'bulk_api_v2', 'VisitDetails__c',
 'sfdc/VisitDetails', 'D', 'Y',
 'LastModifiedDate', '2020-01-01 00:00:00', 50000,
 's3://tcpl-datalake/glue_poc/sfdc/VisitDetails/'),

-- Inventory — 5.2M rows
('SFDC', 'bulk_api_v2', 'Inventory__c',
 'sfdc/Inventory', 'D', 'Y',
 'LastModifiedDate', '2020-01-01 00:00:00', 20000,
 's3://tcpl-datalake/glue_poc/sfdc/Inventory/'),

-- Retailer — 2.4M rows
('SFDC', 'bulk_api_v2', 'Retailer__c',
 'sfdc/Retailer', 'D', 'Y',
 'LastModifiedDate', '2020-01-01 00:00:00', 10000,
 's3://tcpl-datalake/glue_poc/sfdc/Retailer/'),

-- Account — 49K rows
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
-- ACDOCA Finance — 418.25M rows, largest
('SAPS4', 'ZGLUE_ACDOCA_SRV', 'ACDOCA_FINANCE_DATA',
 'sap/ACDOCA_FINANCE_DATA', 'D', 'Y',
 'LastChangeDateTime', '2020-01-01T00:00:00', 5000,
 NULL,
 's3://tcpl-datalake/glue_poc/sap/ACDOCA_FINANCE_DATA/'),

-- BillingDocumentItemPrcgElmnt — 311.17M rows
('SAPS4', 'API_BILLING_DOCUMENT_SRV', 'BillingDocumentItemPrcgElmnt',
 'sap/BillingDocumentItemPrcgElmnt', 'D', 'Y',
 'LastChangeDateTime', '2020-01-01T00:00:00', 5000,
 NULL,
 's3://tcpl-datalake/glue_poc/sap/BillingDocumentItemPrcgElmnt/'),

-- BillingDocumentItem — 24.94M rows
('SAPS4', 'API_BILLING_DOCUMENT_SRV', 'BillingDocumentItem',
 'sap/BillingDocumentItem', 'D', 'Y',
 'LastChangeDateTime', '2020-01-01T00:00:00', 5000,
 NULL,
 's3://tcpl-datalake/glue_poc/sap/BillingDocumentItem/'),

-- BillingDocument — 5.39M rows
('SAPS4', 'API_BILLING_DOCUMENT_SRV', 'BillingDocument',
 'sap/BillingDocument', 'D', 'Y',
 'LastChangeDateTime', '2020-01-01T00:00:00', 5000,
 NULL,
 's3://tcpl-datalake/glue_poc/sap/BillingDocument/'),

-- SalesOrderItem — 23.54M rows
('SAPS4', 'API_SALES_ORDER_SRV', 'SalesOrderItem',
 'sap/SalesOrderItem', 'D', 'Y',
 'LastChangeDateTime', '2020-01-01T00:00:00', 5000,
 NULL,
 's3://tcpl-datalake/glue_poc/sap/SalesOrderItem/'),

-- SalesOrder — 3.79M rows (has static filter)
('SAPS4', 'API_SALES_ORDER_SRV', 'SalesOrder',
 'sap/SalesOrder', 'D', 'Y',
 'LastChangeDateTime', '2020-01-01T00:00:00', 5000,
 'SalesOrderType eq ''OR'' or SalesOrderType eq ''ZOR''',
 's3://tcpl-datalake/glue_poc/sap/SalesOrder/'),

-- CustomerMaster — 80K rows, FULL LOAD always
('SAPS4', 'API_BUSINESS_PARTNER_SRV', 'CustomerMaster',
 'sap/CustomerMaster', 'F', 'Y',
 NULL, NULL, 5000,
 NULL,
 's3://tcpl-datalake/glue_poc/sap/CustomerMaster/');

-- Verify seeded data
SELECT SRC_SYS_NM, SRC_OBJ_NM, LOAD_TYP, BATCH_SIZE, LAST_WATERMARK
FROM GLUE_ENTITY_CONFIG
ORDER BY SRC_SYS_NM, CONFIG_ID;
```

---

## 🐍 STEP 2: SHARED UTILITY MODULE (`glue_utils.py`)

```python
"""
glue_utils.py
─────────────
Shared utilities used by both SAP and SFDC Glue extraction jobs.
Stored in S3: s3://tcpl-datalake/scripts/glue_poc/glue_utils.py
Import using: --extra-py-files s3://tcpl-datalake/scripts/glue_poc/glue_utils.py
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
              load_type: str, wm_from: Optional[datetime],
              wm_to: Optional[datetime], start_time: datetime,
              end_time: datetime, record_count: int,
              s3_path: str, status: str,
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

## 🐍 STEP 3: SFDC EXTRACTION JOB (`glue_poc_sfdc_extract.py`)

```python
"""
AWS Glue Job: tcpl-glue-poc-sfdc-extract
──────────────────────────────────────────
Extracts data from Salesforce using Bulk API 2.0.
Supports: Full Load (F) and Delta/Incremental Load (D)

Job Parameters:
  --ENTITY_NAME   : SFDC object name (e.g. SecondarySalesLineItem__c)
  --LOAD_TYPE     : F=Full, D=Delta (overrides control table if provided)
  --RUN_DATE      : YYYY-MM-DD (defaults to today)

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
from pyspark.sql.types import StructType

# Import shared utils
from glue_utils import (
    get_secret, get_snowflake_conn, get_entity_config,
    update_watermark, log_audit, get_s3_output_path, write_parquet_to_s3
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ── Init Glue ────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "ENTITY_NAME",
    "--LOAD_TYPE",   # Optional override
    "--RUN_DATE",    # Optional override
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

JOB_RUN_ID  = args.get("JOB_RUN_ID", "local-test")
ENTITY_NAME = args["ENTITY_NAME"]
RUN_DATE    = args.get("RUN_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

# ── Get Secrets & Config ─────────────────────────────────────────────
sfdc_creds = get_secret("tcpl/glue_poc/sfdc_oauth")
sf_creds   = get_secret("tcpl/glue_poc/snowflake")
sf_conn    = get_snowflake_conn(sf_creds)
config     = get_entity_config(sf_conn, "SFDC", ENTITY_NAME)

LOAD_TYPE  = args.get("LOAD_TYPE", config["LOAD_TYP"])  # F or D
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
# SALESFORCE BULK API 2.0 FUNCTIONS
# ══════════════════════════════════════════════════════════════════════

class SFDCBulkAPI:
    """Salesforce Bulk API 2.0 client."""

    def __init__(self, creds: Dict):
        self.login_url    = creds.get("login_url", "https://login.salesforce.com")
        self.client_id    = creds["client_id"]
        self.client_secret= creds["client_secret"]
        self.username     = creds["username"]
        self.password     = creds["password"]
        self.security_token = creds.get("security_token", "")
        self.access_token = None
        self.instance_url = None
        self.api_version  = "v57.0"

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
            "operation":   "queryAll",   # queryAll includes soft-deleted records
            "query":       soql,
            "contentType": "CSV",
            "columnDelimiter": "COMMA",
            "lineEnding":  "LF",
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
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/query/{job_id}"
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

            # Parse CSV response
            reader = csv.DictReader(io.StringIO(response.text))
            page_records = list(reader)
            all_records.extend(page_records)
            logger.info(f"Page {page}: {len(page_records):,} records (total: {len(all_records):,})")

            # Check for next page
            locator = response.headers.get("Sforce-Locator")
            if not locator or locator == "null":
                break

        return all_records

    def build_soql(self, obj_name: str, watermark_col: str,
                   last_watermark: Optional[str],
                   load_type: str) -> str:
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
        obj_name        = ENTITY_NAME,
        watermark_col   = config.get("CDC_WATERMARK_COL", "LastModifiedDate"),
        last_watermark  = config.get("LAST_WATERMARK"),
        load_type       = LOAD_TYPE,
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
        # Step 6: Convert to Spark DataFrame and write Parquet
        if records:
            # Use Spark to handle large datasets efficiently
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

            logger.info(f"DataFrame schema:\n{df.printSchema()}")
            logger.info(f"Sample record:\n{df.show(3, truncate=False)}")

            # Write to S3
            total_records = write_parquet_to_s3(spark, df, S3_PATH)

        # Step 7: Update watermark in control table
        if LOAD_TYPE == "D":
            update_watermark(sf_conn, config["CONFIG_ID"], wm_to)

        status = "SUCCESS"
        logger.info(f"✅ SFDC extraction complete: {total_records:,} records → {S3_PATH}")

except Exception as e:
    error_msg = str(e)
    logger.error(f"❌ SFDC extraction FAILED: {error_msg}")
    raise

finally:
    # Always log to audit table
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

## 🐍 STEP 4: SAP EXTRACTION JOB (`glue_poc_sap_extract.py`)

```python
"""
AWS Glue Job: tcpl-glue-poc-sap-extract
─────────────────────────────────────────
Extracts data from SAP S/4HANA using OData APIs.
Handles: Full Load (F), Delta Load (D), pagination ($skip/$top).
Supports: Parent-child relationships, static filters.

Job Parameters:
  --ENTITY_NAME   : SAP entity name (e.g. BillingDocument)
  --LOAD_TYPE     : F=Full, D=Delta (overrides control table if provided)
  --RUN_DATE      : YYYY-MM-DD
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

# ── Init Glue ────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "ENTITY_NAME",
    "--LOAD_TYPE",
    "--RUN_DATE",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

JOB_RUN_ID  = args.get("JOB_RUN_ID", "local-test")
ENTITY_NAME = args["ENTITY_NAME"]
RUN_DATE    = args.get("RUN_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

# ── Get Secrets & Config ─────────────────────────────────────────────
sap_creds = get_secret("tcpl/glue_poc/sap_odata")
sf_creds  = get_secret("tcpl/glue_poc/snowflake")
sf_conn   = get_snowflake_conn(sf_creds)
config    = get_entity_config(sf_conn, "SAPS4", ENTITY_NAME)

LOAD_TYPE  = args.get("LOAD_TYPE", config["LOAD_TYP"])
BATCH_SIZE = config.get("BATCH_SIZE", 5000)     # $top value per page
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
# SAP ODATA CLIENT
# ══════════════════════════════════════════════════════════════════════

class SAPODataClient:
    """SAP OData API client with pagination and CSRF token handling."""

    def __init__(self, creds: Dict, api_name: str):
        self.base_url    = creds["base_url"].rstrip("/")
        self.username    = creds["username"]
        self.password    = creds["password"]
        self.api_name    = api_name
        self.session     = requests.Session()
        self.session.auth = (self.username, self.password)
        self.session.headers.update({
            "Accept":       "application/json",
            "Content-Type": "application/json",
            "sap-client":   "100",
        })
        self.csrf_token  = None

    def fetch_csrf_token(self) -> None:
        """SAP OData requires CSRF token for modifying operations. Fetch it."""
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
        """Test connectivity by fetching 1 record."""
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
                        last_watermark: Optional[str],
                        load_type: str, static_params: Optional[str],
                        skip: int = 0) -> str:
        """Build OData URL with filters, pagination, and format."""
        base = f"{self.base_url}/{self.api_name}/{entity}"
        params = [f"$format=json", f"$top={BATCH_SIZE}", f"$skip={skip}"]

        filters = []

        # Delta filter
        if load_type == "D" and watermark_col and last_watermark:
            wm_dt = datetime.fromisoformat(str(last_watermark).replace(" ", "T"))
            # Add 2-hour overlap to catch late-arriving records
            wm_with_overlap = wm_dt - timedelta(hours=2)
            wm_str = wm_with_overlap.strftime("%Y-%m-%dT%H:%M:%SZ")
            filters.append(f"{watermark_col} gt datetime'{wm_str}'")

        # Static params (e.g. SalesOrderType filters)
        if static_params:
            filters.append(f"({static_params})")

        if filters:
            params.append(f"$filter={quote(' and '.join(filters))}")

        # Always order by watermark for consistent pagination
        if watermark_col:
            params.append(f"$orderby={watermark_col} asc")

        return f"{base}?{'&'.join(params)}"

    def extract_all_pages(self, entity: str, watermark_col: Optional[str],
                          last_watermark: Optional[str],
                          load_type: str, static_params: Optional[str]
                          ) -> Generator[List[Dict], None, None]:
        """
        Generator that yields pages of records using $skip/$top pagination.
        Yields a list of dicts per page.
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

            if response.status_code == 429:  # Rate limit
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Rate limited. Waiting {retry_after}s...")
                import time; time.sleep(retry_after)
                continue

            response.raise_for_status()
            data = response.json()

            # Handle OData v2 and v4 response structures
            records = (data.get("d", {}).get("results", []) or    # OData v2
                       data.get("value", []))                       # OData v4

            if not records:
                logger.info(f"No more records at page {page}. Total: {total_fetched:,}")
                break

            total_fetched += len(records)
            logger.info(f"Page {page}: {len(records):,} records (total: {total_fetched:,})")
            yield records

            if len(records) < BATCH_SIZE:
                logger.info(f"Last page reached (got {len(records)} < {BATCH_SIZE}). Done.")
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

    # Test connectivity first (fail fast)
    if not client.test_connectivity(ENTITY_NAME):
        raise ConnectionError(f"Cannot connect to SAP OData: {API_NAME}/{ENTITY_NAME}")

    wm_from = config.get("LAST_WATERMARK")

    logger.info(f"Starting extraction: entity={ENTITY_NAME}, "
                f"load={'FULL' if LOAD_TYPE=='F' else 'DELTA'}, "
                f"from={wm_from}")

    # Stream pages → convert to Spark DataFrames → write in chunks
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
        # Convert page to Spark DF
        rdd = spark.sparkContext.parallelize(page_records, numSlices=4)
        page_df = spark.read.json(rdd.map(lambda r: json.dumps(r)))
        all_spark_dfs.append(page_df)

        # Write in chunks of 50 pages to avoid OOM on very large tables
        if batch_num % 50 == 0:
            combined = all_spark_dfs[0]
            for df in all_spark_dfs[1:]:
                combined = combined.unionByName(df, allowMissingColumns=True)
            chunk_path = f"{S3_PATH}chunk_{batch_num // 50}/"
            count = write_parquet_to_s3(spark, combined, chunk_path)
            total_records += count
            all_spark_dfs = []  # Free memory
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

    # Update watermark only for Delta loads
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

## 🐍 STEP 5: CONNECTIVITY TEST JOBS

### SAP Connectivity Test (`glue_poc_sap_connectivity_test.py`)

```python
"""
Lightweight connectivity test job — run this FIRST in Week 1.
Just tests if Glue can reach SAP OData and fetch 5 records.
Run before building full extraction jobs.
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

# Test 1: Basic network reach
test_url = f"{sap_creds['base_url']}/API_BILLING_DOCUMENT_SRV/BillingDocument?$top=5&$format=json"
logger.info(f"Testing SAP connectivity: {test_url}")

response = requests.get(
    test_url,
    auth=(sap_creds["username"], sap_creds["password"]),
    headers={"Accept": "application/json"},
    timeout=30
)

logger.info(f"HTTP Status: {response.status_code}")
logger.info(f"Response Headers: {dict(response.headers)}")

if response.status_code == 200:
    data = response.json()
    records = data.get("d", {}).get("results", data.get("value", []))
    logger.info(f"✅ SAP CONNECTIVITY TEST PASSED")
    logger.info(f"Records returned: {len(records)}")
    logger.info(f"Sample record keys: {list(records[0].keys()) if records else 'empty'}")
elif response.status_code == 401:
    logger.error("❌ AUTHENTICATION FAILED — check username/password in Secrets Manager")
elif response.status_code == 403:
    logger.error("❌ AUTHORIZATION FAILED — technical user lacks permission to this OData service")
elif response.status_code == 404:
    logger.error("❌ OData endpoint not found — check API service name")
elif response.status_code in [502, 503, 504]:
    logger.error("❌ NETWORK ERROR — SAP endpoint unreachable. Firewall whitelisting may be pending.")
else:
    logger.error(f"❌ Unexpected status: {response.status_code} → {response.text[:500]}")
    response.raise_for_status()

job.commit()
```

### SFDC Connectivity Test (`glue_poc_sfdc_connectivity_test.py`)

```python
"""
SFDC connectivity test — tests OAuth + Bulk API in isolation.
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

# Test 1: OAuth Token
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
    logger.info(f"✅ OAuth PASSED — Instance: {instance_url}")
else:
    logger.error(f"❌ OAuth FAILED: {token_response.status_code} → {token_response.text}")
    raise RuntimeError("SFDC OAuth failed")

# Test 2: REST API Query (small)
logger.info("Test 2: Testing REST API with SOQL query...")
headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
query_url = f"{instance_url}/services/data/v57.0/query?q=SELECT+Id,Name+FROM+Account+LIMIT+5"
query_resp = requests.get(query_url, headers=headers, timeout=30)

if query_resp.status_code == 200:
    records = query_resp.json().get("records", [])
    logger.info(f"✅ REST API PASSED — {len(records)} Account records returned")
else:
    logger.error(f"❌ REST API FAILED: {query_resp.text[:500]}")

# Test 3: Bulk API 2.0 job submission
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
    logger.info(f"✅ BULK API PASSED — Job ID: {job_id}")
    # Cleanup: abort the test job
    requests.patch(
        f"{bulk_url}/{job_id}",
        headers=headers,
        json={"state": "Aborted"},
        timeout=30
    )
    logger.info("Test job aborted (cleanup)")
else:
    logger.error(f"❌ BULK API FAILED: {bulk_resp.text[:500]}")

logger.info("═══ SFDC CONNECTIVITY TEST COMPLETE ═══")
job.commit()
```

---

## 🐍 STEP 6: AIRFLOW DAGs

### Main POC DAG (`tcpl_glue_poc_dag.py`)

```python
"""
Airflow DAG: tcpl_glue_poc_pipeline
─────────────────────────────────────
Orchestrates the full Glue POC pipeline:
  1. SFDC extraction for all 5 SFDC tables
  2. SAP extraction for all 7 SAP tables
  3. Load to Snowflake POC schema
  4. Run validation queries

Schedule: Daily (mirrors production pattern)
"""

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import json

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

    # ── SFDC EXTRACTION TASKS ────────────────────────────────────────

    sfdc_secondary_sales = GlueJobOperator(
        task_id="sfdc_extract_secondary_sales_line_item",
        job_name="tcpl-glue-poc-sfdc-extract",
        script_args={
            "--ENTITY_NAME": "SecondarySalesLineItem__c",
            "--LOAD_TYPE":   "D",
            "--RUN_DATE":    run_date,
        },
        aws_conn_id="aws_tcpl",
        region_name="ap-south-1",
        wait_for_completion=True,
        num_of_dpus=10,   # High DPU for 122.6M row table
    )

    sfdc_visit_details = GlueJobOperator(
        task_id="sfdc_extract_visit_details",
        job_name="tcpl-glue-poc-sfdc-extract",
        script_args={
            "--ENTITY_NAME": "VisitDetails__c",
            "--LOAD_TYPE":   "D",
            "--RUN_DATE":    run_date,
        },
        aws_conn_id="aws_tcpl",
        num_of_dpus=8,
    )

    sfdc_inventory = GlueJobOperator(
        task_id="sfdc_extract_inventory",
        job_name="tcpl-glue-poc-sfdc-extract",
        script_args={
            "--ENTITY_NAME": "Inventory__c",
            "--LOAD_TYPE":   "D",
            "--RUN_DATE":    run_date,
        },
        aws_conn_id="aws_tcpl",
        num_of_dpus=4,
    )

    sfdc_retailer = GlueJobOperator(
        task_id="sfdc_extract_retailer",
        job_name="tcpl-glue-poc-sfdc-extract",
        script_args={
            "--ENTITY_NAME": "Retailer__c",
            "--LOAD_TYPE":   "D",
            "--RUN_DATE":    run_date,
        },
        aws_conn_id="aws_tcpl",
        num_of_dpus=4,
    )

    sfdc_account = GlueJobOperator(
        task_id="sfdc_extract_account",
        job_name="tcpl-glue-poc-sfdc-extract",
        script_args={
            "--ENTITY_NAME": "Account",
            "--LOAD_TYPE":   "D",
            "--RUN_DATE":    run_date,
        },
        aws_conn_id="aws_tcpl",
        num_of_dpus=2,
    )

    # ── SAP EXTRACTION TASKS ─────────────────────────────────────────

    sap_acdoca = GlueJobOperator(
        task_id="sap_extract_acdoca_finance_data",
        job_name="tcpl-glue-poc-sap-extract",
        script_args={
            "--ENTITY_NAME": "ACDOCA_FINANCE_DATA",
            "--LOAD_TYPE":   "D",
            "--RUN_DATE":    run_date,
        },
        aws_conn_id="aws_tcpl",
        num_of_dpus=20,   # Largest table — 418M rows
    )

    sap_billing_prcg = GlueJobOperator(
        task_id="sap_extract_billing_doc_item_prcg_elmnt",
        job_name="tcpl-glue-poc-sap-extract",
        script_args={
            "--ENTITY_NAME": "BillingDocumentItemPrcgElmnt",
            "--LOAD_TYPE":   "D",
            "--RUN_DATE":    run_date,
        },
        aws_conn_id="aws_tcpl",
        num_of_dpus=16,
    )

    sap_billing_doc_item = GlueJobOperator(
        task_id="sap_extract_billing_doc_item",
        job_name="tcpl-glue-poc-sap-extract",
        script_args={
            "--ENTITY_NAME": "BillingDocumentItem",
            "--LOAD_TYPE":   "D",
            "--RUN_DATE":    run_date,
        },
        aws_conn_id="aws_tcpl",
        num_of_dpus=8,
    )

    sap_billing_doc = GlueJobOperator(
        task_id="sap_extract_billing_document",
        job_name="tcpl-glue-poc-sap-extract",
        script_args={
            "--ENTITY_NAME": "BillingDocument",
            "--LOAD_TYPE":   "D",
            "--RUN_DATE":    run_date,
        },
        aws_conn_id="aws_tcpl",
        num_of_dpus=4,
    )

    sap_so_item = GlueJobOperator(
        task_id="sap_extract_sales_order_item",
        job_name="tcpl-glue-poc-sap-extract",
        script_args={
            "--ENTITY_NAME": "SalesOrderItem",
            "--LOAD_TYPE":   "D",
            "--RUN_DATE":    run_date,
        },
        aws_conn_id="aws_tcpl",
        num_of_dpus=8,
    )

    sap_so = GlueJobOperator(
        task_id="sap_extract_sales_order",
        job_name="tcpl-glue-poc-sap-extract",
        script_args={
            "--ENTITY_NAME": "SalesOrder",
            "--LOAD_TYPE":   "D",
            "--RUN_DATE":    run_date,
        },
        aws_conn_id="aws_tcpl",
        num_of_dpus=4,
    )

    sap_customer_master = GlueJobOperator(
        task_id="sap_extract_customer_master",
        job_name="tcpl-glue-poc-sap-extract",
        script_args={
            "--ENTITY_NAME": "CustomerMaster",
            "--LOAD_TYPE":   "F",    # Always full load
            "--RUN_DATE":    run_date,
        },
        aws_conn_id="aws_tcpl",
        num_of_dpus=2,
    )

    # ── SNOWFLAKE LOAD TASKS (using existing transform templates) ────

    load_sfdc_to_snowflake = GlueJobOperator(
        task_id="load_sfdc_to_snowflake_poc",
        job_name="tcpl-glue-sfdc-to-snowflake-core",   # EXISTING template
        script_args={
            "--SOURCE_PATH":     f"s3://tcpl-datalake/glue_poc/sfdc/",
            "--TARGET_SCHEMA":   "GLUE_POC_DB.SFDC_CORE",
            "--RUN_DATE":        run_date,
        },
        aws_conn_id="aws_tcpl",
        num_of_dpus=8,
    )

    load_sap_to_snowflake = GlueJobOperator(
        task_id="load_sap_to_snowflake_poc",
        job_name="tcpl-glue-sap-to-snowflake-core",    # EXISTING template
        script_args={
            "--SOURCE_PATH":     f"s3://tcpl-datalake/glue_poc/sap/",
            "--TARGET_SCHEMA":   "GLUE_POC_DB.SAPS4_CORE",
            "--RUN_DATE":        run_date,
        },
        aws_conn_id="aws_tcpl",
        num_of_dpus=10,
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
    # SFDC and SAP run in parallel
    sfdc_tasks = [sfdc_secondary_sales, sfdc_visit_details,
                  sfdc_inventory, sfdc_retailer, sfdc_account]
    sap_tasks  = [sap_acdoca, sap_billing_prcg, sap_billing_doc_item,
                  sap_billing_doc, sap_so_item, sap_so, sap_customer_master]

    # All extractions → load to Snowflake
    sfdc_tasks >> load_sfdc_to_snowflake
    sap_tasks  >> load_sap_to_snowflake

    # Both loads done → validation
    [load_sfdc_to_snowflake, load_sap_to_snowflake] >> run_validation
```

---

## 🔍 STEP 7: DATA VALIDATION QUERIES (Task 14)

```sql
-- ══════════════════════════════════════════════════════════════════════
-- VALIDATION STORED PROCEDURE
-- Run this after Snowflake load to compare Glue vs SnapLogic data
-- ══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE GLUE_POC_DB.PROCEDURES.RUN_FULL_VALIDATION(RUN_DATE DATE)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- ── Create validation results table ─────────────────────────────
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

    -- ── VALIDATION 1: BillingDocument row count ─────────────────────
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
        CASE WHEN ABS(
            (SELECT COUNT(*) FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT) -
            (SELECT COUNT(*) FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT)
        ) = 0 THEN 'PASS' ELSE 'FAIL' END,
        NULL;

    -- ── VALIDATION 2: SecondarySalesLineItem row count ───────────────
    INSERT INTO GLUE_POC_DB.VALIDATION.RESULTS
    SELECT
        :RUN_DATE,
        'SecondarySalesLineItem',
        'ROW_COUNT',
        (SELECT COUNT(*) FROM SFDC_DB.SFDC_CORE.SECONDARYSALESLINEITEM),
        (SELECT COUNT(*) FROM GLUE_POC_DB.SFDC_CORE.SECONDARYSALESLINEITEM),
        ABS(
            (SELECT COUNT(*) FROM GLUE_POC_DB.SFDC_CORE.SECONDARYSALESLINEITEM) -
            (SELECT COUNT(*) FROM SFDC_DB.SFDC_CORE.SECONDARYSALESLINEITEM)
        ),
        ROUND(
            (SELECT COUNT(*) FROM GLUE_POC_DB.SFDC_CORE.SECONDARYSALESLINEITEM) /
            NULLIF((SELECT COUNT(*) FROM SFDC_DB.SFDC_CORE.SECONDARYSALESLINEITEM),0) * 100,
        4),
        CASE WHEN ABS(
            (SELECT COUNT(*) FROM GLUE_POC_DB.SFDC_CORE.SECONDARYSALESLINEITEM) -
            (SELECT COUNT(*) FROM SFDC_DB.SFDC_CORE.SECONDARYSALESLINEITEM)
        ) = 0 THEN 'PASS' ELSE 'FAIL' END,
        NULL;

    RETURN 'Validation complete for ' || :RUN_DATE;
END;
$$;


-- ══════════════════════════════════════════════════════════════════════
-- MANUAL VALIDATION QUERIES — Run these yourself during Task 14
-- ══════════════════════════════════════════════════════════════════════

-- VALIDATION A: Row Count Comparison for ALL tables
SELECT
    'BillingDocument'        AS table_name,
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


-- VALIDATION B: MINUS Query — Records in SnapLogic MISSING from Glue
SELECT 'Missing in Glue'           AS issue,
       BILLINGDOCUMENT, BILLINGDOCUMENTITEM
FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT
MINUS
SELECT 'Missing in Glue',
       BILLINGDOCUMENT, BILLINGDOCUMENTITEM
FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT;
-- Expected result: 0 rows ✅


-- VALIDATION C: MINUS Query — Extra records in Glue NOT in SnapLogic
SELECT 'Extra in Glue'             AS issue,
       BILLINGDOCUMENT, BILLINGDOCUMENTITEM
FROM GLUE_POC_DB.SAPS4_CORE.BILLINGDOCUMENT
MINUS
SELECT 'Extra in Glue',
       BILLINGDOCUMENT, BILLINGDOCUMENTITEM
FROM SAPS4_DB.SAPS4_CORE.BILLINGDOCUMENT;
-- Expected result: 0 rows ✅


-- VALIDATION D: Checksum — Compare key column hash
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
-- Expected: Both checksums identical ✅


-- VALIDATION E: Delta progression check
-- Verify delta load captured all records in the window
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


-- VALIDATION F: Audit log summary — POC health check
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

## 📋 STEP 8: VALIDATION REPORT TEMPLATE (Task 15)

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
│ BillingDocument                 │ 5,390,000  │ 5,390,000  │ ✅ YES │
│ BillingDocumentItem             │ 24,940,000 │ 24,940,000 │ ✅ YES │
│ BillingDocumentItemPrcgElmnt    │ 311,170,000│ 311,170,000│ ✅ YES │
│ SalesOrder                      │ 3,790,000  │ 3,790,000  │ ✅ YES │
│ SalesOrderItem                  │ 23,540,000 │ 23,540,000 │ ✅ YES │
│ ACDOCA_FINANCE_DATA             │ 418,250,000│ 418,250,000│ ✅ YES │
│ CustomerMaster                  │ 80,000     │ 80,000     │ ✅ YES │
│ SecondarySalesLineItem          │ 122,600,000│ 122,600,000│ ✅ YES │
│ VisitDetails                    │ 63,400,000 │ 63,400,000 │ ✅ YES │
│ Inventory                       │ 5,200,000  │ 5,200,000  │ ✅ YES │
│ Retailer                        │ 2,400,000  │ 2,400,000  │ ✅ YES │
│ Account                         │ 49,000     │ 49,000     │ ✅ YES │
└─────────────────────────────────┴────────────┴────────────┴────────┘
RESULT: 12/12 tables match 100% ✅

2. MINUS QUERY RESULTS
───────────────────────
All MINUS queries returned 0 rows.
No records missing between SnapLogic and Glue extracts. ✅

3. DELTA LOAD VALIDATION
──────────────────────────
BillingDocument delta window (2024-03-03 → 2024-03-04):
  SnapLogic delta: 12,450 records
  Glue delta:      12,450 records ✅
  
SecondarySalesLineItem delta:
  SnapLogic delta: 345,200 records
  Glue delta:      345,200 records ✅

4. PERFORMANCE COMPARISON
──────────────────────────
┌─────────────────────────────────┬────────────┬────────────┬────────┐
│ TABLE                           │ SNAPLOGIC  │ GLUE       │ FASTER?│
├─────────────────────────────────┼────────────┼────────────┼────────┤
│ BillingDocument (5.4M)          │ 8 min      │ 6 min      │ ✅ +25%│
│ SecondarySalesLineItem (122.6M) │ 45 min     │ 38 min     │ ✅ +16%│
│ ACDOCA_FINANCE_DATA (418.25M)   │ 2h 10m     │ 1h 55m     │ ✅ +12%│
└─────────────────────────────────┴────────────┴────────────┴────────┘

5. RECOMMENDATION
──────────────────
DECISION: ✅ GO — Proceed with full migration to AWS Glue

RATIONALE:
  ✅ 100% data accuracy across all 12 tables
  ✅ MINUS queries return 0 records
  ✅ Delta loads capture correct records with watermark approach
  ✅ Performance comparable (10-25% faster due to native AWS)
  ✅ Cost reduction: ~40% lower than SnapLogic licensing cost
  ✅ Operational simplification: single platform (no external tool)

NEXT STEPS:
  1. Migrate remaining SnapLogic pipelines to Glue (8 tables per sprint)
  2. Run parallel operations for 2 weeks during cutover
  3. Decommission SnapLogic after validation sign-off
══════════════════════════════════════════════════════════════════════
```

---

## 🗓️ YOUR DAY-BY-DAY ACTION PLAN

```
══════════════════════════════════════════════════════════════════════
WEEK 1: Feb 24 – Mar 1 — CONNECTIVITY
══════════════════════════════════════════════════════════════════════

MON Feb 24:
  □ Create Secrets Manager entries for SAP + SFDC + Snowflake
  □ Deploy glue_utils.py to S3 scripts folder
  □ Create GLUE_ENTITY_CONFIG + GLUE_AUDIT_LOG in Snowflake
  □ Run SQL seed script — insert all 12 entity configs
  □ Follow up on SAP firewall whitelisting (already In Progress)

TUE Feb 25:
  □ Get SFDC Connected App credentials from TCPL SFDC Admin
  □ Upload glue_poc_sfdc_connectivity_test.py to Glue
  □ Run SFDC connectivity test Glue job
  □ If passes → document ✅ | If fails → debug OAuth

WED Feb 26:
  □ Get SAP OData technical user credentials from TCPL SAP team
  □ Upload glue_poc_sap_connectivity_test.py to Glue
  □ Run SAP connectivity test (depends on firewall being open)
  □ If passes → document ✅ | If fails → debug network/auth

THU Feb 27:
  □ Fix any connectivity issues from Wed
  □ Decide primary source for Week 2 (whichever connected first)
  □ Start building first extraction job for primary source

FRI Feb 28:
  □ Both connectivity tests working ✅
  □ Document: HTTP status, response format, auth method
  □ Note any API quirks (SAP OData v2 vs v4 format, SFDC field names)
  □ Send Week 1 status to manager

══════════════════════════════════════════════════════════════════════
WEEK 2: Mar 3 – Mar 8 — EXTRACTION (2 TABLES)
══════════════════════════════════════════════════════════════════════

MON Mar 3:
  □ Build SFDC extraction job for SecondarySalesLineItem (or SAP BillingDocument)
  □ Test with small sample first ($top=100 in SOQL)
  □ Verify output Parquet files in S3

TUE Mar 4:
  □ Run FULL LOAD for Table 1
  □ Monitor: Glue CloudWatch logs, DPU usage, duration
  □ Verify row count in S3 output = expected

WED Mar 5:
  □ Build Table 2 extraction job
  □ Test with sample

THU Mar 6:
  □ Run FULL LOAD for Table 2
  □ Run DELTA LOAD for Table 1 (test incremental works)
  □ Verify: only new/changed records in delta output

FRI Mar 7-8:
  □ Fix any extraction issues
  □ Run both delta loads to confirm watermark updates correctly
  □ Check GLUE_AUDIT_LOG — all entries look correct?
  □ Send Week 2 status to manager

══════════════════════════════════════════════════════════════════════
WEEK 3: Mar 10 – Mar 15 — LOAD & VALIDATE (MILESTONE)
══════════════════════════════════════════════════════════════════════

MON Mar 10:
  □ Load Table 1 to Snowflake GLUE_POC_DB using existing transform template
  □ Verify row count in Snowflake matches S3

TUE Mar 11:
  □ Load Table 2 to Snowflake
  □ Run row count validation queries (Validation A)

WED Mar 12:
  □ Run MINUS queries (Validation B + C)
  □ Run checksum validation (Validation D)
  □ Run delta progression check (Validation E)

THU Mar 13:
  □ Document all validation results
  □ Note any failures and root causes
  □ Prepare performance comparison (Glue vs SnapLogic timing)

FRI Mar 14-15:
  □ Complete Validation Report document
  □ Write Go/No-Go recommendation with supporting data
  □ Present findings to Vishnu Rao (Business Sponsor) + Sam Joseph (Platform Owner)
  □ MILESTONE REVIEW ← This is your deadline
```

---

## 🚨 COMMON ISSUES & FIXES

```
ISSUE 1: SAP firewall not yet open (most likely Week 1 blocker)
────────────────────────────────────────────────────────────────
Symptom: Connection timeout or "Connection refused" from Glue
Fix:     Cannot fix without infra. Escalate daily.
         In meantime: build the SAP extraction code completely
         Use mock data to test logic offline
         Switch to SFDC first (more likely to be ready faster)

ISSUE 2: SFDC "INVALID_LOGIN" error
─────────────────────────────────────
Symptom: OAuth returns 400 "INVALID_LOGIN"
Cause A: Wrong username/password in Secrets Manager
Fix A:   Re-verify credentials with TCPL SFDC admin
Cause B: Security token needed but not provided
Fix B:   Add security_token to the Secrets Manager entry
         (required when logging in from non-whitelisted IP)
Cause C: Connected App "IP Relaxation" not set
Fix C:   SFDC Admin must set IP Relaxation = "Relax IP restrictions"

ISSUE 3: SAP OData pagination stops early
──────────────────────────────────────────
Symptom: Glue extracts 5000 rows then stops (even though table has more)
Cause:   SAP OData server-side page limit or response truncated
Fix:     Check response for @odata.nextLink token
         SAP OData v4 uses nextLink; v2 uses __next in response
         Update pagination logic accordingly:
         next_url = data.get("@odata.nextLink") or
                    data.get("d", {}).get("__next")

ISSUE 4: Parquet write fails for large SFDC tables
────────────────────────────────────────────────────
Symptom: OOM or Glue job killed after collecting all CSV
Cause:   122M rows collected in memory before write
Fix:     Stream pages directly to Parquet (page-by-page write)
         Write each page as separate S3 file, then compact:
         df.write.mode("append").parquet(s3_path)
         Increase DPU count (10 → 20)

ISSUE 5: Snowflake watermark update fails but extraction succeeded
───────────────────────────────────────────────────────────────────
Symptom: Job succeeds, data in S3, but LAST_WATERMARK not updated
Cause:   Snowflake connection closed before update, or commit missed
Fix:     Ensure update_watermark() is called BEFORE sf_conn.close()
         Wrap in try-finally to guarantee execution
         Check GLUE_AUDIT_LOG — if status=SUCCESS but no WM update,
         manually update:
         UPDATE GLUE_ENTITY_CONFIG SET LAST_WATERMARK = '2024-03-04'
         WHERE SRC_OBJ_NM = 'BillingDocument';

ISSUE 6: MINUS query returns records even when row counts match
────────────────────────────────────────────────────────────────
Symptom: Row counts identical but MINUS finds differences
Cause A: Same records but different data values (data mismatch)
Cause B: Numeric precision difference (SFDC returns 1.0, SAP returns 1)
Cause C: Timestamp format difference
Fix:     CAST both to same type in MINUS query
         Check specific differing rows to identify pattern
         Add CAST to validation queries for sensitive columns
```

---

## 🎯 SUCCESS CHECKLIST — March 15 Review

```
CONNECTIVITY ✅
  □ SAP OData connectivity test PASSED
  □ SFDC Bulk API 2.0 connectivity test PASSED
  □ All credentials stored in Secrets Manager

CONTROL TABLE ✅
  □ GLUE_ENTITY_CONFIG created and seeded with 12 entities
  □ GLUE_AUDIT_LOG created

EXTRACTION ✅
  □ SFDC extraction job built and tested
  □ SAP extraction job built and tested
  □ Full load working for at least 2 tables
  □ Delta/incremental load working (watermark updates correctly)
  □ Output in S3 as Parquet ✅
  □ GLUE_AUDIT_LOG populated with run details ✅

SNOWFLAKE LOAD ✅
  □ Table 1 loaded to GLUE_POC_DB schema
  □ Table 2 loaded to GLUE_POC_DB schema
  □ Row counts verified in Snowflake

VALIDATION ✅
  □ Row count comparison: 100% match
  □ MINUS queries: 0 records returned
  □ Delta validation: correct records captured
  □ Checksum validation: matching

DOCUMENTATION ✅
  □ Validation report written
  □ Performance comparison documented
  □ Issues encountered + resolutions documented
  □ Go/No-Go recommendation with evidence
```

---

*You are replacing SnapLogic with AWS Glue for TCPL's data extraction layer.*
*12 tables. 981M records. 62 GB. 3 weeks. You've got this bro 💪*
*Keep this file open every day. Follow the week-by-week plan.*
