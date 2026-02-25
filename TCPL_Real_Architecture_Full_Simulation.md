# 🏭 TCPL — Real Technical Architecture
## Full End-to-End Simulation (Based on Actual Architecture Diagram)
### Decision Point Analytics × Tata Consumer Products Limited

---

> ⚡ **This is the REAL architecture your manager shared.**
> Every system, every layer, every flow — simulated end to end.

---

## 🗺️ ACTUAL ARCHITECTURE AT A GLANCE

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              SOURCE SYSTEMS (8 Systems)                                  │
│  SAP S4HANA │ Salesforce │ Botree DMS │ SuccessFactors │ PEGASUS │ Infiniti │ Arkieva  │
│                                              UNIFY MDM                                   │
└─────────────────────────────────────┬───────────────────────────────────────────────────┘
                                      │
                    ┌─────────────────┼───────────────────┐
                    ▼                 ▼                   ▼
             SnapLogic          AWS Lambda           Direct S3 / APIs
          (CDC & Full Load)  (SharePoint → S3)      (Flat Files, Manual)
                    │                 │                   │
                    └─────────────────┴───────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                           AWS S3 DATA LAKE (Medallion)                                   │
│   Raw (CSV) → Processed (Parquet) → Refined (Parquet) → Curated (Parquet) → Archive    │
└─────────────────────────────────────┬───────────────────────────────────────────────────┘
                                      │
                                      ▼
                              AWS GLUE (134 Jobs)
                              ETL & Loading
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                         SNOWFLAKE DATA WAREHOUSE                                         │
│                                                                                         │
│   CORE Layer (Source Aligned 1:1)        BR/DM Layer (Business Ready)                  │
│   SAP_DB │ SFDC_DB │ BOTREE_DB           SALES_DM │ OPS_DM │ FINANCE_DM                │
│   PEGASUS_DB │ MDM_DB          ──SQL──►  HR_DM    │ ESG_DM                             │
└─────────────────────────────────────┬───────────────────────────────────────────────────┘
                                      │
                    ┌─────────────────┼───────────────────┐
                    ▼                 ▼                   ▼
               ThoughtSpot        Power BI           SageMaker
            (Primary BI ~70%)  (Detailed Reports)   (ML & Predictions)
                                                          │
                                                    predictions written
                                                    back to Snowflake ◄──┘

APACHE AIRFLOW (MWAA) — 250+ DAGs — Orchestrates ALL layers above
```

---

## 🔵 LAYER 1: SOURCE SYSTEMS — Deep Dive
### "Where every piece of TCPL data is born"

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        8 SOURCE SYSTEMS AT TCPL                              │
├──────────────┬─────────────┬─────────────────────────────────────────────────┤
│ SYSTEM       │ TYPE        │ WHAT DATA COMES FROM IT                         │
├──────────────┼─────────────┼─────────────────────────────────────────────────┤
│ SAP S4/HANA  │ ERP         │ - Primary sales orders                          │
│              │             │ - Finance / GL entries                           │
│              │             │ - Manufacturing / production orders              │
│              │             │ - Vendor / purchase orders                       │
│              │             │ - Material master (SKU data)                     │
│              │             │ - Customer master                                │
│              │             │ FREQUENCY: Daily batch extract                  │
├──────────────┼─────────────┼─────────────────────────────────────────────────┤
│ Salesforce   │ CRM         │ - Lead and opportunity data                      │
│              │             │ - Account (customer) data                        │
│              │             │ - Sales rep activity                             │
│              │             │ - Key Account Management data                    │
│              │             │ FREQUENCY: Daily / Real-time API                │
├──────────────┼─────────────┼─────────────────────────────────────────────────┤
│ Botree DMS   │ Distribution│ - SECONDARY SALES (distributor → retailer)      │
│              │             │ - Distributor stock levels                       │
│              │             │ - Beat-level sales (salesman routes)            │
│              │             │ - Retailer coverage data                         │
│              │             │ - Scheme redemption data                         │
│              │             │ WHY IMPORTANT: Primary sales = TCPL → Dist.    │
│              │             │ Secondary sales = Dist. → Retailer              │
│              │             │ Botree captures secondary (real market demand)  │
│              │             │ FREQUENCY: Daily                                │
├──────────────┼─────────────┼─────────────────────────────────────────────────┤
│ SuccessFactors│ HR         │ - Employee master data                           │
│              │             │ - Org hierarchy (who reports to whom)            │
│              │             │ - Headcount data                                 │
│              │             │ - Payroll cost allocation                        │
│              │             │ FREQUENCY: Weekly                               │
├──────────────┼─────────────┼─────────────────────────────────────────────────┤
│ PEGASUS      │ Planning    │ - Annual Operating Plan (AOP)                   │
│              │             │ - Budget vs actuals targets                      │
│              │             │ - Business planning assumptions                  │
│              │             │ FREQUENCY: Monthly / On demand                  │
├──────────────┼─────────────┼─────────────────────────────────────────────────┤
│ Infiniti     │ Analytics   │ - Legacy analytics / reports                    │
│              │             │ - Historical data pre-modernization              │
│              │             │ FREQUENCY: As needed                            │
├──────────────┼─────────────┼─────────────────────────────────────────────────┤
│ Arkieva      │ Supply Chain│ - Demand planning data                          │
│              │             │ - Supply network planning                        │
│              │             │ - Inventory optimization outputs                 │
│              │             │ - Production planning                            │
│              │             │ FREQUENCY: Daily / Weekly                       │
├──────────────┼─────────────┼─────────────────────────────────────────────────┤
│ UNIFY MDM    │ Master Data │ - GOLDEN RECORD for SKU codes                   │
│              │             │ - Customer master (unified across systems)       │
│              │             │ - Vendor codes                                   │
│              │             │ - Geography hierarchy                            │
│              │             │ WHY CRITICAL: Same product has different         │
│              │             │ codes in SAP vs Botree vs Salesforce.           │
│              │             │ MDM is the single source of truth.              │
│              │             │ FREQUENCY: Real-time / On change                │
└──────────────┴─────────────┴─────────────────────────────────────────────────┘

KEY INSIGHT FOR YOU:
─────────────────────
  Primary Sales   = SAP → tracks TCPL → Distributor movement
  Secondary Sales = Botree → tracks Distributor → Retailer movement
  
  The GAP between primary and secondary = pipeline stock at distributor
  TCPL management watches this gap closely every week.
  YOUR DATA PIPELINES FEED THIS ANALYSIS.
```

---

## 🔵 LAYER 2: INGESTION LAYER — How Data Moves to Cloud

### SnapLogic (Primary Ingestion)

```
WHAT SNAPLOGIC DOES AT TCPL:
──────────────────────────────
  Connects to all 8 source systems.
  Extracts data using CDC (Change Data Capture) or Full Load.
  Lands data in S3 raw zone as CSV.

CDC vs FULL LOAD:
──────────────────
  CDC (Change Data Capture):
    Reads ONLY records that changed since last run.
    Efficient. Used for high-volume transactional tables.
    Example: SAP sales orders — only today's new orders extracted
    How it works: Reads SAP change log (change pointers / timestamps)
  
  Full Load:
    Reads ENTIRE table every time.
    Used for small/reference tables.
    Example: MDM master data, PEGASUS planning targets
    Used when CDC not supported by source system.

SNAPLOGIC PIPELINE EXAMPLE — SAP Sales:
─────────────────────────────────────────
  ┌─────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌─────────────┐
  │SAP Snap │──►│CDC Filter│──►│ Mapper   │──►│CSV Format│──►│S3 Write Snap│
  │         │   │          │   │ Snap     │   │ Snap     │   │             │
  │Reads RFC│   │Only new/ │   │Rename    │   │Convert   │   │Lands in     │
  │/BAPI    │   │changed   │   │columns   │   │to CSV    │   │s3://raw/sap │
  │tables   │   │records   │   │to snake  │   │          │   │/sales/      │
  └─────────┘   └──────────┘   └──────────┘   └──────────┘   └─────────────┘

SNAPLOGIC PIPELINE EXAMPLE — Botree DMS:
──────────────────────────────────────────
  Botree DMS exposes REST API / SFTP
  SnapLogic REST Snap → pulls JSON/CSV → maps to standard schema → S3
  
  What gets pulled:
  - secondary_sales_daily.csv     (Dist → Retailer movement)
  - distributor_stock.csv          (how much stock each distributor holds)
  - scheme_redemption.csv          (which promo schemes redeemed where)

SNAPLOGIC PIPELINE EXAMPLE — Salesforce:
──────────────────────────────────────────
  Salesforce Snap → SOQL query → Mapper → S3
  SOQL: SELECT Id, AccountId, Amount, CloseDate FROM Opportunity
        WHERE LastModifiedDate > :last_run_timestamp
```

### AWS Lambda — Finance Files (SharePoint → S3)

```
THE PROBLEM:
─────────────
  TCPL Finance team maintains Excel/CSV files in SharePoint.
  (Budget files, Actuals, P&L adjustments)
  These can't go through SnapLogic easily.
  
  Lambda automates: "New file in SharePoint → auto-land in S3"

HOW IT WORKS:
──────────────
  SharePoint has webhook / event trigger configured.
  New file uploaded → triggers Lambda.
  Lambda downloads file from SharePoint API.
  Lambda uploads to s3://tcpl-datalake/raw/finance/sharepoint/

LAMBDA CODE (Finance SharePoint → S3):
────────────────────────────────────────
  import boto3, requests, os, json

  def lambda_handler(event, context):
      # Event from SharePoint webhook contains file URL
      file_url   = event["file_url"]
      file_name  = event["file_name"]
      sp_token   = get_sharepoint_token()  # from Secrets Manager
      
      # Download from SharePoint
      headers = {"Authorization": f"Bearer {sp_token}"}
      response = requests.get(file_url, headers=headers)
      
      # Upload to S3
      s3 = boto3.client("s3")
      s3.put_object(
          Bucket="tcpl-datalake",
          Key=f"raw/finance/sharepoint/{file_name}",
          Body=response.content
      )
      
      # Trigger Airflow DAG to process this file
      trigger_airflow_dag("tcpl_finance_file_processor", {"file": file_name})
      
      return {"status": "success", "file": file_name}
```

### Direct S3 / APIs — Flat Files & Manual Uploads

```
WHAT THIS COVERS:
──────────────────
  Some data can't come through SnapLogic or Lambda.
  Teams manually upload CSVs/Excel to S3.
  External data providers send files via SFTP → S3.
  
  Examples:
  - Nielsen market share data (external vendor sends weekly CSV)
  - Trade promotion calendar (marketing team uploads monthly)
  - Weather data API → Lambda → S3
  - Competitor pricing (manual research, Excel upload)
  
  S3 PATHS:
  s3://tcpl-datalake/raw/external/nielsen/
  s3://tcpl-datalake/raw/external/weather/
  s3://tcpl-datalake/raw/finance/manual_uploads/
  s3://tcpl-datalake/raw/trade_promos/
```

---

## 🔵 LAYER 3: AWS S3 DATA LAKE — Medallion Architecture

```
MEDALLION ARCHITECTURE = Bronze → Silver → Gold
At TCPL it's: Raw → Processed → Refined → Curated → Archive

┌──────────────────────────────────────────────────────────────────────────────┐
│                    MEDALLION ZONES IN DETAIL                                 │
├──────────────┬──────────────┬────────────────────────────────────────────────┤
│ ZONE         │ FORMAT       │ WHAT HAPPENS HERE                              │
├──────────────┼──────────────┼────────────────────────────────────────────────┤
│ RAW (Bronze) │ CSV          │ - Data lands EXACTLY as received               │
│              │              │ - NEVER modified, NEVER deleted                │
│              │              │ - Immutable archive of source truth            │
│              │              │ - Partitioned by source/date                   │
│              │              │ s3://tcpl-datalake/raw/                        │
├──────────────┼──────────────┼────────────────────────────────────────────────┤
│ PROCESSED    │ Parquet      │ - CSV → Parquet conversion (10x compression)   │
│ (Silver)     │              │ - Basic type casting (string → date, int)      │
│              │              │ - Remove completely blank rows                 │
│              │              │ - Rename columns to snake_case standard        │
│              │              │ - Partition by year/month/day                  │
│              │              │ s3://tcpl-datalake/processed/                  │
├──────────────┼──────────────┼────────────────────────────────────────────────┤
│ REFINED      │ Parquet      │ - Business logic applied                       │
│              │              │ - Deduplication (remove duplicate rows)        │
│              │              │ - Null handling (fill / flag / drop)           │
│              │              │ - MDM joins (apply unified SKU codes)          │
│              │              │ - Anomaly flagging                             │
│              │              │ s3://tcpl-datalake/refined/                    │
├──────────────┼──────────────┼────────────────────────────────────────────────┤
│ CURATED      │ Parquet      │ - Business-ready, analysis-ready               │
│ (Gold)       │              │ - Star schema ready tables                     │
│              │              │ - Feature-engineered (for ML)                  │
│              │              │ - Final layer before Snowflake load            │
│              │              │ s3://tcpl-datalake/curated/                    │
├──────────────┼──────────────┼────────────────────────────────────────────────┤
│ ARCHIVE      │ Glacier      │ - Data older than 1 year                       │
│              │              │ - AWS Glacier (very cheap storage)             │
│              │              │ - Retrieval takes hours (cold storage)         │
│              │              │ - Used for compliance / audit only             │
│              │              │ s3://tcpl-datalake/archive/                    │
└──────────────┴──────────────┴────────────────────────────────────────────────┘

FULL S3 STRUCTURE:
───────────────────
s3://tcpl-datalake/
├── raw/
│   ├── sap/
│   │   ├── sales_orders/year=2024/month=01/day=15/data.csv
│   │   ├── inventory/year=2024/month=01/day=15/
│   │   ├── finance/year=2024/month=01/day=15/
│   │   └── material_master/full_load/2024-01-15/
│   ├── salesforce/
│   │   └── opportunities/year=2024/month=01/day=15/
│   ├── botree/
│   │   ├── secondary_sales/year=2024/month=01/day=15/
│   │   └── distributor_stock/year=2024/month=01/day=15/
│   ├── successfactors/
│   ├── pegasus/
│   ├── arkieva/
│   ├── mdm/
│   ├── finance/
│   │   └── sharepoint/         ← Lambda drops here
│   └── external/
│       ├── nielsen/
│       └── weather/
├── processed/    ← CSV → Parquet
├── refined/      ← Business logic applied
├── curated/      ← Snowflake-ready
├── ml-features/  ← SageMaker feature store
│   ├── demand_forecast/
│   └── mmm/
├── model-artifacts/
│   ├── demand_forecast/
│   └── predictions/
└── archive/      ← Glacier

WHY PARQUET OVER CSV?
──────────────────────
  CSV:     100 MB file, full scan, slow queries
  Parquet: 10 MB (10x compression), columnar, predicate pushdown
  
  Example: "Give me only net_revenue column for Maharashtra"
  CSV:     Reads ALL columns, then filters  → SLOW
  Parquet: Reads ONLY net_revenue column    → 10x FASTER
  
  Parquet is why Glue queries on S3 are fast.
```

---

## 🔵 LAYER 4: AWS GLUE — 134 Jobs

```
WHY 134 JOBS?
──────────────
  134 Glue jobs = one job per source × per zone transition
  
  Example breakdown (rough):
  ┌────────────────────────────────────────────────────────┐
  │  SAP → Processed:      ~15 jobs (one per SAP table)    │
  │  SAP → Refined:        ~15 jobs                        │
  │  SAP → Curated:        ~10 jobs                        │
  │  Botree → all zones:   ~20 jobs                        │
  │  Salesforce → zones:   ~10 jobs                        │
  │  MDM joins:            ~10 jobs (cross-source joins)   │
  │  Feature engineering:  ~10 jobs (ML prep)              │
  │  Finance files:        ~15 jobs                        │
  │  Other sources:        ~29 jobs                        │
  │  TOTAL:                ~134 jobs                       │
  └────────────────────────────────────────────────────────┘

GLUE JOB NAMING CONVENTION AT TCPL:
──────────────────────────────────────
  tcpl-glue-{source}-{zone}-{table}
  
  Examples:
  tcpl-glue-sap-raw-to-processed-sales
  tcpl-glue-sap-processed-to-refined-sales
  tcpl-glue-sap-refined-to-curated-sales
  tcpl-glue-botree-raw-to-processed-secondary-sales
  tcpl-glue-mdm-join-sku-master
  tcpl-glue-feature-eng-demand-forecast

KEY GLUE JOB — SAP Sales Raw → Processed:
────────────────────────────────────────────

  import sys
  from pyspark.context import SparkContext
  from awsglue.context import GlueContext
  from awsglue.job import Job
  from awsglue.utils import getResolvedOptions
  from pyspark.sql import functions as F
  from pyspark.sql.types import *

  args = getResolvedOptions(sys.argv, ["JOB_NAME","JOB_DATE"])
  sc = SparkContext()
  glueContext = GlueContext(sc)
  spark = glueContext.spark_session
  job = Job(glueContext)
  job.init(args["JOB_NAME"], args)

  # READ Raw CSV
  raw_df = spark.read.option("header","true").option("inferSchema","true").csv(
      f"s3://tcpl-datalake/raw/sap/sales_orders/year={args['JOB_DATE'][:4]}/"
      f"month={args['JOB_DATE'][5:7]}/day={args['JOB_DATE'][8:10]}/"
  )

  # CONVERT to Parquet with proper types
  processed_df = (
      raw_df
      .withColumn("order_date",    F.to_date("order_date", "dd/MM/yyyy"))
      .withColumn("net_revenue",   F.col("net_revenue").cast(DoubleType()))
      .withColumn("quantity",      F.col("quantity").cast(IntegerType()))
      .withColumn("sku_code",      F.upper(F.trim("sku_code")))
      .withColumn("region_code",   F.upper(F.trim("region_code")))
      .withColumn("_ingested_at",  F.current_timestamp())
      .withColumn("_source",       F.lit("SAP_S4HANA"))
      .withColumn("_job_date",     F.lit(args["JOB_DATE"]))
  )

  # WRITE Parquet partitioned
  processed_df.write \
      .mode("overwrite") \
      .partitionBy("year","month") \
      .parquet("s3://tcpl-datalake/processed/sap/sales_orders/")

  job.commit()


KEY GLUE JOB — MDM Join (Critical at TCPL):
──────────────────────────────────────────────
  # WHY THIS IS CRITICAL:
  # SAP calls product "TEA-TTP-500-MH"
  # Botree calls same product "TT-PREM-500"
  # MDM has the GOLDEN CODE: "TCPL-SKU-00123"
  # This job joins everything to MDM golden codes

  sap_df    = spark.read.parquet("s3://tcpl-datalake/refined/sap/sales/")
  botree_df = spark.read.parquet("s3://tcpl-datalake/refined/botree/secondary_sales/")
  mdm_df    = spark.read.parquet("s3://tcpl-datalake/refined/mdm/sku_master/")

  # Join SAP sales to MDM golden SKU
  sap_with_mdm = sap_df.join(
      mdm_df.select("sap_sku_code","golden_sku_code","brand","category","pack_size"),
      on="sap_sku_code",
      how="left"
  )

  # Join Botree sales to MDM golden SKU
  botree_with_mdm = botree_df.join(
      mdm_df.select("botree_sku_code","golden_sku_code","brand","category","pack_size"),
      on="botree_sku_code",
      how="left"
  )

  # Now both use golden_sku_code → can be compared!
  # Write to curated
  sap_with_mdm.write.mode("overwrite").parquet("s3://tcpl-datalake/curated/sales/primary/")
  botree_with_mdm.write.mode("overwrite").parquet("s3://tcpl-datalake/curated/sales/secondary/")
```

---

## 🔵 LAYER 5: APACHE AIRFLOW (MWAA) — 250+ DAGs

```
250+ DAGs — WHY SO MANY?
──────────────────────────
  Each source system has multiple DAGs:
  ┌──────────────────────────────────────────────────────────┐
  │  SAP daily ingestion DAG                                 │
  │  SAP weekly full load DAG                                │
  │  Botree daily secondary sales DAG                        │
  │  Salesforce CRM daily DAG                                │
  │  SuccessFactors weekly HR DAG                            │
  │  PEGASUS monthly planning DAG                            │
  │  MDM daily sync DAG                                      │
  │  Finance SharePoint file processor DAG                   │
  │  Nielsen weekly DAG                                      │
  │  SALES_DM refresh DAG                                    │
  │  FINANCE_DM refresh DAG                                  │
  │  OPS_DM refresh DAG                                      │
  │  HR_DM refresh DAG                                       │
  │  ESG_DM refresh DAG                                      │
  │  SageMaker demand forecast DAG                           │
  │  SageMaker MMM DAG                                       │
  │  Data quality monitoring DAG                             │
  │  ... and 230+ more                                       │
  └──────────────────────────────────────────────────────────┘

MASTER ORCHESTRATION DAG — tcpl_master_daily:
───────────────────────────────────────────────

  from airflow import DAG
  from airflow.operators.trigger_dagrun import TriggerDagRunOperator
  from airflow.sensors.s3_key_sensor import S3KeySensor
  from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
  from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
  from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
  from datetime import datetime, timedelta

  default_args = {
      "owner": "decision_point_analytics",
      "retries": 2,
      "retry_delay": timedelta(minutes=10),
      "email_on_failure": True,
      "email": ["data-ops@decisionpoint.ai","tcpl-data@tcpl.in"],
  }

  with DAG(
      dag_id="tcpl_master_daily_pipeline",
      default_args=default_args,
      schedule_interval="0 20 * * *",  # 2AM IST
      start_date=datetime(2024,1,1),
      catchup=False,
      tags=["tcpl","master","production"],
  ) as dag:

      # ── STEP 1: Check all source files arrived ──────────────────────────
      check_sap_files = S3KeySensor(
          task_id="wait_for_sap_files",
          bucket_name="tcpl-datalake",
          bucket_key="raw/sap/sales_orders/year={{ ds[:4] }}/month={{ ds[5:7] }}/day={{ ds[8:10] }}/",
          wildcard_match=True,
          timeout=3600,          # wait up to 1 hour
          poke_interval=300,     # check every 5 minutes
          aws_conn_id="aws_tcpl",
      )

      check_botree_files = S3KeySensor(
          task_id="wait_for_botree_files",
          bucket_name="tcpl-datalake",
          bucket_key="raw/botree/secondary_sales/year={{ ds[:4] }}/month={{ ds[5:7] }}/day={{ ds[8:10] }}/",
          wildcard_match=True,
          timeout=3600,
          poke_interval=300,
          aws_conn_id="aws_tcpl",
      )

      # ── STEP 2: Raw → Processed (parallel per source) ──────────────────
      glue_sap_raw_to_processed = GlueJobOperator(
          task_id="glue_sap_raw_to_processed",
          job_name="tcpl-glue-sap-raw-to-processed-sales",
          script_args={"--JOB_DATE": "{{ ds }}"},
          aws_conn_id="aws_tcpl",
          region_name="ap-south-1",
          num_of_dpus=8,
      )

      glue_botree_raw_to_processed = GlueJobOperator(
          task_id="glue_botree_raw_to_processed",
          job_name="tcpl-glue-botree-raw-to-processed-secondary-sales",
          script_args={"--JOB_DATE": "{{ ds }}"},
          aws_conn_id="aws_tcpl",
          num_of_dpus=4,
      )

      glue_sf_raw_to_processed = GlueJobOperator(
          task_id="glue_salesforce_raw_to_processed",
          job_name="tcpl-glue-salesforce-raw-to-processed",
          script_args={"--JOB_DATE": "{{ ds }}"},
          aws_conn_id="aws_tcpl",
          num_of_dpus=2,
      )

      # ── STEP 3: Processed → Refined (MDM joins, dedup) ─────────────────
      glue_sap_processed_to_refined = GlueJobOperator(
          task_id="glue_sap_processed_to_refined",
          job_name="tcpl-glue-sap-processed-to-refined-sales",
          script_args={"--JOB_DATE": "{{ ds }}"},
          aws_conn_id="aws_tcpl",
          num_of_dpus=6,
      )

      glue_mdm_join = GlueJobOperator(
          task_id="glue_mdm_join_all_sources",
          job_name="tcpl-glue-mdm-join-sku-master",
          script_args={"--JOB_DATE": "{{ ds }}"},
          aws_conn_id="aws_tcpl",
          num_of_dpus=4,
      )

      # ── STEP 4: Data Quality Validation ────────────────────────────────
      validate_quality = LambdaInvokeFunctionOperator(
          task_id="validate_data_quality",
          function_name="tcpl-data-quality-validator",
          payload='{"date":"{{ ds }}","zones":["processed","refined"]}',
          aws_conn_id="aws_tcpl",
      )

      # ── STEP 5: Refined → Curated ───────────────────────────────────────
      glue_refined_to_curated = GlueJobOperator(
          task_id="glue_refined_to_curated",
          job_name="tcpl-glue-all-refined-to-curated",
          script_args={"--JOB_DATE": "{{ ds }}"},
          aws_conn_id="aws_tcpl",
          num_of_dpus=10,
      )

      # ── STEP 6: Load to Snowflake CORE layer ───────────────────────────
      load_snowflake_core = SnowflakeOperator(
          task_id="load_snowflake_core_layer",
          sql="""
              CALL TCPL_CPG_DB.PROCEDURES.LOAD_SAP_CORE('{{ ds }}');
              CALL TCPL_CPG_DB.PROCEDURES.LOAD_BOTREE_CORE('{{ ds }}');
              CALL TCPL_CPG_DB.PROCEDURES.LOAD_SFDC_CORE('{{ ds }}');
              CALL TCPL_CPG_DB.PROCEDURES.LOAD_MDM_CORE('{{ ds }}');
          """,
          snowflake_conn_id="snowflake_tcpl",
          warehouse="TCPL_LOAD_WH",
          database="TCPL_CPG_DB",
      )

      # ── STEP 7: SQL Transforms → BR/DM Layer ───────────────────────────
      refresh_sales_dm = SnowflakeOperator(
          task_id="refresh_sales_dm",
          sql="CALL TCPL_CPG_DB.PROCEDURES.REFRESH_SALES_DM('{{ ds }}')",
          snowflake_conn_id="snowflake_tcpl",
          warehouse="TCPL_TRANSFORM_WH",
      )

      refresh_finance_dm = SnowflakeOperator(
          task_id="refresh_finance_dm",
          sql="CALL TCPL_CPG_DB.PROCEDURES.REFRESH_FINANCE_DM('{{ ds }}')",
          snowflake_conn_id="snowflake_tcpl",
          warehouse="TCPL_TRANSFORM_WH",
      )

      refresh_ops_dm = SnowflakeOperator(
          task_id="refresh_ops_dm",
          sql="CALL TCPL_CPG_DB.PROCEDURES.REFRESH_OPS_DM('{{ ds }}')",
          snowflake_conn_id="snowflake_tcpl",
          warehouse="TCPL_TRANSFORM_WH",
      )

      # ── STEP 8: ML Pipeline (SageMaker) ────────────────────────────────
      trigger_sagemaker_forecast = TriggerDagRunOperator(
          task_id="trigger_demand_forecast_ml_dag",
          trigger_dag_id="tcpl_demand_forecast_sagemaker",
          execution_date="{{ ds }}",
          wait_for_completion=True,
      )

      # ── STEP 9: Success notification ───────────────────────────────────
      notify_success = LambdaInvokeFunctionOperator(
          task_id="notify_success",
          function_name="tcpl-slack-alerter",
          payload='{"channel":"#tcpl-pipeline-success","message":"✅ Pipeline complete for {{ ds }}"}',
          aws_conn_id="aws_tcpl",
      )

      # ── DEPENDENCY GRAPH ────────────────────────────────────────────────
      [check_sap_files, check_botree_files] >> glue_sap_raw_to_processed
      check_sap_files    >> glue_sf_raw_to_processed
      check_botree_files >> glue_botree_raw_to_processed

      [glue_sap_raw_to_processed,
       glue_botree_raw_to_processed,
       glue_sf_raw_to_processed] >> glue_sap_processed_to_refined

      glue_sap_processed_to_refined >> glue_mdm_join
      glue_mdm_join >> validate_quality
      validate_quality >> glue_refined_to_curated
      glue_refined_to_curated >> load_snowflake_core
      load_snowflake_core >> [refresh_sales_dm, refresh_finance_dm, refresh_ops_dm]
      [refresh_sales_dm, refresh_finance_dm, refresh_ops_dm] >> trigger_sagemaker_forecast
      trigger_sagemaker_forecast >> notify_success
```

---

## 🔵 LAYER 6: SNOWFLAKE DATA WAREHOUSE

### CORE Layer — Source Aligned 1:1 Fidelity

```
WHAT "1:1 FIDELITY" MEANS:
────────────────────────────
  Core layer mirrors source systems EXACTLY in SQL.
  No business logic applied. No joins across systems.
  It's a digital replica of SAP, Botree, Salesforce in Snowflake.
  
  Think of it as: "SAP data, but in Snowflake SQL format"

SAP_DB — Core Schema:
───────────────────────
  -- Mirrors SAP tables exactly
  CREATE TABLE SAP_DB.CORE.VBAK (           -- SAP Sales Order Header
      vbeln    VARCHAR(10),                  -- Sales Order Number
      erdat    DATE,                         -- Order Date
      auart    VARCHAR(4),                   -- Order Type
      kunnr    VARCHAR(10),                  -- Customer Number
      vkorg    VARCHAR(4),                   -- Sales Org
      vtweg    VARCHAR(2),                   -- Distribution Channel
      spart    VARCHAR(2),                   -- Division
      _load_date DATE,
      _source    VARCHAR(50) DEFAULT 'SAP_S4HANA'
  );

  CREATE TABLE SAP_DB.CORE.VBAP (           -- SAP Sales Order Line Items
      vbeln    VARCHAR(10),                  -- Sales Order Number
      posnr    VARCHAR(6),                   -- Line Item Number
      matnr    VARCHAR(18),                  -- Material Number (SKU)
      kwmeng   DECIMAL(15,3),               -- Order Quantity
      netwr    DECIMAL(15,2),               -- Net Value
      waerk    VARCHAR(5),                   -- Currency
      _load_date DATE,
      _source    VARCHAR(50) DEFAULT 'SAP_S4HANA'
  );

BOTREE_DB — Core Schema:
──────────────────────────
  CREATE TABLE BOTREE_DB.CORE.SECONDARY_SALES (
      sale_id          VARCHAR(50),
      sale_date        DATE,
      distributor_code VARCHAR(20),
      retailer_code    VARCHAR(20),
      product_code     VARCHAR(30),   -- Botree's own product code
      units_sold       INTEGER,
      sale_value       DECIMAL(12,2),
      beat_code        VARCHAR(20),   -- Salesman beat/route
      scheme_id        VARCHAR(20),   -- If promo was applied
      _load_date       DATE,
      _source          VARCHAR(50) DEFAULT 'BOTREE_DMS'
  );

  CREATE TABLE BOTREE_DB.CORE.DISTRIBUTOR_STOCK (
      stock_date       DATE,
      distributor_code VARCHAR(20),
      product_code     VARCHAR(30),
      opening_stock    INTEGER,
      closing_stock    INTEGER,
      days_of_stock    DECIMAL(8,2),
      _load_date       DATE
  );

MDM_DB — Core Schema (the ROSETTA STONE):
───────────────────────────────────────────
  CREATE TABLE MDM_DB.CORE.SKU_MASTER (
      golden_sku_code  VARCHAR(20),    -- TCPL standard code (source of truth)
      sap_matnr        VARCHAR(18),    -- What SAP calls it
      botree_product_code VARCHAR(30), -- What Botree calls it
      sf_product_id    VARCHAR(20),    -- What Salesforce calls it
      sku_name         VARCHAR(200),
      brand            VARCHAR(100),   -- Tata Tea, Tetley, Himalayan...
      category         VARCHAR(100),   -- Tea, Coffee, Salt, Water...
      sub_category     VARCHAR(100),
      pack_size_grams  INTEGER,
      pack_type        VARCHAR(50),
      mrp              DECIMAL(10,2),
      launch_date      DATE,
      is_active        BOOLEAN,
      _load_date       DATE,
      _source          VARCHAR(50) DEFAULT 'UNIFY_MDM'
  );

  -- THIS IS THE MOST IMPORTANT TABLE IN SNOWFLAKE AT TCPL
  -- All DM layer joins flow through this golden SKU master
```

### BR / DM Layer — Business Ready Cross-Source Integrated

```
SQL TRANSFORMATION LOGIC:
──────────────────────────
  This is where Core layer tables are joined across sources
  to create business-ready data marts.

SALES_DM — Primary + Secondary Sales Unified:
───────────────────────────────────────────────
  CREATE OR REPLACE TABLE TCPL_CPG_DB.SALES_DM.FACT_UNIFIED_SALES AS
  
  -- PRIMARY SALES (SAP: TCPL → Distributor)
  SELECT
      'PRIMARY'                   AS sale_type,
      h.erdat                     AS sale_date,
      m.golden_sku_code           AS sku_code,
      m.brand,
      m.category,
      m.sub_category,
      m.pack_size_grams,
      h.kunnr                     AS customer_code,
      geo.region_name,
      geo.zone,
      geo.state,
      SUM(l.kwmeng)               AS units_sold,
      SUM(l.netwr)                AS net_revenue,
      NULL                        AS distributor_code,
      NULL                        AS retailer_code,
      h._load_date
  FROM SAP_DB.CORE.VBAP l
  JOIN SAP_DB.CORE.VBAK h         ON l.vbeln = h.vbeln
  JOIN MDM_DB.CORE.SKU_MASTER m   ON l.matnr = m.sap_matnr
  JOIN MDM_DB.CORE.GEO_MASTER geo ON h.vkorg = geo.sales_org_code
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,15

  UNION ALL

  -- SECONDARY SALES (Botree: Distributor → Retailer)
  SELECT
      'SECONDARY'                 AS sale_type,
      s.sale_date,
      m.golden_sku_code           AS sku_code,
      m.brand,
      m.category,
      m.sub_category,
      m.pack_size_grams,
      NULL                        AS customer_code,
      geo.region_name,
      geo.zone,
      geo.state,
      SUM(s.units_sold)           AS units_sold,
      SUM(s.sale_value)           AS net_revenue,
      s.distributor_code,
      s.retailer_code,
      s._load_date
  FROM BOTREE_DB.CORE.SECONDARY_SALES s
  JOIN MDM_DB.CORE.SKU_MASTER m   ON s.product_code = m.botree_product_code
  JOIN MDM_DB.CORE.GEO_MASTER geo ON s.beat_code = geo.beat_code
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,14,15,16;


FINANCE_DM — P&L with Budget vs Actuals:
──────────────────────────────────────────
  CREATE OR REPLACE VIEW TCPL_CPG_DB.FINANCE_DM.PNL_BUDGET_VS_ACTUAL AS
  SELECT
      d.fiscal_year,
      d.fiscal_quarter,
      d.month_name,
      m.brand,
      m.category,
      geo.zone,
      geo.state,
      SUM(f.net_revenue)          AS actual_revenue,
      SUM(f.cogs)                 AS actual_cogs,
      SUM(f.gross_margin)         AS actual_gross_margin,
      SUM(p.budgeted_revenue)     AS budget_revenue,
      SUM(p.budgeted_margin)      AS budget_margin,
      SUM(f.net_revenue - p.budgeted_revenue) AS revenue_variance,
      ROUND(SUM(f.net_revenue - p.budgeted_revenue)
            / NULLIF(SUM(p.budgeted_revenue),0) * 100, 2) AS variance_pct
  FROM SAP_DB.CORE.FINANCIAL_ACTUALS f
  JOIN PEGASUS_DB.CORE.ANNUAL_PLAN p
    ON f.cost_center = p.cost_center AND f.period = p.period
  JOIN MDM_DB.CORE.SKU_MASTER m   ON f.matnr = m.sap_matnr
  JOIN MDM_DB.CORE.GEO_MASTER geo ON f.profit_center = geo.profit_center
  JOIN MDM_DB.CORE.DATE_DIM d     ON f.posting_date = d.full_date
  GROUP BY 1,2,3,4,5,6,7;


ESG_DM — Environmental/Sustainability Reporting:
──────────────────────────────────────────────────
  -- TCPL is a listed company with ESG obligations
  -- This mart tracks sustainability KPIs
  CREATE OR REPLACE TABLE TCPL_CPG_DB.ESG_DM.SUSTAINABILITY_METRICS AS
  SELECT
      year,
      quarter,
      plant_code,
      SUM(energy_consumption_kwh) AS total_energy_kwh,
      SUM(water_consumption_kl)   AS total_water_kl,
      SUM(carbon_emissions_tons)  AS total_carbon_tons,
      SUM(waste_generated_tons)   AS total_waste_tons,
      SUM(waste_recycled_tons)    AS recycled_waste_tons,
      ROUND(SUM(waste_recycled_tons)/NULLIF(SUM(waste_generated_tons),0)*100,2)
                                  AS recycling_rate_pct
  FROM SAP_DB.CORE.PLANT_OPERATIONS
  GROUP BY 1,2,3;


SNOWFLAKE WAREHOUSES:
──────────────────────
  TCPL_LOAD_WH         (MEDIUM)  ← Glue ETL writes, Lambda inserts
  TCPL_TRANSFORM_WH    (LARGE)   ← SQL transforms, stored procedures
  TCPL_ANALYTICS_WH    (SMALL)   ← ThoughtSpot + Power BI queries
  TCPL_ML_WH           (MEDIUM)  ← SageMaker prediction loads


COMPLETE SNOWFLAKE DATABASE MAP:
──────────────────────────────────
  TCPL_CPG_DB
  ├── SAP_DB          ← Core: SAP tables 1:1
  ├── SFDC_DB         ← Core: Salesforce tables 1:1
  ├── BOTREE_DB       ← Core: Botree DMS tables 1:1
  ├── PEGASUS_DB      ← Core: Planning targets
  ├── MDM_DB          ← Core: Golden master data
  ├── SALES_DM        ← BR: Primary + Secondary unified
  ├── OPS_DM          ← BR: Supply chain + inventory
  ├── FINANCE_DM      ← BR: P&L, Budget vs Actual
  ├── HR_DM           ← BR: Headcount, org hierarchy
  ├── ESG_DM          ← BR: Sustainability metrics
  └── ML_OUTPUTS      ← SageMaker predictions written back
```

---

## 🔵 LAYER 7: ANALYTICS & CONSUMPTION

### ThoughtSpot — Primary BI (~70% usage)

```
WHAT IS THOUGHTSPOT?
─────────────────────
  AI-powered search analytics.
  Business users type natural language questions.
  ThoughtSpot generates SQL automatically.
  No need to know SQL or wait for IT reports.

HOW TCPL USES IT:
──────────────────
  Business user types: "revenue by brand last quarter vs previous year"
  ThoughtSpot auto-generates SQL against Snowflake SALES_DM
  Shows chart instantly.
  
  More examples:
  "Top 10 SKUs by revenue in Maharashtra this month"
  "Secondary sales growth rate by zone"
  "Which distributors had stockout last week"
  "Budget vs actual by region for Tata Tea"

WHO USES THOUGHTSPOT AT TCPL:
───────────────────────────────
  Sales managers   → daily secondary sales tracking
  Demand planners  → forecast vs actual
  Finance team     → budget vs actual
  Supply chain     → inventory and stockout analysis
  Leadership       → executive dashboards


Power BI — Detailed Reports (~30% usage):
──────────────────────────────────────────
  Used for pixel-perfect structured reports.
  More static than ThoughtSpot.
  
  Use cases:
  - Monthly P&L report (formatted for CFO)
  - Weekly sales deck (structured layout)
  - Regulatory / compliance reports
  - ESG reporting


SageMaker → Snowflake (Predictions Feedback Loop):
─────────────────────────────────────────────────────
  DASHED LINE in architecture = predictions written BACK to Snowflake
  
  Flow:
  1. SageMaker runs demand forecast
  2. Predictions CSV → S3
  3. Lambda loads predictions → ML_OUTPUTS schema in Snowflake
  4. ThoughtSpot / Power BI shows "Forecast vs Actual" reports
  5. Demand planners override predictions if needed (via UI → back to Snowflake)
  
  Tables in ML_OUTPUTS:
  ML_OUTPUTS.DEMAND_FORECAST_PREDICTIONS
  ML_OUTPUTS.MMM_ATTRIBUTION_RESULTS
  ML_OUTPUTS.PROMO_RECOMMENDATIONS
  ML_OUTPUTS.STOCKOUT_RISK_SCORES


External Systems — Mavic / EY:
────────────────────────────────
  Some Snowflake data is EXPORTED to external parties.
  
  "Mavic - EY External" = Ernst & Young external auditors
  They get access to specific Snowflake views for audit purposes.
  Secure data share via Snowflake Data Sharing feature.
  EY gets a read-only view — no raw data exposure.
```

---

## 🔵 COMPLETE DAILY TIMELINE

```
TIME (IST)     WHAT HAPPENS
───────────────────────────────────────────────────────────────────────
10:00 PM       SAP batch jobs run at TCPL
               Extracts: Sales orders, Inventory, Finance postings
               SnapLogic job triggers automatically

11:00 PM       SnapLogic pushes SAP CSV → S3 raw zone
               SnapLogic pushes Botree secondary sales → S3 raw
               SnapLogic pushes Salesforce CRM data → S3 raw

12:00 AM       MDM daily sync runs
               UNIFY MDM → SnapLogic → S3 raw/mdm/

01:00 AM       Airflow DAG wakes up (S3KeySensors checking)
               Waiting for all source files to arrive

02:00 AM  ★    All files confirmed arrived
               AIRFLOW MASTER DAG STARTS

02:05 AM       PARALLEL Glue jobs start:
               → tcpl-glue-sap-raw-to-processed-sales (8 DPUs)
               → tcpl-glue-botree-raw-to-processed (4 DPUs)
               → tcpl-glue-sf-raw-to-processed (2 DPUs)
               CSV → Parquet, type casting, column rename

03:00 AM       Processed → Refined Glue jobs start
               → MDM joins apply (golden SKU codes)
               → Deduplication runs
               → Business rules applied

04:00 AM       Data Quality Lambda runs
               Checks all refined datasets
               Emits CloudWatch metrics
               Branches: pass → continue, fail → alert + stop

04:15 AM       Refined → Curated Glue jobs
               Final transformations, star-schema ready data

04:45 AM       Snowflake CORE layer load begins
               Glue writes directly to Snowflake via Spark connector
               SAP_DB, BOTREE_DB, SFDC_DB, MDM_DB refreshed

05:30 AM       SQL Transforms run (PARALLEL):
               → REFRESH_SALES_DM stored proc
               → REFRESH_FINANCE_DM stored proc
               → REFRESH_OPS_DM stored proc
               → REFRESH_HR_DM stored proc
               → REFRESH_ESG_DM stored proc

06:00 AM  ★    ThoughtSpot + Power BI connected to fresh Snowflake DMs
               Business dashboards show overnight data

06:05 AM       SageMaker Demand Forecast DAG triggers
               Feature engineering → Training (if retraining day)
               OR Batch inference (daily predictions)

07:30 AM       ML predictions land in Snowflake ML_OUTPUTS
               Slack: ✅ "TCPL pipeline complete for 2024-01-15"

09:00 AM  ★    Business users log in
               ThoughtSpot has fresh data
               Power BI reports updated
               Demand planners see today's forecast
```

---

## 🔵 COMPLETE SNOWFLAKE DATA FLOW SIMULATION

```sql
-- ════════════════════════════════════════════════════════════════════
-- STORED PROCEDURE: REFRESH_SALES_DM
-- Called by Airflow SnowflakeOperator after CORE layer is loaded
-- ════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE TCPL_CPG_DB.PROCEDURES.REFRESH_SALES_DM(JOB_DATE DATE)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
BEGIN
    -- Step 1: Truncate staging
    TRUNCATE TABLE SALES_DM.STAGING_UNIFIED_SALES;

    -- Step 2: Load Primary Sales from SAP_DB CORE
    INSERT INTO SALES_DM.STAGING_UNIFIED_SALES
    SELECT
        'PRIMARY'                         AS sale_type,
        TO_DATE(h.erdat)                  AS sale_date,
        YEAR(TO_DATE(h.erdat))            AS sale_year,
        MONTH(TO_DATE(h.erdat))           AS sale_month,
        WEEKOFYEAR(TO_DATE(h.erdat))      AS sale_week,
        m.golden_sku_code,
        m.brand,
        m.category,
        m.sub_category,
        m.pack_size_grams,
        m.mrp,
        h.kunnr                           AS customer_code,
        cust.customer_name,
        cust.customer_type,
        geo.region_code,
        geo.region_name,
        geo.zone,
        geo.state,
        geo.city_tier,
        'modern_trade'                    AS channel,
        SUM(l.kwmeng)                     AS units_sold,
        SUM(l.netwr)                      AS net_revenue,
        SUM(l.netwr) / NULLIF(SUM(l.kwmeng),0) AS revenue_per_unit,
        NULL::VARCHAR                     AS distributor_code,
        NULL::VARCHAR                     AS retailer_code,
        NULL::INTEGER                     AS beat_code,
        :JOB_DATE                         AS load_date
    FROM SAP_DB.CORE.VBAP l
    JOIN SAP_DB.CORE.VBAK h           ON l.vbeln = h.vbeln
    JOIN MDM_DB.CORE.SKU_MASTER m     ON l.matnr = m.sap_matnr
    JOIN MDM_DB.CORE.CUSTOMER_MASTER cust ON h.kunnr = cust.sap_kunnr
    JOIN MDM_DB.CORE.GEO_MASTER geo   ON h.vkorg = geo.sales_org_code
    WHERE TO_DATE(h.erdat) = :JOB_DATE
      AND h.auart IN ('ZOR','ZCON')   -- Standard order types only
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,27;

    -- Step 3: Load Secondary Sales from BOTREE_DB CORE
    INSERT INTO SALES_DM.STAGING_UNIFIED_SALES
    SELECT
        'SECONDARY'                       AS sale_type,
        s.sale_date,
        YEAR(s.sale_date)                 AS sale_year,
        MONTH(s.sale_date)                AS sale_month,
        WEEKOFYEAR(s.sale_date)           AS sale_week,
        m.golden_sku_code,
        m.brand,
        m.category,
        m.sub_category,
        m.pack_size_grams,
        m.mrp,
        NULL::VARCHAR                     AS customer_code,
        NULL::VARCHAR                     AS customer_name,
        'Distributor'                     AS customer_type,
        geo.region_code,
        geo.region_name,
        geo.zone,
        geo.state,
        geo.city_tier,
        geo.channel_type                  AS channel,
        SUM(s.units_sold)                 AS units_sold,
        SUM(s.sale_value)                 AS net_revenue,
        SUM(s.sale_value)/NULLIF(SUM(s.units_sold),0) AS revenue_per_unit,
        s.distributor_code,
        s.retailer_code,
        s.beat_code,
        :JOB_DATE                         AS load_date
    FROM BOTREE_DB.CORE.SECONDARY_SALES s
    JOIN MDM_DB.CORE.SKU_MASTER m     ON s.product_code = m.botree_product_code
    JOIN MDM_DB.CORE.GEO_MASTER geo   ON s.beat_code = geo.beat_code
    WHERE s.sale_date = :JOB_DATE
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,24,25,26,27;

    -- Step 4: MERGE into FACT table (upsert)
    MERGE INTO SALES_DM.FACT_UNIFIED_SALES t
    USING SALES_DM.STAGING_UNIFIED_SALES s
    ON  t.sale_type = s.sale_type
    AND t.sale_date = s.sale_date
    AND t.golden_sku_code = s.golden_sku_code
    AND t.region_code = s.region_code
    WHEN MATCHED THEN UPDATE SET
        t.units_sold       = s.units_sold,
        t.net_revenue      = s.net_revenue,
        t.revenue_per_unit = s.revenue_per_unit,
        t.load_date        = s.load_date
    WHEN NOT MATCHED THEN INSERT VALUES (
        s.sale_type, s.sale_date, s.sale_year, s.sale_month, s.sale_week,
        s.golden_sku_code, s.brand, s.category, s.sub_category,
        s.pack_size_grams, s.mrp, s.customer_code, s.customer_name,
        s.customer_type, s.region_code, s.region_name, s.zone, s.state,
        s.city_tier, s.channel, s.units_sold, s.net_revenue,
        s.revenue_per_unit, s.distributor_code, s.retailer_code,
        s.beat_code, s.load_date
    );

    v_rows_inserted := SQLROWCOUNT;

    -- Step 5: Refresh analytics views
    CREATE OR REPLACE VIEW SALES_DM.PRIMARY_VS_SECONDARY_GAP AS
    SELECT
        sale_date,
        golden_sku_code,
        brand,
        region_name,
        zone,
        SUM(CASE WHEN sale_type='PRIMARY' THEN units_sold ELSE 0 END) AS primary_units,
        SUM(CASE WHEN sale_type='SECONDARY' THEN units_sold ELSE 0 END) AS secondary_units,
        SUM(CASE WHEN sale_type='PRIMARY' THEN units_sold ELSE 0 END) -
        SUM(CASE WHEN sale_type='SECONDARY' THEN units_sold ELSE 0 END) AS pipeline_gap,
        ROUND(
            (SUM(CASE WHEN sale_type='PRIMARY' THEN units_sold ELSE 0 END) -
             SUM(CASE WHEN sale_type='SECONDARY' THEN units_sold ELSE 0 END)) /
            NULLIF(SUM(CASE WHEN sale_type='PRIMARY' THEN units_sold ELSE 0 END),0) * 100, 2
        ) AS pipeline_gap_pct
    FROM SALES_DM.FACT_UNIFIED_SALES
    GROUP BY 1,2,3,4,5;

    RETURN 'SUCCESS: ' || v_rows_inserted || ' rows merged into SALES_DM for ' || :JOB_DATE;
END;
$$;
```

---

## 🔵 ERROR HANDLING & MONITORING

```
WHAT YOU MONITOR DAILY:
────────────────────────

  1. AIRFLOW UI (your first stop every morning)
     URL: https://your-mwaa-env.us.airflow.amazonaws.com
     Check: All 250+ DAGs — green = good, red = investigate
     
  2. CLOUDWATCH DASHBOARDS
     Namespace: TCPL/DataPipeline
     Key metrics:
     - Glue job duration (alert if >2x normal)
     - Row counts per zone per source
     - Null rates per dataset
     - Failed DAG runs count

  3. SLACK CHANNELS
     #tcpl-pipeline-success  ← ✅ messages every morning
     #tcpl-data-alerts       ← ⚠️ failures, quality issues
     #tcpl-ml-monitoring     ← Model drift, MAPE alerts

COMMON FAILURE SCENARIOS:
──────────────────────────
  FAILURE 1: SAP file late
  ────────────────────────
  S3KeySensor waits up to 1 hour (configured)
  After 1 hour → SLAMissCallback fires → Slack alert
  Action: Call TCPL SAP BASIS team
  Resolution: Once file arrives → clear sensor → pipeline resumes

  FAILURE 2: Glue job OOM (Out of Memory)
  ────────────────────────────────────────
  Symptom: Glue task red, error: "Container killed (OOM)"
  Cause:   Data volume grew too big for current DPU setting
  Action:  Increase DPUs from 4 → 8 in Glue job config
  Fix:     Update Terraform config for Glue job DPU count

  FAILURE 3: MDM join producing nulls
  ─────────────────────────────────────
  Symptom: SALES_DM has NULL golden_sku_code for some rows
  Cause:   New SKU in SAP not yet in UNIFY MDM
  Action:  Flag to TCPL MDM team to register the new SKU
  Temp fix: Filter out nulls, process known SKUs

  FAILURE 4: Snowflake warehouse suspended mid-load
  ──────────────────────────────────────────────────
  Symptom: Snowflake operator timeout
  Cause:   Auto-suspend triggered during large load
  Action:  Increase auto_suspend timeout on TCPL_LOAD_WH
  Fix:     ALTER WAREHOUSE TCPL_LOAD_WH SET AUTO_SUSPEND = 600;

  FAILURE 5: Botree data missing secondary sales for some regions
  ──────────────────────────────────────────────────────────────
  Symptom: Row count drops 30% in BOTREE_DB
  Cause:   Botree API timeout / regional server issue
  Action:  Check Botree system status with TCPL IT
  Resolution: Botree will resend — trigger Botree DAG manually
```

---

## 🔵 INFRASTRUCTURE AS CODE (Terraform)

```hcl
# terraform/main.tf — TCPL Real Architecture

# S3 Data Lake with Lifecycle Rules
resource "aws_s3_bucket" "tcpl_datalake" {
  bucket = "tcpl-datalake-prod"
  tags   = { Project = "TCPL-Data-Platform", Owner = "DecisionPointAnalytics" }
}

resource "aws_s3_bucket_lifecycle_configuration" "datalake_lifecycle" {
  bucket = aws_s3_bucket.tcpl_datalake.id
  
  rule {
    id = "archive-raw-after-1-year"
    status = "Enabled"
    filter { prefix = "raw/" }
    transition { days = 365; storage_class = "GLACIER" }
  }
  
  rule {
    id = "delete-processed-after-90-days"
    status = "Enabled"
    filter { prefix = "processed/" }
    expiration { days = 90 }  # processed zone purged after 90d (data in refined)
  }
}

# MWAA (Airflow) — 250+ DAGs
resource "aws_mwaa_environment" "tcpl_airflow" {
  name              = "tcpl-mwaa-prod"
  airflow_version   = "2.8.1"
  environment_class = "mw1.large"   # Large for 250+ DAGs
  max_workers       = 20
  min_workers       = 2
  source_bucket_arn = aws_s3_bucket.tcpl_datalake.arn
  dag_s3_path       = "dags/"
  execution_role_arn = aws_iam_role.mwaa_role.arn

  airflow_configuration_options = {
    "core.max_active_runs_per_dag"      = "1"
    "core.max_active_tasks_per_dag"     = "16"
    "scheduler.dag_dir_list_interval"   = "30"
    "core.parallelism"                  = "50"
  }

  logging_configuration {
    dag_processing_logs { enabled = true; log_level = "WARNING" }
    scheduler_logs      { enabled = true; log_level = "WARNING" }
    task_logs           { enabled = true; log_level = "INFO" }
    webserver_logs      { enabled = true; log_level = "WARNING" }
    worker_logs         { enabled = true; log_level = "WARNING" }
  }
}

# Glue — 134 jobs (example of one)
resource "aws_glue_job" "sap_raw_to_processed" {
  name     = "tcpl-glue-sap-raw-to-processed-sales"
  role_arn = aws_iam_role.glue_role.arn
  
  command {
    name            = "glueetl"
    script_location = "s3://tcpl-datalake/scripts/glue/sap_raw_to_processed_sales.py"
    python_version  = "3"
  }
  
  default_arguments = {
    "--job-language"              = "python"
    "--enable-metrics"            = ""
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"           = "true"
    "--spark-event-logs-path"     = "s3://tcpl-datalake/spark-logs/"
    "--TempDir"                   = "s3://tcpl-datalake/glue-temp/"
  }
  
  max_capacity      = 8     # 8 DPUs
  glue_version      = "4.0"
  timeout           = 120   # 2 hour max
  number_of_workers = 8
  worker_type       = "G.1X"
}

# Lambda functions
resource "aws_lambda_function" "sharepoint_to_s3" {
  filename         = "lambdas/sharepoint_to_s3.zip"
  function_name    = "tcpl-sharepoint-finance-to-s3"
  role             = aws_iam_role.lambda_role.arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.11"
  timeout          = 300
  memory_size      = 512
  environment {
    variables = {
      BUCKET_NAME         = "tcpl-datalake"
      SHAREPOINT_SITE_URL = var.sharepoint_url
      SECRET_ARN          = aws_secretsmanager_secret.sharepoint_creds.arn
    }
  }
}
```

---

## 🔵 QUICK REFERENCE — YOUR DAY-1 CHEATSHEET

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    TCPL ARCHITECTURE — QUICK REFERENCE                       │
├──────────────┬─────────────────────────┬──────────────────────────────────┤
│ WHAT          │ WHERE (Tool/System)     │ YOUR JOB                         │
├──────────────┼─────────────────────────┼──────────────────────────────────┤
│ 8 Sources     │ SAP,Botree,SF,MDM,etc  │ Understand what each produces    │
│ Ingestion     │ SnapLogic + Lambda      │ Monitor, fix if files missing    │
│ Data Lake     │ AWS S3 (4 zones)        │ Check file counts, sizes         │
│ ETL           │ AWS Glue (134 jobs)     │ Debug failed jobs, add columns   │
│ Orchestration │ Airflow MWAA (250 DAGs) │ Monitor runs, re-trigger         │
│ Warehouse     │ Snowflake (CORE + DM)   │ Write SQL, build views           │
│ BI            │ ThoughtSpot + Power BI  │ Support business questions       │
│ ML            │ SageMaker               │ Monitor MAPE, trigger retrains   │
│ Alerts        │ Slack + CloudWatch      │ First responder on failures      │
├──────────────┼─────────────────────────┼──────────────────────────────────┤
│ MOST IMPORTANT TABLE:  MDM_DB.CORE.SKU_MASTER  (golden product codes)       │
│ MOST IMPORTANT DAG:    tcpl_master_daily_pipeline                            │
│ MOST IMPORTANT DM:     SALES_DM (primary + secondary sales unified)          │
│ PIPELINE RUNS:         2:00 AM IST daily, done by 6-7:30 AM                 │
│ AWS REGION:            ap-south-1 (Mumbai)                                   │
│ ALERT CHANNEL:         #tcpl-data-alerts (Slack)                             │
└─────────────────────────────────────────────────────────────────────────────┘

KEY INSIGHT FOR YOUR WORK:
───────────────────────────
  The most complex thing at TCPL is reconciling PRIMARY and SECONDARY sales.
  
  Primary sales (SAP)  = TCPL books revenue when it sells to distributor
  Secondary sales (Botree) = What actually sold at retailer shelf
  
  The DIFFERENCE = how much stock is sitting at distributors (pipeline stock)
  
  Your Snowflake SALES_DM.PRIMARY_VS_SECONDARY_GAP view tracks this.
  This is what TCPL management watches EVERY SINGLE DAY.
  This is why your pipeline matters.
```

---

*This is the REAL architecture from your manager's diagram.*
*You now know every layer, every system, every table, every job.*
*You've got this bro 💪 — Go crush it at TCPL!*
