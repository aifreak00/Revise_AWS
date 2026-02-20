# 🧠 TCPL × Decision Point Analytics
## Complete End-to-End Architecture Mindmap
### "What Actually Happens" — Your Field Guide for Day 1

---

```
                        ┌─────────────────────────────────────────┐
                        │     TCPL (Tata Consumer Products)        │
                        │         CLIENT — Your Workplace          │
                        │                                          │
                        │  Tata Tea · Tetley · Himalayan Water     │
                        │  Tata Salt · Tata Sampann · Eight O'Clock│
                        └──────────────────┬──────────────────────┘
                                           │
                        What does TCPL need from data?
                                           │
               ┌───────────────────────────┼────────────────────────┐
               ▼                           ▼                        ▼
     How much product          Which promotions         Where is my
     will sell next            actually worked?         inventory low?
     3 months?                 (Trade Promo ROI)        (Stockouts?)
     (Demand Forecast)         (MMM — Mkt Mix Model)    (Supply Chain)
```

---

## 🌍 THE BIG PICTURE — One Line Per Layer

```
REAL WORLD                 →    SAP / ERP / Nielsen APIs      (where data is born)
         │
         ▼
DATA LANDS IN AWS S3       →    Raw files arrive daily        (data lake = storage)
         │
         ▼
AIRFLOW WAKES UP           →    Apache Airflow (MWAA)         (boss — tells everyone what to do)
         │
         ▼
GLUE CLEANS THE DATA       →    AWS Glue PySpark              (factory — heavy lifting)
         │
         ▼
LAMBDA CHECKS THE DATA     →    AWS Lambda                    (security guard — validates)
         │
         ▼
DATA GOES TO SNOWFLAKE     →    Snowflake DW                  (warehouse — final clean home)
         │
         ▼
ML MODEL RUNS              →    AWS SageMaker                 (brain — forecasts future)
         │
         ▼
PREDICTIONS IN SNOWFLAKE   →    ML outputs stored             (forecast available to business)
         │
         ▼
BUSINESS SEES DASHBOARDS   →    Tableau / PowerBI             (TCPL managers see charts)
```

---

## 📦 LAYER 1 — DATA SOURCES
### "Where does data come from at TCPL?"

```
┌─────────────────────────────────────────────────────────────────┐
│                      DATA SOURCES                                │
├─────────────────┬──────────────────┬───────────────┬────────────┤
│                 │                  │               │            │
│   SAP S4/HANA   │  Nielsen / IRI   │  Trade Promo  │  External  │
│                 │                  │  Portal       │  Sources   │
│ - Sales Orders  │ - Market Share   │               │            │
│ - Inventory     │ - Competitor     │ - Schemes     │ - Weather  │
│ - Purchase Ord  │   pricing        │ - Discounts   │ - Holidays │
│ - Finance data  │ - Volume data    │ - BTL spends  │ - GDP data │
│ - Customer master│ - Distribution  │ - Retailer    │            │
│ - SKU master    │   reach          │   coverage    │            │
│                 │                  │               │            │
│ FORMAT: Parquet │ FORMAT: CSV      │ FORMAT: Excel │ FORMAT: API│
│ FREQ: Daily     │ FREQ: Weekly     │ FREQ: Monthly │ FREQ: Daily│
└────────┬────────┴──────────────────┴───────────────┴────────────┘
         │
         │  How does it get to AWS?
         │
         ├── Batch Export → SFTP → S3 (SAP)
         ├── API Pull → Lambda → S3 (Nielsen)
         └── Manual Upload → S3 (Trade Promos)
```

---

## 🗄️ LAYER 2 — AWS S3 DATA LAKE
### "The big hard disk in the cloud"

```
s3://tcpl-datalake/
│
├── 📂 raw/                     ← UNTOUCHED original files (never modify)
│   ├── sap/
│   │   ├── sales_orders/
│   │   │   └── year=2024/month=01/day=15/    ← PARTITIONED by date
│   │   │       └── sales_20240115.parquet
│   │   ├── inventory/
│   │   └── purchase_orders/
│   ├── nielsen/
│   │   └── market_share/week=2024-W03/
│   └── trade_promotions/
│       └── promo_calendar/
│
├── 📂 curated/                 ← CLEANED data after Glue ETL
│   ├── sales/                  ← Deduplicated, validated, enriched
│   ├── inventory/
│   ├── market_data/
│   └── promotions/
│
├── 📂 ml-features/             ← INPUT to SageMaker (feature engineered)
│   ├── demand_forecast/
│   │   └── lag_features/
│   └── mmm/
│
├── 📂 model-artifacts/         ← OUTPUT from SageMaker (trained models)
│   ├── demand_forecast/
│   │   └── v1.2/model.tar.gz
│   └── predictions/            ← Forecast CSV files
│       └── 2024-01-15/
│
└── 📂 scripts/                 ← Glue + SageMaker code lives here
    ├── glue/
    └── sagemaker/

KEY RULE: raw/ = read only forever. Never overwrite raw data.
```

---

## 🧠 LAYER 3 — APACHE AIRFLOW (MWAA)
### "The Boss. Does nothing itself. Tells everyone else when to run."

```
WHAT IS AIRFLOW?
────────────────
  Think of it like a PROJECT MANAGER.
  It does NOT do any data work.
  It just says: "Hey Glue, start now." / "Hey Lambda, check this."
  If something fails → it retries → if still fails → sends alert

  Runs on AWS MWAA (Managed Workflows for Apache Airflow)
  You access it via a web URL (Airflow UI)

WHAT IS A DAG?
──────────────
  DAG = Directed Acyclic Graph = a PIPELINE defined in Python
  Each box in the pipeline = a TASK
  Tasks have ORDER and DEPENDENCIES

  DAG File Location: s3://tcpl-datalake/dags/

THE MAIN DAG: tcpl_cpg_full_pipeline
─────────────────────────────────────
  Schedule: Every day at 2:00 AM IST
  
  TASK 1 ──► TASK 2 ──► TASK 3 ──► TASK 4 ──► TASK 5
  (Check)    (ETL)      (Validate)  (Snowflake) (ML)


FULL DAG FLOW:
──────────────
  [START 2AM]
       │
       ▼
  ┌─────────────────────────────┐
  │  TASK 1: check_source_files  │  ← Lambda checks S3 for today's files
  │  (Lambda)                    │    "Did SAP files arrive? Nielsen files?"
  └──────────────┬──────────────┘
                 │
        ┌────────┴─────────┐
        │   Files Present? │
        │                  │
       YES                NO
        │                  │
        ▼                  ▼
  Continue             STOP + Alert
                       Slack + Email

       YES path continues...
        │
        ▼
  ┌──────────────────────────────────────────────────┐
  │  TASK 2,3,4: Glue ETL Jobs (run in PARALLEL)     │
  │                                                  │
  │  ┌──────────────┐  ┌──────────────┐  ┌────────┐ │
  │  │ SAP Sales    │  │  Inventory   │  │Nielsen │ │
  │  │ ETL Job      │  │  ETL Job     │  │ETL Job │ │
  │  │ (Glue)       │  │  (Glue)      │  │(Glue)  │ │
  │  └──────────────┘  └──────────────┘  └────────┘ │
  │  All 3 run at the SAME TIME (parallel)           │
  └─────────────────────┬────────────────────────────┘
                        │
                        │ All 3 must finish before next step
                        ▼
  ┌──────────────────────────────────────┐
  │  TASK 5: validate_data_quality       │ ← Lambda checks the cleaned data
  │  (Lambda)                            │   "Are null rates ok? Row count ok?"
  └──────────────────┬───────────────────┘
                     │
           ┌─────────┴──────────┐
           │  Quality Passed?   │
          YES                  NO
           │                    │
           ▼                    ▼
     Continue             STOP + Alert
                          "Bad data! Don't load!"

          YES path continues...
           │
           ▼
  ┌──────────────────────────────────────┐
  │  TASK 6: load_to_snowflake           │ ← Run SQL in Snowflake
  │  (SnowflakeOperator)                 │   Load curated data to DW
  └──────────────────┬───────────────────┘
                     │
                     ▼
  ┌──────────────────────────────────────┐
  │  TASK 7: glue_feature_engineering    │ ← Build ML features from clean data
  │  (Glue)                              │   Lag values, rolling averages, etc.
  └──────────────────┬───────────────────┘
                     │
                     ▼
  ┌──────────────────────────────────────┐
  │  TASK 8: sagemaker_demand_forecast   │ ← Train + predict demand
  │  (SageMaker Pipeline)                │   Takes ~30-60 min
  └──────────────────┬───────────────────┘
                     │
                     ▼
  ┌──────────────────────────────────────┐
  │  TASK 9: store_predictions           │ ← Lambda loads predictions → Snowflake
  │  (Lambda)                            │
  └──────────────────┬───────────────────┘
                     │
                     ▼
  ┌──────────────────────────────────────┐
  │  TASK 10: refresh_data_marts         │ ← Snowflake stored proc
  │  (SnowflakeOperator)                 │   Refresh all BI views
  └──────────────────┬───────────────────┘
                     │
                     ▼
  ┌──────────────────────────────────────┐
  │  TASK 11: pipeline_success_alert     │ ← Slack: ✅ Pipeline done!
  │  (Lambda)                            │
  └──────────────────────────────────────┘
  
  [END ~6AM] — Dashboards ready for business by morning

WHAT YOU'LL DO IN AIRFLOW UI:
──────────────────────────────
  → Monitor DAG runs (green = success, red = failed)
  → Re-trigger failed tasks manually
  → Check logs of each task
  → See how long each task took
```

---

## ⚙️ LAYER 4 — AWS GLUE
### "The Factory. Does all heavy data lifting."

```
WHAT IS GLUE?
─────────────
  Serverless Spark (PySpark) on AWS.
  You write Python/PySpark code.
  AWS handles the servers — you just pay per DPU (compute unit).
  
  Think of it as: "Run PySpark without managing clusters"

GLUE JOB 1: tcpl-glue-sap-sales-transform
──────────────────────────────────────────
  INPUT  → s3://tcpl-datalake/raw/sap/sales_orders/year=2024/month=01/day=15/
  OUTPUT → s3://tcpl-datalake/curated/sales/
         → Snowflake table: CURATED.SALES_ORDERS

  WHAT IT DOES STEP BY STEP:
  
  Step 1: READ raw parquet from S3
           spark.read.parquet("s3://tcpl-datalake/raw/sap/...")
  
  Step 2: VALIDATE schema
           Check required columns exist
           (order_id, sku_code, quantity, net_revenue, etc.)
  
  Step 3: CLEAN
           - Remove duplicates (dropDuplicates on order_id)
           - Fix date formats (to_date)
           - Uppercase SKU codes (TRIM + UPPER)
           - Filter out negative quantities
           - Filter out zero revenue orders
  
  Step 4: ENRICH
           - Calculate revenue_per_unit = net_revenue / quantity
           - Add year, month, quarter columns
           - Flag modern_trade vs general_trade
           - Add processing_timestamp
  
  Step 5: ANOMALY DETECTION
           - 4-week rolling average per SKU per region
           - If revenue > 3x rolling avg → flag as anomaly
           - Business team investigates anomalies manually
  
  Step 6: WRITE to S3 curated (partitioned by year/month)
  
  Step 7: WRITE to Snowflake via JDBC connector


GLUE JOB 2: tcpl-glue-inventory-transform
──────────────────────────────────────────
  Similar process for inventory data
  Calculates: Days of Supply, Stockout flags, Overstock flags


GLUE JOB 3: tcpl-glue-feature-engineering  (ML prep)
──────────────────────────────────────────────────────
  INPUT  → s3://tcpl-datalake/curated/sales/ + promotions/
  OUTPUT → s3://tcpl-datalake/ml-features/demand_forecast/

  WHAT IT DOES:
  
  Aggregates to WEEKLY level per SKU per Region
  
  Creates LAG FEATURES:
  ┌──────────────────────────────────────────────────────────┐
  │  lag_1w_units  = sales from 1 week ago                   │
  │  lag_2w_units  = sales from 2 weeks ago                  │
  │  lag_4w_units  = sales from 4 weeks ago (1 month)        │
  │  lag_8w_units  = sales from 8 weeks ago (2 months)       │
  │  yoy_change    = compared to same week last year          │
  │  rolling_4w_avg = average of last 4 weeks                │
  └──────────────────────────────────────────────────────────┘
  
  Creates SEASONALITY FEATURES:
  ┌──────────────────────────────────────────────────────────┐
  │  is_festive_season  (Diwali, Holi, Puja weeks)           │
  │  is_summer          (April-June)                         │
  │  week_of_year, month, quarter                            │
  └──────────────────────────────────────────────────────────┘
  
  Joins PROMOTION data:
  ┌──────────────────────────────────────────────────────────┐
  │  promo_discount_pct = % discount during that week        │
  │  is_promoted = 1 if promotion was running                │
  └──────────────────────────────────────────────────────────┘

HOW TO MONITOR GLUE:
─────────────────────
  → AWS Console → Glue → Jobs → tcpl-glue-xxx
  → See run history, duration, DPU used
  → CloudWatch logs for errors
  → Airflow task logs also show Glue output
```

---

## 🔔 LAYER 5 — AWS LAMBDA
### "The Lightweight Helper. Quick tasks only."

```
WHAT IS LAMBDA?
───────────────
  Serverless Python function.
  Runs in milliseconds to minutes.
  No servers to manage.
  Triggered by Airflow, S3 events, SNS, or schedule.

  Rule of thumb: If it takes < 15 minutes → Lambda
                 If it takes > 15 minutes → Glue

LAMBDA 1: tcpl-preflight-source-check
──────────────────────────────────────
  WHEN: First task in Airflow DAG (2:00 AM)
  
  DOES:
    - Checks S3 if today's SAP file landed
    - Checks S3 if Nielsen weekly file is present
    - If any file missing → send SNS alert → email + Slack
    - Returns: { "all_sources_available": true/false }
  
  WHY IMPORTANT:
    Without this check, Glue would run on empty folders
    and produce zero records with no error — silent failure!
    This catches it early.


LAMBDA 2: tcpl-data-quality-validator
──────────────────────────────────────
  WHEN: After all Glue ETL jobs finish
  
  DOES:
    - Checks NULL rate (should be < 5%)
    - Checks ROW COUNT (should be > 1000 rows)
    - Checks DUPLICATE rate (should be < 1%)
    - Checks SCHEMA is correct
    - Emits CloudWatch metrics (visible on dashboards)
    - Returns: { "quality_passed": true/false }
  
  AIRFLOW BRANCHES based on result:
    true  → continue to Snowflake load
    false → stop pipeline + alert team


LAMBDA 3: tcpl-slack-alerter
─────────────────────────────
  WHEN: On failure OR on success
  
  DOES:
    - Sends formatted Slack message to #tcpl-data-alerts
    - Includes: date, failed task, error message
    - Severity levels: HIGH (red) / MEDIUM (yellow) / INFO (green)
  
  Example Slack message:
  ┌─────────────────────────────────────────────┐
  │ ⚠️ HIGH ALERT — TCPL Pipeline               │
  │ Date: 2024-01-15                            │
  │ Failed Task: glue_sap_sales_etl             │
  │ Error: Row count = 0 (expected > 5000)      │
  │ Action: Check SAP export for today          │
  └─────────────────────────────────────────────┘


LAMBDA 4: tcpl-predictions-loader
────────────────────────────────────
  WHEN: After SageMaker batch predictions are ready
  
  DOES:
    - Reads prediction CSV from S3
    - MERGES into Snowflake (UPSERT — no duplicates)
    - Adds model_version and load_date
```

---

## 🤖 LAYER 6 — AWS SAGEMAKER
### "The ML Lab. Trains models and predicts the future."

```
WHAT IS SAGEMAKER?
──────────────────
  AWS managed ML platform.
  You define a PIPELINE (series of ML steps).
  AWS handles compute — spins up ml.m5.xlarge etc.
  
  At TCPL, SageMaker runs DEMAND FORECASTING.

USE CASE:
─────────
  QUESTION: "How many units of Tata Tea Premium 500g
             will sell in Maharashtra in the next 90 days?"
  
  ANSWER: SageMaker XGBoost model gives weekly predictions
          with confidence intervals (lower/upper bounds)

THE SAGEMAKER PIPELINE STEPS:
──────────────────────────────

  STEP 1: PREPROCESSING
  ─────────────────────
  Input:  s3://tcpl-datalake/ml-features/demand_forecast/
  Output: train (70%) / validation (20%) / test (10%) splits
  Code:   SKLearnProcessor runs preprocess_demand.py
  
  What happens:
  - Normalize features (scale 0 to 1)
  - Handle remaining nulls (fill with median)
  - Encode categorical variables (brand, region → numbers)
  - Create final feature matrix for XGBoost


  STEP 2: TRAINING
  ────────────────
  Algorithm: XGBoost (gradient boosted trees)
  Instance:  ml.m5.2xlarge (8 cores, 32GB RAM)
  
  Key Hyperparameters:
  ┌──────────────────────────────────┐
  │  max_depth       = 8             │ ← tree depth
  │  eta             = 0.1           │ ← learning rate
  │  num_round       = 500           │ ← training iterations
  │  early_stopping  = 20            │ ← stop if no improvement
  │  objective       = reg:squarederror │ ← regression task
  └──────────────────────────────────┘
  
  Output: model.tar.gz saved to S3


  STEP 3: EVALUATION
  ───────────────────
  Runs on TEST data (never seen during training)
  
  Metric: MAPE (Mean Absolute Percentage Error)
  
  MAPE = average of |actual - predicted| / actual × 100
  
  Example:
  ┌────────────────────────────────────────────────────┐
  │  Actual: 1000 units                                │
  │  Predicted: 950 units                              │
  │  Error: |1000-950|/1000 = 5% MAPE                 │
  │                                                    │
  │  Target: MAPE < 15% (if >= 15% → model rejected)  │
  └────────────────────────────────────────────────────┘


  STEP 4: QUALITY GATE
  ─────────────────────
  IF MAPE < 15% → register model + proceed to predictions
  IF MAPE >= 15% → FAIL STEP fires → pipeline stops
                    Alert sent → data science team investigates


  STEP 5: MODEL REGISTRY
  ───────────────────────
  Good models get registered in SageMaker Model Registry
  Status: "PendingManualApproval"
  Data Science lead approves → moves to "Approved"
  Only Approved models go to production


  STEP 6: BATCH TRANSFORM (Predictions)
  ──────────────────────────────────────
  Runs the approved model on ALL SKU-Region combinations
  Generates 90-day weekly forecast for each
  
  Output format (CSV):
  ┌──────────────────────────────────────────────────────────┐
  │ sku_code │ region │ week       │ predicted │ lower │ upper│
  │ TTP-500  │ MH     │ 2024-01-22 │ 15200     │ 13800 │16600 │
  │ TTP-500  │ MH     │ 2024-01-29 │ 15800     │ 14200 │17400 │
  │ TTP-500  │ MH     │ 2024-02-05 │ 14900     │ 13500 │16300 │
  │ ...      │ ...    │ ...        │ ...       │ ...   │ ...  │
  └──────────────────────────────────────────────────────────┘


  STEP 7: MODEL MONITOR (ongoing)
  ────────────────────────────────
  Watches LIVE predictions every hour
  Detects DATA DRIFT (is today's data very different from training data?)
  Alerts if drift detected → model may need retraining
```

---

## 🏢 LAYER 7 — SNOWFLAKE DATA WAREHOUSE
### "The Final Clean Home. Where business users live."

```
WHAT IS SNOWFLAKE?
──────────────────
  Cloud Data Warehouse.
  SQL-based (you write SELECT, JOIN, GROUP BY etc.)
  Separates storage and compute (scales independently).
  Very fast for analytics queries.

SNOWFLAKE ARCHITECTURE AT TCPL:
────────────────────────────────

  DATABASE: TCPL_CPG_DB
  │
  ├── SCHEMA: RAW           ← Direct landing from Glue (staging)
  ├── SCHEMA: CURATED        ← Cleaned, validated data
  ├── SCHEMA: DW             ← Star schema (facts + dimensions)
  ├── SCHEMA: MARTS          ← Subject-area views for BI teams
  ├── SCHEMA: ML_OUTPUTS     ← SageMaker predictions
  └── SCHEMA: MONITORING     ← Pipeline health logs

WAREHOUSES (compute clusters):
────────────────────────────────
  ┌────────────────────────────────────────────────────┐
  │  TCPL_LOAD_WH        ← Glue + Lambda loads data   │
  │  (MEDIUM, auto-suspend 2 min)                      │
  ├────────────────────────────────────────────────────┤
  │  TCPL_TRANSFORM_WH   ← dbt models + stored procs  │
  │  (LARGE, auto-suspend 5 min)                       │
  ├────────────────────────────────────────────────────┤
  │  TCPL_ANALYTICS_WH   ← Tableau / analyst queries  │
  │  (SMALL, auto-suspend 1 min)                       │
  └────────────────────────────────────────────────────┘
  
  WHY SEPARATE WAREHOUSES?
  So analyst queries don't slow down ETL loads
  And ETL loads don't slow down dashboards

STAR SCHEMA:
────────────

         DIM_DATE
             │
             │
  DIM_SKU ──────────── FACT_SALES ──────────── DIM_CUSTOMER
  (product info)  │    (numbers: qty,         (who bought)
                  │     revenue, margin)
             DIM_GEOGRAPHY
             (where: state, zone)

  DIM = dimension table = WHO, WHAT, WHERE, WHEN (descriptive)
  FACT = fact table = the actual NUMBERS (quantity, revenue)

  Example query the business runs:
  ─────────────────────────────────
  "Show me Tata Tea Premium monthly revenue in Maharashtra
   for last 12 months compared to forecast"

  SELECT
    d.month_name,
    sk.sku_name,
    g.state,
    SUM(f.net_revenue) AS actual_revenue,
    AVG(p.predicted_units * sk.mrp) AS forecast_revenue
  FROM FACT_SALES f
  JOIN DIM_DATE d ON f.date_key = d.date_key
  JOIN DIM_SKU sk ON f.sku_key = sk.sku_key
  JOIN DIM_GEOGRAPHY g ON f.geo_key = g.geo_key
  JOIN ML_OUTPUTS.DEMAND_FORECAST_PREDICTIONS p
    ON p.sku_code = sk.sku_code AND p.region_code = g.region_code
  WHERE sk.sku_name = 'Tata Tea Premium 500g'
    AND g.state = 'Maharashtra'
    AND d.fiscal_year = 2024
  GROUP BY 1,2,3
  ORDER BY d.month;

DATA FLOW INTO SNOWFLAKE:
──────────────────────────
  Glue     ──► writes via Snowflake Spark connector
  Lambda   ──► writes via snowflake-connector-python (MERGE)
  Airflow  ──► runs SQL via SnowflakeOperator
  SageMaker──► predictions via Lambda → Snowflake MERGE
```

---

## 📊 LAYER 8 — BI / DASHBOARDS
### "What TCPL business managers actually see every morning"

```
┌──────────────────────────────────────────────────────────────┐
│                    TABLEAU / POWER BI                         │
│               (connects to Snowflake MARTS schema)            │
└───────────────────────────────┬──────────────────────────────┘
                                │
             ┌──────────────────┼──────────────────┐
             ▼                  ▼                  ▼
    ┌─────────────────┐ ┌──────────────────┐ ┌─────────────────┐
    │  Sales          │ │  Demand Forecast  │ │  Trade Promo    │
    │  Performance    │ │  Dashboard        │ │  Effectiveness  │
    │  Dashboard      │ │                  │ │  Dashboard      │
    │                 │ │ - Next 90 day     │ │                 │
    │ - Daily sales   │ │   units forecast  │ │ - Which promos  │
    │ - Brand-wise    │ │ - Confidence      │ │   drove sales?  │
    │ - Region-wise   │ │   intervals       │ │ - ROI per       │
    │ - Channel-wise  │ │ - Actual vs       │ │   scheme        │
    │ - YoY growth    │ │   forecast MAPE   │ │ - Region-wise   │
    │ - Anomaly flags │ │ - Stockout risk   │ │   effectiveness │
    └─────────────────┘ └──────────────────┘ └─────────────────┘
    
    WHO USES WHAT:
    ──────────────
    Sales Head          → Sales Performance Dashboard
    Demand Planner      → Forecast Dashboard (daily)
    Trade Marketing     → Promo Effectiveness Dashboard
    Supply Chain Team   → Inventory + Stockout Dashboard
    CFO / Leadership    → Executive Summary Dashboard
```

---

## 🚨 LAYER 9 — ERROR HANDLING & MONITORING
### "What happens when things break (they will)"

```
MONITORING STACK:
──────────────────

  Airflow UI ──────────────────► See task status (green/red)
                                 Re-run failed tasks
                                 View logs per task

  CloudWatch Dashboards ───────► Pipeline KPIs in real-time
                                 Glue DPU usage
                                 Lambda errors
                                 SageMaker training metrics

  CloudWatch Alarms ───────────► Trigger SNS when:
                                 - Null rate > 5%
                                 - Row count = 0
                                 - Glue job runs > 2 hours
                                 - Model drift detected

  SNS → Slack/Email ───────────► #tcpl-data-alerts Slack channel
                                 data-ops@decisionpoint.ai

COMMON FAILURES YOU'LL FACE:
──────────────────────────────

  PROBLEM 1: SAP file didn't arrive on time
  ─────────────────────────────────────────
  Symptom: Pre-flight Lambda fails
  Slack:   "SAP sales file missing for 2024-01-15"
  Action:  Check with TCPL SAP team → manual trigger once file arrives
  Fix:     Airflow → re-trigger from Task 1

  PROBLEM 2: Glue job fails with "NullPointerException"
  ──────────────────────────────────────────────────────
  Symptom: glue_sap_sales_etl task turns red
  Cause:   Schema change in SAP export (new column added/removed)
  Action:  Check Glue CloudWatch logs
           Update schema validation in Glue script
  Fix:     Redeploy Glue script → re-trigger from Task 2

  PROBLEM 3: Row count drops 90% suddenly
  ─────────────────────────────────────────
  Symptom: Data quality Lambda flags failure
  Cause:   SAP partial extract / network issue during export
  Action:  Check raw S3 file size → compare to yesterday
           Contact SAP team for re-extract
  Fix:     Once re-extract done → re-trigger full pipeline

  PROBLEM 4: SageMaker MAPE > 15%
  ─────────────────────────────────
  Symptom: FailStep fires in SageMaker pipeline
  Cause:   Model drift — real data changed from training data
  Action:  Retrain with more recent data
           Check if major business event happened (new promo, COVID etc.)
  Fix:     Data Science team retrains → update model version
```

---

## 🗓️ DAILY TIMELINE — What Happens Every Day

```
TIME (IST)    EVENT
──────────────────────────────────────────────────────────────────
 11:00 PM     SAP batch export job runs at TCPL
              Data extracted from SAP S4/HANA → SFTP server

 12:00 AM     SFTP → S3 transfer job runs
              Files land in s3://tcpl-datalake/raw/sap/

 01:00 AM     Nielsen weekly data arrives (Mondays only)
              Trade promo file arrives (monthly)

 02:00 AM  ★  AIRFLOW DAG STARTS
              Task 1: Pre-flight check (Lambda)

 02:05 AM     Glue ETL jobs start in parallel
              Sales + Inventory + Market ETL

 03:30 AM     Glue jobs finish
              Task 5: Data quality Lambda runs

 03:35 AM     Snowflake load begins (Task 6)
              Stored procedure: LOAD_CURATED_DATA

 04:00 AM     Feature engineering Glue job starts (Task 7)

 04:30 AM     SageMaker demand forecast pipeline starts (Task 8)

 05:45 AM     SageMaker finishes predictions
              Lambda loads to Snowflake (Task 9)

 06:00 AM  ★  Snowflake marts refreshed (Task 10)
              Dashboards ready

 06:05 AM     Slack: ✅ "TCPL pipeline completed for 2024-01-15"

 09:00 AM     Business users log into Tableau / PowerBI
              See fresh data from last night's pipeline
```

---

## 📁 WHERE CODE LIVES — Repository Structure

```
tcpl-cpg-platform/
│
├── 📂 dags/                           ← Airflow DAGs (Python)
│   ├── tcpl_cpg_full_pipeline_dag.py  ← Main daily pipeline
│   ├── tcpl_weekly_nielsen_dag.py     ← Weekly Nielsen processing
│   └── tcpl_monthly_mmm_dag.py        ← Monthly MMM model
│
├── 📂 glue/                           ← Glue PySpark scripts
│   ├── sap_sales_transform.py
│   ├── inventory_transform.py
│   ├── nielsen_transform.py
│   └── feature_engineering.py
│
├── 📂 lambdas/                        ← Lambda functions
│   ├── preflight_check/
│   │   └── handler.py
│   ├── data_quality/
│   │   └── handler.py
│   ├── predictions_loader/
│   │   └── handler.py
│   └── slack_alerter/
│       └── handler.py
│
├── 📂 sagemaker/                      ← ML code
│   ├── pipelines/
│   │   └── demand_forecast_pipeline.py
│   ├── scripts/
│   │   ├── preprocess_demand.py
│   │   └── evaluate_demand.py
│   └── notebooks/
│       └── demand_forecast_eda.ipynb
│
├── 📂 snowflake/                      ← SQL
│   ├── ddl/
│   │   ├── create_dim_tables.sql
│   │   ├── create_fact_tables.sql
│   │   └── create_ml_outputs.sql
│   ├── procedures/
│   │   ├── load_curated_data.sql
│   │   └── refresh_marts.sql
│   └── views/
│       ├── sales_performance_mart.sql
│       └── forecast_accuracy_mart.sql
│
├── 📂 terraform/                      ← Infrastructure as Code
│   ├── main.tf
│   ├── s3.tf
│   ├── glue.tf
│   ├── lambda.tf
│   ├── mwaa.tf
│   └── variables.tf
│
└── 📂 tests/                          ← Unit + integration tests
    ├── test_glue_transforms.py
    └── test_lambda_handlers.py
```

---

## 🔐 SECURITY & ACCESS

```
WHO ACCESSES WHAT:
──────────────────

  Decision Point Engineers ──► AWS Console (Glue, Lambda, S3, SageMaker)
                               Airflow UI (MWAA)
                               Snowflake (all schemas)
                               Git repository
  
  TCPL Data Team     ──────► Airflow UI (monitor only)
                               Snowflake (MARTS schema only)
                               Tableau / PowerBI
  
  TCPL Business Users ─────► Tableau / PowerBI only
                               NO direct Snowflake access

  IAM ROLES:
  ──────────
  TCPLGlueRole      → Read S3 raw, Write S3 curated, Write Snowflake
  TCPLLambdaRole    → Read/Write S3, Invoke SNS, Write CloudWatch
  TCPLSageMakerRole → Read S3 features, Write S3 artifacts
  TCPLMWAARole      → Trigger Glue, Invoke Lambda, Trigger SageMaker

  SECRETS MANAGER:
  ────────────────
  tcpl-snowflake-credentials  ← Snowflake user/password
  tcpl-nielsen-api-key        ← Nielsen API key
  tcpl-sap-sftp-key           ← SAP SFTP credentials
  (Never hardcode credentials in code!)
```

---

## 💡 KEY CONCEPTS TO REMEMBER

```
CONCEPT               WHAT IT MEANS IN SIMPLE WORDS
──────────────────────────────────────────────────────────────────
Partitioning     →   Organizing S3 files by date folder
                     year=2024/month=01/day=15/
                     Makes queries 100x faster

DPU              →   Glue compute unit (like 1 worker)
                     More DPUs = faster job = more cost
                     Typical TCPL jobs: 4-10 DPUs

MAPE             →   How wrong is the forecast in %?
                     MAPE = 10% means predictions are 10% off on avg
                     Target: < 15% at TCPL

DAG              →   Your pipeline in Airflow (Python file)
                     Has tasks + order + schedule

MERGE (UPSERT)   →   INSERT if new, UPDATE if already exists
                     Used for loading predictions to Snowflake
                     Prevents duplicate records

Data Drift       →   When real-world data changes pattern
                     vs what the model was trained on
                     SageMaker Monitor detects this

Star Schema      →   Data warehouse design pattern
                     FACT table in center (numbers)
                     DIM tables around it (descriptions)

Auto-suspend     →   Snowflake WH shuts down when idle
                     Saves cost — resumes automatically on query

Feature Store    →   ML-ready dataset with engineered features
                     Lives in s3://tcpl-datalake/ml-features/
```

---

## 🚀 YOUR FIRST WEEK AT TCPL — What To Do

```
DAY 1 — ORIENTATION
────────────────────
  ✅ Get AWS Console access (ap-south-1 region)
  ✅ Get Airflow UI URL and login
  ✅ Get Snowflake account URL and login
  ✅ Get Git repo access
  ✅ Look at Airflow — find the tcpl_cpg_full_pipeline DAG
  ✅ Watch one full pipeline run (green tasks = success)

DAY 2 — EXPLORE THE DATA
──────────────────────────
  ✅ Open Snowflake → TCPL_CPG_DB
  ✅ SELECT * FROM DW.FACT_SALES LIMIT 100
  ✅ SELECT * FROM ML_OUTPUTS.DEMAND_FORECAST_PREDICTIONS LIMIT 50
  ✅ Look at DIM tables — understand SKU master, region master
  ✅ Run the sales performance mart view query

DAY 3 — UNDERSTAND THE CODE
─────────────────────────────
  ✅ Open git repo → read dags/tcpl_cpg_full_pipeline_dag.py
  ✅ Read glue/sap_sales_transform.py — understand each step
  ✅ Look at lambdas/data_quality/handler.py
  ✅ Find a recent Glue job in AWS Console → read its CloudWatch logs

DAY 4 — SHADOW A PIPELINE RUN
──────────────────────────────
  ✅ Stay online during 2AM run (or check logs next morning)
  ✅ Open Airflow → see all tasks go green one by one
  ✅ Check CloudWatch → see DQ metrics update
  ✅ Check Snowflake → verify new rows loaded for today's date

DAY 5 — FIRST SMALL TASK
──────────────────────────
  Common first tasks assigned:
  - Add a new column to Glue ETL script
  - Write a new Snowflake view for a business request
  - Update a CloudWatch alarm threshold
  - Add a new DAG task for a new data source
```

---

## 🧩 QUICK REFERENCE CARD

```
┌──────────────┬──────────────────────────────────┬──────────────────────────┐
│  COMPONENT   │  WHERE TO FIND IT                │  WHAT YOU DO THERE       │
├──────────────┼──────────────────────────────────┼──────────────────────────┤
│ Airflow      │ MWAA → Airflow UI URL             │ Monitor, re-trigger DAGs │
│ Glue         │ AWS Console → Glue → Jobs         │ Run/debug ETL jobs       │
│ Lambda       │ AWS Console → Lambda → Functions  │ Edit, test functions     │
│ SageMaker    │ AWS Console → SageMaker → Studio  │ Monitor pipelines        │
│ S3           │ AWS Console → S3 → tcpl-datalake  │ Check files landed       │
│ Snowflake    │ Snowflake URL (SnowSight UI)       │ SQL queries, data check  │
│ CloudWatch   │ AWS Console → CloudWatch          │ Logs, metrics, alarms    │
│ Secrets Mgr  │ AWS Console → Secrets Manager     │ Credentials (read-only)  │
│ Git          │ GitHub / CodeCommit repo           │ All code changes         │
├──────────────┼──────────────────────────────────┼──────────────────────────┤
│ REGION       │ AWS ap-south-1 (Mumbai)            │ Always check region!     │
│ SCHEDULE     │ Pipeline: 2AM IST daily            │                          │
│ ALERTS       │ Slack: #tcpl-data-alerts           │                          │
│ CONTACT      │ TCPL SAP team for file issues      │                          │
└──────────────┴──────────────────────────────────┴──────────────────────────┘
```

---

*You've got this bro 💪 — The stack is AWS Glue + MWAA + Lambda + SageMaker + Snowflake.
Airflow is the brain. Glue is the factory. Lambda is the helper. SageMaker is the ML lab. Snowflake is the warehouse.*

*Read the Airflow DAG first on Day 1 — that tells you the ENTIRE pipeline story in one file.*
