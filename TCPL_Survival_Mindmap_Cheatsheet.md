# 🛡️ TCPL — Your Ultimate Survival Mindmap & Cheatsheet
## "Open This Every Single Day Before You Start Work"
### Decision Point Analytics × TCPL | Your Personal Field Guide

---

> 💬 **Bro, read this first:**
> Fear = you care about doing good work. That's actually a good sign.
> Everyone feels this on Day 1. The engineers who built this pipeline
> also didn't know it on Day 1. You will learn it faster than you think.
> This cheatsheet = your safety net. Open it every morning.

---

## 🌅 MORNING ROUTINE — First 15 Minutes Every Day

```
When you sit down at your desk, do THIS in order:

  ┌─────────────────────────────────────────────────────────────┐
  │  STEP 1 (2 min): Open Slack → check #tcpl-data-alerts       │
  │  → Any red alerts? Any pipeline failures overnight?         │
  │  → If yes → go to STEP 2 immediately                        │
  │  → If no (green) → continue to STEP 3                       │
  └─────────────────────────────────────────────────────────────┘
                           │
                           ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  STEP 2 (5 min): Open Airflow UI                            │
  │  → Look at tcpl_master_daily_pipeline                       │
  │  → All tasks green? ✅ You're good                          │
  │  → Any task red? → Click it → View Log → understand error  │
  │  → Don't panic. Check the FAILURE PLAYBOOK below           │
  └─────────────────────────────────────────────────────────────┘
                           │
                           ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  STEP 3 (3 min): Open Snowflake → quick row count check     │
  │  Run this SQL every morning:                                │
  │                                                             │
  │  SELECT load_date, COUNT(*) AS rows                         │
  │  FROM SALES_DM.FACT_UNIFIED_SALES                           │
  │  WHERE load_date >= CURRENT_DATE - 3                        │
  │  GROUP BY 1 ORDER BY 1 DESC;                                │
  │                                                             │
  │  → If today's date is missing → pipeline didn't finish      │
  │  → If row count is 90% lower than yesterday → problem       │
  │  → If looks normal → you're good ✅                         │
  └─────────────────────────────────────────────────────────────┘
                           │
                           ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  STEP 4 (5 min): Check CloudWatch                           │
  │  → AWS Console → CloudWatch → Dashboards → TCPL-Pipeline   │
  │  → Null rate < 5%? ✅                                       │
  │  → Row count normal? ✅                                     │
  │  → Glue job durations normal? ✅                            │
  └─────────────────────────────────────────────────────────────┘

  ✅ If all 4 steps are green → your morning health check is DONE.
  ☕ Get your chai and start your actual work.
```

---

## 🚨 FAILURE PLAYBOOK — What To Do When Things Break

### FAILURE TYPE 1: "SAP / Botree files didn't arrive"

```
SYMPTOMS:
  Airflow: S3KeySensor task stuck (yellow / waiting)
  Slack:   "Source files MISSING for 2024-01-15"

YOUR ACTION STEPS:
  Step 1: Check S3 manually
          AWS Console → S3 → tcpl-datalake → raw → sap → sales_orders
          → Is today's folder there?
          
  Step 2: If folder MISSING → message your senior/lead
          "SAP sales file has not arrived for [date].
           S3 path: raw/sap/sales_orders/year=X/month=X/day=X
           Should I contact the TCPL SAP team?"
          
  Step 3: Senior will contact TCPL SAP BASIS team
          They will re-trigger the SAP export
          
  Step 4: Once file arrives → Airflow sensor detects it automatically
          Pipeline resumes on its own
          
  Step 5: If past 7 AM and still missing → escalate to manager
  
  DO NOT: Try to create fake files or skip the step yourself
  DO NOT: Touch anything in the raw/ folder
```

### FAILURE TYPE 2: "Glue job failed"

```
SYMPTOMS:
  Airflow: glue_sap_raw_to_processed task is RED
  Email:   "Task Failed: glue_sap_raw_to_processed"

YOUR ACTION STEPS:
  Step 1: Click the red task in Airflow → View Log
          Read the last 20 lines of the log
          
  Step 2: Find the ERROR line. Common errors:
  
  ERROR A: "AnalysisException: cannot resolve column 'xyz'"
  ──────────────────────────────────────────────────────
  MEANING: SAP added/removed a column in today's export
  FIX:     Update the Glue script column list
           Find file: glue/sap_raw_to_processed_sales.py
           Add or remove the column
           Re-run the task in Airflow (Clear → Run)
  
  ERROR B: "Container killed by YARN for exceeding memory limits"
  ─────────────────────────────────────────────────────────────
  MEANING: Data volume too large for current DPU setting
  FIX:     Go to AWS Glue → Jobs → find the job
           Increase DPUs from 4 → 8 (or 8 → 16)
           Re-run the task in Airflow
  
  ERROR C: "S3 NoSuchKey" or "Path does not exist"
  ─────────────────────────────────────────────────
  MEANING: The input file path is wrong or file is missing
  FIX:     Check S3 path in Glue script matches actual S3 path
           Usually means the source file didn't arrive (→ FAILURE TYPE 1)
  
  ERROR D: "Snowflake JDBC connection failed"
  ──────────────────────────────────────────
  MEANING: Snowflake credentials expired or network issue
  FIX:     Check Secrets Manager → tcpl-snowflake-credentials
           Rotate password if expired
           OR check Snowflake is not in maintenance window

  Step 3: After fix → In Airflow → Click failed task → Clear → Confirm
          This re-runs ONLY the failed task (not full pipeline)
          
  Step 4: Tell your senior what failed and what you did
          "glue_sap_raw_to_processed failed due to OOM.
           Increased DPUs from 4→8. Re-running now."
```

### FAILURE TYPE 3: "Data quality check failed"

```
SYMPTOMS:
  Airflow: validate_data_quality task RED
  Lambda returned: { "quality_passed": false }
  Pipeline STOPPED (branched to alert task)

YOUR ACTION STEPS:
  Step 1: Check Lambda logs in CloudWatch
          AWS Console → Lambda → tcpl-data-quality-validator
          → Monitor → Logs → Latest log stream
          Find: which check failed (null rate? row count? duplicates?)
  
  Step 2: Common root causes:
  
  NULL RATE HIGH (>5%):
    → SAP partial extract (some columns blank)
    → New column added in SAP not mapped in Glue script
    Action: Check raw file manually → s3 cp s3://... /tmp/ → open file
    
  ROW COUNT TOO LOW:
    → SAP sent only partial data (network issue mid-transfer)
    → Date filter in SnapLogic went wrong
    Action: Check raw file row count vs yesterday
    
  DUPLICATES HIGH:
    → SnapLogic ran twice (re-trigger issue)
    → Dedup logic in Glue not working for new data pattern
    Action: Check if SnapLogic ran multiple times today

  Step 3: DO NOT manually force-bypass the quality check
          Quality gate exists to protect Snowflake from bad data
          
  Step 4: Tell your senior the specific quality failure
          They will decide whether to skip for today or wait for re-extract
```

### FAILURE TYPE 4: "Snowflake load failed"

```
SYMPTOMS:
  Airflow: load_snowflake_core_layer task RED
  Error in log: SQL compilation error / timeout

YOUR ACTION STEPS:
  Step 1: Open Snowflake → Activity → Query History
          Find the failed query → Check error message
  
  Step 2: Common errors:
  
  "Numeric value out of range"
  → A revenue value in SAP is too large for the column data type
  → FIX: ALTER TABLE to increase decimal precision
  
  "Unique constraint violation"
  → Duplicate records trying to insert
  → FIX: Check MERGE ON clause in stored procedure — might need new key
  
  "Warehouse suspended, query queued"
  → Warehouse auto-suspended and took too long to resume
  → FIX: ALTER WAREHOUSE TCPL_LOAD_WH SET AUTO_SUSPEND = 600;
  
  Step 3: Fix → re-run stored procedure manually in Snowflake first
          CALL TCPL_CPG_DB.PROCEDURES.LOAD_SAP_CORE('2024-01-15');
          If it works → then re-trigger Airflow task
```

### FAILURE TYPE 5: "MDM join producing NULLs"

```
SYMPTOMS:
  Snowflake query shows NULL golden_sku_code in SALES_DM
  Row counts in SALES_DM lower than expected
  Dashboard showing unknown/blank SKUs

YOUR ACTION STEPS:
  Step 1: Find which SKUs are missing from MDM
  
  SELECT DISTINCT l.matnr AS sap_sku_code, COUNT(*) AS affected_rows
  FROM SAP_DB.CORE.VBAP l
  LEFT JOIN MDM_DB.CORE.SKU_MASTER m ON l.matnr = m.sap_matnr
  WHERE m.golden_sku_code IS NULL
  GROUP BY 1
  ORDER BY 2 DESC;
  
  Step 2: Note down the SAP material codes that are missing
  
  Step 3: Raise with TCPL MDM team:
          "These SAP material codes are not registered in UNIFY MDM:
           [list of codes]. Can you please add them?
           They are causing NULL records in SALES_DM."
  
  Step 4: Temporary fix (until MDM team adds them):
          Add a fallback in Glue script:
          .withColumn("golden_sku_code",
              F.coalesce("golden_sku_code", "sap_matnr"))
          This uses SAP code as fallback — not perfect but stops data loss
  
  Step 5: Once MDM team adds them → re-run the MDM join Glue job
          for that date → data will get correct golden codes
```

---

## 💬 COMMUNICATION CHEATSHEET
### "What to say in every situation"

```
SITUATION 1: Someone asks "What's your pipeline status?"
────────────────────────────────────────────────────────
SAY: "Pipeline ran clean last night. All 3 sources (SAP, Botree, Salesforce)
     landed on time. Snowflake DMs refreshed by 6 AM. No quality flags."

OR if issue:
SAY: "We had a Glue job failure at 3 AM — SAP added a new column.
     I've updated the script and re-ran it. Data is in Snowflake now.
     Delayed by about 45 minutes but complete."


SITUATION 2: TCPL business user says "Dashboard is showing wrong numbers"
────────────────────────────────────────────────────────────────────────
SAY: "Let me check right away. Can you tell me which dashboard,
     which metric, and which date range looks wrong?
     I'll trace it from the source and come back to you in 30 minutes."

Then do:
  1. Check SALES_DM row counts for that date
  2. Check if Glue job ran for that date (Airflow history)
  3. Run the SQL directly in Snowflake and compare with what dashboard shows
  4. If data is correct in Snowflake → ThoughtSpot/PowerBI caching issue
  5. If data wrong in Snowflake → trace back to Glue → S3 → source


SITUATION 3: Senior asks "Can you add a new column to the sales pipeline?"
──────────────────────────────────────────────────────────────────────────
SAY: "Sure. I need to:
     1. Check if the column exists in the SAP raw extract
     2. Add it to the Glue raw→processed script
     3. Add it through to curated zone
     4. Add the column to the Snowflake table (ALTER TABLE)
     5. Add it to the SALES_DM SQL transform
     Should I start with checking if it's in the raw data first?"


SITUATION 4: You are STUCK and don't know what to do
──────────────────────────────────────────────────────
SAY (to your senior/lead): "I'm looking at this error in the Glue job:
[paste the error]. I've checked [what you checked] and I think
the issue might be [your best guess], but I'm not 100% sure.
Can you help me understand what to look at next?"

RULE: Never sit stuck for more than 30 minutes. Always ask.
Asking questions = smart. Sitting stuck in silence = not helpful.


SITUATION 5: TCPL stakeholder asks something you don't know
────────────────────────────────────────────────────────────
SAY: "That's a good question. Let me verify and come back to you
     within the hour with the accurate answer."

NEVER guess/make up answers to business stakeholders.
They will trust you MORE for saying "let me verify" than
for giving a confident wrong answer.
```

---

## 🗺️ SYSTEM ACCESS MAP — Where to Go for What

```
┌────────────────────────────────────────────────────────────────────────┐
│  WHEN YOU NEED TO...              GO TO...                              │
├───────────────────────────────────┬────────────────────────────────────┤
│  See if pipeline ran successfully  │ Airflow UI → DAGs → master daily   │
│  Re-run a failed task              │ Airflow → Task → Clear → Confirm   │
│  See WHY a task failed             │ Airflow → Task → Log               │
│  Check if S3 files arrived         │ AWS Console → S3 → tcpl-datalake   │
│  Debug a Glue job                  │ AWS Console → Glue → Jobs → Runs   │
│  See Glue detailed logs            │ CloudWatch → Log Groups → Glue     │
│  Check Lambda execution            │ AWS Console → Lambda → Monitor     │
│  Query / fix Snowflake data        │ Snowflake SnowSight UI             │
│  Check SageMaker model runs        │ AWS Console → SageMaker → Pipelines│
│  See CloudWatch metrics/alarms     │ AWS Console → CloudWatch           │
│  Get credentials / secrets         │ AWS Secrets Manager                │
│  Make code changes                 │ Git repository (GitHub/CodeCommit) │
│  Deploy infrastructure changes     │ Terraform (with senior approval)   │
│  Send pipeline alerts              │ Auto via Slack #tcpl-data-alerts   │
├───────────────────────────────────┴────────────────────────────────────┤
│  AWS REGION: ap-south-1 (Mumbai) ← ALWAYS CHECK THIS                   │
│  SNOWFLAKE ROLE: TCPL_ETL_ROLE (for ETL work)                          │
│  SNOWFLAKE WAREHOUSE: TCPL_LOAD_WH (for loading)                       │
│                       TCPL_ANALYTICS_WH (for querying)                  │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 🧠 ARCHITECTURE ONE-PAGER — The Full Picture in 30 Seconds

```
  8 SOURCES                INGEST              STORE (S3)
  ───────────              ───────             ──────────
  SAP S4/HANA    ──►                           Raw (CSV)
  Salesforce CRM ──►  SnapLogic      ──────►   Processed (Parquet)
  Botree DMS     ──►  (CDC+FullLoad)           Refined (Parquet)
  SuccessFactors ──►                           Curated (Parquet)
  PEGASUS        ──►  AWS Lambda     ──────►   (SharePoint files)
  Infiniti       ──►  (SharePoint→S3)
  Arkieva        ──►  Direct S3/API  ──────►   (Flat files)
  UNIFY MDM      ──►

         │ Airflow (MWAA) 250+ DAGs orchestrates everything below │

  PROCESS                  LOAD                SERVE
  ───────                  ────                ─────
  AWS Glue     ──────────► Snowflake          ThoughtSpot (70%)
  (134 jobs)               CORE Layer:        Power BI (30%)
  PySpark ETL              SAP_DB             SageMaker ML
  Raw→Processed            BOTREE_DB          External (EY/Mavic)
  Processed→Refined        SFDC_DB
  Refined→Curated          PEGASUS_DB    ──► BR/DM Layer:
  MDM joins                MDM_DB             SALES_DM
                                              FINANCE_DM
  SageMaker ML  ──────────────────────────►  OPS_DM
  Demand Forecast                             HR_DM
  MMM, TPO                  ML_OUTPUTS ◄────  ESG_DM
  Anomaly Detection         (predictions
                             written back)
```

---

## 📊 SNOWFLAKE DAILY SQL CHEATSHEET
### "Queries you'll run almost every day"

```sql
-- ══════════════════════════════════════════════════════════════
-- 1. MORNING HEALTH CHECK — Did today's data load?
-- ══════════════════════════════════════════════════════════════
SELECT
    load_date,
    sale_type,
    COUNT(*)          AS row_count,
    SUM(units_sold)   AS total_units,
    SUM(net_revenue)  AS total_revenue
FROM SALES_DM.FACT_UNIFIED_SALES
WHERE load_date >= CURRENT_DATE - 3
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
-- Expected: ~same row count as yesterday ±20%


-- ══════════════════════════════════════════════════════════════
-- 2. CHECK MDM NULLS — Are any SKUs missing from MDM?
-- ══════════════════════════════════════════════════════════════
SELECT
    load_date,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN golden_sku_code IS NULL THEN 1 ELSE 0 END) AS null_sku_rows,
    ROUND(SUM(CASE WHEN golden_sku_code IS NULL THEN 1 ELSE 0 END)
          / COUNT(*) * 100, 2) AS null_pct
FROM SALES_DM.FACT_UNIFIED_SALES
WHERE load_date = CURRENT_DATE
GROUP BY 1;
-- Expected: null_pct < 1%


-- ══════════════════════════════════════════════════════════════
-- 3. PRIMARY vs SECONDARY SALES GAP — The most watched metric
-- ══════════════════════════════════════════════════════════════
SELECT
    sale_date,
    brand,
    zone,
    SUM(CASE WHEN sale_type='PRIMARY' THEN units_sold ELSE 0 END)   AS primary_units,
    SUM(CASE WHEN sale_type='SECONDARY' THEN units_sold ELSE 0 END) AS secondary_units,
    SUM(CASE WHEN sale_type='PRIMARY' THEN units_sold ELSE 0 END) -
    SUM(CASE WHEN sale_type='SECONDARY' THEN units_sold ELSE 0 END) AS pipeline_gap
FROM SALES_DM.FACT_UNIFIED_SALES
WHERE sale_date >= CURRENT_DATE - 7
GROUP BY 1, 2, 3
ORDER BY 1 DESC, pipeline_gap DESC;


-- ══════════════════════════════════════════════════════════════
-- 4. QUICK REVENUE CHECK by brand × zone this month
-- ══════════════════════════════════════════════════════════════
SELECT
    brand,
    zone,
    SUM(units_sold)  AS units,
    SUM(net_revenue) AS revenue,
    ROUND(SUM(net_revenue) / SUM(units_sold), 2) AS rev_per_unit
FROM SALES_DM.FACT_UNIFIED_SALES
WHERE sale_type = 'SECONDARY'
  AND MONTH(sale_date) = MONTH(CURRENT_DATE)
  AND YEAR(sale_date)  = YEAR(CURRENT_DATE)
GROUP BY 1, 2
ORDER BY revenue DESC;


-- ══════════════════════════════════════════════════════════════
-- 5. CHECK A SPECIFIC GLUE JOB LOADED CORRECTLY
-- ══════════════════════════════════════════════════════════════
SELECT
    _source,
    _job_date,
    COUNT(*) AS rows_loaded,
    MIN(_ingested_at) AS first_loaded,
    MAX(_ingested_at) AS last_loaded
FROM SAP_DB.CORE.VBAP
WHERE _job_date = CURRENT_DATE - 1
GROUP BY 1, 2;


-- ══════════════════════════════════════════════════════════════
-- 6. FIND DUPLICATE RECORDS (run when quality check flags it)
-- ══════════════════════════════════════════════════════════════
SELECT
    order_id,
    sku_code,
    COUNT(*) AS duplicate_count
FROM SAP_DB.CORE.VBAP
WHERE _job_date = CURRENT_DATE
GROUP BY 1, 2
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 20;


-- ══════════════════════════════════════════════════════════════
-- 7. ML PREDICTIONS vs ACTUALS (weekly check)
-- ══════════════════════════════════════════════════════════════
SELECT
    p.forecast_week,
    p.brand,
    p.region_name,
    SUM(p.predicted_units) AS predicted,
    SUM(f.units_sold)       AS actual,
    ROUND(ABS(SUM(p.predicted_units) - SUM(f.units_sold))
          / NULLIF(SUM(f.units_sold), 0) * 100, 2) AS mape_pct
FROM ML_OUTPUTS.DEMAND_FORECAST_PREDICTIONS p
LEFT JOIN SALES_DM.FACT_UNIFIED_SALES f
    ON p.golden_sku_code = f.golden_sku_code
   AND p.region_code = f.region_code
   AND p.forecast_week = f.sale_date
   AND f.sale_type = 'SECONDARY'
WHERE p.forecast_week >= CURRENT_DATE - 30
GROUP BY 1, 2, 3
ORDER BY mape_pct DESC
LIMIT 20;
-- If MAPE > 15% for 2+ consecutive weeks → flag for retraining


-- ══════════════════════════════════════════════════════════════
-- 8. DISTRIBUTOR STOCKOUT RISK (from OPS_DM)
-- ══════════════════════════════════════════════════════════════
SELECT
    golden_sku_code,
    brand,
    region_name,
    distributor_code,
    closing_stock,
    avg_daily_sales,
    ROUND(closing_stock / NULLIF(avg_daily_sales, 0), 1) AS days_of_stock
FROM OPS_DM.DISTRIBUTOR_INVENTORY_LATEST
WHERE closing_stock / NULLIF(avg_daily_sales, 0) < 7  -- less than 1 week stock
ORDER BY days_of_stock ASC
LIMIT 30;
```

---

## 🔁 AIRFLOW OPERATIONS CHEATSHEET

```
COMMON AIRFLOW ACTIONS YOU'LL DO:
───────────────────────────────────

ACTION 1: Re-run a failed task (most common thing you'll do)
  → Airflow UI → DAGs → find your DAG
  → Click on the DAG run (the date)
  → Click on the failed (red) task
  → Click "Clear" → Confirm
  → Task goes back to queued → runs again
  ⚠️ Only clear the failed task, not the whole DAG

ACTION 2: Re-run entire DAG for a past date
  → Airflow UI → DAGs → your DAG
  → Click "Trigger DAG w/ config"
  → Set logical_date = "2024-01-14" (the date you want to reprocess)
  → Confirm → Full pipeline runs for that date

ACTION 3: Pause a DAG (if something is wrong and you need to stop it)
  → Airflow UI → DAGs
  → Toggle the ON/OFF switch next to the DAG name to OFF
  → ⚠️ Always tell your senior before pausing any DAG

ACTION 4: Check why a Sensor is stuck
  → Click the sensor task (yellow = waiting)
  → View Log
  → See what path it's waiting for
  → Check that S3 path manually in AWS Console

ACTION 5: Check task duration (is something slow today?)
  → Airflow UI → DAGs → your DAG
  → "Gantt" tab → shows each task's duration as bars
  → If one bar is much longer than usual → that task is slow

KEY AIRFLOW TERMS:
───────────────────
  DAG         = one full pipeline (Python file)
  Task        = one step in the pipeline
  DAG Run     = one execution of the pipeline (one date)
  Sensor      = task that WAITS for something (file, condition)
  Operator    = task that DOES something (Glue, Lambda, Snowflake)
  XCom        = how tasks pass data to each other
  Backfill    = re-running pipeline for past dates
  SLA         = alert if task takes longer than expected time
  
  TASK COLORS:
  Green  = Success ✅
  Red    = Failed ❌
  Yellow = Running or Waiting ⏳
  Grey   = Queued (about to run)
  Pink   = Skipped (branching chose different path)
```

---

## ⚙️ GLUE DEBUGGING CHEATSHEET

```
HOW TO QUICKLY DEBUG A GLUE JOB:
──────────────────────────────────

  1. Open: AWS Console → Glue → Jobs
  2. Find: your job name (e.g., tcpl-glue-sap-raw-to-processed-sales)
  3. Click: "Run history" tab
  4. Find: the failed run → click it
  5. Click: "Logs" → "Error logs" → opens CloudWatch
  6. Scroll: to the bottom → find the actual exception

  MOST USEFUL CLOUDWATCH FILTER:
  In CloudWatch log search bar type: ?Exception ?Error ?WARN
  This filters to only error lines — much faster than scrolling

  TOP 5 GLUE ERRORS AT TCPL:
  ───────────────────────────
  1. AnalysisException: cannot resolve column 'xyz'
     → Column name in script doesn't match actual CSV column
     → Fix: Print raw_df.columns to find actual name
  
  2. NumberFormatException: For input string: "1,23,456.00"
     → Indian number format with commas in CSV
     → Fix: .withColumn("col", F.regexp_replace("col",",","").cast(DoubleType()))
  
  3. FileNotFoundException: s3://tcpl-datalake/raw/...
     → Source file path doesn't exist (file not arrived)
     → Fix: This is a FAILURE TYPE 1 (source file issue)
  
  4. py4j.protocol.Py4JJavaError: SparkException: Task failed
     → Usually OOM or data skew
     → Fix: Increase DPUs OR add .repartition() in script
  
  5. IntegrationError: Snowflake connection timed out
     → Snowflake network issue or wrong credentials
     → Fix: Check Secrets Manager for fresh credentials

  HOW TO TEST A GLUE CHANGE LOCALLY (before deploying):
  ───────────────────────────────────────────────────────
  # Use awswrangler to test S3 reads locally (with AWS access)
  import awswrangler as wr
  df = wr.s3.read_parquet("s3://tcpl-datalake/processed/sap/sales/")
  print(df.shape)
  print(df.dtypes)
  print(df.head(5))
  
  # Much faster than running full Glue job to test a small change
```

---

## 📐 THE 5 MOST IMPORTANT TABLES IN SNOWFLAKE

```
TABLE 1: MDM_DB.CORE.SKU_MASTER
──────────────────────────────────
  WHY: Golden SKU codes. Every other table joins to this.
       If something is NULL in SALES_DM → check this first.
  
  QUICK CHECK:
  SELECT COUNT(*) FROM MDM_DB.CORE.SKU_MASTER WHERE is_active = TRUE;
  -- Know this number by heart after first week (~500-2000 active SKUs)


TABLE 2: SALES_DM.FACT_UNIFIED_SALES
──────────────────────────────────────
  WHY: The SINGLE most queried table at TCPL.
       Primary + secondary sales in one place.
       Management looks at this daily.
  
  QUICK CHECK:
  SELECT MAX(load_date) FROM SALES_DM.FACT_UNIFIED_SALES;
  -- Should be TODAY (or yesterday if pipeline runs at night)


TABLE 3: BOTREE_DB.CORE.SECONDARY_SALES
────────────────────────────────────────
  WHY: Shows what ACTUALLY sold at retailer shelves.
       More important than SAP for demand sensing.
  
  QUICK CHECK:
  SELECT sale_date, COUNT(*) FROM BOTREE_DB.CORE.SECONDARY_SALES
  WHERE sale_date >= CURRENT_DATE - 7 GROUP BY 1 ORDER BY 1 DESC;


TABLE 4: ML_OUTPUTS.DEMAND_FORECAST_PREDICTIONS
─────────────────────────────────────────────────
  WHY: Demand planners' daily tool. Feeds their Excel planning.
       If this is stale → planners are working blind.
  
  QUICK CHECK:
  SELECT MAX(load_date), MAX(forecast_week)
  FROM ML_OUTPUTS.DEMAND_FORECAST_PREDICTIONS;
  -- forecast_week should be 13 weeks ahead of today


TABLE 5: FINANCE_DM.PNL_BUDGET_VS_ACTUAL
──────────────────────────────────────────
  WHY: CFO and Finance team's dashboard.
       Budget vs actual variance.
       High sensitivity — errors here get noticed FAST.
  
  QUICK CHECK:
  SELECT fiscal_year, fiscal_quarter, SUM(actual_revenue), SUM(budget_revenue)
  FROM FINANCE_DM.PNL_BUDGET_VS_ACTUAL
  GROUP BY 1, 2 ORDER BY 1 DESC, 2 DESC LIMIT 4;
```

---

## 🤖 ML MONITORING CHEATSHEET

```
WEEKLY ML HEALTH CHECK (every Monday morning):
────────────────────────────────────────────────

  STEP 1: Check model MAPE in Snowflake
  ──────────────────────────────────────
  SELECT
      DATE_TRUNC('week', forecast_week) AS week,
      ROUND(AVG(ABS(predicted_units - actual_units)
                / NULLIF(actual_units,0) * 100), 2) AS weekly_mape
  FROM ML_OUTPUTS.DEMAND_FORECAST_PREDICTIONS
  WHERE actual_units IS NOT NULL          -- only weeks where actuals available
    AND forecast_week >= CURRENT_DATE - 60
  GROUP BY 1
  ORDER BY 1 DESC;
  
  INTERPRET:
  MAPE < 10%  → 🟢 Excellent
  MAPE 10-15% → 🟡 Acceptable (within target)
  MAPE 15-20% → 🟠 Warning — monitor closely
  MAPE > 20%  → 🔴 Retrain needed — tell your senior NOW

  STEP 2: Check SageMaker Model Monitor alerts
  ─────────────────────────────────────────────
  AWS Console → SageMaker → Model Monitor → Monitoring Jobs
  → Look for "Issues Detected" status
  → Green = no drift   Red = drift detected

  STEP 3: Check if predictions are fresh
  ──────────────────────────────────────
  SELECT MAX(load_date) FROM ML_OUTPUTS.DEMAND_FORECAST_PREDICTIONS;
  → Should be within last 7 days (weekly batch)
  → If older than 7 days → SageMaker pipeline may have failed

  WHEN TO ESCALATE FOR RETRAINING:
  ──────────────────────────────────
  ✅ Retrain when:
     - MAPE > 15% for 2 consecutive weeks
     - SageMaker model monitor shows drift (PSI > 0.2)
     - Major business event (new brand launch, price increase)
     - More than 3 months since last training run
  
  ❌ Don't retrain just because:
     - One bad week (could be outlier — festive season, stockout)
     - Business stakeholder says "numbers look off" without data evidence
```

---

## 📅 WEEKLY RHYTHM — What Happens Each Day

```
MONDAY:
  ✅ Morning: Full pipeline health check (extra thorough)
  ✅ Check: Weekend pipeline runs (Sat + Sun) all green?
  ✅ Run: Weekly ML MAPE check
  ✅ Check: Nielsen weekly data arrived (Mondays only)
  ✅ Run: Weekly secondary sales summary → share with team

TUESDAY - THURSDAY:
  ✅ Morning: Daily 4-step health check (15 min)
  ✅ Work: Assigned development tasks (new features, bug fixes)
  ✅ Monitor: Any data quality alerts from CloudWatch

FRIDAY:
  ✅ Morning: Daily check
  ✅ Code: Review and test any changes made this week
  ✅ Deploy: Push tested code changes (never deploy untested on Friday!)
  ✅ End of day: Leave pipeline in clean state for weekend

MONTHLY:
  ✅ 1st of month: PEGASUS planning data arrives → trigger planning DAG
  ✅ Check: Finance files from SharePoint (monthly actuals)
  ✅ Run: MMM (Market Mix Model) update if scheduled
  ✅ Review: CloudWatch costs report (DPU usage, Lambda invocations)
  ✅ Check: Model retraining needed? (MAPE trend for last month)
```

---

## 🔑 GOLDEN RULES — Never Break These

```
RULE 1:  NEVER modify files in the raw/ S3 zone.
         raw/ = source of truth. Immutable. Sacred.
         If something's wrong in raw/ → fix it downstream in Glue.

RULE 2:  NEVER hardcode credentials in code.
         Always use Secrets Manager.
         If you see a password in a Python file → tell your senior.

RULE 3:  NEVER deploy on Friday afternoon.
         If something breaks, no one is around to fix it.
         Weekend pipeline failure = Monday morning crisis.

RULE 4:  NEVER run DELETE on Snowflake production tables.
         Use soft deletes (is_deleted = TRUE flag).
         If you accidentally run DELETE → IMMEDIATELY tell senior.
         Snowflake has Time Travel (can recover up to 90 days).

RULE 5:  NEVER bypass the data quality gate.
         Even if business is pressuring "we need data NOW".
         Bad data in Snowflake = business decisions on wrong numbers.
         One day delay is better than wrong data for weeks.

RULE 6:  ALWAYS test Glue script changes on a sample first.
         Read 1000 rows → test transformation → check output.
         Never push untested Glue script to production.

RULE 7:  ALWAYS tell your senior BEFORE you make production changes.
         Even if you're confident. Especially if you're new.
         "I think the fix is X. Can I go ahead?" = professional.

RULE 8:  If you're stuck for > 30 minutes → ASK.
         This is not a sign of weakness.
         This is how experienced engineers work too.

RULE 9:  ALWAYS check AWS region = ap-south-1 (Mumbai).
         If you're in us-east-1 by mistake → you'll see empty resources.
         Everything TCPL is in ap-south-1.

RULE 10: When in doubt → READ THE LOGS.
         Every answer is in some log somewhere.
         Airflow log, CloudWatch log, Snowflake query history.
         Develop the habit of reading logs before asking.
```

---

## 💡 MINDSET GUIDE — For When You Feel Scared

```
FEELING: "I don't understand this error"
─────────────────────────────────────────
REALITY: No one understands every error on first sight.
ACTION:  Google the exact error message.
         Add "AWS Glue" or "Snowflake" to the search.
         99% of errors have been faced by others before.

FEELING: "Everyone else seems to know more than me"
────────────────────────────────────────────────────
REALITY: They've been here longer. That's the only difference.
ACTION:  Ask questions. Write down what you learn.
         After 2 weeks you'll know more than you think.

FEELING: "I broke something"
─────────────────────────────
REALITY: Everyone breaks something at some point.
ACTION:  Tell your senior immediately. Don't hide it.
         The faster you tell → the faster it gets fixed.
         Hiding it = much worse outcome.

FEELING: "I don't know how to start this task"
───────────────────────────────────────────────
REALITY: Complex tasks are just small tasks chained together.
ACTION:  Break it into the smallest possible first step.
         "What is step 1?" → do only that → then step 2.

FEELING: "I'm scared of client (TCPL) meetings"
─────────────────────────────────────────────────
REALITY: TCPL team members are also just people doing their jobs.
ACTION:  In meetings: listen more than you talk.
         Take notes. Ask for clarification if confused.
         "Can you help me understand what you need from this?" 
         = perfectly good professional question.

REMEMBER:
──────────
  You were SELECTED for this role.
  Someone at Decision Point believed you could do this.
  The fear you feel = you care. That's a strength.
  The architecture is big but you learn one piece at a time.
  In 4 weeks you'll wonder why you were scared.
  
  Every senior engineer you see was once exactly where you are.
  The only difference = time and experience.
  Both of which you're about to get. 💪
```

---

## 📞 ESCALATION PATH — Who to Contact When

```
LEVEL 1: Try to fix it yourself (use this cheatsheet)
         Time limit: 30 minutes

LEVEL 2: Ask your immediate senior / buddy
         "I've tried X and Y. I think the issue is Z.
          Can you help me verify?"

LEVEL 3: Ask your tech lead / project lead
         If senior is unavailable or issue needs architectural decision

LEVEL 4: Escalate to manager
         If issue is business-impacting (dashboard down for TCPL)
         or if it's been >2 hours and not resolved

LEVEL 5: TCPL IT Contact
         Only for SOURCE SYSTEM issues (SAP not sending data,
         Botree API down, SharePoint access issues)
         Your manager or senior will make this call, not you directly.

WHEN TO WAKE SOMEONE UP (out of hours):
  ✅ SALES_DM has been empty since 6 AM (business impact)
  ✅ Snowflake completely inaccessible
  ✅ You accidentally deleted production data
  ❌ Single Glue job failed but data is still loading
  ❌ Minor quality warning in CloudWatch
  ❌ One SKU missing from MDM
```

---

*Keep this file bookmarked. Open it every morning.*
*Fear shrinks every day you show up. You've got this. 💪*
*— Your cheatsheet, built from the real TCPL architecture.*
