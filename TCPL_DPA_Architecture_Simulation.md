# 🏭 TCPL × Decision Point Analytics
## Full End-to-End Data Engineering + MLOps Architecture Simulation
### CPG Industry | AWS Glue · MWAA · Lambda · SageMaker · Snowflake

---

> **Client:** Tata Consumer Products Limited (TCPL)
> **Vendor/Partner:** Decision Point Analytics
> **Domain:** CPG (Consumer Packaged Goods) — Demand Forecasting, Trade Promotion Optimization, Market Mix Modeling

---

## 📐 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        SOURCE SYSTEMS (TCPL)                                │
│  SAP S4/HANA  │  ERP/Oracle  │  Nielsen/IRI  │  REST APIs  │  Flat Files   │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │ Raw Data
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        AWS S3 — DATA LAKE                                   │
│   s3://tcpl-datalake/raw/       s3://tcpl-datalake/curated/                 │
│   s3://tcpl-datalake/archive/   s3://tcpl-datalake/ml-features/             │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
          ┌────────────────────────┼────────────────────────┐
          ▼                        ▼                         ▼
   ┌─────────────┐        ┌──────────────────┐      ┌──────────────┐
   │  AWS GLUE   │        │  APACHE AIRFLOW  │      │ AWS LAMBDA   │
   │  ETL Jobs   │◄───────│  (MWAA) — Brain  │─────►│ Validators   │
   │  PySpark    │        │  Orchestrator    │      │ Alerts       │
   └──────┬──────┘        └──────────────────┘      └──────────────┘
          │                        │
          │                        ▼
          │               ┌──────────────────┐
          │               │  AWS SAGEMAKER   │
          │               │  ML Pipelines    │
          │               │  Training/Infer  │
          │               └────────┬─────────┘
          │                        │
          └────────────┬───────────┘
                       ▼
          ┌────────────────────────┐
          │      SNOWFLAKE         │
          │   Data Warehouse       │
          │  RAW → CURATED → MART  │
          └────────────────────────┘
                       │
                       ▼
          ┌────────────────────────┐
          │   BI / DASHBOARDS      │
          │  Tableau / PowerBI     │
          │  Looker / Snowsight    │
          └────────────────────────┘
```

---

## 🗂️ S3 Data Lake Structure

```
s3://tcpl-datalake/
├── raw/
│   ├── sap/
│   │   ├── sales_orders/year=2024/month=01/day=15/sales_20240115.parquet
│   │   ├── inventory/year=2024/month=01/
│   │   └── purchase_orders/
│   ├── nielsen/
│   │   └── market_share/week=2024-W03/
│   ├── trade_promotions/
│   │   └── promo_calendar/
│   └── external/
│       └── weather_data/
│
├── curated/
│   ├── sales/           ← Cleaned, deduplicated
│   ├── inventory/
│   ├── promotions/
│   └── market_data/
│
├── ml-features/
│   ├── demand_forecast/
│   │   └── feature_store/
│   └── trade_promo_optimization/
│
└── model-artifacts/
    ├── demand_forecast/
    │   └── v1.2/model.tar.gz
    └── mmm/
        └── v0.9/
```

---

## 🧠 Layer 1: Apache Airflow (MWAA) — The Orchestrator

### DAG 1: `tcpl_cpg_full_pipeline_dag.py`

```python
"""
DAG: tcpl_cpg_full_pipeline_dag
Schedule: Daily at 2:00 AM IST
Owner: Decision Point Analytics
Client: TCPL
"""

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerPipelineOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime, timedelta
import json
import boto3

# ─── Default Args ───────────────────────────────────────────────────────────
default_args = {
    "owner": "decision_point_analytics",
    "depends_on_past": False,
    "email": ["data-ops@decisionpoint.ai", "tcpl-data@tcpl.in"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=4),
}

# ─── DAG Definition ──────────────────────────────────────────────────────────
with DAG(
    dag_id="tcpl_cpg_full_pipeline",
    default_args=default_args,
    description="TCPL CPG End-to-End: Ingest → ETL → Validate → ML → Snowflake",
    schedule_interval="0 20 * * *",  # 2AM IST = 8:30PM UTC-ish, adjust
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["tcpl", "cpg", "production", "decision-point"],
) as dag:

    # ──────────────────────────────────────────────────────────
    # STEP 1: Pre-flight Lambda — Check source file availability
    # ──────────────────────────────────────────────────────────
    check_source_files = LambdaInvokeFunctionOperator(
        task_id="check_source_files_lambda",
        function_name="tcpl-preflight-source-check",
        payload=json.dumps({
            "date": "{{ ds }}",
            "sources": ["sap_sales", "sap_inventory", "nielsen_market"],
            "s3_bucket": "tcpl-datalake",
        }),
        aws_conn_id="aws_tcpl_prod",
    )

    # ──────────────────────────────────────────────────────────
    # STEP 2: AWS Glue — SAP Sales ETL
    # ──────────────────────────────────────────────────────────
    glue_sap_sales_etl = GlueJobOperator(
        task_id="glue_sap_sales_etl",
        job_name="tcpl-glue-sap-sales-transform",
        script_args={
            "--JOB_DATE": "{{ ds }}",
            "--SOURCE_PATH": "s3://tcpl-datalake/raw/sap/sales_orders/",
            "--TARGET_PATH": "s3://tcpl-datalake/curated/sales/",
            "--SNOWFLAKE_SECRET": "arn:aws:secretsmanager:ap-south-1:xxx:secret:tcpl-snowflake",
        },
        aws_conn_id="aws_tcpl_prod",
        region_name="ap-south-1",
        wait_for_completion=True,
        num_of_dpus=10,
    )

    # ──────────────────────────────────────────────────────────
    # STEP 3: AWS Glue — Inventory + Market Data ETL (parallel)
    # ──────────────────────────────────────────────────────────
    glue_inventory_etl = GlueJobOperator(
        task_id="glue_inventory_etl",
        job_name="tcpl-glue-inventory-transform",
        script_args={
            "--JOB_DATE": "{{ ds }}",
            "--SOURCE_PATH": "s3://tcpl-datalake/raw/sap/inventory/",
            "--TARGET_PATH": "s3://tcpl-datalake/curated/inventory/",
        },
        aws_conn_id="aws_tcpl_prod",
        region_name="ap-south-1",
        num_of_dpus=4,
    )

    glue_market_data_etl = GlueJobOperator(
        task_id="glue_market_data_etl",
        job_name="tcpl-glue-nielsen-transform",
        script_args={
            "--JOB_DATE": "{{ ds }}",
            "--SOURCE_PATH": "s3://tcpl-datalake/raw/nielsen/",
            "--TARGET_PATH": "s3://tcpl-datalake/curated/market_data/",
        },
        aws_conn_id="aws_tcpl_prod",
        region_name="ap-south-1",
        num_of_dpus=4,
    )

    # ──────────────────────────────────────────────────────────
    # STEP 4: Lambda — Data Quality Validation
    # ──────────────────────────────────────────────────────────
    validate_data_quality = LambdaInvokeFunctionOperator(
        task_id="validate_data_quality_lambda",
        function_name="tcpl-data-quality-validator",
        payload=json.dumps({
            "date": "{{ ds }}",
            "datasets": [
                "s3://tcpl-datalake/curated/sales/",
                "s3://tcpl-datalake/curated/inventory/",
            ],
            "thresholds": {
                "null_rate_max": 0.05,
                "row_count_min": 1000,
                "duplicate_rate_max": 0.01,
            },
        }),
        aws_conn_id="aws_tcpl_prod",
    )

    # ──────────────────────────────────────────────────────────
    # STEP 5: Branching — Pass or Fail Quality Check
    # ──────────────────────────────────────────────────────────
    def branch_on_quality(**context):
        ti = context["ti"]
        result = ti.xcom_pull(task_ids="validate_data_quality_lambda")
        quality_passed = json.loads(result)["quality_passed"]
        return "load_to_snowflake" if quality_passed else "alert_data_quality_failure"

    quality_branch = BranchPythonOperator(
        task_id="quality_gate_branch",
        python_callable=branch_on_quality,
        provide_context=True,
    )

    # ──────────────────────────────────────────────────────────
    # STEP 6A: Alert on failure
    # ──────────────────────────────────────────────────────────
    alert_quality_failure = LambdaInvokeFunctionOperator(
        task_id="alert_data_quality_failure",
        function_name="tcpl-slack-alerter",
        payload=json.dumps({
            "channel": "#tcpl-data-alerts",
            "severity": "HIGH",
            "message": "Data quality check FAILED for {{ ds }}. Pipeline halted.",
        }),
        aws_conn_id="aws_tcpl_prod",
    )

    # ──────────────────────────────────────────────────────────
    # STEP 6B: Load curated data to Snowflake
    # ──────────────────────────────────────────────────────────
    load_to_snowflake = SnowflakeOperator(
        task_id="load_to_snowflake",
        sql="CALL TCPL_CPG_DB.PROCEDURES.LOAD_CURATED_DATA('{{ ds }}')",
        snowflake_conn_id="snowflake_tcpl_prod",
        warehouse="TCPL_LOAD_WH",
        database="TCPL_CPG_DB",
        schema="CURATED",
        role="TCPL_ETL_ROLE",
    )

    # ──────────────────────────────────────────────────────────
    # STEP 7: Glue — Feature Engineering for ML
    # ──────────────────────────────────────────────────────────
    glue_feature_engineering = GlueJobOperator(
        task_id="glue_ml_feature_engineering",
        job_name="tcpl-glue-feature-engineering",
        script_args={
            "--JOB_DATE": "{{ ds }}",
            "--SALES_PATH": "s3://tcpl-datalake/curated/sales/",
            "--PROMO_PATH": "s3://tcpl-datalake/curated/promotions/",
            "--OUTPUT_PATH": "s3://tcpl-datalake/ml-features/demand_forecast/",
            "--LOOKBACK_DAYS": "365",
        },
        aws_conn_id="aws_tcpl_prod",
        region_name="ap-south-1",
        num_of_dpus=8,
    )

    # ──────────────────────────────────────────────────────────
    # STEP 8: SageMaker — Demand Forecasting Pipeline
    # ──────────────────────────────────────────────────────────
    sagemaker_demand_forecast = SageMakerPipelineOperator(
        task_id="sagemaker_demand_forecast_pipeline",
        pipeline_name="tcpl-demand-forecast-pipeline",
        display_name="TCPL Demand Forecast {{ ds }}",
        pipeline_parameters={
            "InputDataPath": "s3://tcpl-datalake/ml-features/demand_forecast/",
            "OutputPath": "s3://tcpl-datalake/model-artifacts/demand_forecast/",
            "ForecastHorizon": "90",
            "ModelVersion": "{{ var.value.demand_forecast_model_version }}",
        },
        aws_conn_id="aws_tcpl_prod",
        wait_for_completion=True,
    )

    # ──────────────────────────────────────────────────────────
    # STEP 9: Lambda — Store Predictions back to Snowflake
    # ──────────────────────────────────────────────────────────
    store_predictions = LambdaInvokeFunctionOperator(
        task_id="store_ml_predictions_lambda",
        function_name="tcpl-predictions-loader",
        payload=json.dumps({
            "date": "{{ ds }}",
            "prediction_s3_path": "s3://tcpl-datalake/model-artifacts/demand_forecast/predictions/",
            "snowflake_target_table": "TCPL_CPG_DB.ML_OUTPUTS.DEMAND_FORECAST_PREDICTIONS",
        }),
        aws_conn_id="aws_tcpl_prod",
    )

    # ──────────────────────────────────────────────────────────
    # STEP 10: Snowflake — Refresh Data Marts
    # ──────────────────────────────────────────────────────────
    refresh_data_marts = SnowflakeOperator(
        task_id="refresh_snowflake_data_marts",
        sql="""
            CALL TCPL_CPG_DB.PROCEDURES.REFRESH_SALES_MART('{{ ds }}');
            CALL TCPL_CPG_DB.PROCEDURES.REFRESH_FORECAST_MART('{{ ds }}');
            CALL TCPL_CPG_DB.PROCEDURES.REFRESH_PROMO_EFFECTIVENESS_MART('{{ ds }}');
        """,
        snowflake_conn_id="snowflake_tcpl_prod",
        warehouse="TCPL_TRANSFORM_WH",
        database="TCPL_CPG_DB",
    )

    # ──────────────────────────────────────────────────────────
    # STEP 11: Success Notification
    # ──────────────────────────────────────────────────────────
    pipeline_success_alert = LambdaInvokeFunctionOperator(
        task_id="pipeline_success_notification",
        function_name="tcpl-slack-alerter",
        payload=json.dumps({
            "channel": "#tcpl-pipeline-success",
            "severity": "INFO",
            "message": "✅ TCPL CPG pipeline completed successfully for {{ ds }}",
        }),
        aws_conn_id="aws_tcpl_prod",
    )

    # ──────────────────────────────────────────────────────────
    # DAG DEPENDENCY GRAPH
    # ──────────────────────────────────────────────────────────
    check_source_files >> glue_sap_sales_etl
    check_source_files >> [glue_inventory_etl, glue_market_data_etl]

    [glue_sap_sales_etl, glue_inventory_etl, glue_market_data_etl] >> validate_data_quality
    validate_data_quality >> quality_branch
    quality_branch >> [load_to_snowflake, alert_quality_failure]
    load_to_snowflake >> glue_feature_engineering
    glue_feature_engineering >> sagemaker_demand_forecast
    sagemaker_demand_forecast >> store_predictions
    store_predictions >> refresh_data_marts
    refresh_data_marts >> pipeline_success_alert
```

---

## ⚙️ Layer 2: AWS Glue — PySpark ETL Jobs

### Glue Job 1: SAP Sales Transform (`tcpl-glue-sap-sales-transform`)

```python
"""
AWS Glue PySpark Job: tcpl-glue-sap-sales-transform
Transforms raw SAP sales orders → curated parquet in S3
Also writes to Snowflake via JDBC
"""

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import boto3
import json

# ─── Init ────────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "JOB_DATE", "SOURCE_PATH", "TARGET_PATH", "SNOWFLAKE_SECRET"
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.adaptive.enabled", "true")

# ─── Fetch Snowflake Credentials from Secrets Manager ─────────────────────
def get_secret(secret_arn: str) -> dict:
    client = boto3.client("secretsmanager", region_name="ap-south-1")
    response = client.get_secret_value(SecretId=secret_arn)
    return json.loads(response["SecretString"])

sf_creds = get_secret(args["SNOWFLAKE_SECRET"])

# ─── Read Raw SAP Data ────────────────────────────────────────────────────────
print(f"[INFO] Reading SAP data for date: {args['JOB_DATE']}")

raw_df = spark.read.parquet(
    f"{args['SOURCE_PATH']}year={args['JOB_DATE'][:4]}/month={args['JOB_DATE'][5:7]}/day={args['JOB_DATE'][8:10]}/"
)

print(f"[INFO] Raw records read: {raw_df.count()}")

# ─── Schema Validation ──────────────────────────────────────────────────────
required_columns = [
    "order_id", "sku_code", "customer_id", "region_code",
    "quantity", "net_revenue", "order_date", "channel"
]

missing_cols = [c for c in required_columns if c not in raw_df.columns]
if missing_cols:
    raise ValueError(f"[CRITICAL] Missing required columns: {missing_cols}")

# ─── Transformations ─────────────────────────────────────────────────────────
# 1. Standardize & clean
cleaned_df = (
    raw_df
    .dropDuplicates(["order_id"])
    .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
    .withColumn("sku_code", F.upper(F.trim("sku_code")))
    .withColumn("region_code", F.upper(F.trim("region_code")))
    .withColumn("channel", F.lower(F.trim("channel")))
    .withColumn("net_revenue", F.col("net_revenue").cast(DoubleType()))
    .withColumn("quantity", F.col("quantity").cast(IntegerType()))
    .filter(F.col("quantity") > 0)
    .filter(F.col("net_revenue") > 0)
    .filter(F.col("order_date").isNotNull())
)

# 2. Derive business metrics
enriched_df = (
    cleaned_df
    .withColumn("revenue_per_unit", F.round(F.col("net_revenue") / F.col("quantity"), 2))
    .withColumn("year", F.year("order_date"))
    .withColumn("month", F.month("order_date"))
    .withColumn("week_of_year", F.weekofyear("order_date"))
    .withColumn("quarter", F.quarter("order_date"))
    .withColumn("is_modern_trade", F.when(F.col("channel") == "modern_trade", 1).otherwise(0))
    .withColumn("processing_timestamp", F.current_timestamp())
    .withColumn("job_run_date", F.lit(args["JOB_DATE"]))
)

# 3. Add 4-week rolling avg for anomaly detection
window_spec = Window.partitionBy("sku_code", "region_code").orderBy("order_date").rowsBetween(-27, 0)
enriched_df = enriched_df.withColumn(
    "rolling_4wk_avg_revenue",
    F.round(F.avg("net_revenue").over(window_spec), 2)
)

# 4. Flag anomalies
enriched_df = enriched_df.withColumn(
    "is_revenue_anomaly",
    F.when(
        F.col("net_revenue") > (F.col("rolling_4wk_avg_revenue") * 3), 1
    ).otherwise(0)
)

print(f"[INFO] Transformed records: {enriched_df.count()}")
print(f"[INFO] Anomalies flagged: {enriched_df.filter(F.col('is_revenue_anomaly') == 1).count()}")

# ─── Write to S3 Curated (Partitioned Parquet) ───────────────────────────────
print(f"[INFO] Writing to S3 curated path: {args['TARGET_PATH']}")

enriched_df.write.mode("overwrite").partitionBy("year", "month").parquet(args["TARGET_PATH"])

# ─── Write to Snowflake ──────────────────────────────────────────────────────
print("[INFO] Writing to Snowflake...")

snowflake_options = {
    "sfURL": sf_creds["sfURL"],
    "sfUser": sf_creds["sfUser"],
    "sfPassword": sf_creds["sfPassword"],
    "sfDatabase": "TCPL_CPG_DB",
    "sfSchema": "CURATED",
    "sfWarehouse": "TCPL_LOAD_WH",
    "sfRole": "TCPL_ETL_ROLE",
    "dbtable": "SALES_ORDERS",
    "truncate_table": "off",
}

enriched_df.write \
    .format("net.snowflake.spark.snowflake") \
    .options(**snowflake_options) \
    .mode("append") \
    .save()

print("[INFO] Snowflake write complete ✅")
job.commit()
```

### Glue Job 2: Feature Engineering (`tcpl-glue-feature-engineering`)

```python
"""
AWS Glue PySpark Job: tcpl-glue-feature-engineering
Builds ML feature store for demand forecasting
"""

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "JOB_DATE", "SALES_PATH", "PROMO_PATH",
    "OUTPUT_PATH", "LOOKBACK_DAYS"
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

lookback = int(args["LOOKBACK_DAYS"])
job_date = args["JOB_DATE"]

# ─── Load curated datasets ───────────────────────────────────────────────────
sales_df = spark.read.parquet(args["SALES_PATH"])
promo_df = spark.read.parquet(args["PROMO_PATH"])

# Filter to lookback window
from datetime import datetime, timedelta
cutoff_date = (datetime.strptime(job_date, "%Y-%m-%d") - timedelta(days=lookback)).strftime("%Y-%m-%d")
sales_df = sales_df.filter(F.col("order_date") >= cutoff_date)

# ─── Lag Features ────────────────────────────────────────────────────────────
w_sku_region = Window.partitionBy("sku_code", "region_code").orderBy("order_date")

# Weekly aggregation
weekly_sales = (
    sales_df
    .withColumn("week", F.date_trunc("week", "order_date"))
    .groupBy("sku_code", "region_code", "week")
    .agg(
        F.sum("quantity").alias("weekly_units"),
        F.sum("net_revenue").alias("weekly_revenue"),
        F.countDistinct("customer_id").alias("unique_customers"),
    )
)

# Lag 1, 2, 4, 8 weeks
w_weekly = Window.partitionBy("sku_code", "region_code").orderBy("week")
feature_df = (
    weekly_sales
    .withColumn("lag_1w_units", F.lag("weekly_units", 1).over(w_weekly))
    .withColumn("lag_2w_units", F.lag("weekly_units", 2).over(w_weekly))
    .withColumn("lag_4w_units", F.lag("weekly_units", 4).over(w_weekly))
    .withColumn("lag_8w_units", F.lag("weekly_units", 8).over(w_weekly))
    .withColumn("rolling_4w_avg", F.avg("weekly_units").over(
        Window.partitionBy("sku_code", "region_code").orderBy("week").rowsBetween(-3, 0)
    ))
    .withColumn("rolling_4w_std", F.stddev("weekly_units").over(
        Window.partitionBy("sku_code", "region_code").orderBy("week").rowsBetween(-3, 0)
    ))
    .withColumn("yoy_units_change", (
        (F.col("weekly_units") - F.lag("weekly_units", 52).over(w_weekly)) /
        F.lag("weekly_units", 52).over(w_weekly)
    ))
)

# ─── Join Promotions ─────────────────────────────────────────────────────────
feature_df = feature_df.join(promo_df, on=["sku_code", "region_code", "week"], how="left")
feature_df = feature_df.fillna({"promo_discount_pct": 0.0, "is_promoted": 0})

# ─── Time Features ───────────────────────────────────────────────────────────
feature_df = (
    feature_df
    .withColumn("week_of_year", F.weekofyear("week"))
    .withColumn("month", F.month("week"))
    .withColumn("quarter", F.quarter("week"))
    .withColumn("is_festive_season", F.when(F.col("week_of_year").between(40, 52), 1).otherwise(0))
    .withColumn("is_summer", F.when(F.col("month").between(4, 6), 1).otherwise(0))
    .dropna(subset=["lag_1w_units", "lag_2w_units"])  # Drop incomplete lag rows
)

print(f"[INFO] Feature rows generated: {feature_df.count()}")

# ─── Write Feature Store ─────────────────────────────────────────────────────
feature_df.write.mode("overwrite").partitionBy("sku_code").parquet(args["OUTPUT_PATH"])

print("[INFO] Feature engineering complete ✅")
job.commit()
```

---

## 🔔 Layer 3: AWS Lambda Functions

### Lambda 1: Pre-flight Source File Check

```python
"""
Lambda: tcpl-preflight-source-check
Triggered by: Airflow (first task)
Purpose: Check if all source files landed in S3 for the given date
"""

import boto3
import json
import os
from datetime import datetime

s3 = boto3.client("s3")

EXPECTED_PATHS = {
    "sap_sales": "raw/sap/sales_orders/year={year}/month={month}/day={day}/",
    "sap_inventory": "raw/sap/inventory/year={year}/month={month}/day={day}/",
    "nielsen_market": "raw/nielsen/market_share/week={isoweek}/",
}

def lambda_handler(event, context):
    date_str = event["date"]        # e.g. "2024-01-15"
    bucket = event["s3_bucket"]
    sources = event["sources"]

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year, month, day = dt.year, f"{dt.month:02d}", f"{dt.day:02d}"
    isoweek = f"{dt.year}-W{dt.isocalendar()[1]:02d}"

    results = {}
    all_ok = True

    for source in sources:
        path_template = EXPECTED_PATHS.get(source)
        if not path_template:
            results[source] = {"status": "UNKNOWN_SOURCE", "found": False}
            all_ok = False
            continue

        prefix = path_template.format(year=year, month=month, day=day, isoweek=isoweek)

        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        found = response.get("KeyCount", 0) > 0
        file_count = response.get("KeyCount", 0)

        results[source] = {
            "status": "FOUND" if found else "MISSING",
            "found": found,
            "prefix": prefix,
            "file_count": file_count
        }

        if not found:
            all_ok = False

    # ── Send alert if any source is missing ──
    if not all_ok:
        missing_sources = [k for k, v in results.items() if not v["found"]]
        send_slack_alert(
            message=f"⚠️ Source files MISSING for {date_str}: {missing_sources}",
            severity="HIGH",
            channel="#tcpl-data-alerts"
        )

    return {
        "statusCode": 200 if all_ok else 500,
        "all_sources_available": all_ok,
        "details": results,
        "date": date_str
    }

def send_slack_alert(message: str, severity: str, channel: str):
    """Send alert via SNS → Slack"""
    sns = boto3.client("sns", region_name="ap-south-1")
    sns.publish(
        TopicArn=os.environ["SNS_ALERT_TOPIC_ARN"],
        Message=json.dumps({
            "channel": channel,
            "severity": severity,
            "message": message,
            "source": "tcpl-preflight-lambda"
        }),
        Subject=f"TCPL Data Alert [{severity}]"
    )
```

### Lambda 2: Data Quality Validator

```python
"""
Lambda: tcpl-data-quality-validator
Purpose: Validate curated datasets for quality before Snowflake load
"""

import boto3
import json
import os

s3 = boto3.client("s3")
cloudwatch = boto3.client("cloudwatch", region_name="ap-south-1")

def lambda_handler(event, context):
    date_str = event["date"]
    datasets = event["datasets"]
    thresholds = event["thresholds"]

    results = {}
    overall_pass = True

    for dataset_path in datasets:
        checks = run_quality_checks(dataset_path, date_str)
        passed = evaluate_thresholds(checks, thresholds)

        results[dataset_path] = {
            "checks": checks,
            "passed": passed,
        }

        if not passed:
            overall_pass = False

        # ── Emit CloudWatch metrics ──
        cloudwatch.put_metric_data(
            Namespace="TCPL/DataQuality",
            MetricData=[
                {
                    "MetricName": "NullRate",
                    "Dimensions": [{"Name": "Dataset", "Value": dataset_path.split("/")[-2]}],
                    "Value": checks.get("null_rate", 0),
                    "Unit": "None"
                },
                {
                    "MetricName": "RowCount",
                    "Dimensions": [{"Name": "Dataset", "Value": dataset_path.split("/")[-2]}],
                    "Value": checks.get("row_count", 0),
                    "Unit": "Count"
                },
            ]
        )

    return {
        "statusCode": 200,
        "quality_passed": overall_pass,
        "details": results,
        "date": date_str
    }

def run_quality_checks(s3_path: str, date_str: str) -> dict:
    """
    In production this would use PyArrow or Athena queries.
    Simulated here for illustration.
    """
    bucket = s3_path.split("/")[2]
    prefix = "/".join(s3_path.split("/")[3:])

    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    file_count = response.get("KeyCount", 0)

    # In production: use Athena or S3 Select to compute stats
    return {
        "file_count": file_count,
        "row_count": 45000,        # Simulated — use Athena in production
        "null_rate": 0.02,          # Simulated
        "duplicate_rate": 0.001,    # Simulated
        "schema_valid": True,
    }

def evaluate_thresholds(checks: dict, thresholds: dict) -> bool:
    if checks["row_count"] < thresholds["row_count_min"]:
        return False
    if checks["null_rate"] > thresholds["null_rate_max"]:
        return False
    if checks["duplicate_rate"] > thresholds["duplicate_rate_max"]:
        return False
    return True
```

### Lambda 3: ML Predictions Loader to Snowflake

```python
"""
Lambda: tcpl-predictions-loader
Purpose: Load SageMaker predictions from S3 → Snowflake
"""

import boto3
import json
import os
import snowflake.connector
import pandas as pd
import io

s3 = boto3.client("s3")
secrets = boto3.client("secretsmanager", region_name="ap-south-1")

def lambda_handler(event, context):
    date_str = event["date"]
    s3_path = event["prediction_s3_path"]
    target_table = event["snowflake_target_table"]

    # ── Read predictions from S3 ──
    bucket = s3_path.split("/")[2]
    prefix = "/".join(s3_path.split("/")[3:])

    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]

    if not files:
        raise Exception(f"No prediction files found at {s3_path}")

    dfs = []
    for file_key in files:
        obj = s3.get_object(Bucket=bucket, Key=file_key)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))
        dfs.append(df)

    predictions_df = pd.concat(dfs, ignore_index=True)
    predictions_df["load_date"] = date_str
    predictions_df["model_version"] = os.environ.get("MODEL_VERSION", "latest")

    print(f"[INFO] Predictions loaded: {len(predictions_df)} rows")

    # ── Write to Snowflake ──
    sf_secret = json.loads(
        secrets.get_secret_value(
            SecretId=os.environ["SNOWFLAKE_SECRET_ARN"]
        )["SecretString"]
    )

    conn = snowflake.connector.connect(
        user=sf_secret["user"],
        password=sf_secret["password"],
        account=sf_secret["account"],
        warehouse="TCPL_LOAD_WH",
        database=target_table.split(".")[0],
        schema=target_table.split(".")[1],
        role="TCPL_ML_ROLE",
    )

    cursor = conn.cursor()

    # MERGE predictions to avoid duplicates
    cursor.execute(f"""
        CREATE TEMP TABLE TEMP_PREDICTIONS AS
        SELECT $1::VARCHAR AS sku_code,
               $2::VARCHAR AS region_code,
               $3::DATE AS forecast_week,
               $4::FLOAT AS predicted_units,
               $5::FLOAT AS prediction_lower_ci,
               $6::FLOAT AS prediction_upper_ci,
               $7::VARCHAR AS load_date,
               $8::VARCHAR AS model_version
        FROM VALUES {",".join([
            f"('{r.sku_code}', '{r.region_code}', '{r.forecast_week}', "
            f"{r.predicted_units}, {r.lower_ci}, {r.upper_ci}, "
            f"'{date_str}', '{r.model_version}')"
            for r in predictions_df.itertuples()
        ])}
    """)

    cursor.execute(f"""
        MERGE INTO {target_table} t
        USING TEMP_PREDICTIONS s
        ON t.sku_code = s.sku_code
           AND t.region_code = s.region_code
           AND t.forecast_week = s.forecast_week
        WHEN MATCHED THEN UPDATE SET
            t.predicted_units = s.predicted_units,
            t.prediction_lower_ci = s.prediction_lower_ci,
            t.prediction_upper_ci = s.prediction_upper_ci,
            t.load_date = s.load_date,
            t.model_version = s.model_version
        WHEN NOT MATCHED THEN INSERT VALUES (
            s.sku_code, s.region_code, s.forecast_week,
            s.predicted_units, s.prediction_lower_ci, s.prediction_upper_ci,
            s.load_date, s.model_version
        )
    """)

    conn.close()
    print(f"[INFO] Predictions written to Snowflake: {target_table} ✅")

    return {
        "statusCode": 200,
        "rows_loaded": len(predictions_df),
        "target_table": target_table
    }
```

---

## 🤖 Layer 4: AWS SageMaker — MLOps Pipeline

### SageMaker Pipeline: Demand Forecasting

```python
"""
SageMaker Pipeline: tcpl-demand-forecast-pipeline
ML Use Case: Demand Forecasting for TCPL SKUs across regions
Algorithm: DeepAR+ / XGBoost Ensemble
"""

import boto3
import sagemaker
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import ProcessingStep, TrainingStep, TransformStep
from sagemaker.workflow.pipeline_parameters import ParameterString, ParameterFloat
from sagemaker.workflow.conditions import ConditionLessThanOrEqualTo
from sagemaker.workflow.condition_step import ConditionStep
from sagemaker.workflow.fail_step import FailStep
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.estimator import Estimator
from sagemaker.inputs import TrainingInput
from sagemaker.transformer import Transformer
from sagemaker.model_monitor import DataCaptureConfig
from sagemaker.workflow.model_step import ModelStep
from sagemaker.model import Model

# ── Config ────────────────────────────────────────────────────────────────────
session = sagemaker.Session()
role = "arn:aws:iam::123456789:role/TCPLSageMakerRole"
region = "ap-south-1"
bucket = "tcpl-datalake"
prefix = "ml-features/demand_forecast"

# ── Pipeline Parameters ───────────────────────────────────────────────────────
input_data_path = ParameterString(name="InputDataPath", default_value=f"s3://{bucket}/{prefix}/")
output_path = ParameterString(name="OutputPath", default_value=f"s3://{bucket}/model-artifacts/demand_forecast/")
forecast_horizon = ParameterString(name="ForecastHorizon", default_value="90")
model_version = ParameterString(name="ModelVersion", default_value="latest")

# ── Step 1: Data Preprocessing ────────────────────────────────────────────────
sklearn_processor = SKLearnProcessor(
    framework_version="1.0-1",
    instance_type="ml.m5.xlarge",
    instance_count=1,
    role=role,
    sagemaker_session=session,
)

preprocessing_step = ProcessingStep(
    name="TCPLDemandPreprocessing",
    processor=sklearn_processor,
    inputs=[
        sagemaker.processing.ProcessingInput(
            source=input_data_path,
            destination="/opt/ml/processing/input"
        )
    ],
    outputs=[
        sagemaker.processing.ProcessingOutput(
            output_name="train",
            source="/opt/ml/processing/output/train",
            destination=f"s3://{bucket}/processed/train/"
        ),
        sagemaker.processing.ProcessingOutput(
            output_name="validation",
            source="/opt/ml/processing/output/validation",
            destination=f"s3://{bucket}/processed/validation/"
        ),
        sagemaker.processing.ProcessingOutput(
            output_name="test",
            source="/opt/ml/processing/output/test",
            destination=f"s3://{bucket}/processed/test/"
        ),
    ],
    code="s3://tcpl-datalake/scripts/sagemaker/preprocess_demand.py",
    job_arguments=[
        "--forecast-horizon", forecast_horizon,
        "--target-col", "weekly_units"
    ]
)

# ── Step 2: Model Training (XGBoost) ─────────────────────────────────────────
xgb_image_uri = sagemaker.image_uris.retrieve("xgboost", region, "1.7-1")

xgb_estimator = Estimator(
    image_uri=xgb_image_uri,
    instance_type="ml.m5.2xlarge",
    instance_count=1,
    role=role,
    output_path=f"s3://{bucket}/model-artifacts/training-output/",
    sagemaker_session=session,
    hyperparameters={
        "max_depth": 8,
        "eta": 0.1,
        "gamma": 0.0,
        "min_child_weight": 1,
        "subsample": 0.8,
        "num_round": 500,
        "early_stopping_rounds": 20,
        "objective": "reg:squarederror",
        "eval_metric": "rmse",
    },
    enable_sagemaker_metrics=True,
)

training_step = TrainingStep(
    name="TCPLDemandTraining",
    estimator=xgb_estimator,
    inputs={
        "train": TrainingInput(
            s3_data=preprocessing_step.properties.ProcessingOutputConfig.Outputs["train"].S3Output.S3Uri,
            content_type="text/csv"
        ),
        "validation": TrainingInput(
            s3_data=preprocessing_step.properties.ProcessingOutputConfig.Outputs["validation"].S3Output.S3Uri,
            content_type="text/csv"
        ),
    }
)

# ── Step 3: Model Evaluation ──────────────────────────────────────────────────
evaluation_processor = SKLearnProcessor(
    framework_version="1.0-1",
    instance_type="ml.m5.large",
    instance_count=1,
    role=role,
)

from sagemaker.workflow.properties import PropertyFile
evaluation_report = PropertyFile(
    name="EvaluationReport",
    output_name="evaluation",
    path="evaluation.json"
)

evaluation_step = ProcessingStep(
    name="TCPLDemandEvaluation",
    processor=evaluation_processor,
    inputs=[
        sagemaker.processing.ProcessingInput(
            source=training_step.properties.ModelArtifacts.S3ModelArtifacts,
            destination="/opt/ml/processing/model"
        ),
        sagemaker.processing.ProcessingInput(
            source=preprocessing_step.properties.ProcessingOutputConfig.Outputs["test"].S3Output.S3Uri,
            destination="/opt/ml/processing/test"
        ),
    ],
    outputs=[
        sagemaker.processing.ProcessingOutput(
            output_name="evaluation",
            source="/opt/ml/processing/evaluation",
        )
    ],
    code="s3://tcpl-datalake/scripts/sagemaker/evaluate_demand.py",
    property_files=[evaluation_report],
)

# ── Step 4: Quality Gate (MAPE < 15%) ────────────────────────────────────────
from sagemaker.workflow.functions import JsonGet

mape_condition = ConditionLessThanOrEqualTo(
    left=JsonGet(
        step_name=evaluation_step.name,
        property_file=evaluation_report,
        json_path="regression_metrics.mape.value"
    ),
    right=0.15  # 15% MAPE threshold
)

fail_step = FailStep(
    name="ModelQualityFailed",
    error_message="Model MAPE exceeded 15% threshold. Deployment blocked."
)

# ── Step 5: Register & Deploy Model ──────────────────────────────────────────
model = Model(
    image_uri=xgb_image_uri,
    model_data=training_step.properties.ModelArtifacts.S3ModelArtifacts,
    sagemaker_session=session,
    role=role,
)

register_step = ModelStep(
    name="RegisterDemandForecastModel",
    step_args=model.register(
        content_types=["text/csv"],
        response_types=["text/csv"],
        inference_instances=["ml.m5.large"],
        transform_instances=["ml.m5.xlarge"],
        model_package_group_name="tcpl-demand-forecast",
        approval_status="PendingManualApproval",
        description=f"TCPL Demand Forecast XGBoost - Horizon {forecast_horizon}d",
    )
)

# ── Step 6: Batch Transform (Generate predictions) ────────────────────────────
transformer = Transformer(
    model_name=register_step.properties.ModelApprovalStatus,
    instance_type="ml.m5.xlarge",
    instance_count=1,
    output_path=f"s3://{bucket}/model-artifacts/demand_forecast/predictions/",
    sagemaker_session=session,
)

transform_step = TransformStep(
    name="TCPLBatchPredictions",
    transformer=transformer,
    inputs=sagemaker.inputs.TransformInput(
        data=f"s3://{bucket}/processed/test/",
        content_type="text/csv",
        split_type="Line",
    )
)

# ── Quality Gate Branch ───────────────────────────────────────────────────────
quality_gate = ConditionStep(
    name="ModelQualityGate",
    conditions=[mape_condition],
    if_steps=[register_step, transform_step],
    else_steps=[fail_step],
)

# ── Build and Submit Pipeline ─────────────────────────────────────────────────
pipeline = Pipeline(
    name="tcpl-demand-forecast-pipeline",
    parameters=[input_data_path, output_path, forecast_horizon, model_version],
    steps=[preprocessing_step, training_step, evaluation_step, quality_gate],
    sagemaker_session=session,
)

pipeline.upsert(role_arn=role)
print("[INFO] SageMaker pipeline upserted ✅")

# ── Model Monitor — Production Drift Detection ────────────────────────────────
from sagemaker.model_monitor import ModelMonitor, DataQualityMonitor

data_quality_monitor = DataQualityMonitor(
    role=role,
    instance_count=1,
    instance_type="ml.m5.large",
    volume_size_in_gb=20,
    max_runtime_in_seconds=3600,
)

data_quality_monitor.suggest_baseline(
    baseline_dataset=f"s3://{bucket}/processed/train/",
    dataset_format=sagemaker.model_monitor.DatasetFormat.csv(),
    output_s3_uri=f"s3://{bucket}/model-monitor/baseline/",
)

data_quality_monitor.create_monitoring_schedule(
    monitor_schedule_name="tcpl-demand-forecast-monitor",
    endpoint_input="tcpl-demand-forecast-endpoint",
    post_analytics_processor_script=None,
    output_s3_uri=f"s3://{bucket}/model-monitor/results/",
    statistics=data_quality_monitor.baseline_statistics(),
    constraints=data_quality_monitor.suggested_constraints(),
    schedule_cron_expression="cron(0 * ? * * *)",  # Every hour
    enable_cloudwatch_metrics=True,
)

print("[INFO] Model monitor schedule created ✅")
```

---

## 🏢 Layer 5: Snowflake Data Warehouse

### Database Architecture

```sql
-- ════════════════════════════════════════════════════════════
-- TCPL CPG Data Warehouse — Snowflake Schema Design
-- Client: TCPL | Vendor: Decision Point Analytics
-- ════════════════════════════════════════════════════════════

-- ── Warehouses ───────────────────────────────────────────────
CREATE WAREHOUSE TCPL_LOAD_WH
    WITH WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    COMMENT = 'Used by Glue ETL loads and Lambda inserts';

CREATE WAREHOUSE TCPL_TRANSFORM_WH
    WITH WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    COMMENT = 'Used for dbt transforms and stored procs';

CREATE WAREHOUSE TCPL_ANALYTICS_WH
    WITH WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'BI dashboards and analyst queries';

-- ── Database and Schemas ──────────────────────────────────────
CREATE DATABASE TCPL_CPG_DB;

CREATE SCHEMA TCPL_CPG_DB.RAW;        -- Landing zone from Glue
CREATE SCHEMA TCPL_CPG_DB.CURATED;    -- Cleaned, normalized
CREATE SCHEMA TCPL_CPG_DB.DW;         -- Star schema dimension/fact tables
CREATE SCHEMA TCPL_CPG_DB.MARTS;      -- Subject-area data marts
CREATE SCHEMA TCPL_CPG_DB.ML_OUTPUTS; -- SageMaker predictions
CREATE SCHEMA TCPL_CPG_DB.MONITORING; -- Pipeline health tracking

-- ══════════════════════════════════════════════════════════════
-- DIM TABLES
-- ══════════════════════════════════════════════════════════════
CREATE OR REPLACE TABLE TCPL_CPG_DB.DW.DIM_SKU (
    sku_key         INTEGER AUTOINCREMENT PRIMARY KEY,
    sku_code        VARCHAR(50) NOT NULL UNIQUE,
    sku_name        VARCHAR(200),
    brand           VARCHAR(100),   -- Tata Tea, Tetley, Himalayan, etc.
    category        VARCHAR(100),   -- Tea, Coffee, Salt, Pulses, etc.
    sub_category    VARCHAR(100),
    pack_size       VARCHAR(50),
    pack_type       VARCHAR(50),    -- Pouch, Box, Can
    mrp             DECIMAL(10,2),
    is_active       BOOLEAN DEFAULT TRUE,
    launch_date     DATE,
    discontinue_date DATE,
    created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE TCPL_CPG_DB.DW.DIM_GEOGRAPHY (
    geo_key         INTEGER AUTOINCREMENT PRIMARY KEY,
    region_code     VARCHAR(20) NOT NULL UNIQUE,
    region_name     VARCHAR(100),
    zone            VARCHAR(50),    -- North, South, East, West
    state           VARCHAR(100),
    city_tier       VARCHAR(20),    -- Metro, Tier1, Tier2, Rural
    population      BIGINT,
    created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE TCPL_CPG_DB.DW.DIM_CUSTOMER (
    customer_key    INTEGER AUTOINCREMENT PRIMARY KEY,
    customer_id     VARCHAR(50) NOT NULL UNIQUE,
    customer_name   VARCHAR(200),
    customer_type   VARCHAR(50),    -- Distributor, Wholesaler, Retailer
    channel         VARCHAR(50),    -- Modern Trade, General Trade, Ecommerce
    account_manager VARCHAR(100),
    credit_limit    DECIMAL(15,2),
    created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE TCPL_CPG_DB.DW.DIM_DATE (
    date_key        INTEGER PRIMARY KEY,           -- YYYYMMDD format
    full_date       DATE NOT NULL,
    year            INTEGER,
    quarter         INTEGER,
    month           INTEGER,
    month_name      VARCHAR(20),
    week_of_year    INTEGER,
    day_of_week     INTEGER,
    day_name        VARCHAR(20),
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN DEFAULT FALSE,
    fiscal_year     INTEGER,       -- TCPL fiscal year (Apr-Mar)
    fiscal_quarter  INTEGER,
    fiscal_month    INTEGER,
    is_festive_season BOOLEAN DEFAULT FALSE  -- Diwali, Puja, etc.
);

-- ══════════════════════════════════════════════════════════════
-- FACT TABLES
-- ══════════════════════════════════════════════════════════════
CREATE OR REPLACE TABLE TCPL_CPG_DB.DW.FACT_SALES (
    sales_key           INTEGER AUTOINCREMENT PRIMARY KEY,
    order_id            VARCHAR(100) NOT NULL,
    sku_key             INTEGER REFERENCES DW.DIM_SKU(sku_key),
    customer_key        INTEGER REFERENCES DW.DIM_CUSTOMER(customer_key),
    geo_key             INTEGER REFERENCES DW.DIM_GEOGRAPHY(geo_key),
    date_key            INTEGER REFERENCES DW.DIM_DATE(date_key),
    quantity            INTEGER,
    gross_revenue       DECIMAL(15,2),
    trade_discount      DECIMAL(15,2),
    net_revenue         DECIMAL(15,2),
    cost_of_goods       DECIMAL(15,2),
    gross_margin        DECIMAL(15,2),
    is_promo_sale       BOOLEAN DEFAULT FALSE,
    promo_id            VARCHAR(50),
    revenue_per_unit    DECIMAL(10,2),
    channel             VARCHAR(50),
    is_revenue_anomaly  BOOLEAN DEFAULT FALSE,
    load_date           DATE,
    source_system       VARCHAR(50) DEFAULT 'SAP',
    CONSTRAINT unique_order UNIQUE (order_id, sku_key)
)
CLUSTER BY (date_key, sku_key);

CREATE OR REPLACE TABLE TCPL_CPG_DB.DW.FACT_INVENTORY (
    inventory_key       INTEGER AUTOINCREMENT PRIMARY KEY,
    sku_key             INTEGER REFERENCES DW.DIM_SKU(sku_key),
    geo_key             INTEGER REFERENCES DW.DIM_GEOGRAPHY(geo_key),
    date_key            INTEGER REFERENCES DW.DIM_DATE(date_key),
    opening_stock       INTEGER,
    closing_stock       INTEGER,
    days_of_supply      DECIMAL(8,2),
    reorder_point       INTEGER,
    stockout_flag       BOOLEAN DEFAULT FALSE,
    overstock_flag      BOOLEAN DEFAULT FALSE,
    load_date           DATE
)
CLUSTER BY (date_key, sku_key);

-- ══════════════════════════════════════════════════════════════
-- ML OUTPUTS
-- ══════════════════════════════════════════════════════════════
CREATE OR REPLACE TABLE TCPL_CPG_DB.ML_OUTPUTS.DEMAND_FORECAST_PREDICTIONS (
    prediction_key      INTEGER AUTOINCREMENT PRIMARY KEY,
    sku_code            VARCHAR(50),
    region_code         VARCHAR(20),
    forecast_week       DATE,
    predicted_units     DECIMAL(12,2),
    prediction_lower_ci DECIMAL(12,2),
    prediction_upper_ci DECIMAL(12,2),
    actual_units        DECIMAL(12,2),         -- Filled in post-actuals
    mape                DECIMAL(8,4),           -- Filled in post-actuals
    load_date           DATE,
    model_version       VARCHAR(50),
    model_type          VARCHAR(50) DEFAULT 'XGBoost',
    CONSTRAINT unique_forecast UNIQUE (sku_code, region_code, forecast_week, model_version)
);

-- ══════════════════════════════════════════════════════════════
-- STORED PROCEDURES
-- ══════════════════════════════════════════════════════════════
CREATE OR REPLACE PROCEDURE TCPL_CPG_DB.PROCEDURES.LOAD_CURATED_DATA(JOB_DATE DATE)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Merge sales into DW fact table
    MERGE INTO TCPL_CPG_DB.DW.FACT_SALES t
    USING (
        SELECT
            s.order_id,
            sk.sku_key,
            c.customer_key,
            g.geo_key,
            TO_NUMBER(TO_VARCHAR(s.order_date, 'YYYYMMDD')) AS date_key,
            s.quantity,
            s.gross_revenue,
            s.trade_discount,
            s.net_revenue,
            s.cost_of_goods,
            s.net_revenue - s.cost_of_goods AS gross_margin,
            s.is_promo_sale,
            s.promo_id,
            s.revenue_per_unit,
            s.channel,
            s.is_revenue_anomaly,
            s.order_date AS load_date
        FROM TCPL_CPG_DB.CURATED.SALES_ORDERS s
        LEFT JOIN TCPL_CPG_DB.DW.DIM_SKU sk ON s.sku_code = sk.sku_code
        LEFT JOIN TCPL_CPG_DB.DW.DIM_CUSTOMER c ON s.customer_id = c.customer_id
        LEFT JOIN TCPL_CPG_DB.DW.DIM_GEOGRAPHY g ON s.region_code = g.region_code
        WHERE s.job_run_date = :JOB_DATE
    ) s ON t.order_id = s.order_id AND t.sku_key = s.sku_key
    WHEN MATCHED THEN UPDATE SET
        t.quantity = s.quantity,
        t.net_revenue = s.net_revenue,
        t.gross_margin = s.gross_margin
    WHEN NOT MATCHED THEN INSERT (
        order_id, sku_key, customer_key, geo_key, date_key,
        quantity, gross_revenue, trade_discount, net_revenue,
        cost_of_goods, gross_margin, is_promo_sale, promo_id,
        revenue_per_unit, channel, is_revenue_anomaly, load_date
    ) VALUES (
        s.order_id, s.sku_key, s.customer_key, s.geo_key, s.date_key,
        s.quantity, s.gross_revenue, s.trade_discount, s.net_revenue,
        s.cost_of_goods, s.gross_margin, s.is_promo_sale, s.promo_id,
        s.revenue_per_unit, s.channel, s.is_revenue_anomaly, s.load_date
    );

    RETURN 'SUCCESS: ' || SQLROWCOUNT || ' rows merged for date ' || :JOB_DATE;
END;
$$;

-- ══════════════════════════════════════════════════════════════
-- DATA MARTS (Analytics-facing views)
-- ══════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW TCPL_CPG_DB.MARTS.SALES_PERFORMANCE_MART AS
SELECT
    d.full_date,
    d.fiscal_year,
    d.fiscal_quarter,
    d.month_name,
    d.is_festive_season,
    sk.brand,
    sk.category,
    sk.sku_name,
    sk.pack_size,
    g.zone,
    g.state,
    g.city_tier,
    c.channel,
    c.customer_type,
    SUM(f.quantity)                                         AS total_units,
    SUM(f.net_revenue)                                      AS total_net_revenue,
    SUM(f.gross_margin)                                     AS total_gross_margin,
    ROUND(SUM(f.gross_margin) / NULLIF(SUM(f.net_revenue), 0) * 100, 2) AS gross_margin_pct,
    COUNT(DISTINCT f.order_id)                              AS order_count,
    COUNT(DISTINCT f.customer_key)                          AS unique_customers,
    SUM(CASE WHEN f.is_promo_sale THEN f.net_revenue ELSE 0 END)  AS promo_revenue,
    SUM(CASE WHEN f.is_revenue_anomaly THEN 1 ELSE 0 END)  AS anomaly_count
FROM TCPL_CPG_DB.DW.FACT_SALES f
JOIN TCPL_CPG_DB.DW.DIM_DATE d     ON f.date_key = d.date_key
JOIN TCPL_CPG_DB.DW.DIM_SKU sk     ON f.sku_key = sk.sku_key
JOIN TCPL_CPG_DB.DW.DIM_GEOGRAPHY g ON f.geo_key = g.geo_key
JOIN TCPL_CPG_DB.DW.DIM_CUSTOMER c  ON f.customer_key = c.customer_key
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14;

CREATE OR REPLACE VIEW TCPL_CPG_DB.MARTS.FORECAST_ACCURACY_MART AS
SELECT
    p.sku_code,
    p.region_code,
    p.forecast_week,
    p.predicted_units,
    p.actual_units,
    p.prediction_lower_ci,
    p.prediction_upper_ci,
    p.model_version,
    ABS(p.predicted_units - p.actual_units) / NULLIF(p.actual_units, 0) AS ape,
    CASE
        WHEN p.actual_units BETWEEN p.prediction_lower_ci AND p.prediction_upper_ci
        THEN 1 ELSE 0
    END AS within_ci
FROM TCPL_CPG_DB.ML_OUTPUTS.DEMAND_FORECAST_PREDICTIONS p
WHERE p.actual_units IS NOT NULL;
```

---

## 📊 Layer 6: Monitoring & Observability

### Airflow SLA + CloudWatch Dashboard Setup

```python
"""
Monitoring Stack: CloudWatch + SNS Alerting
"""

import boto3

cloudwatch = boto3.client("cloudwatch", region_name="ap-south-1")
sns = boto3.client("sns", region_name="ap-south-1")

# ── SNS Topic for Alerts ──────────────────────────────────────────────────────
topic = sns.create_topic(Name="tcpl-data-pipeline-alerts")
topic_arn = topic["TopicArn"]

sns.subscribe(
    TopicArn=topic_arn,
    Protocol="email",
    Endpoint="data-ops@decisionpoint.ai"
)

# ── CloudWatch Alarms ─────────────────────────────────────────────────────────
alarms = [
    {
        "AlarmName": "TCPL-GlueJob-HighDPUCost",
        "MetricName": "glue.driver.ExecutorRunTime",
        "Namespace": "Glue",
        "Threshold": 7200,  # 2 hours
        "ComparisonOperator": "GreaterThanThreshold",
        "EvaluationPeriods": 1,
        "Period": 300,
        "AlarmDescription": "Glue job running unusually long — check for data skew",
    },
    {
        "AlarmName": "TCPL-DataQuality-NullRateHigh",
        "MetricName": "NullRate",
        "Namespace": "TCPL/DataQuality",
        "Threshold": 0.05,
        "ComparisonOperator": "GreaterThanThreshold",
        "EvaluationPeriods": 1,
        "Period": 300,
        "AlarmDescription": "Null rate in curated dataset exceeded 5%",
    },
    {
        "AlarmName": "TCPL-SageMaker-ModelDrift",
        "MetricName": "baseline_violation_count",
        "Namespace": "aws/sagemaker/Endpoints/data-metrics",
        "Threshold": 10,
        "ComparisonOperator": "GreaterThanThreshold",
        "EvaluationPeriods": 3,
        "Period": 3600,
        "AlarmDescription": "SageMaker model monitor detected data drift",
    },
]

for alarm in alarms:
    cloudwatch.put_metric_alarm(
        AlarmName=alarm["AlarmName"],
        MetricName=alarm["MetricName"],
        Namespace=alarm["Namespace"],
        Threshold=alarm["Threshold"],
        ComparisonOperator=alarm["ComparisonOperator"],
        EvaluationPeriods=alarm["EvaluationPeriods"],
        Period=alarm["Period"],
        Statistic="Average",
        ActionsEnabled=True,
        AlarmActions=[topic_arn],
        AlarmDescription=alarm["AlarmDescription"],
    )

print("✅ CloudWatch alarms configured")
```

---

## 🏗️ Infrastructure as Code (Terraform)

```hcl
# terraform/main.tf
# TCPL CPG Data Platform — AWS Infrastructure

provider "aws" {
  region = "ap-south-1"
}

# ── S3 Data Lake ───────────────────────────────────────────────
resource "aws_s3_bucket" "tcpl_datalake" {
  bucket = "tcpl-datalake-prod"
  tags = {
    Project     = "TCPL-CPG-Analytics"
    Owner       = "DecisionPointAnalytics"
    Environment = "Production"
  }
}

resource "aws_s3_bucket_versioning" "datalake_versioning" {
  bucket = aws_s3_bucket.tcpl_datalake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_lifecycle_configuration" "datalake_lifecycle" {
  bucket = aws_s3_bucket.tcpl_datalake.id
  rule {
    id     = "archive-raw-data"
    status = "Enabled"
    filter { prefix = "raw/" }
    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 365
      storage_class = "GLACIER"
    }
  }
}

# ── Glue Catalog ───────────────────────────────────────────────
resource "aws_glue_catalog_database" "tcpl_catalog" {
  name = "tcpl_cpg_catalog"
}

resource "aws_glue_crawler" "tcpl_raw_crawler" {
  name          = "tcpl-raw-data-crawler"
  database_name = aws_glue_catalog_database.tcpl_catalog.name
  role          = aws_iam_role.glue_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.tcpl_datalake.bucket}/curated/"
  }

  schedule = "cron(0 6 * * ? *)"  # Run daily after ETL
}

# ── MWAA (Airflow) ────────────────────────────────────────────
resource "aws_mwaa_environment" "tcpl_airflow" {
  name               = "tcpl-cpg-airflow"
  airflow_version    = "2.8.1"
  environment_class  = "mw1.medium"
  max_workers        = 10
  min_workers        = 1
  source_bucket_arn  = aws_s3_bucket.tcpl_datalake.arn
  dag_s3_path        = "dags/"
  execution_role_arn = aws_iam_role.mwaa_role.arn

  network_configuration {
    security_group_ids = [aws_security_group.mwaa_sg.id]
    subnet_ids         = aws_subnet.private[*].id
  }

  logging_configuration {
    dag_processing_logs { enabled = true; log_level = "WARNING" }
    scheduler_logs      { enabled = true; log_level = "WARNING" }
    task_logs           { enabled = true; log_level = "INFO" }
    webserver_logs      { enabled = true; log_level = "WARNING" }
    worker_logs         { enabled = true; log_level = "WARNING" }
  }

  tags = {
    Project = "TCPL-CPG-Analytics"
    Owner   = "DecisionPointAnalytics"
  }
}

# ── SageMaker Domain ──────────────────────────────────────────
resource "aws_sagemaker_domain" "tcpl_domain" {
  domain_name             = "tcpl-cpg-sagemaker"
  auth_mode               = "IAM"
  vpc_id                  = aws_vpc.main.id
  subnet_ids              = aws_subnet.private[*].id

  default_user_settings {
    execution_role = aws_iam_role.sagemaker_role.arn
  }
}

# ── Secrets Manager ───────────────────────────────────────────
resource "aws_secretsmanager_secret" "snowflake_creds" {
  name        = "tcpl-snowflake-credentials"
  description = "Snowflake connection credentials for TCPL CPG pipeline"
  recovery_window_in_days = 30
}

resource "aws_secretsmanager_secret_version" "snowflake_creds" {
  secret_id = aws_secretsmanager_secret.snowflake_creds.id
  secret_string = jsonencode({
    sfURL      = var.snowflake_url
    sfUser     = var.snowflake_user
    sfPassword = var.snowflake_password
    account    = var.snowflake_account
  })
}

# ── Lambda ───────────────────────────────────────────────────
resource "aws_lambda_function" "preflight_check" {
  filename         = "lambdas/preflight_check.zip"
  function_name    = "tcpl-preflight-source-check"
  role             = aws_iam_role.lambda_role.arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.11"
  timeout          = 60
  memory_size      = 256

  environment {
    variables = {
      SNS_ALERT_TOPIC_ARN = aws_sns_topic.alerts.arn
    }
  }
}

resource "aws_lambda_function" "data_quality_validator" {
  filename         = "lambdas/data_quality.zip"
  function_name    = "tcpl-data-quality-validator"
  role             = aws_iam_role.lambda_role.arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.11"
  timeout          = 300
  memory_size      = 512

  environment {
    variables = {
      SNS_ALERT_TOPIC_ARN    = aws_sns_topic.alerts.arn
      CLOUDWATCH_NAMESPACE   = "TCPL/DataQuality"
    }
  }
}

# ── SNS ───────────────────────────────────────────────────────
resource "aws_sns_topic" "alerts" {
  name = "tcpl-pipeline-alerts"
}

resource "aws_sns_topic_subscription" "email_alerts" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = "data-ops@decisionpoint.ai"
}
```

---

## 🔄 Complete Flow Summary

```
╔══════════════════════════════════════════════════════════════════════════╗
║              TCPL × DECISION POINT — DAILY PIPELINE EXECUTION           ║
╚══════════════════════════════════════════════════════════════════════════╝

02:00 AM IST — Airflow DAG Triggered
    │
    ├─► [Lambda] Pre-flight: All SAP/Nielsen files present? ──► NO ──► Alert + Halt
    │                                                          │
    │                                                         YES
    │
    ├─► [Glue] SAP Sales ETL  ─────────────────────────────┐
    ├─► [Glue] Inventory ETL  ─── (parallel) ───────────────┤──► S3 Curated
    └─► [Glue] Nielsen/Market ETL ─────────────────────────┘
    │
    ├─► [Lambda] Data Quality Validation
    │       Checks: Row count, null rate, duplicates, schema
    │       Emits: CloudWatch metrics
    │       Branch: PASS → continue │ FAIL → Slack alert + halt
    │
    ├─► [Snowflake] LOAD_CURATED_DATA stored procedure
    │       MERGE sales/inventory into DW fact tables
    │
    ├─► [Glue] Feature Engineering
    │       Builds: Lag features, rolling avg, promo flags, seasonality
    │       Output: s3://tcpl-datalake/ml-features/
    │
    ├─► [SageMaker Pipeline]
    │       Step 1: Preprocessing
    │       Step 2: XGBoost Training
    │       Step 3: Evaluation (MAPE check < 15%)
    │       Step 4: Register model if pass
    │       Step 5: Batch transform → predictions CSV → S3
    │
    ├─► [Lambda] Load Predictions
    │       MERGE predictions into Snowflake ML_OUTPUTS
    │
    ├─► [Snowflake] Refresh Data Marts
    │       SALES_PERFORMANCE_MART
    │       FORECAST_ACCURACY_MART
    │       PROMO_EFFECTIVENESS_MART
    │
    └─► [Lambda] Success Notification → Slack #tcpl-pipeline-success

~06:00 AM IST — Dashboards Updated, Business Ready
```

---

## 💼 Interview Answer (Senior Level)

> "At TCPL, we built a fully automated CPG analytics platform on AWS for Decision Point Analytics. The orchestration layer is **Apache Airflow on MWAA**, which acts as the brain of the entire pipeline — it triggers everything but processes nothing itself.
>
> Data flows from SAP S4/HANA and Nielsen into S3 as the raw data lake. **AWS Glue PySpark jobs** handle all heavy ETL — cleansing, deduplication, schema normalization, and feature engineering for ML. These jobs are triggered by Airflow on a daily schedule.
>
> **AWS Lambda** handles all lightweight tasks — pre-flight checks to validate that source files have landed, data quality validation with CloudWatch metric emission, and writing ML predictions back into Snowflake.
>
> For ML, **SageMaker Pipelines** run our demand forecasting model end-to-end — preprocessing, XGBoost training, automated MAPE evaluation with a quality gate, model registration, and batch transform. If MAPE exceeds 15%, the pipeline auto-fails and blocks deployment.
>
> **Snowflake** is our serving layer — star schema with fact/dim tables, curated marts for BI, and a dedicated ML outputs schema. Analysts use Tableau connecting to Snowflake marts, and they never touch raw data.
>
> Infrastructure is provisioned via Terraform, monitored via CloudWatch alarms, and alerting flows through SNS to Slack."

---

## 📋 Component Responsibility Matrix

| Component | Trigger | Input | Output | Owner |
|---|---|---|---|---|
| Airflow MWAA | Schedule (cron) | DAG config | Task execution | Decision Point |
| AWS Glue (ETL) | Airflow | S3 raw | S3 curated + Snowflake | Decision Point |
| AWS Lambda (Preflight) | Airflow | S3 paths | Pass/Fail JSON | Decision Point |
| AWS Lambda (DQ) | Airflow | S3 curated | CloudWatch metrics | Decision Point |
| AWS Lambda (Predictions) | Airflow | S3 predictions CSV | Snowflake ML table | Decision Point |
| SageMaker Pipeline | Airflow | S3 features | S3 predictions + model | Decision Point |
| Snowflake (Load) | Airflow | Glue → S3 | DW fact/dim tables | Decision Point |
| Snowflake (Marts) | Airflow | DW tables | Analytics views | Decision Point |
| Tableau / PowerBI | User | Snowflake marts | Dashboards | TCPL Business |

---

*Simulated Architecture — Decision Point Analytics × TCPL | Data Engineering + MLOps*
