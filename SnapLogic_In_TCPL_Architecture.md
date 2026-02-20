# SnapLogic — Where It Fits in the TCPL Architecture
## "The Bridge Between SAP and AWS"

---

## 🔑 ONE LINE ANSWER

```
SnapLogic sits BEFORE everything else.
It is the tool that pulls data OUT of SAP / ERP
and pushes it INTO AWS S3.

After that — Glue, Airflow, SageMaker, Snowflake take over.
```

---

## 🧠 Simple Mental Model

```
WITHOUT SnapLogic (manual way):
──────────────────────────────
  SAP → manual export → CSV file → someone uploads to S3 → messy, error-prone

WITH SnapLogic (what TCPL does):
─────────────────────────────────
  SAP → SnapLogic pipeline → S3 automatically → clean, scheduled, reliable
```

---

## 🗺️ WHERE IT SITS IN THE FULL ARCHITECTURE

```
┌─────────────────────────────────────────────────────────────────────┐
│                        FULL TCPL PIPELINE                           │
└─────────────────────────────────────────────────────────────────────┘

  ┌──────────────┐
  │  SAP S4/HANA │  ← Lives inside TCPL's internal network
  │  Oracle ERP  │
  │  Trade Promo │
  │  Portal      │
  └──────┬───────┘
         │
         │  ◄── SnapLogic lives RIGHT HERE
         │
         ▼
  ┌──────────────────────────────────────────────────────┐
  │                   SNAPLOGIC                          │
  │         (Integration / Ingestion Layer)              │
  │                                                      │
  │  Connects to SAP → extracts data → transforms lightly│
  │  → loads into S3 or directly into Snowflake          │
  └──────────────────────┬───────────────────────────────┘
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
       AWS S3 (raw/)          Snowflake RAW schema
       (data lake)            (direct load path)
              │
              ▼
       Airflow → Glue → Lambda → SageMaker → Snowflake
       (everything you already know)
```

---

## 🔍 WHAT EXACTLY IS SNAPLOGIC?

```
SnapLogic is an iPaaS tool
(Integration Platform as a Service)

It is a NO-CODE / LOW-CODE visual drag-and-drop tool.
You build pipelines by connecting "Snaps" (pre-built connectors).

Think of it like:
─────────────────
  LEGO blocks for data pipelines.
  Each block (Snap) does one thing:
    - Read from SAP
    - Filter rows
    - Map/rename columns
    - Write to S3
    - Write to Snowflake

You drag and drop them, connect them visually.
No PySpark. No heavy coding needed.

WHO USES IT AT TCPL:
─────────────────────
  Usually the INTEGRATION team or BASIS/SAP team
  They set up the SnapLogic pipelines
  Decision Point team then picks up from S3 onwards
```

---

## 🔌 WHAT SNAPS (CONNECTORS) TCPL LIKELY USES

```
┌─────────────────────────────────────────────────────────────┐
│                    SNAPLOGIC PIPELINE                        │
│                                                             │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌───────┐ │
│  │  SAP     │───►│  Mapper  │───►│  Filter  │───►│  S3   │ │
│  │  Snap    │    │  Snap    │    │  Snap    │    │  Snap │ │
│  │          │    │          │    │          │    │       │ │
│  │ Reads    │    │ Renames  │    │ Removes  │    │ Writes│ │
│  │ SAP BAPI │    │ columns  │    │ bad rows │    │ to S3 │ │
│  │ or RFC   │    │          │    │          │    │parquet│ │
│  └──────────┘    └──────────┘    └──────────┘    └───────┘ │
│                                                             │
│  OR directly:                                               │
│                                                             │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────────┐  │
│  │  SAP     │───►│  Mapper  │───►│  Snowflake Bulk Load │  │
│  │  Snap    │    │  Snap    │    │  Snap                │  │
│  └──────────┘    └──────────┘    └──────────────────────┘  │
└─────────────────────────────────────────────────────────────┘

KEY SNAPS USED:
───────────────
  SAP Snap          → connects to SAP via RFC/BAPI/IDoc
  Mapper Snap       → rename/transform columns (like SELECT in SQL)
  Filter Snap       → filter rows (like WHERE clause)
  CSV/Parquet Snap  → convert file format
  S3 Snap           → write to AWS S3
  Snowflake Snap    → write directly to Snowflake (bulk load)
  Script Snap       → run custom Python if needed
```

---

## ⚡ SNAPLOGIC vs GLUE — KEY DIFFERENCE

```
┌─────────────────┬─────────────────────────────┬──────────────────────────┐
│                 │  SNAPLOGIC                  │  AWS GLUE                │
├─────────────────┼─────────────────────────────┼──────────────────────────┤
│ PURPOSE         │ Connect source systems       │ Heavy ETL transformation │
│                 │ to cloud (ingestion)         │ (cleaning, enriching)    │
├─────────────────┼─────────────────────────────┼──────────────────────────┤
│ CODING          │ Low-code / drag-and-drop     │ PySpark code             │
├─────────────────┼─────────────────────────────┼──────────────────────────┤
│ DATA VOLUME     │ Medium (extracts from SAP)   │ Large (processes GBs)    │
├─────────────────┼─────────────────────────────┼──────────────────────────┤
│ WHO USES IT     │ Integration/SAP team         │ Data engineers           │
├─────────────────┼─────────────────────────────┼──────────────────────────┤
│ WHEN IT RUNS    │ BEFORE Glue (ingestion)      │ AFTER SnapLogic          │
├─────────────────┼─────────────────────────────┼──────────────────────────┤
│ SAP CONNECTION  │ ✅ Native SAP Snaps          │ ❌ No native SAP conn.   │
├─────────────────┼─────────────────────────────┼──────────────────────────┤
│ TRANSFORMATION  │ Light (rename, filter)       │ Heavy (joins, windows,   │
│                 │                             │ ML features, agg)        │
├─────────────────┼─────────────────────────────┼──────────────────────────┤
│ SNOWFLAKE LOAD  │ ✅ Can load directly         │ ✅ Can load directly      │
└─────────────────┴─────────────────────────────┴──────────────────────────┘

SIMPLE WAY TO REMEMBER:
────────────────────────
  SnapLogic = FETCH data from source systems
  Glue      = PROCESS and CLEAN that data
  
  SnapLogic brings the raw ingredients.
  Glue cooks them.
```

---

## 🔄 TWO WAYS SNAPLOGIC IS USED AT TCPL

```
PATH 1 — SnapLogic → S3 → Glue → Snowflake  (most common)
──────────────────────────────────────────────────────────

  SAP
   │
   ▼
  SnapLogic Pipeline
  (extracts SAP sales daily, basic mapping)
   │
   ▼
  s3://tcpl-datalake/raw/sap/sales_orders/  ← lands here as parquet
   │
   ▼
  Airflow triggers Glue ETL job
   │
   ▼
  Glue cleans, transforms, enriches
   │
   ▼
  s3://tcpl-datalake/curated/sales/
   │
   ▼
  Snowflake FACT_SALES table


PATH 2 — SnapLogic → Snowflake DIRECT  (for simpler/reference data)
─────────────────────────────────────────────────────────────────────

  SAP Master Data (SKU master, Customer master, Region hierarchy)
   │
   ▼
  SnapLogic Pipeline
  (these don't need heavy Glue processing)
   │
   ▼
  Snowflake DIM tables directly
  (DIM_SKU, DIM_CUSTOMER, DIM_GEOGRAPHY)
  
  WHY DIRECT FOR MASTER DATA?
  - These tables don't change often
  - Small volume (thousands of rows, not millions)
  - Light transformation only (rename columns)
  - No need to go through full Glue pipeline
```

---

## 📋 UPDATED ARCHITECTURE WITH SNAPLOGIC

```
╔══════════════════════════════════════════════════════════════════════╗
║              TCPL FULL ARCHITECTURE (WITH SNAPLOGIC)                 ║
╚══════════════════════════════════════════════════════════════════════╝

  [SAP S4/HANA]  [Oracle ERP]  [Nielsen API]  [Trade Promo Portal]
        │               │              │                │
        └───────────────┴──────────────┴────────────────┘
                                 │
                                 ▼
                    ╔════════════════════╗
                    ║    SNAPLOGIC       ║  ← NEW LAYER (Ingestion)
                    ║  (iPaaS Bridge)    ║
                    ║                   ║
                    ║ • SAP Snap        ║
                    ║ • Mapper Snap     ║
                    ║ • Filter Snap     ║
                    ║ • S3 Snap         ║
                    ║ • Snowflake Snap  ║
                    ╚════════════════════╝
                         │           │
              ┌──────────┘           └────────────┐
              ▼                                    ▼
    [S3 raw/ zone]                    [Snowflake DIM tables]
    (transaction data)                (master data — direct)
              │
              ▼
    ╔═════════════════╗
    ║ APACHE AIRFLOW  ║  ← Orchestrates from here
    ║ (MWAA) - Brain  ║
    ╚═════════════════╝
          │    │    │
          ▼    ▼    ▼
      [Glue] [Lambda] [SageMaker]
          │    │    │
          └────┴────┘
                │
                ▼
         [SNOWFLAKE DW]
                │
                ▼
         [Tableau / PowerBI]
```

---

## 💡 HOW TO THINK ABOUT IT ON DAY 1

```
QUESTION:  "How does SAP data get into our S3 bucket?"
ANSWER:    "SnapLogic pipeline extracts it from SAP daily
            and lands it in s3://tcpl-datalake/raw/sap/"

QUESTION:  "Who manages SnapLogic?"
ANSWER:    "Usually the integration team or TCPL's IT team.
            Decision Point picks up from S3 onwards."

QUESTION:  "Can SnapLogic replace Glue?"
ANSWER:    "No. SnapLogic does LIGHT transformation only.
            Glue handles heavy PySpark processing —
            rolling windows, ML features, deduplication at scale.
            SnapLogic can't do that efficiently."

QUESTION:  "Does Airflow trigger SnapLogic?"
ANSWER:    "Sometimes yes — Airflow can call SnapLogic REST API
            to trigger a pipeline.
            Or SnapLogic runs on its own schedule and
            Airflow just waits for the S3 file to appear."
```

---

## 🧩 FINAL COMPONENT TABLE (ALL 6 NOW)

```
┌──────────────┬────────────────────────────┬──────────────────────────────┐
│  COMPONENT   │  ROLE                      │  SIMPLE ANALOGY              │
├──────────────┼────────────────────────────┼──────────────────────────────┤
│ SnapLogic    │ 🚚 Delivery Truck          │ Picks up raw data from SAP   │
│              │    (Ingestion/Integration) │ and delivers to S3           │
├──────────────┼────────────────────────────┼──────────────────────────────┤
│ Airflow      │ 🧠 Project Manager         │ Tells everyone when to work  │
│ (MWAA)       │    (Orchestrator)          │ Does nothing itself          │
├──────────────┼────────────────────────────┼──────────────────────────────┤
│ AWS Glue     │ 🏭 Factory                 │ Heavy cleaning & processing  │
│              │    (ETL Engine)            │ PySpark at scale             │
├──────────────┼────────────────────────────┼──────────────────────────────┤
│ AWS Lambda   │ 🔔 Security Guard          │ Quick checks, validations,   │
│              │    (Lightweight tasks)     │ alerts, small loads          │
├──────────────┼────────────────────────────┼──────────────────────────────┤
│ SageMaker    │ 🤖 ML Lab                  │ Trains models, predicts      │
│              │    (ML Platform)           │ demand, detects drift        │
├──────────────┼────────────────────────────┼──────────────────────────────┤
│ Snowflake    │ 🏢 Data Warehouse          │ Final clean home for all     │
│              │    (Serving Layer)         │ data — BI queries run here   │
└──────────────┴────────────────────────────┴──────────────────────────────┘

DATA FLOW ORDER:
─────────────────
SAP → SnapLogic → S3 → Airflow → Glue → Lambda → Snowflake → Tableau
                            └──────────► SageMaker ─────────┘
```

---

*SnapLogic = the truck that brings raw materials to the factory gate.
Everything inside the factory (Glue, Airflow, Lambda, SageMaker) is Decision Point's work.*
