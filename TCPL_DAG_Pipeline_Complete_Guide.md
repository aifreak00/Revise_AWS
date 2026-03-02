# ⚙️ TCPL Pipeline Priority & DAG Guide — Complete Understanding
## For Data Engineers at Decision Point
### *Everything about the 230 Airflow DAGs — what they are, why they exist, what YOU need to focus on*

---

> **Why this document exists:** Your manager shared the Pipeline Priority document written by Hamza. It lists 230 Airflow DAGs across the TCPL platform. If you don't understand what these DAGs do and WHY they are prioritized the way they are, you can't support or work on this platform confidently. This document explains every DAG group, every priority level, and exactly what YOU as a data engineer need to know and focus on.

---

# 📖 TABLE OF CONTENTS

1. [What is a DAG — Quick Refresh](#1-what-is-a-dag)
2. [Why This Document Was Created — The Snowflake Outage Story](#2-why-this-document-was-created)
3. [The Priority System — What Each Level Means](#3-the-priority-system)
4. [The Numbers — Platform at a Glance](#4-the-numbers)
5. [PRIORITY 0 — MISSION CRITICAL (58 DAGs) — Deep Dive](#5-priority-0-mission-critical)
   - 0.1 Core CFA & SAP Transactional (39 DAGs)
   - 0.2 CFA Checkpoint & Orchestration (3 DAGs) ⭐
   - 0.3 Post-CFA Cleanup (5 DAGs)
   - 0.4 DB ARS — Distributor Auto Replenishment (11 DAGs)
6. [PRIORITY 1 — CRITICAL (22 DAGs) — Deep Dive](#6-priority-1-critical)
   - 1.1 Infiniti Tea Platform (12 DAGs)
   - 1.2 Infiniti Delta Pipelines (6 DAGs)
   - 1.3 ML Models (4 DAGs)
7. [PRIORITY 2 — IMPORTANT (89 DAGs) — Deep Dive](#7-priority-2-important)
   - 2.1 Data Marts & Sales (18 DAGs)
   - 2.2 Finance (14 DAGs)
   - 2.3 SAP S4 Masters & Reference (20 DAGs)
   - 2.4 OTIF (5 DAGs)
   - 2.5 ARKIEVA India (2 DAGs)
   - 2.6 ARKIEVA International (2 DAGs)
   - 2.7 Zycus Procurement (4 DAGs)
   - 2.8 Pegasus (4 DAGs)
   - 2.9 PSO & Planning (4 DAGs)
   - 2.10 MDM & Unify (4 DAGs)
   - 2.11 Blue Planner & Blue Yonder (2 DAGs)
   - 2.12 HR SuccessFactor (2 DAGs)
   - 2.13 CFA Month-End (8 DAGs)
8. [PRIORITY 3 — LOW PRIORITY (35 DAGs)](#8-priority-3-low-priority)
9. [UTILITY DAGs (15 DAGs)](#9-utility-dags)
10. [PAUSED DAGs (11 DAGs)](#10-paused-dags)
11. [The Master Flow — How All DAGs Connect](#11-the-master-flow)
12. [Project-wise Breakdown — Who Owns What](#12-project-wise-breakdown)
13. [What YOU Should Focus On — Priority Guide for New Engineers](#13-what-you-should-focus-on)
14. [Outage Recovery Playbook — What to Do When Things Break](#14-outage-recovery-playbook)
15. [Scheduling Patterns — Understand Every Schedule Type](#15-scheduling-patterns)
16. [Master Glossary — Every Term in This Document](#16-master-glossary)

---

# 1. WHAT IS A DAG — QUICK REFRESH

Before anything, let's make sure the foundation is clear.

**DAG = Directed Acyclic Graph**

A DAG in Apache Airflow is a **pipeline definition file** (written in Python) that says:
- What tasks need to run
- In what order
- On what schedule
- What to do if something fails

Think of a DAG as a **recipe**:
- The recipe says what ingredients to use (data sources)
- What cooking steps to follow (SnapLogic extract → Glue transform → Snowflake load)
- In what order (you can't bake before mixing)
- The recipe runs on a timer (every night at 10 PM)

**TCPL has 230 such recipes running across the platform.**

Each DAG is responsible for ONE specific data flow. For example:
- One DAG pulls billing data from SAP every hour
- Another DAG cleans and loads it into Snowflake
- Another DAG builds the sales mart from the cleaned data
- Another DAG refreshes ThoughtSpot cache after the mart is built

All 230 DAGs together form the COMPLETE data platform.

---

# 2. WHY THIS DOCUMENT WAS CREATED — THE SNOWFLAKE OUTAGE STORY

## The Event: December 16-17, 2025

This prioritization document was created because of a **real incident** — a Snowflake outage that lasted across December 16-17, 2025.

**What happened:**
Snowflake (the data warehouse) went down or had a serious issue. When Snowflake is unavailable:
- ALL pipelines that write to Snowflake fail
- All 230 DAGs start failing or backing up
- Data stops flowing across the entire platform

**The problem when Snowflake came back:**
Once Snowflake recovered, the team had to decide: **which pipelines do we run first?**

If you just run all 230 DAGs at once:
- Massive load on Snowflake simultaneously
- Some pipelines depend on others — if you run them in wrong order, they fail because their upstream data isn't ready
- Revenue-impacting operations (billing, CFA) wait while non-critical reports run

**Without a priority document:**
The team was guessing — "which one is more important, the sales mart or the Infiniti Tea pipeline?"

**The solution:**
Hamza created this document so that:
- Everyone knows EXACTLY which 58 DAGs to run FIRST (Mission Critical)
- Then which 22 (Critical)
- Then which 89 (Important)
- Teams can communicate clearly: "Priority 0 is up. Starting Priority 1 now."

**The lesson for you:**
This document is NOT just theoretical. It is an **operational survival guide** that was born from a real crisis. Learn it well.

---

# 3. THE PRIORITY SYSTEM — WHAT EACH LEVEL MEANS

## The 5 Priority Levels

```
┌──────────────────────────────────────────────────────────────────────────┐
│  PRIORITY 0 — MISSION CRITICAL (58 DAGs)                                │
│  Business Impact: Revenue stops. Operations are BLOCKED.                 │
│  Examples: Billing, Sales Orders, CFA operations                         │
│  → If these don't run, TCPL cannot invoice distributors.                 │
│    No invoice = no money collected. This is the most severe impact.      │
└──────────────────────────────────────────────────────────────────────────┘
         ↓ (recover first, then move to Priority 1)
┌──────────────────────────────────────────────────────────────────────────┐
│  PRIORITY 1 — CRITICAL (22 DAGs)                                        │
│  Business Impact: Severe operational impact.                             │
│  Examples: Infiniti Tea procurement, ML models                           │
│  → Tea buying decisions are blocked. ML predictions stop.                │
│    Not as immediate as revenue, but severely impacts operations.         │
└──────────────────────────────────────────────────────────────────────────┘
         ↓
┌──────────────────────────────────────────────────────────────────────────┐
│  PRIORITY 2 — IMPORTANT (89 DAGs)                                       │
│  Business Impact: Can wait a few hours.                                  │
│  Examples: Data marts, Finance reports, OTIF, planning systems           │
│  → Dashboards show stale data. Business users can't do analysis.         │
│    Painful but not revenue-blocking.                                     │
└──────────────────────────────────────────────────────────────────────────┘
         ↓
┌──────────────────────────────────────────────────────────────────────────┐
│  PRIORITY 3 — LOW PRIORITY (35 DAGs)                                    │
│  Business Impact: Minimal. Can delay without problem.                    │
│  Examples: Audit logs, monitoring, monthly reports, SharePoint           │
│  → Nobody's day is ruined if these are delayed.                          │
└──────────────────────────────────────────────────────────────────────────┘
         ↓
┌──────────────────────────────────────────────────────────────────────────┐
│  UTILITY (15 DAGs) — Not part of recovery                               │
│  Monitoring, metadata, test DAGs                                         │
│  → These support the platform but don't move business data.              │
└──────────────────────────────────────────────────────────────────────────┘
```

## The Concept of RTO (Recovery Time Objective)

The document shows "RTO = TBD" (To Be Determined) for all priority levels.

**What is RTO?**
RTO = the maximum acceptable time before a pipeline must be restored after an outage.

Example: If Priority 0 RTO is set to "2 hours", it means if Snowflake goes down at 10 PM, Priority 0 pipelines must be running again by midnight.

**Why is it TBD?**
The team hasn't finalized the exact RTO targets yet. This is likely future work. But the priority ORDER is clear — always recover 0 first, then 1, then 2, then 3.

---

# 4. THE NUMBERS — PLATFORM AT A GLANCE

## DAG Count by Priority

```
TOTAL PLATFORM: 230 DAGs
═══════════════════════════════════════════════════════════

Priority 0 — Mission Critical   ████████████   58 DAGs  (25%)
Priority 1 — Critical           ████           22 DAGs  (10%)
Priority 2 — Important          ████████████████████████  89 DAGs  (39%)
Priority 3 — Low Priority       █████████      35 DAGs  (15%)
UTILITY                         ████            15 DAGs  (7%)
PAUSED                          ███             11 DAGs  (5%)

═══════════════════════════════════════════════════════════
Active (non-paused): 219 DAGs running every day
```

## Key Facts to Remember

| Fact | Number |
|---|---|
| Total DAGs | 230 |
| Active (non-paused) | 219 |
| Mission Critical | 58 |
| Most frequent DAG | Every 30 minutes (appointment details — 19,953 runs!) |
| Highest Priority project | SAP S4 (47 total DAGs, 23 at Priority 0) |
| CFA-related DAGs | 20 across priorities |
| Finance DAGs | 15 across priorities |

---

# 5. PRIORITY 0 — MISSION CRITICAL (58 DAGs) — DEEP DIVE

## Why Priority 0 Exists and What "Mission Critical" Really Means

Priority 0 is the heart of the platform. These are the pipelines that keep TCPL's **revenue operations running**.

Without Priority 0:
- Distributors cannot get their orders processed
- CFA cannot dispatch goods
- TCPL cannot generate invoices
- No invoice = no money collected from distributors
- The entire supply chain stalls

These are divided into 4 sub-groups (0.1 → 0.2 → 0.3 → 0.4):

```
PRIORITY 0 FLOW

0.1 Core SAP & CFA Transactional (39 DAGs)
           ↓ (all must complete)
0.2 CFA Checkpoint & Orchestration (3 DAGs) ← THE MASTER GATE
           ↓ (checkpoint passes)
0.3 Post-CFA Cleanup (5 DAGs)
           
0.4 DB ARS (11 DAGs) ← runs independently in parallel
```

---

## 5.1 Priority 0.1 — Core CFA & SAP Transactional (39 DAGs)

### What is this layer?

These 39 DAGs are called the **"Foundation Layer"** — they must run FIRST because everything else depends on them having fresh data in Snowflake.

They extract the most important transactional data from SAP and load it into Snowflake in near real-time.

### The Most Critical DAGs in this group:

---

#### 🔴 Sales Order DAG (Hourly)
```
DAG Name: prd-saps4-hourly-sales-order-deltaload-pipeline
Schedule: Every hour at :00 (12:00, 1:00, 2:00... all 24 hours)
Source: SAP S4
Note: "Revenue blocking"
```

**What it does:**
Every hour, this DAG pulls the latest Sales Orders from SAP and loads them into Snowflake.

**Why it's revenue-blocking:**
The CFA system uses Sales Order data to know what to dispatch. If Sales Orders aren't in Snowflake, the CFA doesn't know what to ship. If CFA doesn't ship, distributors don't receive goods, TCPL can't bill = no revenue.

**What YOU need to know:**
- This runs 24 times per day (every hour)
- If this fails at 2 PM and isn't noticed until 4 PM, you've missed 2 hourly loads
- Missing hours can cause the CFA checkpoint (0.2) to fail because it won't have complete data

---

#### 🔴 Billing Document DAG (Hourly)
```
DAG Name: prd-saps4-hourly-billing-document-delta-load-pipeline
Schedule: Every hour at :00
Source: SAP S4
Note: "Billing runs"
```

**What it does:**
Pulls new/updated billing documents from SAP every hour.

**Why it's critical:**
The billing document is when the sale is officially recorded. If this DAG doesn't run:
- Snowflake doesn't have the latest billing data
- Revenue dashboards in ThoughtSpot show wrong numbers
- Finance cannot see current day's revenue
- Any downstream processes that depend on billing data are starved

---

#### 🔴 Outbound Delivery DAG (Hourly)
```
DAG Name: prd-saps4-hourly-outbound-delivery-delta-load-pipeline
Schedule: Every hour at :45
Source: SAP S4
Note: "Delivery tracking"
```

**What it does:**
Tracks all delivery orders from SAP — when goods were picked, when dispatched, when delivered.

**Why it's critical:**
The CFA needs to know which deliveries are in progress. If delivery data is stale, CFA can't track shipments or verify receipts.

---

#### 🔴 Customer Credit Management DAG (Hourly)
```
DAG Name: prd-saleskpi-saps4-customercreditmgmt-hourly-fullload-pipeline
Schedule: Every hour at :10
Source: SAP S4
Note: "Credit mgmt — CFA checkpoint waits"
```

**What it does:**
Pulls current credit status of all distributors from SAP every hour.

**Why it's critical:**
Remember from the business document: Before CFA dispatches goods, it checks if the distributor has adequate credit. This check uses data from Snowflake. If credit management data is stale, either:
- CFA approves a delivery for a distributor who actually doesn't have credit (TCPL loses money)
- CFA blocks a delivery for a distributor who actually has credit (delivery delayed unnecessarily)

The note "CFA checkpoint waits" means the CFA daily checkpoint (DAG 0.2) will NOT proceed until this DAG has run successfully.

---

#### 🔴 SAP Core Delta Load (Bi-Hourly)
```
DAG Name: prd-saleskpi-saps4-hourly-deltaload-pipeline
Schedule: Every 2 hours at :30 on WEEKDAYS (06:30, 08:30, 10:30, 12:30, 14:30, 16:30)
Weekend version: Every 4 hours
Note: "Core SAP delta"
```

**What it does:**
This is the core SAP data extraction — pulls a broad set of SAP transactional data.

**Why split weekday/weekend?**
Business is more active on weekdays — more transactions, more orders, more billing. So more frequent extraction is needed on weekdays. Weekends have fewer transactions, so 4-hourly is sufficient.

---

#### 🔴 The Highest Frequency DAG (Every 30 Minutes!)
```
DAG Name: prd-saps4-hourly-multiple-appointment-details-delta-load-pipeline
Schedule: Every 30 minutes (:00 and :30 every hour)
Runs: 19,953 times! (highest frequency on the entire platform)
Note: "Highest frequency"
```

**What it does:**
Extracts appointment/scheduling details from SAP every 30 minutes.

**Why every 30 minutes?**
Appointment/scheduling data changes rapidly during business hours — delivery appointments, meeting slots, scheduling changes. The CFA needs near-real-time scheduling data to coordinate dispatches.

**Data engineering note:**
This DAG has run **19,953 times** — that's the number recorded in the document. This means it's been running for a long time and is highly battle-tested. But it also means if it starts failing, it will generate 48 failure alerts per day — your monitoring should be tuned to handle this.

---

#### 🔴 CFA-Specific DAGs (Multiple times per day)

Several DAGs in 0.1 are specifically for CFA operations. The note **"CFA checkpoint waits"** appears repeatedly. Here's what it means:

```
These DAGs feed data that the CFA checkpoint (DAG 0.2) 
waits for before proceeding.

CFA-specific DAGs in 0.1:
├── prd-saps4-daily-multiple-batch-master-delta-load-pipeline
│   (Batch masters — 5x per day: 07:00, 10:30, 12:30, 14:30, 18:30)
│
├── prd-saps4-daily-multiple-credit-ledger-delta-load-pipeline
│   (CFA credit ledger — 5x per day)
│
├── prd-saps4-daily-multiple-zmrp-prcd-elements-delta-load-pipeline
│   (Pricing elements for CFA — 5x per day)
│
├── prd-saps4-hourly-batch-stock-asterisk-full-load-pipeline
│   (CFA stock data — 7x per day)
│
├── prd-saps4-hourly-mrp-pricing-delta-load-pipeline
│   (MRP/pricing data — 5x per day)
│
└── prd-saps4-hourly-sales-order-scheduleline-delta-load-pipeline
    (Sales order schedule lines — 5x per day)
```

**What "Asterisk" means:**
Asterisk (or "EY Asterix" as mentioned in Sam's document) is the CFA centralization system. When you see "asterisk" in a DAG name, it's data flowing to/from the EY Asterix system that generates billing documents for CFA.

---

#### 🔴 Finance-Related Priority 0 DAG
```
DAG Name: prd-saleskpi-saps4-acdoca-us-dailydelta-load-pipeline
Schedule: 6x per day: 06:30, 15:30, 18:30, 21:30, 00:30, 03:30
Note: "US Trading hours"
```

**What ACDOCA is:**
ACDOCA is the main financial accounting table in SAP S4/HANA. It stands for Universal Journal — it contains ALL financial entries (revenue, costs, adjustments).

**Why 6x per day and at odd hours?**
The schedule covers US trading hours — TCPL has international business including US operations. Financial entries happen throughout US business hours (which are night/early morning in IST). Hence the unusual schedule covering 00:30 and 03:30 IST.

---

## 5.2 Priority 0.2 — CFA Checkpoint & Orchestration (3 DAGs) ⭐ THE MOST IMPORTANT

### This is the MOST IMPORTANT group in the entire Priority 0 setup

```
THE 3 DAGs:

1. snowflakemart-dailyonce-cfacentralization-full-load-pipeline
   → Runs at 17:30 IST daily
   → Prepares CFA master tables

2. prd-saps4-hourly-CFAmart-checkpoint-pipeline  ⭐ THE ORCHESTRATOR
   → Runs at 20:11 IST daily
   → WAITS for 9 upstream DAGs to complete
   → Acts as the GATE — nothing proceeds until this passes

3. snowflakemart-eventbased-multiple-cfafull-load-pipeline
   → Triggered ~20:15 IST (after checkpoint passes)
   → Runs the main CFA mart refresh
```

### The CFA Checkpoint DAG — Explained in Detail

**DAG: prd-saps4-hourly-CFAmart-checkpoint-pipeline**
**Time: 20:11 IST every day**
**Marked: ⭐ CRITICAL ORCHESTRATOR**

This is the **most important single DAG on the platform**. Here's why:

**What it does:**
This DAG sits and WAITS. It checks if all 9 critical upstream DAGs have completed successfully for today. When all 9 pass the check, it triggers the main CFA mart refresh.

**The 9 DAGs it waits for:**

```
CFA Checkpoint waits for ALL of these to be GREEN:

1. Sales Order delta load          ← Latest orders in Snowflake?
2. Billing document delta load     ← Latest billing in Snowflake?
3. Customer credit mgmt            ← Latest credit data in Snowflake?
4. Pegasus PDP Lock Audit Trail    ← CFA tracking data complete?
5. MRP pricing delta               ← Latest pricing in Snowflake?
6. Batch stock asterisk            ← CFA stock data complete?
7. Sales order schedule line       ← Schedule lines complete?
8. ZMRP pricing elements           ← Pricing conditions complete?
9. Credit ledger delta             ← Credit ledger complete?
```

**Think of it as a checklist:**
```
20:11 IST — CFA Checkpoint Checklist:
☐ Sales Orders updated today?
☐ Billing Docs updated today?
☐ Credit Mgmt updated today?
☐ PDP Lock Audit updated today?
☐ MRP Pricing updated today?
☐ Batch Stock updated today?
☐ SO Schedule Lines updated today?
☐ ZMRP Elements updated today?
☐ Credit Ledger updated today?

All ✅? → PROCEED → Trigger CFA Mart Refresh at ~20:15
Any ❌? → FAIL → CFA Mart NOT refreshed → Alert support team
```

**Why 20:11 IST specifically?**
By 20:11 IST (8:11 PM), all the day's SAP transactions have been loaded through the hourly and multi-hourly DAGs. This is late enough to have a complete picture of the day.

**What happens after the checkpoint passes:**
The event-based CFA mart refresh pipeline fires immediately (~20:15), rebuilding the CFA data mart with the day's complete data. This mart is what EY Asterix uses to generate delivery orders and billing documents for the next day.

**As a Data Engineer — why you MUST understand this:**
- Every morning, your FIRST check should be: "Did the CFA checkpoint at 20:11 last night succeed?"
- If it failed, you need to find out WHICH of the 9 upstream DAGs was the problem
- Until you fix that upstream DAG and rerun the checkpoint, the CFA mart is stale
- A stale CFA mart means CFA may not have correct data for today's dispatches

---

## 5.3 Priority 0.3 — Post-CFA Cleanup (5 DAGs)

These run AFTER the CFA orchestration is complete (22:15 IST).

```
Post-CFA Cleanup DAGs:

1. Error Logs Daily (22:15) — Captures any errors from CFA end-to-end process
2. Error Logs Monthly (1st of month) — Monthly error summary
3. Deleted DO Tracking (6x/day) — Tracks Delivery Orders that were deleted
4. Outbound Delivery Daily (22:15) — Final outbound delivery reconciliation
5. Outbound Delivery Monthly (1st) — Monthly outbound delivery summary
```

**Why error logs at 22:15?**
You capture error logs AFTER the whole process is done (CFA mart runs ~20:15-22:00). Only then can you get a complete picture of what errors occurred during the full CFA process.

**Deleted DO Tracking — Why important?**
Sometimes Delivery Orders are created in SAP and then cancelled/deleted. If you don't track deletions, your Snowflake data will show a DO that no longer exists in SAP. This causes incorrect delivery counts and OTIF metrics.

---

## 5.4 Priority 0.4 — DB ARS (11 DAGs) — Distributor Auto Replenishment

### What is DB ARS?

**DB ARS = Distributor Base Auto Replenishment System**

From the business document: ARS is the system where distributor stock levels are monitored, and when stock falls below a threshold, orders are automatically generated.

This group of DAGs handles all the data flows that support ARS — making it a parallel Priority 0 track that can run independently of the CFA pipeline.

```
DB ARS DAGs (11 total):

SFDC Foundation:
├── SFDC Daily Reconciliation (00:00 midnight)
├── SFDC Mart Refresh — Foundation (03:00 AM)
├── Mavic SFA App Feed (05:00 AM) ← outputs to the mobile app
└── ARS Report (05:00 AM) ← the actual ARS report

Customer Data:
├── Customer PDP (05:00 & 07:00 AM)
└── Customer Onboarding (3x daily: 06:00, 11:30, 16:30)

SFDC Master Data:
├── Master Part 1 (06:30 AM)
└── Master Part 2 (triggered after Part 1)

SFDC Transactions:
├── Transaction Part 1 (4x/day: 06:30, 11:30, 16:00, 19:30)
└── Transaction Part 2 (triggered after Part 1)

SAP Distributor Data:
└── Nach Distributors (06:55 AM)
```

### Understanding the Flow

**Step 1: Overnight SFDC sync (00:00-03:00 IST):**
- At midnight, SFDC reconciliation runs — checking what's in Salesforce vs what's in Snowflake
- At 3 AM, the SFDC foundation mart is refreshed with all secondary sales data from previous day

**Step 2: Morning preparation (05:00-07:00 IST):**
- ARS Report is generated (which distributors need auto-replenishment orders?)
- Customer PDP data loaded
- Mavic SFA app feed refreshed (salesmen's mobile apps will show updated data when they start their day at 9 AM)

**Step 3: During business hours (every few hours):**
- SFDC transactions updated 4x per day (new orders, visits, secondary sales)
- Customer onboarding data refreshed 3x per day (new distributors/outlets being added)

**Why ARS is Priority 0:**
Without ARS, distributors don't automatically get orders generated for them. Shelves start running empty at retailers. Secondary sales drop. This directly impacts the supply chain and eventually primary sales.

**The Part 1 → Part 2 pattern:**
Several DAGs use this pattern (Master Part 1 → Part 2, Transaction Part 1 → Part 2). This is because the data volume is so large that splitting it into two sequential jobs is more reliable and performant than one massive job.

---

# 6. PRIORITY 1 — CRITICAL (22 DAGs) — DEEP DIVE

## What Makes Priority 1 Different from Priority 0?

Priority 1 is "severe impact" but NOT "revenue blocking immediately." The main difference:
- Priority 0 failure → TCPL cannot collect money from distributors
- Priority 1 failure → TCPL's tea buying operations stop / ML predictions don't run

Both are serious, but Priority 1 can tolerate a few more hours of delay.

---

## 6.1 Priority 1.1 — Infiniti Tea Platform (12 DAGs)

### What is Infiniti?

**Infiniti is TCPL's tea procurement platform.**

TCPL buys tea leaves from tea estates. Tea is bought through three methods:
1. **Auction** — Tea estate puts tea leaves on auction, TCPL bids
2. **Forward Contracts** — TCPL agrees to buy X kg of tea at price Y on a future date
3. **Private Sales** — Direct negotiation between TCPL and tea estate

Infiniti is the system that manages all of this. It integrates with SAP (for financial processing) and has its own ML model (TTP — Tea Trading Prediction) to help decide whether to buy at a given price.

### The Infiniti Pipeline Flow (Detailed)

```
INFINITI DAG FLOW — Daily Sequence

Step 1: Common Masters (06:00 & 12:30 IST)
        prd-saleskpiinfiniticommonmasterhourlypipeline
        ↓ Loads reference/master data first
        
Step 2: Tea Masters (Triggered after Common Masters)
        prd-saleskpiinfiniti-teamasterhourlypipeline
        ↓ Loads tea-specific master data
        
Step 3: SAP Contract Data (06:30 IST)
        prd-saps4-daily-once-full-load-infinitipi-pipeline
        ↓ Loads SAP contract data into Infiniti
        
Step 4: Daily Reconciliation (07:00 IST)
        prd-saleskpiinfiniti-dailyonce-reconpipeline
        ↓ Verifies yesterday's data is balanced
        
Steps 5-7: TRANSACTION FEEDS (07:00 to 18:30, hourly, 14 runs each)
        ↓ Forward Contract transactions (hourly)
        ↓ Private Sale transactions (hourly)
        ↓ Auction transactions (hourly) — waits for Forward + Private first
        
        These run during tea auction/trading hours (7 AM to 6:30 PM)
        because that's when tea is being actively bought and sold
        
Step 8: Combined BR (~19:00 IST — triggered after auction)
        prd-saleskpiinfinitikombined_brhourlypipeline
        ↓ Merges auction + forward + private into one combined dataset
        
Step 9: BR Final (~19:15 IST — triggered after combined BR)
        prd-saleskpiinfiniti-brfinal-pipeline
        ↓ Final calculation — triggers ML model (TTP)
        
Steps 10-11: Tea Transactions + Digitea (triggered after BR Final)
        ↓ Tea transaction records finalized
        ↓ Digitea system deltas updated
        
Step 12: Digitea Part 2 (15:10 IST — independent)
        ↓ Second part of Digitea data
```

**What is "BR"?**
BR = Buying Report. At the end of each day's trading, a Buying Report is generated that summarizes all tea purchases made that day (auction + forward + private combined). This is used by finance and procurement teams.

**What is Digitea?**
Digitea appears to be another tea procurement/trading platform that TCPL integrates with. It has its own delta updates.

**Why is Infiniti Priority 1 and not Priority 0?**
- Tea buying doesn't stop revenue immediately — if Infiniti is down for one day, TCPL might miss some tea auction bids, but existing contracts and existing stock cover operations
- However, it's Priority 1 because: (a) tea buying happens on strict time schedules — auction windows close, (b) ML predictions for tea pricing need to run each day, (c) missing too many days affects tea supply chain seriously

---

## 6.2 Priority 1.2 — Infiniti Delta Pipelines (6 DAGs)

These run in a tight sequence every morning from 08:10 to 10:15 IST:

```
08:10 — Component Master (Delta_Pipeline_Full_Component_prd)
         What it is: Master data about tea components/blends
        ↓
08:20 — Component Parameters (Delta_Pipeline_Full_Component_Parameters_prd)
         What it is: Parameters/specifications for tea components
        ↓
08:30 — Demand Allocation (Delta_Pipeline_Demand_Allocation_prd)
         What it is: How tea demand is allocated across blends/products
        ↓
08:40 — MR Contract Delta (Delta_Pipeline_Mr_Contract_Delta_prd)
         What it is: MR = Market Requirements contracts, delta changes
        ↓
08:50 — Master Plants (Delta_Pipeline_Master_Plants_prd)
         What it is: Tea processing plant master data
        ↓
10:15 — Vendor Masters (Delta_Pipeline_Vendor_Masters_prd)
         What it is: Tea estate vendor master data
```

**Why 10-minute intervals?**
Each pipeline must complete before the next starts. The 10-minute gaps ensure the previous one finishes (they run fast — master data, not large transactional data).

---

## 6.3 Priority 1.3 — ML Models (4 DAGs) ⭐

### The TTP Machine Learning Pipeline

**TTP = Tea Trading Prediction** — this is the ML model that predicts whether TCPL should buy tea at a given auction price.

```
ML PIPELINE SEQUENCE (all triggered, starting ~19:30 IST):

Step 1: Extract from Salesforce to S3 (~19:30)
        prd-infinitiml-ttp-sf-tos3-hourlyload
        ↓ Features data exported from Snowflake/SF to S3 for SageMaker
        
Step 2: SageMaker Prediction (~19:45)
        prd-infinitiml-ttpsagemakerexecution-hourly
        ↓ SageMaker model runs on the features, generates predictions
        
Step 3: Load Predictions back to Snowflake (~20:00)
        prd-infinitiml-ttp-dbfinal-brhourly-load
        ↓ Predictions loaded into Snowflake so procurement team can see them
        
Step 4: Buying Decision Prediction (21:00 — independent)
        prd-infinitiml-bgdndeltaprediction-daily-pipeline
        ↓ BGDN = Buying Guidance and Decision model — separate daily prediction
```

**Why is this triggered starting at ~19:30?**
Because it waits for the BR Final pipeline (19:15) to complete. The ML model needs the final day's buying report as input before it can generate tomorrow's predictions.

**What the ML outputs:**
- TTP predicts: "At tomorrow's auction, if tea price is X, should we buy? Yes/No/Maybe"
- BGDN predicts: "Based on current data, what buying decisions should be made?"

**As a Data Engineer:**
Your job is to make sure:
- The feature data that flows from Snowflake to S3 is fresh and correct
- SageMaker has the right S3 path to pick up the data
- The predictions written back to Snowflake are in the right table/schema
- If SageMaker fails, you know how to check its logs in AWS and escalate

---

# 7. PRIORITY 2 — IMPORTANT (89 DAGs) — DEEP DIVE

Priority 2 has the most DAGs (89) because most of the analytical/reporting work sits here. These don't block revenue but they are what 3,000+ business users use for their analysis.

---

## 7.1 Priority 2.1 — Data Marts & Sales (18 DAGs)

### What is a Data Mart?

A **data mart** is a subject-specific section of the Snowflake Business-Ready layer. It's a collection of pre-built tables optimized for a specific type of analysis.

The Sales Mart = all the Snowflake tables that power ThoughtSpot sales dashboards.

### The Key Sales Mart DAGs:

#### 🟡 Main India Sales Mart (3x/day)
```
DAG: snowflake-fact-mart
Schedule: 3x/day at 07:30, 12:30, 20:30 IST
Dependency: SAP S4 loads + SFDC loads must be complete
Note: "Main India sales mart"
```

**What this builds:**
This is the primary fact table for India primary sales. It combines:
- SAP billing data (primary sales)
- SFDC secondary sales data
- Geography dimensions
- Product dimensions
- Target data

**Why 3x/day?**
Sales teams want fresh data during business hours. 07:30 ensures morning dashboards have last night's data. 12:30 updates with morning transactions. 20:30 gives the end-of-day picture.

**The "Dependency: SAP S4/SFDC loads must be complete" is critical:**
If the SAP hourly loads (Priority 0) fail and aren't recovered before 07:30, this mart will build with incomplete data. That's why Priority 0 MUST run first.

#### 🟡 Child Mart (triggered ~08:15)
```
DAG: snowflake_fact_mart_child
Triggered by: snowflake-fact-mart completing
Note: "SFDC secondary sales"
```

This builds the secondary sales portion — triggered immediately after the main India sales mart. It uses the SFDC data to build the distributor→retailer secondary sales view.

#### 🟡 US Sales Mart (Hourly — 11 runs)
```
DAG: snowflake-us-sales-fact-mart-daily-refresh
Schedule: Hourly from 09:30 to 03:30 IST (11 runs)
Note: "US sales — highest runs"
```

TCPL's US business is covered here. Runs across almost all hours because US business hours span a wide IST range.

#### 🟡 Sawtooth Report
```
DAG: snowflake-sawtooth-report-daily-once-pipeline
Schedule: Daily at 18:00 IST
```

**What is a Sawtooth report?**
A sawtooth report shows inventory levels over time. The graph looks like a sawtooth wave — inventory builds up (when stock comes in) and drops down (as stock is sold). This pattern repeats. The report helps predict stock-out situations.

#### 🟡 Dimension Mart (Daily at 08:30)
```
DAG: snowflake-dimension-mart-daily-once-full-load
```

This refreshes ALL dimension tables — products, distributors, outlets, geography. Must run early morning so that subsequent fact mart builds have the latest master data to join against.

---

## 7.2 Priority 2.2 — Finance (14 DAGs)

### Finance-Specific Data Flows

#### 🟡 Finance Delta Load (Daily at 01:00 IST)
```
DAG: prd-finance-saps4-daily-once-midnight-delta-load-pipeline
Time: 01:00 AM IST
```

Main finance data extraction from SAP — runs at midnight so it captures the full previous day's financial postings.

#### 🟡 ACDOCA India (Daily at 07:35 IST)
```
DAG: prd-saleskpi-saps4-acdoca-dailyonce-delta-load-pipeline
What is ACDOCA: Universal Journal (all financial accounting entries)
```

#### 🟡 ACDOCA Global (4x/day: 09:00, 12:00, 15:30, 21:00 IST)
```
DAG: prd-saleskpi-saps4-acdoca-uk-canaus-vnm-daily-once-delta-load-pipeline
Countries: UK, Canada, Australia, Vietnam
```

**Why global at 4 different times?**
Each time slot corresponds to business closing time in a different country:
- 09:00 IST ≈ close of business in Vietnam (11:30 local)
- 12:00 IST ≈ UK midday
- 15:30 IST ≈ UK close
- 21:00 IST ≈ Canada/US east coast mid-afternoon

#### 🟡 Product Costing (Daily at 16:30 IST)
```
DAG: prd-Saps4-Productcosting-DailyOnce-DeltaLoad-pipeline
```
Product costing = how much does it cost TCPL to produce each product. Used by Finance for profitability analysis.

#### 🟡 Flight Cost & Travel Expense (Daily)
Expense tracking DAGs — finance needs to track employee travel costs.

---

## 7.3 Priority 2.3 — SAP S4 Masters & Reference (20 DAGs)

### What are Master & Reference Tables?

These are not transactional data (like sales or billing) but rather **reference/configuration data** from SAP:

| DAG | What it loads | Why needed |
|---|---|---|
| Reference Masters | 107 SAP reference tables | Codes, categories, configurations |
| Freight Master | Freight cost rates | How much does shipping cost? |
| Bank Master | Bank account details | Finance payment processing |
| Bill of Material (BOM) | Product recipes — what goes into each product | Supply chain planning |
| Vehicle Master | Delivery vehicle data | CFA dispatch planning |
| Route Master | Delivery routes | CFA routing |
| Holiday Calendar | Company holidays | Planning adjustments |
| Process Orders | Manufacturing orders | Operations tracking |
| Production Orders | Production facility orders | Supply chain |
| Sales Pricing Conditions | Current pricing rules | Revenue calculations |

**The "107 reference tables" DAG:**
```
DAG: prd-saleskpi-saps4-daily-once-fullload-referencemasters-pipeline
Time: 00:00 midnight
Note: "107 reference tables"
```

This single DAG loads **107 different reference/lookup tables** from SAP into Snowflake. It runs at midnight (00:00) so all this reference data is fresh when the day's transactional loads start.

---

## 7.4 Priority 2.4 — OTIF (5 DAGs)

### What is OTIF?

**OTIF = On Time In Full**

OTIF is a key supply chain metric that measures:
- **On Time** — was the delivery made on time?
- **In Full** — was the complete quantity delivered (no partial delivery)?

An OTIF score of 100% means every single delivery was on time and complete.

OTIF is measured by large retail customers (Modern Trade like DMart, BigBazaar) who impose penalties on TCPL if OTIF falls below targets.

### OTIF DAGs

```
OTIF Pipeline:
Part 1 (14:30) → Part 2 (09:00 & 17:00) → Part 3 (10:30)

Additional:
OTIF Core (10:00) — main OTIF calculation
OTIF Asterisk (06:30) — OTIF data flowing to EY Asterix CFA system
```

**Why is OTIF Priority 2 and not Priority 0?**
OTIF calculation is analytical — it tells you how you performed. Poor OTIF has financial penalties but not immediate revenue-blocking impact. However, it's still "important" because penalties accumulate.

---

## 7.5 Priority 2.5 & 2.6 — ARKIEVA (4 DAGs)

### What is ARKIEVA?

**ARKIEVA is a supply chain planning software** — a system that helps TCPL plan how much to produce and when, based on demand forecasts.

**India (Daily):**
- Master data — 13 SQL tasks
- Inventory data — 8 SQL tasks
- Both triggered after SAP overnight loads complete

**International (Weekly/Monthly):**
- International planning runs weekly (every Thursday) and monthly (1st Saturday)
- Less frequent because international planning has longer time horizons

---

## 7.6 Priority 2.7 — Zycus Procurement (4 DAGs)

### What is Zycus?

**Zycus is a procurement/sourcing platform** — TCPL uses it for vendor management and procurement automation.

```
Zycus DAGs:
├── Core load (2x/day: 06:00, 18:00)
├── Autodraft (6x/day: 08:30-19:00) — automated draft requisitions
├── Reporting (triggered after daily load) — procurement reports
└── First Requirement (23:59 daily) — daily requirement compilation
```

**Autodraft running 6x/day:**
Autodraft generates procurement requests automatically. It runs very frequently because procurement requisitions can be created throughout the business day and need to be processed quickly.

---

## 7.7 Priority 2.8 — Pegasus (4 DAGs)

### What is Pegasus?

**Pegasus** appears to be a delivery/logistics management system (the document mentions "MT Ecom RDD" and "EDI").

- **RDD = Required Delivery Date** — when Modern Trade / Ecommerce customers require delivery
- **EDI = Electronic Data Interchange** — automated data exchange with retail customers

```
Pegasus DAGs:
├── Master full load (17:00 daily) — reference data
├── MT Ecom RDD (hourly: 09:00-21:00, 13 runs) — Required delivery dates
├── Delta load (2x/day: 12:00, 18:00) — transaction deltas
└── EDI delta (06:30 daily) — EDI transaction data
```

The "MT Ecom RDD" runs 13 times during business hours because retail customers update their delivery requirements throughout the day.

---

## 7.8 Priority 2.9 — PSO & Planning (4 DAGs)

**PSO = Primary Sales Objectives** — targets and planning data.

The PSO Mart (08:00 daily) builds the Snowflake table that shows:
- What are the sales targets for each distributor?
- How does actual performance compare?

**Tea Blending Stock (2x/day: 15:00 & 03:00):**
Data about stock of tea leaf blends in warehouse — used for tea blending optimization (the ML model mentioned in Sam's document).

---

## 7.9 Priority 2.10 — MDM & Unify (4 DAGs)

### Critical — MDM Data Flows

```
MDM DAGs:
├── Unify MDM Full Load (00:00 midnight) — all MDM master data
├── MM Reference Master (04:00 AM) — Material Management reference tables
├── DQ Metrics (06:00 AM) — Data Quality metrics dashboard
└── Product Delta Mulesoft (2x/day: 12:00, 17:00) — product changes via Mulesoft
```

**Unify MDM at midnight:**
This is the master data synchronization — ensuring Snowflake's dimension tables match the MDM system. Runs at midnight so dimensions are ready for the day's fact loads.

**Mulesoft:**
Mulesoft is an integration platform (like SnapLogic). When product master data changes (new SKU added, product renamed), the change flows from MDM through Mulesoft to Snowflake.

**DQ Metrics (Data Quality):**
This DAG measures the quality of data in Snowflake — how many records pass/fail validations, completeness rates, freshness checks. The results are shown in a DQ Metrics dashboard that the data engineering team monitors.

---

## 7.10 Priority 2.11 — Blue Planner & Blue Yonder (2 DAGs)

**Blue Planner** — US-specific planning system (refreshed daily at 14:30 IST)
**Blue Yonder** — Supply chain planning platform (daily at 07:00 IST)

Blue Yonder is a major supply chain software — used for demand forecasting, replenishment planning, and inventory optimization.

---

## 7.11 Priority 2.12 — HR SuccessFactor (2 DAGs)

**SuccessFactors** is SAP's HR software — handles employee data, payroll, performance management.

```
HR DAGs:
├── SuccessFactor Main (00:00 midnight) — all HR data
└── User Fields (01:00 AM) — custom user attribute fields
```

HR data in Snowflake enables workforce analytics — headcount by region, sales force productivity analysis, etc.

---

## 7.12 Priority 2.13 — CFA Month-End (8 DAGs)

### What Makes Month-End Different?

During the regular 25th-31st period (and 25th-26th pre-month-end), the CFA pipeline runs MORE FREQUENTLY because month-end is the busiest billing period.

```
Regular CFA Checkpoint: Once daily at 20:11
Month-End CFA Checkpoint:
├── 27th-31st: 4x per day (08:11, 12:11, 16:11, 20:11) — every 4 hours!
└── 25th-26th: 2x per day (13:41, 19:41) — pre-month-end ramp-up

Why? Because month-end billing is extremely high volume — 
many distributors place large orders before month-end close.
The data needs to be more current to handle the volume.
```

**Parent ROR (17:30, 25th-31st):**
ROR = Rate of Return or similar financial metric. This runs daily during month-end to track financial metrics.

---

# 8. PRIORITY 3 — LOW PRIORITY (35 DAGs)

## What Sits at Priority 3?

These are pipelines where a delay of hours or even a day causes minimal business disruption.

### 8.1 Audit & Compliance (4 DAGs)

```
└── Failure Capture (every 3 hours) — records which jobs failed
└── Audit Load (bi-hourly) — loads audit trail data
└── Weekly Internal Audit (Friday 10:00)
└── Monthly Audit (Friday 10:00)
```

These capture compliance and audit data — important for regulatory purposes but not for daily operations.

### 8.2 Weekly/Monthly ML (6 DAGs)

**Model Retraining:**
ML models need to be retrained periodically with new data to stay accurate.

```
Weekly:
├── SAP S4 Weekly Full Load (Saturday 07:00) — larger weekly extract
├── BGDN Model Retraining (Sunday 10:15) — retrains buying decision model
├── TTP Accuracy Calculation (Sunday 22:30) — how accurate were predictions?
└── Buying Decision Accuracy (Sunday 10:15) — accuracy metrics

Monthly:
└── TTP Monthly Model Retraining (1st Sunday) — full retraining with all recent data
```

**Why retraining at Priority 3?**
Model retraining is important long-term but not for today's operations. Even if this week's retraining is delayed by one day, the existing model continues to make predictions.

### 8.3 SharePoint Pipelines (9 DAGs)

```
Regular:
├── SharePoint Load 1 & 2 (23:00 daily)
└── Query DAGs (on-demand)

Monthly business data:
├── CPC Cost — Cost Per Case data
├── Inventory Provision
├── Lakshya — likely a business planning/target system
├── Samruddhi Target — Samruddhi is a TCPL business initiative
└── Samruddhi Visibility
```

**What is SharePoint used for here?**
Business teams often maintain planning data, targets, and manual inputs in SharePoint (Excel files on SharePoint). These pipelines read that data and bring it into Snowflake for analysis.

**Lakshya** and **Samruddhi** are likely internal TCPL programs/initiatives — "Lakshya" means "target" in Hindi, suggesting it's a target-setting system.

### 8.4 Monthly Reports (10 DAGs)

End-of-month / beginning-of-month reports:
- Shikhar — likely a specific sales initiative
- Transfer Pricing — inter-company pricing for international business
- Dust Generation Report — dust in tea manufacturing (quality metric)
- PDP Report — Pre-Delivery Preparation monthly summary
- Inventory Ageing — how long has stock been sitting?
- POSM ABC Classification — Point of Sale Material classification
- Project Leap Reconciliation — specific TCPL project reconciliation
- Monthly Reconciliation — overall monthly data reconciliation

### 8.5 Miscellaneous (6 DAGs)

| DAG | What it does |
|---|---|
| MT Monthly | Modern Trade monthly data |
| Agriwatch (Daily 13:00) | Agricultural commodity watch — tea/salt commodity prices |
| Incentive Management (Daily 05:00) | Distributor/salesman incentive calculations |
| Inventory Refresh (On-demand) | Manual inventory data refresh |
| Destination Check Monitoring (10:00) | Checks data destination connectivity |
| Pace Masters (00:05) | Pace — possibly a planning system |

---

# 9. UTILITY DAGs (15 DAGs)

## These Are NOT Data Pipelines — They Are Platform Management Tools

```
Monitoring DAGs (3):
├── Email Bounce (12:00 daily) — checks if alert emails are being delivered
├── DAG Failure Alerts (13:00 daily) — sends consolidated daily failure report
└── Atlas Daily Alerts (14:00) — Atlas is Atlan/data catalog lineage alerts

ATK Table Count Checks (4):
├── ATK tables count Part 1 (04:30)
├── ATK tables count Part 2 (06:00 — after Part 1)
├── SFDC ATK counts Part 1 (00:00)
└── SFDC ATK counts Part 2 (06:30)
ATK = "Automated Testing Kit" — these verify row counts in Snowflake
match expected ranges, catching data anomalies automatically

Metadata DAGs (4):
├── DAG Metadata Collection (on-demand) — collects info about all DAGs
├── Schedule Fetch (18:00 daily) — gets all active DAG schedules
├── DAG Info (on-demand) — detailed DAG information
└── SQL Dependency Identification (on-demand) — maps SQL dependencies

Utility (3):
├── DB Export (on-demand) — exports database snapshots
├── GIB Request (on-demand) — GIB = possibly a reporting system
└── US Flash DEV (on-demand) — dev environment flash report

Test (1):
└── Test SNS Email (on-demand) — verifies SNS alert emails work
```

**Important:** These 15 DAGs are explicitly excluded from the outage recovery sequence — "Not part of data pipeline recovery."

---

# 10. PAUSED DAGs (11 DAGs)

## These Are Dead DAGs — Do NOT Restart During Recovery

```
PAUSED (Do NOT restart):

1. fetch_active_dags_with_full_schedule_in_ist — Utility, paused
2. fetch-dag-interval-and-other-info — Utility, paused
3. prd-saleskpi-saps4-daliy-once-full-load-teablending-wmstock — Paused SAP S4
4. prd-saleskpi-sfdc-master-daily-once-deltaload-pipeline — Paused (Parts 1&2 active instead)
5. prd-saleskpi-sfdc-transaction-daily-multiple-delta-load-pipeline — Paused (Parts 1&2 active)
6. prd-saps4-daily-once-customer-shelf-life-full-load-pipeline — Paused
7. Prd-snowflake-CfaCentralizationParentRorDailyOnceMonthEnd — Duplicate, paused
8. prd-Snowflake-GlobalSales-DailyOnceDelta — Paused Finance/Sales
9. prd-snowflake-OTIF-Asterisk-DailyOnceDeltaLoad — Paused OTIF
10. sharepoint-full-load-pipeline-prd1 — Paused SharePoint
11. sharepoint_query_dag_prd — Paused SharePoint
```

**Why do paused DAGs exist?**
- Old pipelines replaced by better versions (note: SFDC master/transaction parent DAGs replaced by Part 1 + Part 2)
- Duplicate pipelines discovered
- Business process changed making the pipeline obsolete
- Under re-development

**Critical warning:** During outage recovery, someone in a panic might accidentally restart paused DAGs. This can cause duplicate data loads or errors. Always check PAUSED status before triggering any DAG.

---

# 11. THE MASTER FLOW — HOW ALL DAGs CONNECT

## The Complete Daily Timeline

```
TCPL DATA PLATFORM — DAILY PIPELINE TIMELINE (IST)

━━━━ MIDNIGHT (00:00-03:00) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

00:00 — SFDC Daily Reconciliation (P0.4)
00:00 — SAP Reference Masters — 107 tables (P2.3)
00:00 — Bank Master (P2.3)
00:00 — HR SuccessFactor Main (P2.11)
00:00 — Unify MDM (P2.10)
00:05 — Pace Masters (P3)
01:00 — Finance Delta Load (P2.2)
01:00 — HR User Fields (P2.11)
02:00 — SAP Delta Part 2 (P2.3)
03:00 — SFDC Foundation Mart Refresh (P0.4)

━━━━ EARLY MORNING (04:00-06:00) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

04:00 — Process Orders, Production Orders (P2.3)
04:30 — ATK Table Count Checks (UTILITY)
04:30 — PRCD Elements (P2.3)
05:00 — ARS Report (P0.4)
05:00 — Customer PDP (P0.4)
05:00 — Material Independent Requirements (P2.3)
05:00 — Incentive Management (P3)
05:00 — CFA SO Status (P0.1)
05:00 — BOM Load (P2.3)
05:30 — Pricing Conditions, Bill of Material (P2.3)
06:00 — SAP Hourly Delta (P0.1) ← STARTS THE HOURLY CYCLE
06:00 — SAP Stock Data (P0.1)
06:00 — Common Masters (P1.1 Infiniti)
06:00 — Customer Onboarding (P0.4)
06:00 — MDM DQ Metrics (P2.10)
06:30 — SFDC Master Part 1 (P0.4)
06:30 — SFDC Transaction Part 1 (P0.4)
06:30 — ACDOCA India (P2.2)
06:30 — ARKIEVA India (P2.5)
06:30 — ATK SAP Full Load (P3)
06:30 — SAP Contract to Infiniti (P1.2)

━━━━ BUSINESS HOURS MORNING (07:00-12:00) ━━━━━━━━━━━━━━━━━━━━━━━━━

07:00 — Infiniti Daily Recon (P1.1)
07:00 — INFINITI TRADING BEGINS — hourly forward/private/auction runs
07:00 — SFDC Mavic SFA App Feed (P0.4) ← Salesmen's apps refresh
07:00 — Blue Yonder Data (P2.11)
07:20 — Open Balance (P2.3)
07:30 — MAIN INDIA SALES MART BUILD #1 (P2.1) ← Business users start their day
07:30 — Sawtooth Report
07:35 — ACDOCA India extract
08:00 — Infiniti Delta Pipelines BEGIN (08:10-10:15)
08:10 — Component Master (P1.2)
08:11 — CFA CHECKPOINT — MONTH END ONLY (P2.12 — 27th-31st)
08:20 — Component Parameters (P1.2)
08:30 — Demand Allocation (P1.2)
08:30 — Sales Mart Multiple (P2.1)
08:30 — Dimension Mart (P2.1) ← All dimensions refreshed
08:30 — Document Info Records (P0.1)
08:40 — MR Contract Delta (P1.2)
08:50 — Master Plants (P1.2)
08:50 — SAP Daily Core Masters — 43 tasks (P0.1)
09:00 — Finance Mart (P2.2)
09:00 — ACDOCA Global 09:00 batch (P2.2)
09:30 — US SALES MART BEGINS — hourly until 03:30 (P2.1)
10:00 — Infiniti Auction continues...
10:00 — OTIF Core (P2.4)
10:15 — Vendor Masters (P1.2) ← Infiniti deltas complete
11:00 — International ARKIEVA (P2.6)
11:30 — SAP Customer Masters, Stock Data (P0.1)
12:00 — Common Masters Infiniti (2nd run) (P1.1)
12:00 — ACDOCA Global 12:00 batch (P2.2)

━━━━ BUSINESS HOURS AFTERNOON (12:00-18:00) ━━━━━━━━━━━━━━━━━━━━━━━

12:30 — MAIN INDIA SALES MART BUILD #2 (P2.1)
13:00 — Agriwatch, Failure Alerts (UTILITY)
13:00 — CFA Month-End Adhoc (P2.12)
14:00 — Atlas Alerts (UTILITY)
14:30 — Blue Planner US (P2.11)
15:00 — Tea Blending Stock (P2.9)
15:30 — ACDOCA Global 15:30 batch (P2.2)
16:00 — Product Costing (P2.2)
16:30 — CFA Multiple (P0.1)
16:45 — Vehicle Master, Route Master, Material Info (P2.3)
17:00 — GRN Classification, KVI Capping, Buffer Norms (P0.1)
17:30 — CFA MASTER TABLES PREP (P0.2)
18:00 — MAIN INDIA SALES MART BUILD #3 (P2.1)
18:00 — Sawtooth Report

━━━━ EVENING (18:00-21:00) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

18:30 — Evening Export Sales Mart (P2.1)
19:00 — INFINITI BR COMBINED — merges all tea buying data (P1.1)
19:15 — INFINITI BR FINAL — triggers ML (P1.1)
19:30 — ML STEP 1: Extract features for SageMaker (P1.3)
19:41 — CFA MONTH-END ADHOC CHECKPOINT (P2.12 — 25th-26th)
19:45 — ML STEP 2: SageMaker Tea Price Prediction (P1.3)
20:00 — ML STEP 3: Load predictions to Snowflake (P1.3)
20:11 — ⭐ CFA CHECKPOINT — THE DAILY GATE ⭐ (P0.2)
20:15 — CFA MART FULL REFRESH TRIGGERED (P0.2)
20:30 — SAP Core Delta, Sales Mart Final Update (P0.1, P2.1)
21:00 — BGDN ML Predictions (P1.3)

━━━━ NIGHT (21:00-00:00) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

22:00 — CFA Mart refresh completes
22:10 — Deleted DO Tracking (P0.3)
22:15 — CFA ERROR LOGS CAPTURED (P0.3) ← Full CFA process now complete
22:30 — Weekly ML Accuracy (P3 — Sundays only)
23:00 — SharePoint loads (P3)
23:00 — SAP Freight Master (P2.3)
23:50 — DO Tracking Final (P2.1)
23:59 — Zycus First Requirement (P2.7)

━━━━ HOURLY THROUGHOUT DAY ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

:00 every hour — SALES ORDER DELTA LOAD (P0.1) ← Every. Single. Hour.
:00 every hour — BILLING DOCUMENT DELTA LOAD (P0.1) ← Revenue tracking
:00 & :30 — APPOINTMENT DETAILS (P0.1) ← Every 30 minutes, 19,953 runs
:10 every hour — CREDIT MANAGEMENT (P0.1)
:45 every hour — OUTBOUND DELIVERY (P0.1)
```

---

# 12. PROJECT-WISE BREAKDOWN — WHO OWNS WHAT

From the quick reference table:

| Project | P0 | P1 | P2 | P3 | Total | Owner/Domain |
|---|---|---|---|---|---|---|
| **SAP S4** | 23 | 1 | 22 | 1 | **47** | Sales, Finance, Operations |
| **CFA** | 14 | 0 | 6 | 0 | **20** | Sales & Operations (Arnab/Aritra) |
| **INFINITI** | 0 | 18 | 0 | 4 | **22** | Tea Procurement (special) |
| **SFDC** | 9 | 0 | 0 | 0 | **9** | Sales (Arnab) |
| **FINANCE** | 1 | 0 | 14 | 0 | **15** | Finance (Shalvi) |
| **SALES MART** | 0 | 0 | 18 | 0 | **18** | Sales (Arnab) |
| **OTIF** | 1 | 0 | 5 | 0 | **6** | Operations (Aritra) |
| **ARKIEVA** | 0 | 0 | 4 | 0 | **4** | Operations/Planning |
| **PEGASUS** | 1 | 0 | 3 | 0 | **4** | Operations/MT |
| **ZYCUS** | 0 | 0 | 4 | 0 | **4** | Procurement |
| **SHAREPOINT** | 0 | 0 | 0 | 9 | **9** | Various |
| **OTHER** | 9 | 3 | 9 | 21 | **42** | Mixed |
| **TOTAL** | **58** | **22** | **89** | **35** | **204** | |

**Key observations:**
- SAP S4 is the BIGGEST project (47 DAGs) — most of TCPL's data ultimately comes from SAP
- CFA is entirely Priority 0 or 2 — no Priority 3 because CFA is always operational-critical
- SFDC (Salesforce/Mavic) is ALL Priority 0 — secondary sales data is critical
- Infiniti is its own world — Priority 1 across the board (18 out of 22 total)
- Finance is almost all Priority 2 — analytical, not operational

---

# 13. WHAT YOU SHOULD FOCUS ON — PRIORITY GUIDE FOR NEW ENGINEERS

## Your Focus by Experience Level

### In Your First Week:

**Focus on understanding these 5 things:**
1. **The CFA Checkpoint (20:11 DAG)** — understand its 9 dependencies. This is the single most important DAG.
2. **The hourly Sales Order and Billing DAGs** — these run 24 times per day. Know their patterns.
3. **The SFDC DAGs (Priority 0.4)** — especially the Foundation Mart (03:00) and the ARS Report (05:00).
4. **The Main India Sales Mart (07:30, 12:30, 20:30)** — this is what most ThoughtSpot users look at.
5. **The concept of Part 1 → Part 2 triggers** — many DAGs split into sequential parts. If Part 1 fails, Part 2 also fails.

### In Your First Month:

**Expand to understand:**
1. The full Priority 0 timeline — who runs when and why
2. The Infiniti platform flow (06:00 to 19:15) — what is tea procurement?
3. The Finance data flows — ACDOCA tables and why they have global vs India variants
4. MDM DAGs — why master data freshness matters for all downstream marts
5. ATK table count checks (Utility DAGs) — these tell you if row counts are abnormal

### Ongoing:

**Build deep knowledge in:**
1. Which DAGs depend on which others (Airflow dependencies)
2. Normal run times — so you can spot when something is running unusually long
3. Common failure patterns and their fixes
4. The month-end schedule changes (CFA runs 4x instead of 1x)

---

## The Top 10 DAGs Every Engineer Must Know Cold

| Rank | DAG | Why Critical |
|---|---|---|
| 1 | `prd-saps4-hourly-CFAmart-checkpoint-pipeline` (20:11) | The master gate — everything downstream depends on it |
| 2 | `prd-saps4-hourly-sales-order-deltaload-pipeline` | Revenue blocking — runs every hour |
| 3 | `prd-saps4-hourly-billing-document-delta-load-pipeline` | Revenue tracking — runs every hour |
| 4 | `snowflakemart-eventbased-multiple-cfafull-load-pipeline` | The CFA mart refresh after checkpoint |
| 5 | `prd-saleskpi-sfdc-dailyonce-full-load-martrefresh-pipeline` | SFDC Foundation — basis for all secondary sales |
| 6 | `snowflake-fact-mart` (07:30/12:30/20:30) | The main India sales mart — 3,000 users depend on this |
| 7 | `prd-saleskpi-saps4-customercreditmgmt-hourly-fullload` | Credit management — gates CFA dispatches |
| 8 | `prd-unifymdm-dailyfull-load-pipeline` (00:00) | All dimension tables start from MDM |
| 9 | `snowflake-dimension-mart-daily-once-full-load` (08:30) | Dimension mart — all fact tables need fresh dimensions |
| 10 | `prd-infinitiml-ttpsagemakerexecution-hourly` | The ML prediction — critical for tea buying |

---

# 14. OUTAGE RECOVERY PLAYBOOK — WHAT TO DO WHEN THINGS BREAK

## When Snowflake Goes Down (Like December 2025)

**Step 1 — Confirm Snowflake is Actually Down:**
- Try connecting to Snowflake from your SQL client
- Check AWS Service Health Dashboard
- Check Snowflake status page (status.snowflake.com)

**Step 2 — Stop All Airflow DAGs:**
- Pause all active DAGs in Airflow to prevent failures piling up
- Do NOT let DAGs keep failing — each failure creates a backlog

**Step 3 — Document the Outage Window:**
- When did Snowflake go down? (exact time IST)
- When did it come back? (exact time IST)
- Which DAGs were supposed to run during the outage?

**Step 4 — Once Snowflake Recovers, Follow THIS Exact Recovery Sequence:**

```
RECOVERY SEQUENCE (strictly in this order):

PHASE 1 — PRIORITY 0.1 (Foundation — 39 DAGs)
  └── Start the core SAP transactional DAGs
  └── MOST CRITICAL: Sales Order, Billing Document, Credit Mgmt
  └── Let all hourly and multi-daily DAGs catch up
  └── Run manually for all missed hours if needed
  └── DO NOT proceed to Phase 2 until all 39 are green

PHASE 2 — PRIORITY 0.4 (DB ARS — 11 DAGs)
  └── Start SFDC/Mavic pipeline catches
  └── Foundation mart, Master parts, Transaction parts
  └── These can run in parallel with some Phase 1 DAGs

PHASE 3 — PRIORITY 0.2 (CFA Checkpoint — 3 DAGs)
  └── Start CFA master tables prep (17:30 equivalent)
  └── Once all 9 checkpoint dependencies are green:
      └── Manually trigger the CFA Checkpoint DAG
      └── This will then trigger the CFA mart refresh
  └── ⭐ DO NOT trigger CFA checkpoint until ALL 9 dependencies complete ⭐

PHASE 4 — PRIORITY 0.3 (Post-CFA Cleanup — 5 DAGs)
  └── Error logs, outbound delivery tracking
  └── Trigger after CFA mart refresh completes

PHASE 5 — PRIORITY 1 (22 DAGs)
  └── Infiniti pipelines (if during trading hours)
  └── ML models (trigger after BR Final)

PHASE 6 — PRIORITY 2 (89 DAGs)
  └── Dimension mart first (08:30 equivalent)
  └── Then fact marts (Sales Mart, US Sales Mart)
  └── Then Finance, OTIF, planning systems
  └── These can largely run in parallel

PHASE 7 — PRIORITY 3 (35 DAGs)
  └── Start last
  └── Skip non-time-sensitive ones if outage was long
```

## When a Single DAG Fails (Daily Support)

```
SINGLE DAG FAILURE PROTOCOL:

1. IDENTIFY: Which DAG failed? What is its priority?

2. CHECK DEPENDENCIES: 
   - What does this DAG depend on? (check Airflow graph view)
   - Is the upstream data there?
   - Have the upstream DAGs run successfully?

3. READ THE LOGS:
   - Airflow task logs → usually tells you which step failed
   - CloudWatch logs → for Glue job failures
   - SnapLogic dashboard → for extraction failures

4. CLASSIFY THE FAILURE:
   - Data issue (bad record, null in required field)
   - Infrastructure issue (connection timeout, credentials)
   - Logic issue (SQL error, schema mismatch)
   - Volume issue (job took too long → Airflow skip)

5. FIX AND RERUN:
   - Simple issues (timeout, temp connection): just rerun
   - Data issues: fix the data, rerun
   - Logic issues: needs L2, raise ticket

6. CHECK DOWNSTREAM:
   - After fixing, check if downstream DAGs need to be manually triggered
   - Especially critical: if CFA checkpoint dependencies fail, 
     the checkpoint itself won't trigger automatically — you must manually trigger it
```

---

# 15. SCHEDULING PATTERNS — UNDERSTAND EVERY SCHEDULE TYPE

## All Patterns Used in TCPL DAGs

| Pattern | Example | How Often | When Used |
|---|---|---|---|
| **Every 30 minutes** | `*:00 and *:30` | 48x/day | Highest frequency — appointment details |
| **Hourly** | `*:00 every hour` | 24x/day | Sales orders, billing documents |
| **Bi-hourly** | `Even hours :30` | 12x/day | Core SAP delta (weekdays) |
| **4-hourly** | `06:00, 10:00, 14:00, 18:00` | 4-6x/day | Weekend SAP coverage |
| **Multiple daily (5-7x)** | `06:30, 11:30, 15:30, 19:30, 03:30` | 5-7x/day | Stock data, CFA data |
| **3x daily** | `05:15, 09:15, 14:45` | 3x/day | Change documents |
| **Twice daily** | `08:50, 20:50` | 2x/day | Core SAP masters |
| **Once daily** | `03:00 AM` | 1x/day | SFDC foundation |
| **Triggered/Event-based** | After upstream completes | Varies | Part 2 DAGs, CFA mart |
| **Weekly** | `07:00 Saturday` | 1x/week | Weekly full loads, retraining |
| **Monthly** | `10:15 1st of month` | 1x/month | Error summaries, retraining |
| **On-demand** | Manual trigger only | As needed | Reconciliation, one-off loads |
| **Month-end specific** | `27th-31st only` | Varies | CFA month-end runs |

## Why Different Frequencies for Different DAGs?

```
FREQUENCY = HOW FAST DATA CHANGES × HOW CRITICAL FRESHNESS IS

Sales Orders → hourly (change every few minutes, revenue-critical)
Billing Documents → hourly (change every few minutes, revenue-critical)
Appointment Details → every 30 mins (change constantly, very time-sensitive)
Stock Data → 5x per day (changes throughout day but not every minute)
Financial Masters → daily (change rarely — once a day refresh is enough)
HR Data → daily (payroll/headcount changes aren't time-sensitive)
Monthly Reports → monthly (by definition)
ML Model Retraining → weekly/monthly (models don't need daily retraining)
```

---

# 16. MASTER GLOSSARY — EVERY TERM IN THIS DOCUMENT

| Term | What It Means |
|---|---|
| **DAG** | Directed Acyclic Graph — an Airflow pipeline definition |
| **Priority 0** | Mission Critical — revenue stops without these |
| **Priority 1** | Critical — severe operational impact |
| **Priority 2** | Important — can wait hours without major impact |
| **Priority 3** | Low — minimal business impact if delayed |
| **UTILITY DAGs** | Platform management tools, not data pipelines |
| **PAUSED DAGs** | Currently inactive — do not restart |
| **RTO** | Recovery Time Objective — max acceptable downtime |
| **CFA** | Carrying and Forwarding Agent — TCPL's dispatch warehouse |
| **CFA Checkpoint** | The 20:11 IST gate DAG — waits for 9 upstream DAGs |
| **Asterisk/EY Asterix** | CFA centralization system for billing doc generation |
| **ARS** | Auto Replenishment System — automatic distributor ordering |
| **DB ARS** | Distributor Base Auto Replenishment |
| **SFDC / Mavic** | Salesforce-based secondary sales system |
| **Infiniti** | TCPL's tea procurement platform |
| **TTP** | Tea Trading Prediction — ML model for tea buying decisions |
| **BGDN** | Buying Guidance and Decision — another ML model |
| **BR / BR Final** | Buying Report — daily tea purchase summary |
| **Digitea** | Tea procurement/trading platform integrated with Infiniti |
| **ACDOCA** | Universal Journal in SAP S4/HANA — all financial entries |
| **OTIF** | On Time In Full — supply chain delivery performance metric |
| **ARKIEVA** | Supply chain planning software |
| **Zycus** | Procurement/sourcing platform |
| **Pegasus** | Delivery/logistics management system |
| **RDD** | Required Delivery Date — from Modern Trade customers |
| **EDI** | Electronic Data Interchange — automated data exchange with retailers |
| **Blue Yonder** | Supply chain planning platform (demand forecasting) |
| **Blue Planner** | US-specific planning system |
| **SuccessFactor** | SAP HR software |
| **MDM** | Master Data Management — manages product/distributor/outlet masters |
| **Unify MDM** | MDM platform used by TCPL |
| **Mulesoft** | Integration platform for product data changes |
| **PSO** | Primary Sales Objectives — sales targets |
| **POSM** | Point of Sale Material — in-store marketing materials |
| **BOM** | Bill of Materials — what ingredients make up each product |
| **PRCD Elements** | Pricing Condition elements — pricing rules in SAP |
| **GRN** | Goods Receipt Note — confirmation goods were received |
| **MRP** | Material Requirements Planning — how much material to order |
| **Sawtooth Report** | Inventory level trend report that looks like a sawtooth wave |
| **ATK** | Automated Testing Kit — table count validation DAGs |
| **SharePoint** | Microsoft SharePoint — used for business planning data uploads |
| **Lakshya** | TCPL internal target-setting program (means "target" in Hindi) |
| **Samruddhi** | TCPL internal business initiative |
| **Shikhar** | TCPL specific sales/business initiative |
| **Project Leap** | A specific TCPL business transformation project |
| **Agriwatch** | Agricultural commodity price monitoring |
| **PACE** | A planning/scheduling system |
| **SageMaker** | AWS machine learning platform |
| **Delta Load** | Extract only new/changed records since last run |
| **Full Load** | Extract all records from source |
| **Delta Pipeline** | A pipeline that does delta (incremental) extraction |
| **Triggered DAG** | A DAG that starts when another DAG completes (not on a time schedule) |
| **Event-based DAG** | Same as triggered — fires on an event, not a clock |
| **Checkpoint DAG** | A special DAG that waits for multiple upstream DAGs before proceeding |
| **Mart Refresh** | Rebuilding a Snowflake data mart with latest data |
| **Part 1 → Part 2** | Pattern where large pipelines split into sequential jobs |
| **Backfill** | Running a pipeline for historical missed dates |
| **Month-end** | Period 25th-31st when extra pipeline runs happen |
| **Deployment Freeze** | 25th-3rd/4th — no platform changes allowed |
| **SUBDB/MUOB** | Sub-Distributor / Multi-Outlet Business — specialized distributor types |

---

## One Final Note — How to Use This Document in Real Work

When your manager says "the CFA checkpoint failed last night" — you now know:
- It's the 20:11 IST DAG (Priority 0.2)
- It waits for 9 specific upstream DAGs
- First thing: check which of those 9 failed
- Fix that upstream DAG
- Then manually trigger the CFA checkpoint again
- Then verify the CFA mart refresh ran
- Then check if error logs (22:15) captured the issue

When your manager says "sales mart data is stale" — you know:
- It's the snowflake-fact-mart DAG (Priority 2.1)
- It runs at 07:30, 12:30, 20:30
- Check if SAP loads (Priority 0.1) completed before the mart ran
- Check if the mart DAG itself had an error
- If SAP loads were delayed past 07:30, the 07:30 mart will have partial data

When your manager says "ARS report is missing" — you know:
- It's prd-snowflake-CFA-ARSReport (Priority 0.4)
- It runs at 05:00 AM
- Check if the SFDC foundation mart (03:00) completed first
- If SFDC foundation failed, ARS report will also fail

You are now prepared to work on this platform. 💪

---

*Document prepared for Decision Point data engineers working on TCPL project.*
*Based on: Pipeline Priority and Criticality Documentation by Hamza Palengara*
*Reference: Snowflake Outage Catchup (Dec 16-17, 2025)*
*Classification: INTERNAL*
