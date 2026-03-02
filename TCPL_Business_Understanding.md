# 🍵 TCPL Sales & Marketing — Complete Business Guide for Data Engineers
### *Written simply so you understand everything inside-out*

---

## 🧭 First — What is TCPL?

**TCPL = Tata Consumer Products Limited**

They sell everyday products like **Tea, Coffee, Salt, Pulses, Water** etc. Think brands like Tata Tea, Himalayan Water, Tata Salt.

You (Decision Point) are the **data team** working for TCPL. Your job is to build data pipelines, models, and dashboards so that TCPL's business teams can see how their sales are performing.

---

## 📦 Part 1: How Does TCPL Sell Products? (The Big Picture)

Think of it like a chain. Products travel from the factory to the customer through multiple stops:

```
🏭 TCPL Factory
      ↓
🏢 Distributor (DB)        ← PRIMARY SALES happens here
      ↓
🏪 Retailer (Kirana/Shop)  ← SECONDARY SALES happens here
      ↓
🧑 End Consumer            ← TERTIARY SALES (not tracked by TCPL)
```

### The 3 Types of Sales — Super Important!

| Sales Type | Who Sells? | Who Buys? | Is it Tracked? | System Used |
|---|---|---|---|---|
| **Primary Sales** | TCPL | Distributor | ✅ Yes, fully | SAP |
| **Secondary Sales** | Distributor | Retailer | ✅ Yes, partially | SFDC (Mavic) |
| **Tertiary Sales** | Retailer | Customer | ❌ Not tracked (only in Nielsen) | Nielsen |

> 💡 **As a Data Engineer:** Your SAP data = Primary Sales data. Your SFDC/Mavic data = Secondary Sales data. Nielsen data exists but is not fully used in the platform yet.

---

## 🔄 Part 2: Order-to-Delivery Flow (Step by Step)

This is how ONE order moves from start to finish. Understand this deeply — your data models will be built around these steps.

### Step-by-Step Journey of an Order:

```
Step 1️⃣  Distributor places order on mobile app (OMS)
              ↓
Step 2️⃣  Order goes to SAP as a Purchase Order (PO)
              ↓
Step 3️⃣  SAP converts PO → Sales Order (SO)
              ↓
Step 4️⃣  SAP creates Delivery Order (DO)
              ↓
Step 5️⃣  CFA checks: Does distributor have enough credit?
         ├── YES → Billing happens
         └── NO  → Order goes ON HOLD ⛔
              ↓
Step 6️⃣  SAP generates Billing Document (Invoice) 🧾
              ↓
Step 7️⃣  Goods are dispatched (shipped to distributor)
              ↓
Step 8️⃣  Distributor receives goods → DMS shows "In Stock" ✅
              ↓
Step 9️⃣  Distributor sells to Retailer → SFDC (Mavic) records this
```

### What Each Document Means (Data Engineer Perspective):

| Document | What It Is | Why It Matters for You |
|---|---|---|
| **PO (Purchase Order)** | Distributor's request to TCPL | First record in SAP — starting point of a transaction |
| **SO (Sales Order)** | TCPL accepts and processes the order | Main document for billing, stock planning |
| **DO (Delivery Order)** | Instruction to warehouse to pack & ship | Triggers physical movement of goods |
| **Billing Document** | The actual invoice/sale record | This is what you count as "revenue" in reports |

> 💡 **Key insight:** Revenue is only confirmed at the **Billing Document** stage. Before that, it's just an order — not a sale.

---

## 👥 Part 3: Who Are All These People? (Sales Personas / Hierarchy)

TCPL has a structured organization. Think of it like an army — top to bottom.

```
🏭 Vendor           → Supplies raw materials to TCPL
🏢 TCPL             → The company itself (manufacturer + brand owner)
👔 NSH              → National Sales Head (top sales boss in India)
🗺️ Cluster Head     → Manages a large region (e.g., North, South)
    └── Sub-Cluster  → Smaller zone within cluster (e.g., North1, North2)
📍 Region           → Even smaller zone
👨‍💼 ASM              → Area Sales Manager (manages multiple regions)
🧑‍💼 TSE              → Territory Sales Executive (on the ground, manages distributors)
🏢 DB               → Distributor (holds stock, sells to retailers)
🧑‍🤝‍🧑 Salesmen          → Work under distributor, visit shops daily
🏪 Outlet           → Kirana stores, Modern Trade (BigBazaar), Ecommerce (Amazon)
```

### What Each Role Does (Simplified):

| Role | Their Job | What they check in dashboards |
|---|---|---|
| **NSH** | Overall India sales strategy | How is India performing vs target? |
| **Cluster Head** | Manage multiple states/regions | Which region is lagging? |
| **ASM** | Manage distributors in an area | Which distributor is underperforming? |
| **TSE** | Manage 10-15 distributors on ground | Which outlet is not ordering? |
| **Distributor (DB)** | Stock goods, sell to retailers | What's my current stock? What did I sell today? |
| **Salesman** | Visit shops, take orders, deliver goods | Which shops did I visit? What orders did I collect? |

> 💡 **As a Data Engineer:** Your data models need to support filtering/aggregation at every level of this hierarchy. A NSH sees national data. An ASM sees only his area data. This is why **geo hierarchy** (3 levels) and **sales hierarchy** matter so much — and why row-level security in ThoughtSpot is critical.

---

## 💻 Part 4: The Systems — Where Does Data Come From?

### The 3 Key Source Systems:

#### 1. SAP — The Heart of Primary Sales
- Every order placed by a distributor flows through SAP
- SAP manages: PO → SO → DO → Billing
- **This is your primary sales data source**
- Data you'll find: Orders, Deliveries, Invoices, Stock movements, Credit checks

#### 2. SFDC / Mavic — The Heart of Secondary Sales
- SFDC = Salesforce. TCPL calls their version "Mavic"
- Used by Salesmen and TSEs on their phones
- Captures what happens AFTER goods reach the distributor
- **This is your secondary sales data source**

What Mavic captures:

| Data | What It Means |
|---|---|
| Retailer Orders | Distributor taking orders from shops |
| Order-to-invoice | Did the retailer order actually get billed? |
| Stock in/out | Is there stock available at distributor? |
| Returns | Goods sent back from retailers |
| Claims | Distributor claiming money back (e.g., damaged goods) |
| Schemes & Promotions | Discounts/offers given to retailers |
| Visits & Attendance | Did the salesman actually visit the shop today? |
| Collection | Payments collected from retailers |
| SFA Audit | Sales Force Automation checks |
| Distributor Onboarding | New distributor being added |
| Outlet Onboarding | New shop being added to the network |

#### 3. OMS — Order Management System
- Mobile app used by distributors to place orders
- Orders from here flow into SAP automatically
- Different order types exist:

| Order Type | Full Form | What It Means |
|---|---|---|
| **ARS** | Auto Replenishment System | System auto-orders based on past patterns |
| **DBO** | Distributor Bulk Order | Large orders placed by distributor |
| **SFL** | Shelf Lifting | For canteens, hotels |
| **BO** | Bulk Order | Bulk orders by retailers |
| **RO** | Rush Order | Urgent orders |
| **LO** | Liquid Orders | Products near expiry being cleared fast |

---

## 🏗️ Part 5: The Data Platform — YOUR World as a Data Engineer

This is where **you and Decision Point** come in. You take raw data from all these systems and make it useful.

### The Full Data Flow (Technical):

```
📱 OMS (Mobile App — Distributors place orders)
        ↓
🏢 SAP (PO → SO → DO → Billing)          📊 SFDC/Mavic (Secondary Sales)
        ↓                                              ↓
🔌 SnapLogic (Ingestion Layer — pulls data from SAP & Mavic into the platform)
        ↓
🪣 AWS S3 (Raw Zone — raw data stored here, like a staging area)
        ↓
⚙️ AWS Glue (Processing — cleans, transforms, and structures the data)
        ↓
❄️ Snowflake (Data Warehouse — Facts & Dimensions built here)
        ↓
📈 ThoughtSpot (BI Tool — business users see dashboards here)

Also:
❄️ Snowflake → 📋 EY Asterix (for generating PDP, DO, Billing Docs — CFA Centralization project)
```

### What Each Layer Does:

| Layer | Tool | Your Job Here |
|---|---|---|
| **Ingestion** | SnapLogic | Pull data from SAP & Mavic into S3 |
| **Raw Zone** | AWS S3 | Store raw data as-is (don't transform here) |
| **Processing** | AWS Glue | Clean, transform, join, deduplicate |
| **Warehouse** | Snowflake | Build Fact tables, Dimension tables, KPIs |
| **Analytics** | ThoughtSpot | Business users self-serve their queries |

### What You Build in Snowflake:

| What You Build | What It Is | Example |
|---|---|---|
| **Fact Tables** | Transaction-level data | fact_primary_sales, fact_orders, fact_billing |
| **Dimension Tables** | Master data | dim_product, dim_geography, dim_distributor, dim_date |
| **KPIs & Formulas** | Business metrics | Sales Achievement %, Fill Rate, Out-of-Stock % |
| **Analytical Models** | Ready-to-query models for ThoughtSpot | Sales performance model, Secondary sales model |

### The 3 Data Hierarchies You Must Know:

**1. Geography Hierarchy (3 levels):**
```
Country → Cluster/Sub-cluster → Region
```

**2. Sales Hierarchy:**
```
NSH → Cluster Head → ASM → TSE → Distributor → Salesman → Outlet
```

**3. Product Hierarchy (7 levels):**
```
Category → Brand → Variant → Pack Type → SKU → etc.
```

---

## 📊 Part 6: ThoughtSpot — Who Uses Your Data?

ThoughtSpot is the BI (dashboard) tool. Business users log in here to see reports.

### Scale of the Platform:

| Metric | Number |
|---|---|
| Distributors | 30,000 |
| Outlets (Shops) | 1.2 Million |
| Parent SKUs (Products) | 7,000 – 10,000 |
| Product Categories | 8 |
| Product Hierarchies | 7 |
| Geo Hierarchies | 3 |
| ThoughtSpot Users | 3,000 |
| Reports & Liveboards | 2,500+ |

### Who Uses ThoughtSpot?

| User Group | What They Look At |
|---|---|
| **Sales Team (800 users)** | Daily sales vs target, distributor performance |
| **RGM (Revenue Growth Management)** | Pricing, promotions, pack-size analysis |
| **Strategy** | Market share, growth trends |
| **Finance** | Revenue, collections, billing accuracy |
| **Shopper Marketing** | Outlet-level performance, scheme effectiveness |
| **Marketing** | Category & brand performance |

---

## 🔗 Part 7: Complete End-to-End Story (Real Example)

Let me tell you the complete story of ONE product sale, from factory to dashboard:

> **Story:** Tata Salt reaches a Kirana store in Delhi

| Step | What Happens | System |
|---|---|---|
| 1 | TCPL decides to sell Tata Salt through Delhi distributor | — |
| 2 | Distributor opens OMS app, orders 1000 units (ARS order) | OMS |
| 3 | Order lands in SAP as **Purchase Order (PO)** | SAP |
| 4 | SAP converts to **Sales Order (SO)**, creates **Delivery Order (DO)** | SAP |
| 5 | CFA checks credit — distributor has ₹2L credit, OK to proceed | SAP/CFA |
| 6 | SAP generates **Invoice (Billing Document)** = PRIMARY SALE recorded ✅ | SAP |
| 7 | Goods dispatched to Delhi distributor | Warehouse/CFA |
| 8 | Distributor receives goods → **DMS shows "In Stock"** | DMS/Mavic |
| 9 | Distributor's salesman visits Kirana store, delivers 50 units | Field |
| 10 | **SFDC (Mavic)** records this as **Secondary Sale** ✅ | Mavic |
| 11 | SnapLogic pulls data from SAP & Mavic overnight | SnapLogic |
| 12 | Raw data lands in **AWS S3** | S3 |
| 13 | **AWS Glue** cleans and transforms the data | Glue |
| 14 | Clean data goes into **Snowflake** (Facts + Dimensions ready) | Snowflake |
| 15 | ASM opens ThoughtSpot next morning → sees "Tata Salt: 1000 billed, 50 at retail" | ThoughtSpot |

---

## ⚡ Quick Reference Cheat Sheet

### Systems Map:
```
OMS        → Order placement (Distributor mobile app)
SAP        → Primary sales (PO, SO, DO, Billing, Stock)
Mavic/SFDC → Secondary sales (Retailer orders, visits, stock, schemes)
Nielsen    → Tertiary data (not fully used yet)
SnapLogic  → Data ingestion (pulls from SAP & Mavic)
S3         → Raw data lake (staging area)
Glue       → Data processing/transformation
Snowflake  → Data warehouse (Facts, Dimensions, KPIs)
ThoughtSpot→ BI dashboards for business users
EY Asterix → CFA centralization (billing doc generation)
```

### Key Terms Glossary:

| Term | Full Form | What It Means |
|---|---|---|
| TCPL | Tata Consumer Products Limited | Your client |
| DB | Distributor / Distribution Center | Middleman between TCPL and shops |
| NSH | National Sales Head | Top sales boss |
| ASM | Area Sales Manager | Manages a district/area |
| TSE | Territory Sales Executive | On-ground salesman supervisor |
| CFA | Carry & Forwarding Agent | Warehouse agent who dispatches goods |
| DMS | Distributor Management System | Distributor's own inventory system |
| SFA | Sales Force Automation | App/system used by field salesmen |
| OMS | Order Management System | Mobile app for placing orders |
| PO | Purchase Order | Distributor's order request |
| SO | Sales Order | TCPL's confirmed order |
| DO | Delivery Order | Instruction to dispatch goods |
| RGM | Revenue Growth Management | Team that manages pricing & promotions |
| SKU | Stock Keeping Unit | Individual product variant |
| ARS | Auto Replenishment System | Auto-order based on stock levels |

---

## 🎯 What This Means For You as a Data Engineer — Key Takeaways

1. **Primary Sales = SAP = TCPL → Distributor** (your most reliable, complete dataset)
2. **Secondary Sales = Mavic/SFDC = Distributor → Retailer** (important but partial)
3. **Revenue is booked at Billing Document** in SAP — not at order or delivery stage
4. **Credit check by CFA can block an order** — so orders ≠ sales in data (always check status)
5. **The hierarchy (NSH → ASM → TSE → DB)** is critical for row-level security in ThoughtSpot
6. **30,000 distributors + 1.2M outlets = very large data volumes** — optimize your Snowflake queries!
7. **SnapLogic is your ingestion tool** — understand its pipeline schedules (likely nightly batch)
8. **Snowflake is your modeling layer** — know the difference between core zone and business-ready zone

---

*Document prepared for Decision Point Data Engineers working on TCPL project*  
*Based on: Sales & Marketing Process Overview Session — Facilitated by Arnab (S&M Lead)*  
*Classification: INTERNAL*
