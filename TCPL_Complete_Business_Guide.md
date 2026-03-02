# 🍵 TCPL Business — COMPLETE Deep Dive Guide
## For Data Engineers at Decision Point
### *Read this once and you will understand everything inside-out*

---

> **Before you start reading:** Imagine you are a new employee joining TCPL. You don't know anything. By the end of this document, you should be able to explain the entire business to someone else. That is the goal. Every section builds on the previous one. Read it in order.

---

# 📖 TABLE OF CONTENTS

1. [What is TCPL? — The Company Story](#1-what-is-tcpl)
2. [How TCPL Makes Money — The Sales Chain](#2-how-tcpl-makes-money)
3. [The 3 Types of Sales — Explained Deeply](#3-the-3-types-of-sales)
4. [Every Person in the Business — Who Does What](#4-every-person-in-the-business)
5. [How One Order Travels — Step by Step (Very Detailed)](#5-how-one-order-travels)
6. [The Systems — Every Software Explained](#6-the-systems)
7. [Order Types — What Kind of Orders Exist](#7-order-types)
8. [The Data Platform — Your World as Data Engineer](#8-the-data-platform)
9. [ThoughtSpot — Who Uses Your Data and How](#9-thoughtspot)
10. [SFDC Mavic — The Secondary Sales Brain](#10-sfdc-mavic)
11. [Real Life Story — One Product, Full Journey](#11-real-life-story)
12. [The Big Picture — Everything Connected](#12-the-big-picture)
13. [Data Engineering Specific Insights](#13-data-engineering-insights)
14. [Master Glossary — Every Term Explained](#14-master-glossary)

---

# 1. WHAT IS TCPL?

## 1.1 Full Name and Background

**TCPL = Tata Consumer Products Limited**

TCPL is one of India's largest FMCG (Fast Moving Consumer Goods) companies. They are part of the **Tata Group** — one of India's most trusted business groups.

FMCG means products that:
- Are used every day by common people
- Are sold in large quantities
- Are low cost per unit
- Move very fast from factory to shop to home

Think about your daily life. When you make tea in the morning, you use **Tata Tea**. When you cook food, you use **Tata Salt**. When you drink mineral water, you might drink **Himalayan Water**. All of these are TCPL products.

## 1.2 What Products Does TCPL Sell?

TCPL sells products across **8 product categories**:

| Category | Example Brands |
|---|---|
| Tea | Tata Tea, Tetley, Chakra Gold, Kanan Devan |
| Coffee | Eight O'Clock Coffee, Tata Coffee |
| Salt | Tata Salt, Tata Salt Lite, i-Shakti Salt |
| Pulses & Grains | Tata Sampann |
| Spices | Tata Sampann Spices |
| Nuts & Snacks | Tata Sampann, Soulfull |
| Water | Himalayan Mineral Water, Tata Copper Water |
| International | Tetley (UK, Canada), Good Earth (UK) |

They have between **7,000 to 10,000 parent SKUs** (individual product variants).

## 1.3 Where Does TCPL Sell?

TCPL sells across:
- **India** — Primary + Secondary sales tracked
- **International Markets** — Primary sales tracked
- They reach **1.2 million outlets** (shops/stores)
- They work with **30,000 distributors** across India

## 1.4 Why Does This Matter for You as a Data Engineer?

You are not an employee of TCPL. You work for **Decision Point**, which is a **data analytics consulting company**. Decision Point has been hired by TCPL to:
- Build and manage their data infrastructure
- Create data pipelines
- Build analytical models in Snowflake
- Power their ThoughtSpot dashboards

So your work directly affects the decisions that TCPL's 3,000+ business users make every day.

---

# 2. HOW TCPL MAKES MONEY — THE SALES CHAIN

## 2.1 The Basic Problem TCPL Faces

TCPL has factories that make products. But they cannot directly go and sell to every single small Kirana shop in India. Think about it — there are **1.2 million outlets** across India. TCPL cannot have salespeople visiting each of those shops. It would be impossible.

So they created a **distribution network**. This is a chain of middlemen who help products move from the factory to the final consumer.

## 2.2 The Distribution Chain — Visualized

```
🏭 TCPL FACTORY
   (Makes the product)
        ↓
        ↓ sells in large quantity
        ↓
🏢 DISTRIBUTOR (DB)
   (Buys in bulk from TCPL, stores it, sells to shops)
        ↓
        ↓ sells in medium quantity
        ↓
🏪 RETAILER / OUTLET
   (Kirana store, Modern Trade, E-commerce)
        ↓
        ↓ sells 1 or 2 units
        ↓
👨‍👩‍👧 FINAL CONSUMER
   (You and me buying Tata Salt at the shop)
```

## 2.3 Why This Chain Exists

Each level in the chain serves a specific purpose:

**Distributor exists because:**
- They have local warehouses
- They know local retailers in their area
- They have delivery vans and manpower
- They give credit to retailers (so retailers don't need to pay immediately)
- TCPL doesn't need to manage thousands of small retail relationships

**Retailer exists because:**
- They are the final point where the consumer can buy
- They provide convenience — consumer just walks in and buys
- They stock many different brands and products

---

# 3. THE 3 TYPES OF SALES — EXPLAINED DEEPLY

## 3.1 Overview

Every time a product changes hands in this chain, it is counted as a "sale". TCPL tracks these movements and calls them **Primary, Secondary, and Tertiary** sales.

## 3.2 PRIMARY SALES — The Most Important One

### What is Primary Sales?
**Primary Sale = When TCPL sells to a Distributor**

Example:
> TCPL's warehouse in Mumbai ships 5,000 boxes of Tata Tea to a distributor in Pune. The distributor pays TCPL ₹10 lakhs. This is a PRIMARY SALE.

### Key Facts About Primary Sales:
- This is **TCPL's direct revenue** — this is how TCPL earns money
- It covers **India AND International** markets
- The data comes from **SAP** (TCPL's ERP system)
- Every rupee TCPL earns is recorded as a Primary Sale in SAP
- The "billing document" in SAP is when Primary Sale is officially confirmed
- Primary Sales data is the most complete and reliable dataset you will work with

### What Data is Available for Primary Sales?
- Which distributor bought what products
- How many units were ordered vs how many were delivered
- What was the invoice value
- When was the order placed, when was it delivered
- Credit status of the distributor
- Which warehouse/CFA dispatched the goods

### Why Primary Sales Alone is NOT Enough?
Primary sales tells you "TCPL sold to distributor." But it doesn't tell you:
- Did the distributor actually sell those products to retailers?
- Are the products sitting in the distributor's warehouse?
- Are the products reaching the final consumer?

This is why Secondary Sales data is also important.

---

## 3.3 SECONDARY SALES — The Ground Reality

### What is Secondary Sales?
**Secondary Sale = When a Distributor sells to a Retailer**

Example:
> The Pune distributor sends his salesman to 50 Kirana stores. The salesman delivers 200 packets of Tata Tea to a shop in Koregaon Park. The shop pays ₹5,000. This is a SECONDARY SALE.

### Key Facts About Secondary Sales:
- This covers **India ONLY** (not international)
- The data comes from **SFDC (Mavic)** — Salesforce-based system
- This data is captured by the **salesman's mobile app** when he visits the shop
- Secondary Sales data tells you what's actually happening at the ground level
- This data is partially complete (not 100% reliable like SAP)

### Why Secondary Sales Matters So Much?
If primary sales are high but secondary sales are low, it means:
- Products are piling up in distributor warehouses (**channel stuffing** problem)
- Products are not reaching consumers
- There may be a demand problem with the product
- Retailer may not be ordering because consumers aren't buying

The gap between Primary and Secondary Sales is a key business metric.

### What Data is Available for Secondary Sales?
- Which retailer bought what products
- How many units were sold by distributor to retailer
- Stock availability at distributor level
- Schemes and promotions given to retailers
- Returns from retailers
- Visit data — did the salesman visit the shop

---

## 3.4 TERTIARY SALES — The Missing Piece

### What is Tertiary Sales?
**Tertiary Sale = When a Retailer sells to the Final Consumer**

Example:
> You walk into a Kirana store and buy 1 kg Tata Salt for ₹25. This is a TERTIARY SALE.

### Key Facts About Tertiary Sales:
- **TCPL does NOT capture this data directly**
- The retailer does not share their sales data with TCPL
- Some tertiary data is captured by **Nielsen** (a third-party market research company)
- Nielsen sends surveyors to sample stores and estimates consumer sales
- But this Nielsen data is NOT fully utilized in the TCPL data platform yet

### Why Does This Data Gap Exist?
- There are 1.2 million outlets — it's impossible to track every consumer purchase
- Retailers don't want to share their sales data (they see it as proprietary)
- No formal system connection between retailer and TCPL
- Nielsen data exists but is expensive and estimated (not exact)

### Why Does Tertiary Data Matter?
Tertiary sales = actual consumer demand. If TCPL knew exactly what consumers were buying, they could:
- Predict future orders more accurately
- Know which products are losing market share
- Make better production plans

---

## 3.5 Summary: The 3 Sales Types Side by Side

| Aspect | Primary | Secondary | Tertiary |
|---|---|---|---|
| **Transaction** | TCPL → Distributor | Distributor → Retailer | Retailer → Consumer |
| **Geography** | India + International | India Only | India (partial via Nielsen) |
| **System** | SAP | SFDC (Mavic) | Nielsen (not in platform) |
| **Data Quality** | ✅ Complete & Reliable | ⚠️ Partial, needs validation | ❌ Not available directly |
| **Revenue Impact** | Direct TCPL revenue | Distributor revenue | Retailer revenue |
| **Who Monitors** | Finance, Sales Leadership | Sales team, TSE, ASM | RGM, Marketing |
| **Volume** | Large bulk quantities | Medium quantities | Small individual quantities |

---

# 4. EVERY PERSON IN THE BUSINESS — WHO DOES WHAT

## 4.1 The Hierarchy — Top to Bottom

The TCPL sales organization is structured like a pyramid. At the top, one person is responsible for all of India. At the bottom, individual salesmen are responsible for a few streets in a neighbourhood.

```
                    ┌─────────────┐
                    │   VENDOR    │  ← Supplies raw materials
                    └──────┬──────┘
                           ↓
                    ┌─────────────┐
                    │    TCPL     │  ← The Company (HQ)
                    └──────┬──────┘
                           ↓
                    ┌─────────────┐
                    │     NSH     │  ← National Sales Head
                    └──────┬──────┘
                           ↓
               ┌───────────┴───────────┐
          ┌────┴────┐             ┌────┴────┐
          │ Cluster │             │ Cluster │  ← e.g., North, South
          │ (North) │             │ (South) │
          └────┬────┘             └────┬────┘
               ↓                       ↓
     ┌─────────┴─────────┐   ┌─────────┴─────────┐
     │   Sub-Cluster     │   │   Sub-Cluster     │  ← e.g., North1, North2
     │   (North1)        │   │   (North2)        │
     └─────────┬─────────┘   └─────────┬─────────┘
               ↓
         ┌─────┴──────┐
         │   Region   │  ← e.g., Delhi, Noida
         └─────┬──────┘
               ↓
         ┌─────┴──────┐
         │    ASM     │  ← Area Sales Manager
         └─────┬──────┘
               ↓
         ┌─────┴──────┐
         │    TSE     │  ← Territory Sales Executive
         └─────┬──────┘
               ↓
    ┌──────────┴──────────┐
    │   DB (Distributor)  │  ← Distributor
    └──────────┬──────────┘
               ↓
    ┌──────────┴──────────┐
    │      Salesmen       │  ← Field salesman
    └──────────┬──────────┘
               ↓
    ┌──────────┴──────────┐
    │  Outlet (Retailer)  │  ← Kirana, Modern Trade, Ecomm
    └─────────────────────┘
```

---

## 4.2 Each Role Explained in Full Detail

### VENDOR
**Who they are:**
Vendors are companies that supply raw materials to TCPL's factories.

**What they supply:**
- Tea leaves (from tea estates)
- Coffee beans
- Salt (from salt pans)
- Packaging materials (boxes, pouches, cartons)
- Chemicals, preservatives, flavors

**In data:**
- Vendor data sits in SAP (procurement module)
- Not directly relevant to sales analytics
- But important for supply chain planning

---

### TCPL (The Company)
**Who they are:**
TCPL is the parent company — the manufacturer, brand owner, and seller.

**What TCPL does:**
- Manufactures products in factories
- Develops brands and marketing campaigns
- Sets national sales targets and strategies
- Manages the distributor network
- Decides pricing, promotions, and schemes
- Manages export/international business

**In data:**
- All of TCPL's transactions are in SAP
- Marketing data may be in Mavic/SFDC
- Financial data is in SAP

---

### NSH — National Sales Head
**Who they are:**
One person (or a small team) who is responsible for ALL sales across India.

**What they do:**
- Set annual/quarterly national sales targets
- Monitor performance of all clusters and regions
- Make strategic decisions (which products to push, which regions to focus on)
- Escalate issues at national level (distributor problems, scheme effectiveness)
- Represent sales function in TCPL board/leadership meetings

**What they look at in ThoughtSpot:**
- National sales dashboard (monthly, quarterly)
- Sales vs target at national and cluster level
- Which categories/brands are growing or declining
- Market share data (from Nielsen)
- Revenue and billing summaries

**In data:**
- They see aggregated data — not individual transactions
- They need high-level KPIs: Total Revenue, % Achievement vs Target, YOY Growth
- They don't need distributor-level detail — they care about trends

---

### Cluster Head
**Who they are:**
India is divided into large geographic clusters (e.g., North India, South India, East India, West India, Modern Trade, E-commerce).

Each cluster is managed by a **Cluster Head**.

**What they do:**
- Manage multiple regions within their cluster
- Set cluster-level targets based on NSH's national targets
- Monitor region and ASM performance
- Resolve issues that ASMs escalate
- Coordinate with marketing for region-specific campaigns

**Sub-Clusters:**
Clusters are further divided into sub-clusters. For example:
- North Cluster → North1 (UP, Bihar) and North2 (Delhi, Haryana)

**What they look at in ThoughtSpot:**
- Cluster-level sales performance
- Region comparisons within their cluster
- Distributor health in their cluster
- Out-of-stock situations in their region
- Scheme effectiveness

---

### Region
**What is a Region?**
A region is a smaller geographic zone within a cluster. For example:
- Delhi could be one region
- Noida could be another region
- Pune could be a region

Each region has an ASM responsible for it.

**Why does the region level matter in data?**
- Geo hierarchy in your data models will have region as a dimension
- Row-level security in ThoughtSpot is often set at region level
- Sales targets are assigned at region level

---

### ASM — Area Sales Manager
**Who they are:**
The ASM is the mid-level sales manager. They typically manage 1-2 regions.

**What they do:**
- Manage a team of TSEs (usually 5-10 TSEs report to one ASM)
- Set territory-level targets for TSEs
- Monitor distributor performance closely
- Ensure schemes and promotions are being implemented
- Handle escalations from TSEs (credit issues, delivery problems, etc.)
- Visit distributors and key retailers occasionally
- Focus heavily on secondary sales performance in their area

**What they look at in ThoughtSpot:**
- Their region's primary and secondary sales
- Distributor-wise performance (which distributor is underperforming)
- TSE-wise performance (which TSE is not meeting targets)
- Out-of-stock status at distributors
- Scheme utilization (are distributors using the offered schemes?)
- Visit compliance (are salesmen visiting their assigned shops?)

**In data:**
- They need data filtered to their region/area only
- They care about both primary (SAP) and secondary (Mavic) data
- They compare week-over-week, month-over-month performance

---

### TSE — Territory Sales Executive
**Who they are:**
The TSE is the ground-level manager. They are the link between TCPL and the distributors.

**What they do:**
- Manage 10-15 distributors in their territory
- Visit distributors regularly (sometimes daily)
- Ensure distributors are ordering enough stock
- Coordinate delivery and logistics
- Train salesman teams of distributors
- Execute schemes and promotions at the retail level
- Onboard new distributors and outlets
- Handle retailer complaints and returns
- Do route planning for salesmen

**What they look at in ThoughtSpot:**
- Their distributor-wise sales
- Which distributor hasn't ordered in X days (inactive distributor)
- Stock levels at each distributor
- Outlet-wise coverage (which shops are being visited vs missed)
- Beat plans (planned route for salesmen each day)

**In data:**
- TSEs are very operational users — they need daily, sometimes hourly data
- They care about which outlets haven't been visited, which haven't placed orders
- They check Mavic on their phone constantly

---

### DB — Distributor / Distribution Center
**Who they are:**
The distributor is a third-party business partner of TCPL. They are NOT TCPL employees. They are independent businesses who have a contract with TCPL to distribute their products in a specific geography.

**What they do:**
- Place bulk orders with TCPL for products (Primary Sales trigger)
- Store products in their warehouse
- Manage their own inventory
- Sell products to retailers in their area (Secondary Sales)
- Manage their own salesmen and delivery teams
- Give credit to retailers (retailers pay distributors later)
- Collect payments from retailers
- Return unsold/expired/damaged goods to TCPL
- Implement schemes and promotions at the retail level

**Their Economics:**
- They buy from TCPL at a specific price (distributor price)
- They sell to retailers at a slightly higher price
- Their profit is the margin between buying and selling price
- Plus they get incentives/bonuses from TCPL if they achieve targets

**In data (SAP — Primary Side):**
- Distributor master data (name, location, credit limit, code)
- Their purchase orders, delivery orders, billing documents
- Their credit balance and outstanding dues
- Their primary sales history

**In data (Mavic — Secondary Side):**
- Their secondary sales to retailers
- Their salesman's visit reports
- Their stock levels
- Their collections from retailers

---

### Salesmen (Under Distributor)
**Who they are:**
Each distributor has their own team of salesmen. These are the people who physically go to shops every day.

**What they do:**
- Follow a "beat plan" — a fixed route of shops to visit each day
- Visit each assigned shop as per schedule
- Take orders from shopkeepers
- Deliver products (sometimes carry van with stock)
- Collect payments from retailers
- Ensure product visibility (proper placement of products on shelves)
- Report any competitor activities
- Onboard new outlets (register new shops in Mavic)

**Beat Plan:**
A beat plan is a pre-decided schedule. For example:
- Monday: Salesman A visits 30 shops in Koregaon Park area
- Tuesday: Salesman A visits 25 shops in Viman Nagar area
- Wednesday: Salesman A visits Kalyani Nagar area
This ensures systematic coverage of all outlets.

**In data (Mavic):**
- Visit check-in/check-out records (GPS-tagged)
- Orders placed at each outlet
- Attendance records
- Payment collections recorded

---

### Outlet / Retailer
**What are outlets?**
Outlets are the shops where consumers buy products. TCPL categorizes them as:

| Outlet Type | Description | Examples |
|---|---|---|
| **Kirana Store** | Small independent grocery shop | Your local neighbourhood provision store |
| **Modern Trade (MT)** | Large organized retail chains | BigBazaar, DMart, Reliance Smart, More |
| **E-Commerce** | Online platforms | Amazon, Flipkart, Blinkit, Zepto, Swiggy Instamart |
| **HoReCa** | Hotel, Restaurant, Catering | Hotels, restaurant chains, canteens |
| **Institutional** | Offices, corporates | Office cafeterias, bulk buyers |

**Why outlet classification matters in data:**
- Different outlet types have different buying patterns
- Modern Trade buys in larger quantities but demands more discounts
- Kirana stores are more numerous but each buys small quantities
- E-commerce is growing rapidly — TCPL tracks this separately
- Schemes and promotions differ by outlet type

---

# 5. HOW ONE ORDER TRAVELS — STEP BY STEP (VERY DETAILED)

## 5.1 The Complete Journey — With Every Detail

Let's follow ONE order completely from the moment a distributor decides to order, to the moment the goods reach his warehouse.

---

### STEP 1: Distributor Places Order via OMS (Mobile App)

**What happens:**
The distributor or his staff opens the **OMS (Order Management System)** mobile application. This is a TCPL-provided app that is installed on the distributor's phone/tablet.

**Inside the OMS app:**
- The distributor can see all TCPL products
- He can see current stock he has (so he knows what to order)
- He can see his credit limit and available credit
- He selects products and quantities he wants to order
- He selects the order type (ARS, DBO, RO, etc. — more on these later)
- He submits the order

**What data is created:**
- Order ID (unique identifier)
- Distributor ID
- List of products (SKUs) and quantities
- Order type
- Timestamp of when order was placed
- Delivery address

**Why this step matters for data:**
This is the very first record in the entire order journey. Every subsequent step traces back to this original order.

---

### STEP 2: Order Reaches SAP as a Purchase Order (PO)

**What happens:**
The OMS app is connected to SAP via an API integration. When the distributor submits the order, the data is automatically sent to SAP.

**In SAP, this creates:**
- A **Purchase Order (PO)** — this represents the distributor's request to purchase products from TCPL

**Important nuance:**
The term "Purchase Order" here is from TCPL's perspective. TCPL is the seller. The distributor is purchasing. So in SAP, when TCPL receives this order, they see it as the distributor's Purchase Order (i.e., "here is what the distributor wants to buy from us").

**What data is in the PO:**
- PO Number (key field — you'll use this to join data)
- Distributor code and name
- Product codes (material numbers) and quantities
- Requested delivery date
- Pricing conditions (how much to charge the distributor)
- Status: Open (just created)

---

### STEP 3: SAP Converts PO to Sales Order (SO)

**What happens:**
Inside SAP, the system (automatically or with approval) converts the Purchase Order into a **Sales Order (SO)**.

**Why does this conversion happen?**
- The PO is what the distributor wants
- The SO is how TCPL internally processes and tracks this request
- The SO is the primary document that drives the entire fulfillment process
- All subsequent activities (delivery, billing, inventory) reference the SO

**What does the SO contain that PO doesn't?**
- Assigned warehouse/plant that will fulfill the order
- Confirmed delivery date (after checking stock availability)
- Credit block status (if distributor has credit issues)
- Pricing finalized
- Route and shipping details

**Status at this point:**
SO is created. Order is being processed. Goods haven't moved yet.

---

### STEP 4: SAP Creates a Delivery Order (DO)

**What happens:**
Based on the Sales Order, SAP creates a **Delivery Order (DO)**.

**What is a Delivery Order?**
The Delivery Order is an instruction to the warehouse/CFA (Carrying & Forwarding Agent) to:
1. Pick the required goods from shelves
2. Pack them properly
3. Make them ready for dispatch

**What happens at the warehouse:**
- Warehouse staff receives the DO
- They go to the shelves and pick the products
- They check the quantities against the DO
- They pack the goods
- They mark them as "ready for dispatch" in SAP

**Why the DO matters in data:**
- You can track the gap between SO and DO — this tells you how long it takes to process orders
- DO quantity vs SO quantity tells you if there were stock shortages (partial fulfillment)
- If DO is not created, it means there's a problem (stock shortage, system issue)

---

### STEP 5: CFA Credit Check — The Critical Gate

**What is a CFA?**
**CFA = Carrying and Forwarding Agent**

A CFA is a third-party logistics/warehousing company that TCPL contracts to store and dispatch products. CFAs are the physical warehouses from which goods are dispatched to distributors.

**Why does the CFA do a credit check?**
TCPL doesn't want to dispatch goods to a distributor who hasn't paid previous dues. Before goods are dispatched, the CFA verifies:

- **Distributor's credit limit** — the maximum outstanding amount the distributor is allowed to have
- **Current outstanding dues** — how much does the distributor currently owe TCPL
- **Available credit** = Credit Limit - Current Outstanding

**The Two Outcomes:**

#### If Credit is Sufficient ✅
```
Available Credit ≥ Value of Current Order
→ Order is APPROVED for billing
→ Process continues to next step
```

#### If Credit is Insufficient ❌
```
Available Credit < Value of Current Order
→ Order is put ON HOLD
→ TSE and ASM are notified
→ Distributor must either:
   a) Pay outstanding dues to free up credit, OR
   b) Request TCPL for a temporary credit limit increase
→ Once credit is freed, order is released from hold
```

**Why this matters CRITICALLY for data:**
- In your data, you will see orders at different statuses — "open", "on hold", "released", "cancelled"
- An order on hold is NOT a sale — you must exclude on-hold orders from revenue figures
- Orders that were on hold and later released may show a time gap — this is real and normal
- This is why counting orders ≠ counting sales

---

### STEP 6: SAP Generates Billing Document (Invoice)

**What happens:**
Once credit is approved and goods are ready, SAP generates the **Billing Document** — also called the **Invoice**.

**What is the Billing Document?**
This is the official financial document that:
- Confirms the sale has happened
- States the quantity, products, and amount the distributor must pay
- Updates TCPL's accounts receivable (money owed to TCPL)
- Reduces inventory in SAP (stock goes down because it's been "sold")
- This is what TCPL counts as **REVENUE**

**Key Insight for Data Engineers:**
> **The Billing Document is the "moment of truth" for Primary Sales.**
> Before billing, you have an order. After billing, you have a sale.
> When building revenue reports, you MUST use billing document data — not SO or PO data.

**What data the billing document contains:**
- Billing Document Number
- Billing Date
- Distributor details
- Product-wise quantities and prices
- Taxes (GST)
- Net value, gross value
- Reference to original Sales Order

---

### STEP 7: Goods are Dispatched (Shipment)

**What happens:**
After billing, the goods are physically loaded onto a truck/vehicle and sent to the distributor's location.

**In SAP:**
- A Shipment record is created
- Vehicle number, driver details, route
- Estimated delivery date
- Actual dispatch date and time

**Tracking:**
- Some TCPL operations have GPS tracking integrated with SAP
- TSE and distributor can track when goods will arrive

---

### STEP 8: Distributor Receives Goods — DMS Records "In Stock"

**What happens:**
The goods arrive at the distributor's warehouse. The distributor (or his staff) verifies the delivery against the invoice:
- Are all products delivered?
- Are quantities correct?
- Are there any damaged goods?

**DMS = Distributor Management System**
The DMS is software used by the distributor to manage their own business. When goods are received, the DMS is updated to show:
- New stock received (increased inventory)
- Status changes from "In Transit" to "In Stock"

**Important:**
The DMS is part of SFDC/Mavic (or sometimes a separate system integrated with Mavic). TCPL can see the distributor's stock levels through Mavic.

**Why this matters for data:**
- The gap between billing date (Step 6) and goods received date (Step 8) = transit time
- If distributor's DMS shows "in stock" but no secondary sales — it means products are sitting idle
- This is called **inventory pile-up** — an important business metric

---

### STEP 9: Secondary Sales Begin — Distributor to Retailer (via SFDC/Mavic)

**What happens:**
Now the distributor has stock. His salesmen go out to retailers to sell these products. This is where the Secondary Sales process begins.

**How it happens:**
1. Salesman opens Mavic app on his phone in the morning
2. He sees his beat plan (list of shops to visit today)
3. He visits each shop
4. At the shop, he takes an order from the shopkeeper
5. He checks if he has stock to fulfill the order
6. He delivers goods
7. He records the sale in Mavic (invoice generated)
8. He collects payment (cash or digital)

**All of this is recorded in SFDC/Mavic:**
- Visit records with GPS coordinates and timestamp
- Orders taken at each outlet
- Billing done to each outlet
- Collections made
- Any returns or complaints

**Why this step is complex:**
- There are 30,000 distributors, each with multiple salesmen
- Each salesman visits 20-40 shops per day
- That's potentially millions of transactions per day across India
- Data quality varies — some distributors use Mavic religiously, others don't
- This is why secondary data is "partially complete" compared to SAP

---

# 6. THE SYSTEMS — EVERY SOFTWARE EXPLAINED

## 6.1 SAP — The Central ERP

**Full Name:** SAP ERP (Systems, Applications & Products in Data Processing)

**What SAP Does:**
SAP is the brain of TCPL's operations. It manages:
- **Order Management** — PO, SO, DO, Billing
- **Inventory Management** — stock levels at all warehouses
- **Finance & Accounting** — revenue, accounts receivable, payables
- **Procurement** — buying from vendors
- **Production Planning** — how much to produce
- **HR & Payroll** (may be separate module)

**Modules Relevant to You:**
| SAP Module | What It Contains |
|---|---|
| **SD (Sales & Distribution)** | Customer orders, billing, shipping — THIS IS YOUR PRIMARY DATA SOURCE |
| **MM (Materials Management)** | Inventory, stock levels, goods movement |
| **FI (Finance)** | Accounting entries, revenue records |
| **CO (Controlling)** | Cost and profitability analysis |

**Key SAP Tables/Objects You'll Encounter:**
| Object | Description |
|---|---|
| **VBAK / VBAP** | Sales Order header and line items |
| **VBFA** | Document flow (links PO → SO → DO → Billing) |
| **VBRK / VBRP** | Billing document header and items |
| **LIKP / LIPS** | Delivery document header and items |
| **KNA1** | Customer master (distributors) |
| **MARA / MAKT** | Material master (products) |

---

## 6.2 SFDC / Mavic — The Field Sales System

**SFDC = Salesforce (the platform)**
**Mavic = TCPL's custom name for their Salesforce implementation**

**What Mavic Does:**
Mavic is the system that manages everything that happens AFTER products reach the distributor. It is used by:
- TSEs (on laptop/tablet)
- Salesmen (on mobile app)
- Distributors (on tablet/desktop)

**Mavic Modules:**
| Module | What It Does |
|---|---|
| **DMS (Distributor Management System)** | Distributor's stock, sales to retailers, collections |
| **SFA (Sales Force Automation)** | Salesman visit tracking, order taking, attendance |
| **Pulse** | Performance dashboards for field sales team |

**Why "SFDC (Mavic)" is written together:**
Mavic is BUILT ON Salesforce. So when people say SFDC they mean Salesforce. When they say Mavic, they specifically mean TCPL's version of Salesforce. They are the same system.

---

## 6.3 OMS — Order Management System

**What OMS Does:**
OMS is the mobile application used by distributors to place orders with TCPL.

**How OMS Works:**
1. Distributor opens app
2. Sees product catalog with prices and schemes
3. Checks his current stock (integrated with DMS)
4. Places order
5. OMS sends order to SAP

**OMS is the entry point** — all primary sales start here.

---

## 6.4 Nielsen — The Market Research System

**What Nielsen Does:**
Nielsen is a third-party market research company. They don't work for TCPL directly, but TCPL buys data from them.

**How Nielsen Collects Data:**
- They have a panel of sample retail stores across India
- They send field auditors to these stores regularly
- Auditors count what stock is present and what sold
- Nielsen extrapolates this to estimate total market data

**What TCPL Gets from Nielsen:**
- Estimated tertiary sales (consumer purchases)
- Market share (what % of the tea/salt market does TCPL have vs competitors)
- Category trends
- Competitor performance

**Current Status in Platform:**
- Nielsen data EXISTS but is **not fully integrated** into the TCPL data platform
- Some data is present but not used in regular dashboards
- Future work may bring Nielsen more centrally into the platform

---

## 6.5 EY Asterix — CFA Centralization System

**What EY Asterix Does:**
This is a newer system (part of the CFA Centralization project). Instead of each CFA doing manual work:
- EY Asterix receives order data from the Data Platform (Snowflake)
- It automatically generates: PDP (Pre-Delivery Preparation), Delivery Orders, Billing Documents
- This reduces manual work at CFA level and centralizes billing

**Your Role:**
Your data platform sends data TO EY Asterix. So you need to make sure the data flowing from Snowflake to EY Asterix is accurate.

---

# 7. ORDER TYPES — WHAT KIND OF ORDERS EXIST

## 7.1 Why Order Types Matter

Not every order is the same. Different order types have different:
- Urgency levels
- Process flows in SAP
- Handling by CFA
- Reporting requirements

Understanding order types helps you filter and segment order data correctly.

## 7.2 All Order Types Explained

### ARS — Auto Replenishment System
**What it is:**
The system automatically generates an order based on the distributor's historical purchase patterns and current stock levels.

**How it works:**
- System monitors distributor's DMS stock levels
- When stock falls below a threshold, OMS automatically creates an order
- Distributor may or may not manually review before submitting

**Why it exists:**
- Prevents stock-outs at distributor level
- Reduces manual effort in ordering
- Ensures consistent supply

**Data significance:**
ARS orders are the most common and regular type. They form the bulk of primary sales volume.

---

### DBO — Distributor Bulk Order
**What it is:**
A large, manually placed order by the distributor — usually when they anticipate high demand or want to stock up.

**When it happens:**
- Before festivals (Diwali, Dussehra — people buy more food/tea)
- Before a promotion where buying more gives extra discount
- Distributor expects supply disruption and wants to build buffer stock

**Data significance:**
DBO orders are larger than usual and may cause temporary spikes in primary sales data.

---

### SFL — Shelf Lifting
**What it is:**
Orders placed for institutional customers like canteens, hotels, corporate cafeterias.

**How it's different:**
- Larger quantities of specific products
- Different pricing may apply (bulk institutional pricing)
- Delivery to non-standard locations (not regular shops)

**Data significance:**
SFL orders may need to be segmented separately as they represent a different channel.

---

### BO — Bulk Order (by Retailers)
**What it is:**
In some cases, large retailers can place bulk orders directly or through the distributor for large quantities.

**When it happens:**
- Large Modern Trade stores wanting to buy in bulk for a season
- Corporate gift orders (e.g., Tata Salt hampers)

---

### RO — Rush Order
**What it is:**
An urgent order that needs faster processing and delivery than normal.

**Why it happens:**
- Distributor ran out of a fast-moving product
- Sudden unexpected demand (like a local event)
- Stock got damaged and needs immediate replacement

**Data significance:**
RO orders may bypass some normal steps or have expedited processing. Interesting for studying supply chain responsiveness.

---

### LO — Liquid Orders
**What it is:**
Orders for products that are near their expiry date and need to be liquidated quickly.

**Why it exists:**
- Products have expiry dates
- Products approaching expiry cannot be sold at full price
- TCPL offers them at discounted prices to distributors to move them quickly

**Data significance:**
LO orders usually have different pricing (discounted). Mixing them with normal orders in revenue analysis can distort the picture. Always tag order type in your data models.

---

# 8. THE DATA PLATFORM — YOUR WORLD AS A DATA ENGINEER

## 8.1 Why the Data Platform Exists

TCPL's data was scattered across multiple systems:
- Primary Sales in SAP
- Secondary Sales in Mavic/SFDC
- Field operations in Mavic
- Market data in Nielsen

Business users couldn't get a unified view. They had to go to multiple systems, pull data manually to Excel, and spend hours trying to analyze it.

The Data Platform was built to:
1. **Bring all data together** in one place
2. **Clean and standardize** the data
3. **Build analytical models** that answer business questions
4. **Serve dashboards** via ThoughtSpot so everyone can analyze self-service

## 8.2 The Full Data Pipeline — Layer by Layer

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LAYER 1: SOURCE SYSTEMS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📱 OMS App          🏢 SAP ERP          📊 SFDC/Mavic
(Distributor        (Primary Sales,      (Secondary Sales,
 Orders)            Inventory,           Field Operations)
                    Billing)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LAYER 2: INGESTION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🔌 SnapLogic
(Integration Platform — pulls data from SAP and Mavic,
 handles connectors, schedules, error handling)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LAYER 3: RAW ZONE (Landing)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🪣 AWS S3 — Raw Zone
(Data lands here in its original form — no transformation,
 exactly as extracted from source.
 Think of this as a digital "dump zone" or staging area.
 SAP data in SAP format, Mavic data in Mavic format)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LAYER 4: PROCESSING
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚙️ AWS Glue
(Serverless data processing service.
 Jobs written in Python/Spark.
 What it does:
 - Reads raw data from S3
 - Cleans data (remove nulls, fix data types, standardize formats)
 - Transforms (rename columns, apply business logic)
 - Deduplicates (remove duplicate records)
 - Joins data from different sources
 - Loads into Snowflake)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LAYER 5: DATA WAREHOUSE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

❄️ Snowflake — Data Warehouse
(This is where you build the analytical models)

  ┌─────────────────────────────────┐
  │  CORE ZONE (Raw in Snowflake)   │  ← Direct copy from S3, minimal transform
  └─────────────┬───────────────────┘
                ↓
  ┌─────────────────────────────────┐
  │  BUSINESS-READY ZONE            │  ← Cleaned, modeled, ready for analysis
  │  - Dimension Tables             │
  │  - Fact Tables                  │
  │  - KPI Layers                   │
  └─────────────┬───────────────────┘
                ↓
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LAYER 6: CONSUMPTION / ANALYTICS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📈 ThoughtSpot             📋 EY Asterix
(Self-service BI           (Operational system —
 for 3,000+ users)          CFA billing generation)
```

## 8.3 Snowflake Data Modeling — What You Build

### Dimension Tables (Master Data)
These are reference tables that describe the "who, what, where" of your business.

| Dimension | What It Contains | Key Columns |
|---|---|---|
| **dim_product** | All products/SKUs | SKU code, product name, category, brand, variant, pack size, hierarchy levels |
| **dim_distributor** | All distributors | Distributor code, name, location, region, cluster, credit limit, active/inactive |
| **dim_outlet** | All retailers/shops | Outlet code, name, type (kirana/MT/ecomm), location, associated distributor |
| **dim_geography** | Geographic hierarchy | Region, Sub-cluster, Cluster, Zone, State, Country |
| **dim_sales_hierarchy** | Sales org hierarchy | TSE, ASM, Cluster Head, NSH |
| **dim_date** | Calendar table | Date, Week, Month, Quarter, Year, Festival flag, Fiscal year |
| **dim_order_type** | Order types | ARS, DBO, SFL, BO, RO, LO with descriptions |

### Fact Tables (Transaction Data)
These are the large tables that contain actual business events.

| Fact Table | What It Contains | Key Metrics |
|---|---|---|
| **fact_primary_sales** | Billing documents from SAP | Revenue, quantity, distributor, product, date, order type |
| **fact_orders** | Sales orders from SAP | Ordered qty, fulfilled qty, order status, SO number |
| **fact_delivery** | Delivery orders from SAP | Delivery qty, dispatch date, received date, transit time |
| **fact_secondary_sales** | Secondary billing from Mavic | Retailer, distributor, product, qty, value |
| **fact_distributor_stock** | Distributor inventory from Mavic | In-stock qty, out-of-stock flag, days of inventory |
| **fact_salesman_visits** | Visit records from Mavic | Outlet visited, check-in time, GPS location, order taken flag |
| **fact_collections** | Payments collected | Distributor, retailer, amount, date |
| **fact_returns** | Returned goods | Return qty, reason, distributor, product |

### KPI Layer
On top of fact and dimension tables, you build KPI formulas:

| KPI | Formula | Business Question It Answers |
|---|---|---|
| **Primary Sales Achievement %** | (Actual Billing / Target) × 100 | Are distributors buying as much as we targeted? |
| **Fill Rate %** | (Delivered Qty / Ordered Qty) × 100 | What % of ordered quantities are we fulfilling? |
| **Out of Stock %** | (SKUs out of stock / Total SKUs) × 100 | What % of products are unavailable at distributor? |
| **Secondary Sales Achievement %** | (Actual Secondary / Target) × 100 | Are distributors selling to retailers as expected? |
| **Primary to Secondary Ratio** | Primary Sales / Secondary Sales | Is stock flowing through or piling up? |
| **Lines Sold** | Count of distinct SKUs sold | How many different products did the distributor sell? |
| **Numeric Distribution** | Outlets stocking product / Total outlets | What % of outlets carry a specific product? |
| **Visit Compliance %** | Actual visits / Planned visits × 100 | Are salesmen visiting all their assigned shops? |
| **Order Conversion %** | Orders billed / Orders placed × 100 | What % of placed orders actually resulted in billing? |

---

## 8.4 SnapLogic — The Data Ingestion Layer

**What SnapLogic Does:**
SnapLogic is an integration platform (iPaaS — Integration Platform as a Service). It has pre-built connectors to systems like SAP, Salesforce, etc.

**Your SnapLogic Work:**
- Build pipelines (called "Pipelines" in SnapLogic) that extract data from SAP
- Extract data from Mavic/SFDC
- Load data into S3 in raw form
- Handle incremental loads (pull only new/changed data each day)
- Handle errors (if a pipeline fails, alert and retry)

**Common SnapLogic Operations:**
- **Full Load** — Extract everything from source (usually done once at setup)
- **Incremental Load** — Extract only records created/modified since last run (daily job)
- **Delta Detection** — Identify what changed and pull only changes

---

# 9. THOUGHTSPOT — WHO USES YOUR DATA AND HOW

## 9.1 What is ThoughtSpot?

ThoughtSpot is a **Business Intelligence (BI) tool** that allows business users to analyze data WITHOUT writing SQL. Users can:
- Type a question in plain English: "Show me primary sales for North region this month"
- ThoughtSpot translates this to SQL, queries Snowflake, and shows a chart/table
- Users can create "Liveboards" (dashboards with multiple charts)
- Data is live — always showing latest data from Snowflake

## 9.2 The Scale

| Metric | Value |
|---|---|
| Total Users | 3,000+ |
| Sales Team Users | 800 |
| Reports & Liveboards | 2,500+ |
| Distributors tracked | 30,000 |
| Outlets tracked | 1.2 Million |
| SKUs | 7,000 – 10,000 |

This is a MASSIVE scale. Your Snowflake models need to be optimized to handle these query volumes.

## 9.3 How Different Teams Use ThoughtSpot

### Sales Team (800 users — largest user group)
**Daily Usage:**
- Morning check: How did yesterday's sales compare to target?
- Which distributors didn't order this week?
- Which TSE's territory is underperforming?
- Which SKUs are out of stock in which regions?

**Key Liveboards They Use:**
- Daily Sales Dashboard
- Distributor Performance Tracker
- TSE Scorecard
- Beat Plan Compliance Dashboard

---

### RGM — Revenue Growth Management Team
**What RGM does:**
RGM is a specialized analytics team that focuses on maximizing revenue through smart pricing and promotion decisions.

**Their Usage:**
- Which pack sizes (250g vs 500g vs 1kg) are selling more?
- Which promotions ("Buy 2 Get 1 Free") are driving incremental sales?
- Are price increases causing volume decline?
- Which channel (Kirana vs Modern Trade vs Ecomm) is most profitable?
- Competitor pricing analysis (using Nielsen data)

**Key Liveboards They Use:**
- Price Realization Dashboard
- Scheme Effectiveness Dashboard
- Pack Mix Analysis
- Channel Mix Dashboard

---

### Strategy Team
**Their Usage:**
- Long-term trend analysis
- Market share trends (using Nielsen)
- New product performance after launch
- Geographic white spaces (regions where TCPL has low presence)
- Competitive benchmarking

---

### Finance Team
**Their Usage:**
- Revenue reconciliation (SAP billing vs analytics platform — they should match exactly)
- Collection efficiency (how fast are distributors paying)
- Scheme cost tracking (how much was given out as promotions)
- Credit note and debit note analysis
- Outstanding dues analysis

---

### Shopper Marketing Team
**Their Usage:**
- In-store visibility (are planograms being followed)
- Display compliance (are products placed prominently)
- Scheme utilization at outlet level
- New outlet acquisition rates

---

### Marketing Team (Category & Brand)
**Their Usage:**
- How is Tata Tea performing vs Tetley Tea?
- Which brand is growing in which region?
- New launch tracking (new product introduced — how fast is it getting distributed)
- Category growth trends

---

## 9.4 Row-Level Security in ThoughtSpot

**What is Row-Level Security (RLS)?**
Different users should see different data:
- NSH sees ALL of India
- South Cluster Head sees ONLY South India data
- Delhi Region ASM sees ONLY Delhi data
- A TSE in Pune sees ONLY his distributors' data

This is implemented through **Row-Level Security** in ThoughtSpot, based on the user's role and geography assigned in the system.

**As a Data Engineer, you need to:**
- Build the user-geography mapping table in Snowflake
- Configure RLS rules in ThoughtSpot
- Ensure the hierarchy is correctly maintained (if an ASM moves regions, their access must update)

---

# 10. SFDC MAVIC — THE SECONDARY SALES BRAIN

## 10.1 Complete Coverage of What Mavic Captures

### Retailer Orders
- When salesman visits a shop, he takes an order
- This order is recorded in Mavic
- Each order has: outlet ID, product SKUs, quantities, prices, date, salesman ID

### Order-to-Invoice Conversion
- Was the order actually billed?
- If retailer placed order for 10 items but only 7 were available → partial billing
- This conversion rate is a key secondary sales metric

### Schemes & Promotions
- TCPL runs schemes for retailers: "Buy 5 get 1 free", "10% off on Tata Salt this month"
- These schemes are configured in Mavic
- When salesman bills a retailer, applicable schemes are automatically applied
- Scheme data lets RGM team calculate the cost and effectiveness of promotions

### Stock In / Out
- **In Stock:** Products that are available at distributor warehouse
- **Out of Stock (OOS):** Products that are not available
- Out-of-stock is a critical metric — if a product is out of stock at distributor, retailer can't order it, consumer can't buy it = LOST SALE
- Mavic tracks which SKUs are OOS at each distributor daily

### Returns
- Sometimes retailers return goods: damaged products, wrong delivery, expired stock
- Returns are recorded in Mavic
- Returns need to be subtracted from secondary sales in your data models
- Net Secondary Sales = Gross Secondary Sales - Returns

### Claims
- A claim is when the distributor says "I'm owed money for something"
- Examples:
  - "The goods arrived damaged, I want a credit note"
  - "You promised me a scheme discount that wasn't applied"
  - "I participated in a promotion, I'm owed extra boxes"
- Claims management is in Mavic
- Finance team monitors claims closely (they represent liabilities for TCPL)

### Debit Notes / Credit Notes
- **Debit Note:** Distributor charges TCPL (e.g., "you owe me money for damaged goods")
- **Credit Note:** TCPL gives distributor a discount/refund (e.g., "here is ₹5,000 credit for your promotional expenses")
- These are financial adjustments that need to be tracked separately in data

### Visits & Attendance
- Every salesman visit to a shop is recorded in Mavic
- GPS timestamp at check-in and check-out
- This proves the salesman actually visited (prevents fake reporting)
- Visit data enables **Visit Compliance** metric: planned visits vs actual visits

### Collection
- After billing, retailers owe money to distributors
- Salesman collects cash/cheque/digital payment during visits
- Collection recorded in Mavic
- **Collection Efficiency** = Amount Collected / Amount Billed × 100

### SFA Audit
- SFA = Sales Force Automation
- Regular audits of salesman activities
- Are they visiting the right shops?
- Are they following the beat plan?
- Are they applying schemes correctly?
- Audit records in Mavic

### Distributor Onboarding
- When a new distributor is appointed, their profile is set up in Mavic
- Their territory, credit limit, contact details, assigned TSE — all in Mavic

### Outlet Onboarding
- When a new shop is added to the network, it's registered in Mavic
- Outlet GPS location, type, owner contact, assigned distributor — all in Mavic
- 1.2 million outlets are all registered here

---

# 11. REAL LIFE STORY — ONE PRODUCT, FULL JOURNEY

## The Story of 1 KG Tata Salt Reaching a Small Shop in Jaipur

Let me trace ONE unit of Tata Salt from the factory all the way to a consumer's kitchen, and show where data gets created at every step.

---

**DAY 1 — Factory**
- TCPL's factory in Gujarat produces 10,000 units of Tata Salt 1KG
- Production entry is made in SAP (inventory increases)

**DAY 3 — Stock Reaches CFA/Warehouse**
- Goods are transported to CFA warehouse in Jaipur
- SAP records stock transfer
- CFA system shows 10,000 units available in Jaipur warehouse

**DAY 5 — Distributor Places Order**
- Ramesh Distributors in Jaipur opens OMS app
- His current stock is low (ARS system detects this automatically)
- ARS generates an order: 500 units of Tata Salt 1KG
- Ramesh reviews and submits
- **DATA CREATED:** OMS Order Record

**DAY 5 — SAP Receives Order**
- OMS sends order to SAP via API
- SAP creates PO-12345 (Purchase Order)
- **DATA CREATED IN SAP:** PO Record

**DAY 5 — SO and DO Created**
- SAP auto-converts PO-12345 → SO-67890 (Sales Order)
- SAP creates DO-11111 (Delivery Order) → sent to CFA
- **DATA CREATED:** SO Record, DO Record

**DAY 5 — Credit Check**
- CFA checks: Ramesh's credit limit is ₹5 lakhs, outstanding is ₹3 lakhs
- 500 units × ₹20 = ₹10,000 → available credit ₹2 lakhs > ₹10,000 → APPROVED ✅
- **DATA CREATED:** Credit check log in SAP

**DAY 5 — Billing**
- SAP generates Billing Document INV-99999
- ₹10,000 + GST = ₹11,800 total invoice to Ramesh
- **DATA CREATED:** Billing Document — this is NOW a PRIMARY SALE
- Ramesh's outstanding increases by ₹11,800 in SAP

**DAY 7 — Dispatch**
- CFA warehouse picks 500 units, packs them, loads on truck
- Truck leaves for Ramesh's warehouse in Jaipur
- **DATA CREATED:** Shipment Record in SAP

**DAY 8 — Delivery to Distributor**
- Truck arrives at Ramesh's warehouse
- Ramesh's staff counts and verifies 500 units received
- Mavic DMS updated: Tata Salt 1KG stock = 500 units (In Stock)
- **DATA CREATED:** Goods Receipt in DMS/Mavic

**DAY 9 — Morning at Distributor**
- Ramesh's salesman Suresh starts his beat plan for the day
- Opens Mavic app, sees 15 shops to visit in the Civil Lines area

**DAY 9 — Suresh Visits Shop #7: Sharma General Store**
- Suresh checks in at Sharma Store at 10:32 AM (GPS recorded in Mavic)
- Shopkeeper Sharma says: "Give me 10 packets of Tata Salt 1KG"
- Suresh checks stock availability in Mavic — available ✅
- Suresh records the order in Mavic app
- Pulls out 10 packets from delivery van and hands to Sharma
- Generates invoice of ₹250 (10 × ₹25) in Mavic
- Sharma pays ₹250 cash
- Suresh records collection in Mavic
- Suresh checks out at 10:47 AM (GPS recorded)
- **DATA CREATED IN MAVIC:** Visit record, Order, Secondary Billing, Collection

**DAY 9 — Evening**
- Suresh has visited all 15 shops
- Sold 120 units of Tata Salt total across all shops
- All data synced to Mavic server

**DAY 9 — Night — Data Pipeline Runs**
- SnapLogic job runs at 11 PM
- Pulls today's billing documents from SAP (the ₹11,800 invoice from Day 5)
- Pulls today's Mavic data (Suresh's visits, secondary sales, collections)
- Loads raw data to AWS S3

**DAY 10 — Morning — Glue Processing**
- AWS Glue job runs at 2 AM
- Reads S3 data
- Cleans and transforms
- Loads into Snowflake fact tables

**DAY 10 — 9 AM — Business Users Check ThoughtSpot**
- The ASM for Jaipur opens ThoughtSpot
- Sees: Yesterday's primary billing in his region = ₹11,800 (Ramesh's order)
- Sees: Yesterday's secondary sales = 120 units of Tata Salt at retail level
- Sees: Suresh visited 15/15 planned shops = 100% visit compliance ✅

**DAY 15 — Consumer Buys the Salt**
- A consumer walks into Sharma General Store
- Buys 1 KG Tata Salt for ₹28
- TCPL has no record of this sale
- This is TERTIARY SALES — not captured (unless Nielsen auditors happen to visit this store in their sample survey)

---

**THE DATA TRAIL SUMMARY:**

| Event | System | Data Created |
|---|---|---|
| Production | SAP | Inventory increase |
| Distributor Order | OMS → SAP | PO Record |
| Order Processing | SAP | SO, DO Records |
| Credit Check | SAP/CFA | Credit log |
| Invoice | SAP | **Billing Document = PRIMARY SALE** |
| Shipment | SAP | Delivery Record |
| Received at Distributor | Mavic DMS | Stock "In Stock" update |
| Salesman Visit | Mavic | Visit record (GPS + time) |
| Retailer Sale | Mavic | **Secondary Invoice = SECONDARY SALE** |
| Collection | Mavic | Payment Record |
| Data Ingestion | SnapLogic → S3 | Raw files |
| Processing | Glue → Snowflake | Fact + Dimension updates |
| Analytics | ThoughtSpot | ASM sees dashboard |
| Consumer Purchase | Nobody | ❌ NOT CAPTURED |

---

# 12. THE BIG PICTURE — EVERYTHING CONNECTED

## 12.1 Complete System Architecture

```
╔══════════════════════════════════════════════════════════════════════╗
║                        SOURCE SYSTEMS                               ║
╠══════════════════════════╦═══════════════╦══════════════════════════╣
║                          ║               ║                          ║
║  📱 OMS Mobile App       ║  🏢 SAP ERP   ║  📊 SFDC / Mavic         ║
║  (Distributor orders)    ║  (Primary     ║  (Secondary Sales,       ║
║                          ║   Sales)      ║   Field Operations)      ║
║                          ║               ║                          ║
╚══════════════════════════╩═══════════════╩══════════════════════════╝
                                  ↓
╔══════════════════════════════════════════════════════════════════════╗
║                    INGESTION LAYER                                  ║
║                                                                      ║
║            🔌 SnapLogic (Integration Platform)                      ║
║         Pulls data on schedule, handles connectors                  ║
╚══════════════════════════════════════════════════════════════════════╝
                                  ↓
╔══════════════════════════════════════════════════════════════════════╗
║                      RAW ZONE                                        ║
║                                                                      ║
║                    🪣 AWS S3                                         ║
║               (Raw data storage — unprocessed)                      ║
╚══════════════════════════════════════════════════════════════════════╝
                                  ↓
╔══════════════════════════════════════════════════════════════════════╗
║                    PROCESSING LAYER                                  ║
║                                                                      ║
║                  ⚙️ AWS Glue (Spark Jobs)                           ║
║         Clean → Transform → Join → Deduplicate → Load               ║
╚══════════════════════════════════════════════════════════════════════╝
                                  ↓
╔══════════════════════════════════════════════════════════════════════╗
║                    DATA WAREHOUSE                                    ║
║                                                                      ║
║                  ❄️ SNOWFLAKE                                        ║
║  ┌───────────────────┐    ┌──────────────────────────────────────┐  ║
║  │  CORE ZONE        │    │  BUSINESS-READY ZONE                 │  ║
║  │  (Raw in SF)      │ →  │  Fact Tables + Dimensions + KPIs     │  ║
║  └───────────────────┘    └──────────────────────────────────────┘  ║
╚══════════════════════════════════════════════════════════════════════╝
                    ↓                           ↓
╔═══════════════════════════╗    ╔══════════════════════════════════╗
║  📈 ThoughtSpot           ║    ║  📋 EY Asterix                   ║
║  (BI — 3,000 users        ║    ║  (CFA Centralization —           ║
║   2,500+ Liveboards)      ║    ║   DO & Billing Generation)       ║
╚═══════════════════════════╝    ╚══════════════════════════════════╝
```

---

# 13. DATA ENGINEERING SPECIFIC INSIGHTS

## 13.1 Critical Things to Know Before Building Data Models

### 1. Revenue Confirmation Point
> ⚠️ **ALWAYS use Billing Document date as the sale date — NOT the order date, NOT the delivery date.**
> A sale is only a sale in SAP when the billing document is created. Orders can be cancelled. Deliveries can be partial. Only billing = confirmed revenue.

### 2. Order Status Filtering
In your fact_orders table, always include order status. Common statuses:
- **Open** — Order placed, not yet processed
- **In Process** — Being picked and packed
- **Credit Block** — Held due to credit issues
- **Delivered** — Goods dispatched
- **Billed** — Invoice generated (= sale confirmed)
- **Cancelled** — Order cancelled before billing
- **Partial** — Only some items fulfilled

Never include Cancelled or Credit Block orders in revenue calculations without filtering.

### 3. Primary ≠ Secondary Sales Numbers
Do NOT expect Primary Sales = Secondary Sales at any point. They will never match exactly because:
- Primary Sales is what TCPL sold to distributors
- Secondary Sales is what distributors sold to retailers
- There is always inventory at the distributor level
- Distributors may stock up before festival season (high primary, secondary catches up later)
- Transit time causes lag

### 4. The Data Latency Challenge
- SAP data: Usually available next day (batch extraction nightly)
- Mavic data: Usually available same day or next day (more real-time)
- ThoughtSpot users see "yesterday's data" typically
- Be clear about data freshness when building dashboards

### 5. Data Quality Issues to Watch For
- **Duplicate billing documents** — can happen during system restores; always deduplicate
- **Backdated orders** — distributors sometimes book orders with past dates; use posting date, not entry date
- **Missing distributor codes** — new distributors may not be in SAP yet; handle NULLs carefully
- **Scheme data discrepancies** — scheme amounts in Mavic may not match finance records in SAP

### 6. Hierarchy Changes Over Time
- ASMs change territories → a TSE's data moves from one ASM to another
- Clusters are reorganized → region mappings change
- Always track **validity dates** on hierarchy dimension tables (slowly changing dimensions)
- If you don't handle this, an ASM who was transferred will see all historical data from their old territory when they should only see from their transfer date onward

### 7. SKU Rationalization
- Products get discontinued
- New products launch
- Pack sizes change
- Always keep historical product data (don't delete old SKUs)
- Use active/inactive flags, not hard deletes

### 8. Currency and Pricing
- Primary sales values are in INR (for India)
- International sales may be in foreign currency — need conversion to INR for consolidated reports
- Scheme discounts reduce the net realization price — both gross and net values should be available

---

## 13.2 Important Metrics and How to Calculate Them

### Primary Sales Achievement %
```sql
-- At distributor level for current month
SELECT 
    distributor_code,
    SUM(billing_value) as actual_primary_sales,
    target_value,
    (SUM(billing_value) / target_value) * 100 as achievement_pct
FROM fact_primary_sales fps
JOIN dim_targets dt ON fps.distributor_code = dt.distributor_code
WHERE billing_month = CURRENT_MONTH
GROUP BY distributor_code, target_value
```

### Out of Stock %
```sql
-- At distributor-SKU level for today
SELECT 
    distributor_code,
    COUNT(CASE WHEN stock_qty = 0 THEN 1 END) * 100.0 / COUNT(*) as oos_pct
FROM fact_distributor_stock
WHERE snapshot_date = CURRENT_DATE
GROUP BY distributor_code
```

### Visit Compliance
```sql
-- Salesman level visit compliance
SELECT 
    salesman_code,
    COUNT(actual_visits) as actual,
    COUNT(planned_visits) as planned,
    (COUNT(actual_visits) / COUNT(planned_visits)) * 100 as compliance_pct
FROM fact_beat_plan fbp
LEFT JOIN fact_visits fv ON fbp.outlet_id = fv.outlet_id AND fbp.visit_date = fv.visit_date
WHERE visit_date = YESTERDAY
GROUP BY salesman_code
```

---

## 13.3 The Daily Data Engineering Routine (What Happens Every Night)

```
9:00 PM    — SAP batch closes (day's transactions locked)
10:00 PM   — SnapLogic SAP extraction job runs (pulls day's POs, SOs, DOs, Billings)
11:00 PM   — SnapLogic Mavic extraction job runs (pulls visits, secondary sales, collections)
12:00 AM   — S3 landing zone has today's raw files
1:00 AM    — AWS Glue processing jobs start
2:00 AM    — Glue completes, Snowflake tables updated
3:00 AM    — ThoughtSpot cache refresh (new data now available in dashboards)
9:00 AM    — Business users open ThoughtSpot and see yesterday's data
```

---

# 14. MASTER GLOSSARY — EVERY TERM EXPLAINED

| Term | Full Form | Simple Explanation |
|---|---|---|
| **TCPL** | Tata Consumer Products Limited | Your client — makes Tea, Salt, Water etc. |
| **Decision Point** | — | Your company — the data analytics consultant |
| **FMCG** | Fast Moving Consumer Goods | Everyday products sold in large volumes, low price |
| **Primary Sales** | — | TCPL selling to Distributor |
| **Secondary Sales** | — | Distributor selling to Retailer |
| **Tertiary Sales** | — | Retailer selling to Consumer (not tracked by TCPL) |
| **SAP** | Systems, Applications & Products | TCPL's main ERP — contains all primary sales data |
| **SFDC** | Salesforce.com | The technology platform on which Mavic is built |
| **Mavic** | — | TCPL's Salesforce implementation for secondary sales |
| **OMS** | Order Management System | Mobile app distributors use to place orders |
| **DMS** | Distributor Management System | Software distributor uses to manage their stock |
| **SFA** | Sales Force Automation | App/system used by salesmen in the field |
| **CFA** | Carrying and Forwarding Agent | Third-party warehouse that stores and dispatches goods |
| **DB** | Distributor / Distribution Center | The middleman between TCPL and retailers |
| **NSH** | National Sales Head | Head of all India sales |
| **ASM** | Area Sales Manager | Manages a specific area/region |
| **TSE** | Territory Sales Executive | Ground-level manager for distributors |
| **PO** | Purchase Order | Distributor's request to buy from TCPL |
| **SO** | Sales Order | TCPL's internal processing of a distributor order |
| **DO** | Delivery Order | Instruction to warehouse to pick, pack, dispatch |
| **Billing Document** | — | The invoice — confirms a Primary Sale in SAP |
| **RGM** | Revenue Growth Management | Team managing pricing and promotional strategy |
| **SKU** | Stock Keeping Unit | One specific product variant (e.g., Tata Salt 1KG packet) |
| **ARS** | Auto Replenishment System | System that auto-generates orders based on stock levels |
| **DBO** | Distributor Bulk Order | A large manually placed order by distributor |
| **SFL** | Shelf Lifting | Orders for canteens, hotels |
| **RO** | Rush Order | Urgent order |
| **LO** | Liquid Order | Order for near-expiry products |
| **SnagLogic** | — | Integration platform — pulls data from SAP/Mavic |
| **S3** | Amazon Simple Storage Service | Cloud storage — raw data landing zone |
| **AWS Glue** | — | Amazon's data processing service — transforms data |
| **Snowflake** | — | Cloud data warehouse — where you model and store data |
| **ThoughtSpot** | — | BI tool — business users see dashboards here |
| **EY Asterix** | — | System for CFA billing centralization |
| **Nielsen** | — | Third-party market research — tertiary/consumer data |
| **Liveboard** | — | ThoughtSpot's term for a dashboard |
| **RLS** | Row Level Security | Restricting which data each user can see |
| **OOS** | Out of Stock | Product not available at distributor |
| **Fact Table** | — | Large table with transactional records (sales, orders) |
| **Dimension Table** | — | Reference/master table (products, geography, distributors) |
| **KPI** | Key Performance Indicator | A metric used to measure business performance |
| **Beat Plan** | — | Pre-defined route/schedule of shops a salesman must visit |
| **Channel Stuffing** | — | When primary is high but secondary is low — stock piling at distributor |
| **Scheme** | — | A promotion or discount offered (e.g., buy 5 get 1 free) |
| **Credit Limit** | — | Maximum outstanding amount a distributor can have |
| **Credit Note** | — | TCPL gives money back to distributor |
| **Debit Note** | — | Distributor charges TCPL |
| **SCD** | Slowly Changing Dimension | Handling changes in master data over time (e.g., TSE changes region) |
| **iPaaS** | Integration Platform as a Service | Cloud-based integration tool (like SnapLogic) |
| **Incremental Load** | — | Pulling only new/changed records (not everything) |
| **Full Load** | — | Pulling all records from source system |
| **Core Zone** | — | First layer in Snowflake — raw data, minimal transform |
| **Business-Ready Zone** | — | Final layer in Snowflake — modeled, clean, KPI-ready |
| **Cluster** | — | Large geographic unit (e.g., North India) |
| **Sub-Cluster** | — | Smaller geographic unit within cluster (e.g., North1) |
| **Modern Trade (MT)** | — | Large organized retail chains (DMart, BigBazaar) |
| **Kirana** | — | Small independent grocery shop |
| **HoReCa** | Hotel, Restaurant, Catering | Institutional channel |

---

## Final Note for You as a Data Engineer

You are not just building tables and pipelines. Every query that an ASM runs in ThoughtSpot, every decision that the NSH makes based on a chart — all of that depends on the quality and accuracy of what you build.

When you build the `fact_primary_sales` table, you are enabling TCPL's finance team to close their books accurately. When you build `fact_salesman_visits`, you are enabling TSEs to hold their salesmen accountable. When you build the distributor stock model, you are helping prevent stock-outs that could mean a consumer in a small town can't find Tata Salt on a shop shelf.

Understanding the business deeply makes you a better data engineer — because you can anticipate what the business needs, flag data quality issues intelligently, and build models that actually answer real questions.

---

*This document is prepared for Decision Point data engineers working on the TCPL project.*
*Based on: Sales & Marketing Process Overview Session — Facilitated by Arnab (S&M Lead)*
*Classification: INTERNAL*
