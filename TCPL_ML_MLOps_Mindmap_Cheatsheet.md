# 🤖 ML + MLOps in CPG — Complete Mindmap & Cheatsheet
## TCPL × Decision Point Analytics
### "Which Algorithm, Why, How, and End-to-End MLOps"

---

## 🧠 THE BIG PICTURE — ML IN CPG IN ONE VIEW

```
BUSINESS QUESTION                ML USE CASE              ALGORITHM FAMILY
─────────────────────────────────────────────────────────────────────────────

"How much will sell              DEMAND FORECASTING    →  Time Series / XGBoost
 next 3 months?"                                          LightGBM / DeepAR

"Which promo worked?"            TRADE PROMO           →  Causal Inference /
                                 OPTIMIZATION             XGBoost / MMM

"What drives my sales?"          MARKET MIX            →  Bayesian MMM /
                                 MODELING (MMM)           Linear Regression /
                                                          Facebook Robyn

"Will this customer              CHURN PREDICTION      →  XGBoost Classifier /
 stop buying?"                                            Logistic Regression /
                                                          Random Forest

"What should I sell              PRODUCT               →  Association Rules /
 together?"                      RECOMMENDATION           Collaborative Filter

"Where will stock run            STOCKOUT              →  XGBoost / LSTM /
 out?"                           PREDICTION               Gradient Boosting

"Is this sales number            ANOMALY               →  Isolation Forest /
 suspicious?"                    DETECTION                Z-Score / Autoencoder

"What price maximizes            PRICE                 →  Price Elasticity /
 revenue?"                       OPTIMIZATION             Regression / RL
```

---

## 📦 ML USE CASE 1 — DEMAND FORECASTING
### "The #1 ML use case in all of CPG"

```
BUSINESS PROBLEM:
─────────────────
  TCPL makes Tata Tea Premium.
  They need to know: "How many units will sell
  in Maharashtra in the next 90 days?"
  
  Too much stock → wastage, storage cost
  Too little stock → stockout, lost sales
  
  This is why demand forecasting = MOST IMPORTANT ML in CPG.

WHAT THE DATA LOOKS LIKE:
──────────────────────────
  One row = one SKU + one Region + one Week
  
  ┌─────────┬────────┬────────────┬──────────┬─────────┬────────┬──────────┐
  │ sku     │ region │ week       │ units    │ lag_1w  │ lag_4w │ is_promo │
  ├─────────┼────────┼────────────┼──────────┼─────────┼────────┼──────────┤
  │ TTP-500 │ MH     │ 2024-01-01 │ 15000    │ 14200   │ 13800  │ 0        │
  │ TTP-500 │ MH     │ 2024-01-08 │ 16500    │ 15000   │ 14200  │ 1        │
  │ TTP-500 │ MH     │ 2024-01-15 │ 18200    │ 16500   │ 15000  │ 1        │
  │ ...     │ ...    │ ...        │ ...      │ ...     │ ...    │ ...      │
  └─────────┴────────┴────────────┴──────────┴─────────┴────────┴──────────┘
  
  TARGET VARIABLE (what you predict): units sold next week / next 4 weeks

FEATURES YOU ENGINEER:
───────────────────────
  LAG FEATURES:
    lag_1w_units   = sales 1 week ago
    lag_2w_units   = sales 2 weeks ago
    lag_4w_units   = sales 4 weeks ago (1 month)
    lag_8w_units   = sales 8 weeks ago (2 months)
    lag_52w_units  = sales same week last year (YoY)
  
  ROLLING FEATURES:
    rolling_4w_avg   = avg of last 4 weeks
    rolling_4w_std   = std dev of last 4 weeks (volatility)
    rolling_12w_avg  = avg of last 12 weeks (quarter trend)
  
  GROWTH FEATURES:
    wow_growth = (this week - last week) / last week
    yoy_growth = (this week - same week last year) / last year
  
  SEASONALITY FEATURES:
    week_of_year     (1 to 52)
    month            (1 to 12)
    quarter          (1 to 4)
    is_festive       (Diwali, Holi, Navratri, Eid weeks → 1)
    is_summer        (April-June → 1)
    is_year_end      (March → 1, TCPL fiscal push)
  
  PROMOTION FEATURES:
    is_promoted           (1/0)
    promo_discount_pct    (0.10 = 10% off)
    promo_type            (buyXgetY, cashback, bundled)
    days_since_last_promo
    promo_duration_days
  
  PRODUCT FEATURES:
    brand_encoded    (Tata Tea=1, Tetley=2, Himalayan=3...)
    category_encoded
    pack_size_grams
    mrp              (price)
    is_new_launch    (< 6 months old)
  
  EXTERNAL FEATURES:
    weather_temp     (hot weather → cold drinks, iced tea)
    rainfall_mm
    state_gdp_growth
    competitor_promo (did HUL / Wagh Bakri run promo this week?)

ALGORITHMS — WHICH ONE AND WHY:
─────────────────────────────────

  ┌─────────────────┬────────────────────────────┬──────────────────────────┐
  │ ALGORITHM       │ WHEN TO USE                │ MAPE TARGET              │
  ├─────────────────┼────────────────────────────┼──────────────────────────┤
  │ XGBoost         │ Most common. Works great   │ 10-15% MAPE              │
  │ (PRIMARY)       │ with tabular features.     │ ← TCPL's main model      │
  │                 │ Fast, interpretable.       │                          │
  ├─────────────────┼────────────────────────────┼──────────────────────────┤
  │ LightGBM        │ Same as XGBoost but faster │ 10-14% MAPE              │
  │                 │ on large datasets.         │ Often beats XGBoost      │
  │                 │ Use when > 10M rows.       │                          │
  ├─────────────────┼────────────────────────────┼──────────────────────────┤
  │ Prophet         │ Simple time series.        │ 15-25% MAPE              │
  │ (Facebook)      │ Good baseline model.       │ Use as BASELINE only     │
  │                 │ Handles seasonality well.  │                          │
  ├─────────────────┼────────────────────────────┼──────────────────────────┤
  │ DeepAR+         │ Deep learning. Best for    │ 8-12% MAPE               │
  │ (AWS built-in)  │ many SKUs at once.         │ Best accuracy            │
  │                 │ Native in SageMaker.       │ Slow to train            │
  ├─────────────────┼────────────────────────────┼──────────────────────────┤
  │ ARIMA/SARIMA    │ Classical. Simple series.  │ 20-30% MAPE              │
  │                 │ Few data points.           │ Baseline only            │
  │                 │ Rarely used in prod now.   │                          │
  ├─────────────────┼────────────────────────────┼──────────────────────────┤
  │ Ensemble        │ Combine XGBoost + Prophet  │ 8-12% MAPE               │
  │ (ADVANCED)      │ + DeepAR weighted avg.     │ Best production result   │
  │                 │ More stable predictions.   │                          │
  └─────────────────┴────────────────────────────┴──────────────────────────┘

  AT TCPL: XGBoost is primary → LightGBM challenger → Ensemble in roadmap

HOW YOU CHOOSE ALGORITHM — DECISION TREE:
──────────────────────────────────────────

  Data > 2 years history?
      YES → Use XGBoost / LightGBM (enough lags available)
      NO  → Use Prophet (handles short history better)
  
  More than 500 SKUs?
      YES → Use DeepAR+ (trains all SKUs in one model)
      NO  → Use XGBoost per SKU group
  
  Need confidence intervals?
      YES → Use DeepAR+ or Quantile Regression in XGBoost
      NO  → Standard XGBoost fine
  
  Strong seasonality (festive, summer)?
      YES → Add Fourier features OR use Prophet for seasonality component
      NO  → Standard lags enough

EVALUATION METRICS:
────────────────────
  MAPE  = Mean Absolute Percentage Error
          Lower is better. Target < 15% at TCPL.
          MAPE = mean(|actual - predicted| / actual) × 100
  
  WAPE  = Weighted MAPE (better when low-volume SKUs exist)
          WAPE = sum(|actual - predicted|) / sum(actual) × 100
  
  RMSE  = Root Mean Squared Error (penalizes large errors more)
  
  Bias  = Are you consistently over or under predicting?
          Positive bias = over-forecasting (too much stock)
          Negative bias = under-forecasting (stockouts)
  
  WHICH TO USE AT TCPL:
  ─────────────────────
  Primary metric: WAPE (because TCPL has low-volume specialty SKUs)
  Secondary:      MAPE at brand level
  Alert threshold: MAPE > 15% → retrain model
```

---

## 📦 ML USE CASE 2 — MARKET MIX MODELING (MMM)
### "What is driving my sales? Is my ad spend working?"

```
BUSINESS PROBLEM:
─────────────────
  TCPL spends crores on:
  - TV ads (Tata Tea "Jaago Re" campaign)
  - Digital ads (Facebook, Google)
  - Trade promotions (discounts to retailers)
  - In-store displays (shelf visibility)
  
  QUESTION: "Which of these actually drove sales?
             If I cut TV ads by 20%, what happens to sales?"

WHAT MMM DOES:
──────────────
  Decomposes sales into:
  
  Total Sales = Base Sales + Media Effect + Promo Effect + Seasonality + Error
  
  BASE SALES = what you'd sell with ZERO marketing
  MEDIA EFFECT = incremental sales FROM ads
  PROMO EFFECT = incremental sales FROM promotions
  SEASONALITY = festive / summer / winter effects
  
  Gives you: MARKETING ROI per channel
  Example output:
  ┌────────────────────────────────────────────────────────┐
  │  TV Ads     → ₹1 spent → ₹2.3 incremental revenue (ROI 2.3x)│
  │  Digital    → ₹1 spent → ₹3.1 incremental revenue (ROI 3.1x)│
  │  Trade Promo→ ₹1 spent → ₹1.8 incremental revenue (ROI 1.8x)│
  │  In-Store   → ₹1 spent → ₹1.2 incremental revenue (ROI 1.2x)│
  └────────────────────────────────────────────────────────┘
  → DECISION: Shift budget from In-Store to Digital

DATA NEEDED FOR MMM:
─────────────────────
  Weekly data for 2-3 years:
  - Sales (units or revenue)
  - TV GRPs (Gross Rating Points — how many people saw the ad)
  - Digital spend (₹ amount)
  - Trade promo spend
  - Price / discounts
  - Distribution reach (% stores stocked)
  - Seasonality index
  - Competitor activity (if available)

ALGORITHMS FOR MMM:
────────────────────
  ┌──────────────────────┬───────────────────────────────────────────────┐
  │ ALGORITHM            │ WHEN TO USE                                   │
  ├──────────────────────┼───────────────────────────────────────────────┤
  │ Bayesian MMM         │ BEST PRACTICE in 2024. Handles                │
  │ (PyMC / Meridian)    │ uncertainty well. Gives credible intervals.   │
  │                      │ Google's Meridian is open source MMM tool.    │
  ├──────────────────────┼───────────────────────────────────────────────┤
  │ Facebook Robyn       │ Open source. R-based. Good for TCPL if        │
  │                      │ team uses R. Ridge regression + Nevergrad     │
  │                      │ optimizer. Industry standard.                 │
  ├──────────────────────┼───────────────────────────────────────────────┤
  │ Ridge / Lasso        │ Simple baseline. Linear regression with       │
  │ Regression           │ regularization. Good first MMM attempt.       │
  ├──────────────────────┼───────────────────────────────────────────────┤
  │ LightweightMMM       │ Google's Bayesian MMM library. Python-based.  │
  │ (Google)             │ Easy to implement on SageMaker.               │
  └──────────────────────┴───────────────────────────────────────────────┘

  KEY CONCEPT — ADSTOCK:
  ──────────────────────
  Ads don't work instantly. TV ad today affects sales for 3-4 weeks after.
  This lingering effect = ADSTOCK (also called carryover effect)
  
  MMM models MUST account for this.
  Adstock formula: adstock_t = spend_t + decay_rate × adstock_(t-1)
  decay_rate typically 0.3-0.7 (need to tune per brand)

  KEY CONCEPT — SATURATION:
  ──────────────────────────
  Beyond a point, more ad spend gives diminishing returns.
  This = SATURATION (also called diminishing returns curve)
  MMM models use Hill function or S-curve to capture this.
```

---

## 📦 ML USE CASE 3 — TRADE PROMOTION OPTIMIZATION (TPO)
### "Which promotion to run, when, and how much discount?"

```
BUSINESS PROBLEM:
─────────────────
  TCPL runs thousands of trade promotions yearly.
  "Buy 2 get 1 free on Tata Tea during Diwali in UP"
  
  QUESTIONS:
  - Did this promo actually LIFT sales?
  - Was the lift worth the margin loss?
  - What discount % maximizes profit (not just revenue)?

TWO ML PROBLEMS IN TPO:
────────────────────────

  PROBLEM A: PROMO UPLIFT MEASUREMENT (was it worth it?)
  ──────────────────────────────────────────────────────
  Compare promoted period vs counterfactual (what would have happened without promo)
  
  Algorithm: Causal Inference
  ─────────────────────────────
  - Difference-in-Differences (DiD)
  - Synthetic Control Method
  - CausalImpact (Google's library — Bayesian structural time series)
  
  Example:
    Sales during promo week: 18,000 units
    Predicted without promo: 13,000 units
    UPLIFT = 5,000 units (38% lift)
    Cost of promo: ₹50,000
    Incremental revenue: 5000 × ₹20 margin = ₹1,00,000
    Promo ROI = (1,00,000 - 50,000) / 50,000 = 100% ROI ✅

  PROBLEM B: PROMO RECOMMENDATION (what promo to run next?)
  ──────────────────────────────────────────────────────────
  Predict which promo TYPE, DEPTH, TIMING maximizes revenue
  
  Algorithm: XGBoost Regressor / Gradient Boosting
  ────────────────────────────────────────────────
  Features:
    - Historical promo uplift for this SKU × region
    - Seasonality (festive vs non-festive)
    - Competitor promo activity
    - Current inventory level
    - Base price and MRP
    - Distribution reach
  
  Target: Predicted incremental units OR predicted ROI
  
  Output: Recommendation engine
  ┌──────────────────────────────────────────────────────────┐
  │ SKU: Tata Tea Premium 500g │ Region: UP │ Week: Diwali   │
  │ Recommended Promo: 15% discount                          │
  │ Predicted Uplift: +32%                                   │
  │ Predicted ROI: 2.1x                                      │
  │ Confidence: HIGH                                         │
  └──────────────────────────────────────────────────────────┘
```

---

## 📦 ML USE CASE 4 — STOCKOUT PREDICTION
### "Will we run out of stock before the next delivery?"

```
BUSINESS PROBLEM:
─────────────────
  TCPL distributors hold X days of stock.
  If sales spike (festival / promo) → stock runs out → lost sales.
  
  PREDICT: Which SKU × Region will face stockout in next 14 days?

DATA FEATURES:
──────────────
  current_stock_days     (how many days of stock remaining)
  avg_daily_sales_7d     (recent velocity)
  upcoming_promo_flag    (is a promo planned?)
  is_festive_next_14d    (is Diwali / Holi coming?)
  lead_time_days         (how long to replenish?)
  historical_stockout    (has this SKU stocked out before?)
  distribution_reach     (% stores covered)
  weather_forecast       (heatwave → beverage spike)

ALGORITHM:
──────────
  XGBoost Binary Classifier
  
  TARGET: stockout_in_14d = 1 (yes) or 0 (no)
  
  OUTPUT for supply chain team:
  ┌─────────────────────────────────────────────────────────┐
  │ HIGH RISK STOCKOUTS — Next 14 Days                      │
  │                                                         │
  │ Himalayan Water 1L  │ Gujarat    │ 92% probability      │
  │ Tata Tea Gold 250g  │ Punjab     │ 87% probability      │
  │ Tetley Green Tea    │ Delhi      │ 81% probability      │
  └─────────────────────────────────────────────────────────┘
  
  METRIC: Recall is more important than Precision here!
  REASON: Missing a stockout (false negative) is worse than
          false alarm (false positive) — stockout = lost revenue
  Target: Recall > 85%
```

---

## 📦 ML USE CASE 5 — ANOMALY DETECTION
### "This sales number looks wrong — is it real or a data error?"

```
TWO TYPES OF ANOMALIES IN CPG:
────────────────────────────────
  TYPE 1: DATA ANOMALY (bad data entered in SAP)
          Example: One store reported 10x normal sales — data entry error
          
  TYPE 2: BUSINESS ANOMALY (real but unusual event)
          Example: Competitor product recall → TCPL sales spike 3x

ALGORITHMS:
────────────
  ┌──────────────────┬──────────────────────────────────────────────────┐
  │ ALGORITHM        │ HOW IT WORKS                                     │
  ├──────────────────┼──────────────────────────────────────────────────┤
  │ Z-Score          │ Simplest. Flag if value > 3 standard deviations  │
  │                  │ from rolling mean. Fast. Good starting point.    │
  ├──────────────────┼──────────────────────────────────────────────────┤
  │ IQR Method       │ Flag if value outside Q1-1.5×IQR or Q3+1.5×IQR  │
  │                  │ Non-parametric. Works without normal distribution │
  ├──────────────────┼──────────────────────────────────────────────────┤
  │ Isolation Forest │ Tree-based. Anomalies are "easier to isolate".   │
  │                  │ Great for multidimensional anomalies.            │
  │                  │ Used in Glue ETL at TCPL.                        │
  ├──────────────────┼──────────────────────────────────────────────────┤
  │ Prophet + bounds │ Use Prophet forecast confidence intervals.       │
  │                  │ If actual falls outside → anomaly.               │
  │                  │ Great for time-aware anomaly detection.          │
  ├──────────────────┼──────────────────────────────────────────────────┤
  │ Autoencoder      │ Neural network. Reconstructs normal patterns.    │
  │                  │ High reconstruction error = anomaly.             │
  │                  │ Advanced. Use when others fail.                  │
  └──────────────────┴──────────────────────────────────────────────────┘
  
  AT TCPL: Z-Score in Glue ETL (fast, inline) + Isolation Forest in
           SageMaker batch job weekly (deeper analysis)
```

---

## 📦 ML USE CASE 6 — PRICE ELASTICITY & OPTIMIZATION
### "If I raise price by 5%, how much volume will I lose?"

```
CONCEPT: PRICE ELASTICITY
──────────────────────────
  Elasticity = % change in demand / % change in price
  
  Elasticity = -1.5 means:
  "If price goes up 10% → demand goes DOWN 15%"
  
  Inelastic (|e| < 1): Price change has small demand effect (premium brands)
  Elastic   (|e| > 1): Price change has large demand effect (commodity SKUs)
  
  TCPL example:
  - Tata Tea Gold (premium) → elasticity ~ -0.8 (inelastic, loyal buyers)
  - Tata Tea Regular (mass) → elasticity ~ -1.8 (elastic, price sensitive)

ALGORITHMS:
────────────
  LOG-LOG REGRESSION (most common in CPG):
    log(demand) = α + β × log(price) + controls
    β directly = price elasticity coefficient
  
  XGBOOST with price features:
    Add price, competitor_price, price_index as features
    Use SHAP values to understand price impact
  
  BAYESIAN HIERARCHICAL MODEL:
    Best for multi-SKU elasticity estimation
    Borrows strength across similar SKUs
    Handles sparse data for new products
```

---

## 🔄 HOW ML IS ACTUALLY CARRIED OUT AT TCPL — END TO END

```
PHASE 1: PROBLEM DEFINITION (Week 1-2)
────────────────────────────────────────
  Business stakeholder says:
  "Our demand planners are doing forecasting in Excel.
   It takes 3 days and accuracy is terrible."
  
  You (Decision Point) translate to ML problem:
  "We need a weekly demand forecast per SKU per region
   for 90-day horizon with < 15% MAPE"
  
  Define:
  ✅ Target variable: weekly_units_sold
  ✅ Forecast horizon: 13 weeks (90 days)
  ✅ Granularity: SKU × Region × Week
  ✅ Success metric: MAPE < 15%, WAPE < 12%
  ✅ Business impact: Reduce planner time 3 days → 2 hours


PHASE 2: DATA DISCOVERY & EDA (Week 2-4)
──────────────────────────────────────────
  Pull 2-3 years of sales from Snowflake:
  
    SELECT * FROM DW.FACT_SALES
    WHERE year >= 2021
    ORDER BY sku_code, region_code, order_date;
  
  EDA questions you answer:
  ┌────────────────────────────────────────────────────────┐
  │  How many unique SKUs? (maybe 500+)                    │
  │  How many regions? (maybe 20+)                         │
  │  Any SKUs with < 1 year history? (new launches)        │
  │  Missing weeks? (data gaps)                            │
  │  Seasonality patterns? (Diwali spike? Summer dip?)     │
  │  Promo impact visible? (spikes during promotions?)     │
  │  Any SKUs with near-zero sales? (long tail problem)    │
  └────────────────────────────────────────────────────────┘
  
  Tools: SageMaker Studio Jupyter Notebooks
         Python: pandas, matplotlib, seaborn, statsmodels


PHASE 3: FEATURE ENGINEERING (Week 3-5)
─────────────────────────────────────────
  Glue job: tcpl-glue-feature-engineering
  
  Builds feature store in S3:
  s3://tcpl-datalake/ml-features/demand_forecast/
  
  Features built (as covered in Use Case 1):
  - Lag features (1w, 2w, 4w, 8w, 52w)
  - Rolling stats (mean, std, min, max)
  - Seasonality flags
  - Promo features
  - Product attributes
  - External signals


PHASE 4: MODEL TRAINING (Week 5-7)
─────────────────────────────────────
  Train-Validation-Test SPLIT for time series:
  
  ⚠️ NEVER do random split for time series!
  ⚠️ Must respect time order!
  
  CORRECT SPLIT:
  ────────────────
  
  ←──── TRAIN ────────────────→ ←─ VAL ─→ ←─ TEST ─→
  Jan 2021 ─────────── Dec 2022  Jan-Sep23  Oct-Dec23
  
  Why?
  Model trains on past → evaluated on "future" it never saw
  Simulates real production environment
  
  WALK-FORWARD VALIDATION (best practice):
  ──────────────────────────────────────────
  Round 1: Train on 2021 → Predict Q1 2022 → measure MAPE
  Round 2: Train on 2021+Q1 → Predict Q2 2022 → measure MAPE
  Round 3: Train on 2021+H1 → Predict Q3 2022 → measure MAPE
  ...
  Average MAPE across all rounds = true model performance
  
  XGBoost Training on SageMaker:
  ───────────────────────────────
  HYPERPARAMETERS to tune:
  
  ┌───────────────────┬──────────────┬────────────────────────────────────┐
  │ PARAMETER         │ RANGE        │ WHAT IT CONTROLS                   │
  ├───────────────────┼──────────────┼────────────────────────────────────┤
  │ max_depth         │ 3 to 10      │ Tree complexity (depth of trees)   │
  │ eta (lr)          │ 0.01 to 0.3  │ Learning rate (how fast it learns) │
  │ num_round         │ 100 to 1000  │ Number of trees to build           │
  │ subsample         │ 0.6 to 1.0   │ % of data per tree (overfitting)   │
  │ colsample_bytree  │ 0.6 to 1.0   │ % of features per tree             │
  │ min_child_weight  │ 1 to 10      │ Min samples in leaf (regularize)   │
  │ gamma             │ 0 to 5       │ Min gain to make split             │
  └───────────────────┴──────────────┴────────────────────────────────────┘
  
  HYPERPARAMETER TUNING with SageMaker:
    Use SageMaker Automatic Model Tuning (Bayesian optimization)
    It tries different combos and finds the best MAPE automatically


PHASE 5: MODEL EVALUATION (Week 7)
────────────────────────────────────
  Evaluate on TEST set (Oct-Dec 2023 — never seen by model)
  
  Report format:
  ┌────────────────────────────────────────────────────────────────┐
  │ MODEL EVALUATION REPORT — TCPL Demand Forecast                 │
  ├─────────────────┬──────────────────────────────────────────────┤
  │ Overall MAPE    │ 12.3%  ✅ (target < 15%)                     │
  │ Overall WAPE    │ 9.8%   ✅ (target < 12%)                     │
  │ Bias            │ +1.2%  ✅ (slight over-forecast, acceptable)  │
  ├─────────────────┼──────────────────────────────────────────────┤
  │ Tata Tea brand  │ MAPE 10.1% ✅                                 │
  │ Tetley brand    │ MAPE 13.4% ✅                                 │
  │ Himalayan Water │ MAPE 17.2% ⚠️ (highly seasonal, harder)      │
  │ Tata Salt       │ MAPE 8.9%  ✅                                 │
  ├─────────────────┼──────────────────────────────────────────────┤
  │ North Zone      │ MAPE 11.2% ✅                                 │
  │ South Zone      │ MAPE 14.8% ✅ (borderline)                   │
  ├─────────────────┼──────────────────────────────────────────────┤
  │ Festive weeks   │ MAPE 18.5% ⚠️ (hard to predict spikes)       │
  │ Regular weeks   │ MAPE 10.7% ✅                                 │
  └─────────────────┴──────────────────────────────────────────────┘

  FEATURE IMPORTANCE (SHAP values):
  ───────────────────────────────────
  Understand WHY the model makes predictions
  
  Top features typically in CPG demand forecasting:
  1. lag_52w_units     (same week last year — most predictive)
  2. rolling_4w_avg    (recent trend)
  3. is_festive        (Diwali spike)
  4. lag_4w_units      (1 month ago)
  5. promo_discount_pct (promotions matter a lot)
  6. brand_encoded     (brand matters)
  7. is_summer         (seasonal effect)
  8. mrp               (price level)


PHASE 6: MODEL DEPLOYMENT (Week 8-9)
──────────────────────────────────────
  TWO DEPLOYMENT MODES at TCPL:

  MODE A: BATCH INFERENCE (what TCPL mainly uses)
  ─────────────────────────────────────────────────
  - Run every Sunday night
  - Predict next 13 weeks for ALL SKUs × Regions
  - Results → Snowflake → Tableau dashboard Monday morning
  
  SageMaker Batch Transform:
    Input:  s3://tcpl-datalake/ml-features/latest/
    Output: s3://tcpl-datalake/predictions/2024-01-21/
    Lambda: Loads predictions → Snowflake

  MODE B: REAL-TIME INFERENCE (for what-if analysis)
  ────────────────────────────────────────────────────
  - Demand planner enters: "What if I run 20% discount on Tata Tea in MH?"
  - API call → SageMaker endpoint → instant prediction
  
  SageMaker Endpoint:
    POST /predict
    Body: { sku_code: "TTP-500", region: "MH", promo_pct: 0.20 }
    Returns: { predicted_units: 18500, lower: 16200, upper: 20800 }


PHASE 7: MONITORING IN PRODUCTION (Ongoing)
─────────────────────────────────────────────
  3 TYPES OF MONITORING:

  1. DATA QUALITY MONITORING
     ─────────────────────────
     SageMaker Model Monitor checks feature distribution daily
     Alert if: new_data_distribution ≠ training_data_distribution
     Example: Suddenly all promo_discount_pct = 0 (data pipeline bug!)

  2. MODEL PERFORMANCE MONITORING
     ──────────────────────────────
     Every week: compare predictions vs actuals (once actuals come in)
     Track MAPE rolling 4-week average
     Alert if MAPE > 15% for 2 consecutive weeks → RETRAIN

  3. CONCEPT DRIFT MONITORING
     ───────────────────────────
     Is the relationship between features and sales changing?
     Example: Post-COVID consumer behavior changed (people buying more online)
     Your 2021-trained model won't capture this
     Alert if PSI (Population Stability Index) > 0.2 → RETRAIN
     
     PSI > 0.1 = slight drift (monitor closely)
     PSI > 0.2 = significant drift (retrain now)


PHASE 8: RETRAINING STRATEGY
──────────────────────────────
  SCHEDULED RETRAINING:
    Monthly full retrain with latest data (standard practice)
    
  TRIGGERED RETRAINING:
    MAPE > 15% for 2 weeks → auto-trigger retrain DAG in Airflow
    Data drift PSI > 0.2 → auto-trigger retrain
    
  RETRAINING AIRFLOW DAG:
    tcpl_model_retrain_dag.py
    → Glue: rebuild features with latest 2 years
    → SageMaker: retrain XGBoost
    → Evaluate: MAPE check
    → If pass: register new version, deprecate old
    → Snowflake: update model_version in predictions table
```

---

## 🔧 FULL MLOPS PIPELINE — WHAT ACTUALLY RUNS ON SAGEMAKER

```
                    MLOPS PIPELINE AT TCPL
                    ══════════════════════

  CODE COMMIT (Git push)
          │
          ▼
  CI/CD TRIGGER (CodePipeline / GitHub Actions)
          │
          ▼
  ┌───────────────────────────────────────────────────┐
  │           SAGEMAKER PIPELINE                       │
  │                                                   │
  │  Step 1: DATA VALIDATION                          │
  │  ─────────────────────────                        │
  │  Check feature store has today's data             │
  │  Validate no nulls in key features                │
  │  Check row counts per SKU group                   │
  │          │                                        │
  │          ▼                                        │
  │  Step 2: PREPROCESSING                            │
  │  ────────────────────────                         │
  │  SKLearnProcessor (ml.m5.xlarge)                  │
  │  - Normalize features                             │
  │  - Encode categoricals                            │
  │  - Train/val/test split (time-aware)              │
  │  - Save scaler/encoder artifacts                  │
  │          │                                        │
  │          ▼                                        │
  │  Step 3: TRAINING                                 │
  │  ─────────────────                                │
  │  XGBoost Estimator (ml.m5.2xlarge)               │
  │  - Reads train + validation from S3               │
  │  - Trains with early stopping                     │
  │  - Logs metrics to CloudWatch                     │
  │  - Saves model.tar.gz to S3                       │
  │          │                                        │
  │          ▼                                        │
  │  Step 4: EVALUATION                               │
  │  ────────────────────                             │
  │  SKLearnProcessor                                 │
  │  - Loads model + test data                        │
  │  - Calculates MAPE, WAPE, Bias, RMSE              │
  │  - Saves evaluation.json to S3                    │
  │          │                                        │
  │          ▼                                        │
  │  Step 5: QUALITY GATE                             │
  │  ─────────────────────                            │
  │  ConditionStep: MAPE < 15%?                       │
  │          │                                        │
  │      YES │              NO │                      │
  │          ▼                 ▼                      │
  │  Step 6: REGISTER      FAIL STEP                  │
  │  Model Registry        Alert + Stop               │
  │  Status: Pending                                  │
  │  Manual Approval                                  │
  │          │                                        │
  │     [Human approves in SageMaker UI]              │
  │          │                                        │
  │          ▼                                        │
  │  Step 7: BATCH TRANSFORM                          │
  │  ────────────────────────                         │
  │  Generate predictions for all SKU × Region        │
  │  Output: predictions CSV → S3                     │
  │          │                                        │
  │          ▼                                        │
  │  Step 8: LOAD TO SNOWFLAKE                        │
  │  (Lambda tcpl-predictions-loader)                 │
  │  MERGE into ML_OUTPUTS.DEMAND_FORECAST            │
  └───────────────────────────────────────────────────┘
          │
          ▼
  MONITORING SCHEDULE (SageMaker Model Monitor)
  - Hourly: data quality check
  - Weekly: performance check vs actuals
  - Monthly: full retrain trigger


ARTIFACT VERSIONING IN S3:
────────────────────────────
  s3://tcpl-datalake/model-artifacts/
  ├── demand_forecast/
  │   ├── v1.0/  (Jan 2024 — baseline)
  │   │   ├── model.tar.gz
  │   │   ├── evaluation.json  (MAPE: 14.1%)
  │   │   └── feature_importance.csv
  │   ├── v1.1/  (Feb 2024 — added weather features)
  │   │   ├── model.tar.gz
  │   │   ├── evaluation.json  (MAPE: 12.8%)
  │   │   └── feature_importance.csv
  │   └── v1.2/  (Mar 2024 — current production)
  │       ├── model.tar.gz
  │       ├── evaluation.json  (MAPE: 11.9%) ← PROD
  │       └── feature_importance.csv
  └── mmm/
      └── v0.9/
```

---

## 🎯 ALGORITHM CHEAT SHEET — CPG MASTER TABLE

```
┌─────────────────────┬──────────────────────┬─────────────────┬──────────────────────────────┐
│ USE CASE            │ ALGORITHM            │ METRIC          │ WHEN TO USE                  │
├─────────────────────┼──────────────────────┼─────────────────┼──────────────────────────────┤
│ Demand Forecasting  │ XGBoost (PRIMARY)    │ MAPE < 15%      │ Tabular, lots of features    │
│                     │ LightGBM             │ WAPE < 12%      │ Large dataset (>10M rows)    │
│                     │ DeepAR+ (SageMaker)  │                 │ Many SKUs, native AWS        │
│                     │ Prophet              │                 │ Baseline, simple seasonality │
│                     │ Ensemble             │                 │ Production best practice     │
├─────────────────────┼──────────────────────┼─────────────────┼──────────────────────────────┤
│ Market Mix Model    │ Bayesian MMM (PyMC)  │ R² > 0.85       │ Media attribution            │
│                     │ Facebook Robyn       │ MAPE < 20%      │ If team uses R               │
│                     │ Ridge Regression     │                 │ Simple baseline              │
│                     │ LightweightMMM       │                 │ Python, Google's library     │
├─────────────────────┼──────────────────────┼─────────────────┼──────────────────────────────┤
│ Trade Promo         │ CausalImpact         │ Promo ROI       │ Measure uplift causally      │
│ Optimization        │ DiD (Diff-in-Diff)   │ p-value < 0.05  │ A/B test regions             │
│                     │ XGBoost Regressor    │                 │ Predict best promo type      │
├─────────────────────┼──────────────────────┼─────────────────┼──────────────────────────────┤
│ Stockout Prediction │ XGBoost Classifier   │ Recall > 85%    │ Primary model                │
│                     │ Random Forest        │ F1 > 0.80       │ Interpretable alternative    │
│                     │ Logistic Regression  │                 │ Baseline + explainability    │
├─────────────────────┼──────────────────────┼─────────────────┼──────────────────────────────┤
│ Anomaly Detection   │ Z-Score              │ Precision > 70% │ Fast inline (Glue ETL)       │
│                     │ Isolation Forest     │ Recall > 80%    │ Batch weekly (SageMaker)     │
│                     │ Prophet + bounds     │                 │ Time-series aware            │
│                     │ Autoencoder          │                 │ Complex multivariate         │
├─────────────────────┼──────────────────────┼─────────────────┼──────────────────────────────┤
│ Price Elasticity    │ Log-Log Regression   │ R² > 0.75       │ Elasticity measurement       │
│                     │ XGBoost + SHAP       │                 │ Price impact in demand model │
│                     │ Bayesian Hierarchical│                 │ Multi-SKU estimation         │
├─────────────────────┼──────────────────────┼─────────────────┼──────────────────────────────┤
│ Customer Churn      │ XGBoost Classifier   │ AUC > 0.80      │ Distributor churn risk       │
│ (Distributor)       │ Logistic Regression  │ Recall > 75%    │ Interpretable, fast          │
├─────────────────────┼──────────────────────┼─────────────────┼──────────────────────────────┤
│ Product Rec.        │ Association Rules    │ Lift > 2        │ "Bought together" analysis   │
│ (Cross-sell)        │ (Apriori/FP-Growth)  │                 │ Trade promotion bundling     │
└─────────────────────┴──────────────────────┴─────────────────┴──────────────────────────────┘
```

---

## 🐍 PYTHON LIBRARIES YOU WILL USE

```
DATA PROCESSING:
────────────────
  pandas          → dataframes, groupby, merge
  numpy           → numerical operations
  pyspark         → big data processing in Glue

ML MODELS:
──────────
  xgboost         → primary forecasting model
  lightgbm        → alternative to XGBoost (faster)
  scikit-learn    → preprocessing, evaluation, Random Forest
  prophet         → Facebook's time series library
  statsmodels     → ARIMA, statistical tests

DEEP LEARNING / ADVANCED:
──────────────────────────
  pytorch         → custom neural nets (if needed)
  tensorflow      → Keras-based models

CAUSAL / MMM:
──────────────
  causalimpact    → Google's Bayesian causal inference
  pymc            → Bayesian MMM
  robyn           → Facebook's MMM (R-based)

EVALUATION & EXPLAINABILITY:
──────────────────────────────
  shap            → Feature importance (MUST use in CPG)
  mlflow          → Experiment tracking (if using MLflow)
  evidently       → Data drift monitoring

VISUALIZATION (for EDA):
──────────────────────────
  matplotlib      → basic plots
  seaborn         → statistical plots
  plotly          → interactive dashboards

AWS SPECIFIC:
─────────────
  sagemaker       → SageMaker SDK
  boto3           → AWS SDK for Python
  awswrangler     → pandas for AWS (S3, Glue Catalog, Athena)
  snowflake-connector-python → Snowflake from Lambda/local
```

---

## 🧭 HOW TO KNOW WHICH ALGORITHM TO PICK — DECISION GUIDE

```
STEP 1: WHAT TYPE OF PROBLEM IS IT?
─────────────────────────────────────
  Predict a NUMBER (units, revenue) → REGRESSION
    → XGBoost Regressor / LightGBM / DeepAR
  
  Predict YES/NO (stockout, churn) → CLASSIFICATION
    → XGBoost Classifier / Random Forest / Logistic Regression
  
  Find unusual patterns → ANOMALY DETECTION
    → Isolation Forest / Z-Score / Autoencoder
  
  Understand cause and effect → CAUSAL INFERENCE
    → CausalImpact / DiD / Bayesian MMM
  
  Predict sequence over time → TIME SERIES
    → XGBoost with lags / LightGBM / DeepAR+ / Prophet

STEP 2: HOW MUCH DATA DO YOU HAVE?
────────────────────────────────────
  < 1 year history     → Prophet (handles short history)
  1-3 years history    → XGBoost with lag features
  3+ years history     → XGBoost / LightGBM / DeepAR+ all work

STEP 3: HOW MANY SKUs?
────────────────────────
  < 50 SKUs    → Train one model per SKU (local model)
  50-500 SKUs  → Train one global XGBoost model (all SKUs together)
  500+ SKUs    → DeepAR+ (designed for this, native SageMaker)

STEP 4: DO YOU NEED EXPLAINABILITY?
─────────────────────────────────────
  YES → XGBoost + SHAP values
        (TCPL business team needs to trust the model)
  NO  → DeepAR+ / Neural nets (black box but more accurate)
  
  PRO TIP: In CPG always choose interpretable models first.
           Business stakeholders at TCPL will ask:
           "WHY is the model predicting low sales for Diwali?"
           You must be able to explain it using SHAP values.

STEP 5: LATENCY REQUIREMENT?
──────────────────────────────
  Real-time (<1 sec) → Lightweight model + SageMaker endpoint
  Batch overnight    → XGBoost / LightGBM / DeepAR+ (no latency constraint)
  
  AT TCPL: Batch overnight (weekly predictions) is primary.
           Real-time endpoint only for what-if scenario tool.
```

---

## 📊 MLOPS MATURITY AT TCPL — WHERE YOU ARE NOW

```
LEVEL 0: MANUAL (Excel-based — where TCPL was BEFORE Decision Point)
  - Demand planners use Excel
  - No ML, gut-feel forecasting
  - Takes 3 days, 30% MAPE
  - No monitoring, no automation

LEVEL 1: ML BUT MANUAL DEPLOYMENT (where project started)
  - ML models built in Jupyter notebooks
  - Data scientist manually runs script
  - Model deployed manually to endpoint
  - No monitoring, retrain manually
  ← DECISION POINT started here

LEVEL 2: AUTOMATED TRAINING (where you are now → your work)
  - Airflow triggers SageMaker pipeline automatically
  - Quality gate in pipeline (MAPE check)
  - Model registered in Model Registry
  - Predictions auto-loaded to Snowflake
  - Basic CloudWatch monitoring
  ← CURRENT STATE AT TCPL

LEVEL 3: FULL MLOPS (roadmap — your next 6 months)
  - Automated retraining on drift detection
  - A/B testing: two models serve predictions, better one wins
  - Feature store (SageMaker Feature Store)
  - Explainability reports automated (SHAP per SKU per region)
  - Real-time endpoint for planning tool
  ← WHERE YOU ARE HEADED
```

---

## 💬 HOW TO TALK ABOUT ML AT TCPL ON DAY 1

```
WHEN BUSINESS ASKS: "How accurate is the model?"
────────────────────────────────────────────────
  SAY: "Our XGBoost model achieves 12% MAPE overall, which means
        on average our weekly predictions are within 12% of actuals.
        For festive weeks it's harder — around 18%. We monitor this
        weekly and retrain monthly."

WHEN TCPL ASKS: "Why did the model predict low for Diwali?"
────────────────────────────────────────────────────────────
  SAY: "Good question. Let me pull up the SHAP values for this SKU
        in this region. SHAP shows us which features drove the prediction.
        If festive_flag = 1 but model still predicted low, it might be
        because last year's Diwali data had a stockout — so the model
        learned lower sales, not true demand. We should correct for that."

WHEN SOMEONE ASKS: "Can we use ChatGPT/LLM for this?"
───────────────────────────────────────────────────────
  SAY: "LLMs are great for text tasks, but demand forecasting is a
        structured tabular regression problem. XGBoost trained on
        2 years of TCPL sales history with business-specific features
        will massively outperform any LLM. We might use LLMs for
        report generation or business narrative generation around
        the forecast numbers."
```

---

*You've got this bro 💪*
*CPG ML = Demand Forecasting (XGBoost) + MMM (Bayesian) + TPO (Causal) + Anomaly (Isolation Forest)*
*Every model goes through: EDA → Feature Eng → Train → Evaluate → SageMaker Pipeline → Snowflake → Monitor*
