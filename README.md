# NYC Yellow Taxi 2026 — Databricks · Delta Lake · SQL · Medallion Architecture

## Overview

End-to-end data engineering and analytics project on NYC Yellow Taxi trip records
(January–May 2026) using a full medallion architecture (Bronze → Silver → Gold)
on Databricks. Raw Parquet files are ingested into Delta tables, cleaned and
enriched in Silver, and aggregated into nine analytical Gold tables that power
an interactive Databricks dashboard.

The project demonstrates the complete data lifecycle: ingestion, cleaning,
quality validation, analytical modelling, and visualisation — combining SQL
and PySpark within the same Databricks notebook.

**Dataset:** NYC Taxi and Limousine Commission (TLC) — Yellow Taxi Trip Records  
**Source:** https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page  
**Period:** January–May 2026 · June excluded (source file contained only one trip record)  
**Scale:** ~12.25M trips after cleaning · $380M total revenue  

---

## Objectives

- Implement a production-style medallion architecture (Bronze / Silver / Gold) on Databricks Unity Catalog
- Apply rigorous data quality rules in Silver: invalid timestamps, meter errors, outlier durations, and unknown vendor/rate codes
- Build nine Gold analytical Delta tables covering trip efficiency, fare breakdown, surge detection, geographic corridors, airport analysis, congestion policy impact, vendor comparison, and monthly trends
- Deliver findings through an interactive multi-page Databricks dashboard

---

## Tools & Technologies

| Tool | Purpose |
|---|---|
| Databricks (Community Edition) | Unified platform — notebooks, Unity Catalog, Delta Lake, dashboard |
| Apache Spark (PySpark) | Distributed data processing |
| Delta Lake | ACID-compliant storage with time travel and versioning |
| SQL (Databricks SQL) | Bronze exploration, Silver cleaning, Gold analytical tables |
| Python | Data loading, schema definition, transformation logic |
| Unity Catalog | Three-tier namespace: `taxi.taxi_bronze / taxi_silver / taxi_gold` |

---

## Medallion Architecture

```
Source (NYC TLC)
│
▼
┌─────────────┐
│ BRONZE │ Raw ingestion — no transformations
│ │ · yellow_taxi_trip_raw_data (~12.6M rows, 5 Parquet files)
│ │ · yellow_taxi_lookup_raw_data (265 rows, zone reference)
└──────┬──────┘
│
▼
┌─────────────┐
│ SILVER │ Cleaned + enriched (~12.25M rows after filtering)
│ │
│ │ Removed:
│ │ · Invalid fares and zero-distance trips
│ │ · Impossible timestamps (dropoff ≤ pickup)
│ │ · Trip duration < 1 min or > 180 mins (meter errors)
│ │ · Timestamps outside 2026
│ │ · June 2026 (single-trip incomplete export)
│ │
│ │ Added:
│ │ · vendorid_label, rate_type, payment_type_label
│ │ · pickup_hour, pickup_day_of_week, pickup_month
│ │ · trip_duration_mins, avg_speed_mph
│ │ · tip_pct (credit card only)
│ │ · trip_type, congestion_zone_flag
└──────┬──────┘
│
▼
┌─────────────┐
│ GOLD │ Nine analytical Delta tables — serving layer for dashboard
└─────────────┘
```

---

## Analyses (Gold Tables)

| # | Table | Description |
|---|---|---|
| 1 | `trip_duration_speed_analysis` | Fare efficiency by duration bucket (5-15, 15-30, 30-45, 45+ min) |
| 2 | `fare_components_breakdown` | Fare component share and tip rate by payment method |
| 3a | `surge_detection_general` | Average revenue across all 168 hour × day combinations |
| 3b | `surge_detection` | Top 5 highest-revenue hour × day combinations |
| 4 | `top_pickup_dropoff_zones` | Zones in top 10 for both pickup and dropoff volume |
| 5a | `zone_to_zone_frequency` | Top 20 most frequent pickup → dropoff corridors |
| 5b | `zone_to_zone_profitability` | Top 20 most profitable corridors by fare per minute (min. 20 trips) |
| 6 | `trip_type_analysis` | Airport vs standard trip comparison + top 15 airport pickup zones |
| 7a | `congestion_impact_by_corridor` | Congestion zone vs standard trips by borough corridor |
| 7b | `congestion_impact_monthly` | Monthly trend by congestion zone flag with MoM growth |
| 8 | `vendor_performance` | Vendor comparison — fares, tips, payment method breakdown |
| 9 | `monthly_trend_analysis` | Month-over-month growth across all key metrics |

---

## Key Findings

- **Airport trips generate 3.5× higher average fares** ($56.75 vs $17.16) despite representing only 10.2% of total trips — yet account for nearly 48% of all tip revenue
- **Early morning hours (4–5am) produce the highest average revenue per trip** (~$42 vs ~$29 at midday), driven by airport runs and longer cross-borough journeys
- **Queens → Airport corridors dominate profitability rankings** — South Ozone Park → JFK generates $10.38/minute, the highest fare-per-minute of any corridor with sufficient trip volume
- **Curb Mobility (VeriFone) processes 3.5× more trips** than Creative Mobile Technologies but charges $1.43 less per trip on average — while its passengers tip nearly 5 percentage points more on credit card payments
- **Congestion zone trip volume grew 25%** from January to May 2026 vs 17% growth for standard trips — suggesting congestion pricing has not reduced inner-city taxi demand
- **Total revenue grew 25.8%** from $68.75M in January to $86.50M in May, driven by both volume growth (+23%) and modest fare increases (+2.9%)
- **VendorID 6 (Myle Technologies) and VendorID 7 (Helix)**, both valid per the 2026 TLC data dictionary, produced zero clean records after Silver filtering — suggesting metering system incompatibility with TLC reporting standards

---

## Data Quality Decisions

| Issue | Decision |
|---|---|
| Trip duration < 1 min — meter not properly reset | Removed in Silver |
| Trip duration > 180 mins — meter left running | Removed in Silver |
| Timestamps outside 2026 — meter clock resets | Removed in Silver |
| June 2026 — only 1 trip in source file | Excluded — incomplete export |
| RatecodeID = 99 — valid per 2026 data dictionary | Kept, labelled `unknown` |
| payment_type = 0 — Flex Fare, valid per 2026 data dictionary | Kept, labelled `flex_fare` |
| VendorID 6 & 7 — valid vendors, zero clean records | Kept in Bronze, absent from Silver |
| Profitability corridors — single-trip statistical noise | Minimum 20 trips per corridor enforced |

---

## Dashboard

Built on Databricks native dashboard — 8 pages querying Gold Delta tables directly.

| Page | Content |
|---|---|
| Overview | 6 KPI cards — total trips, revenue, avg fare, avg distance, avg duration, avg tips |
| Monthly Trend | Trip and revenue line charts, MoM % change, avg fare trend |
| Trip Duration & Surge | Duration bucket analysis, fare/min efficiency, hourly heatmap |
| Geography | Frequency and profitability corridor tables, busiest interchange zones |
| Fare & Payment | Revenue and trip share by payment type, fare component breakdown |
| Vendors Analysis | Trip volume, fares, tips, cash vs card split by vendor |
| Airport Analysis | Airport vs standard comparison, top 15 pickup zones, tip revenue share |
| Congestion Analysis | Congestion zone vs standard monthly trends |

### Screenshots

#### Overview
![Overview](dashboard/screenshots/overview.png)

#### Monthly Trend
![Monthly Trend](dashboard/screenshots/monthly_trend.png)

#### Trip Duration & Surge
![Trip Duration](dashboard/screenshots/trip_duration.png)

#### Geography
![Geography](dashboard/screenshots/geography.png)

#### Fare & Payment
![Fare & Payment](dashboard/screenshots/fare_payment.png)

#### Vendors Analysis
![Vendors](dashboard/screenshots/vendors_analysis.png)

#### Airport Analysis
![Airport](dashboard/screenshots/airport_analysis.png)

#### Congestion Analysis
![Congestion](dashboard/screenshots/congestion_analysis.png)

---

## Project Structure

nyc-yellow-taxi-2026/
├── README.md
├── notebook/
│ └── Taxi_dataset.ipynb # Full Databricks notebook — Bronze to Gold
├── dashboard/
│ └── screenshots/ # Dashboard page screenshots
└── data/
└── taxi_zone_lookup.csv # TLC zone reference table (265 rows)
└── 5 .parquet files

---

## How to Run

1. Create a Databricks workspace with Unity Catalog — schemas: `taxi.taxi_bronze`, `taxi.taxi_silver`, `taxi.taxi_gold`
2. Upload the 5 monthly Parquet files and `taxi_zone_lookup.csv` to a Bronze Volume
3. Open `Taxi_dataset.ipynb` and update the Volume path in the Bronze ingestion cells
4. Run all cells top to bottom — Bronze → Silver → Gold builds sequentially
5. Open Databricks Dashboards and connect each widget to the corresponding Gold table

---
