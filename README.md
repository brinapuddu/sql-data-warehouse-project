# Data Warehouse and Analytics Project

Welcome to the **Data Warehouse and Analytics Project** repository! 🚀

This project demonstrates a comprehensive, end-to-end data warehousing and analytics solution — from designing the warehouse infrastructure to delivering actionable business insights through structured reporting. Built as a portfolio project, it reflects industry best practices across data engineering, data modeling, and business intelligence.

---

## 📋 Project Overview

This project covers four core disciplines:

1. **Data Architecture**: Designing a modern data warehouse using the Medallion Architecture, structured across **Bronze**, **Silver**, and **Gold** layers to progressively refine raw data into business-ready insights.
2. **ETL Pipelines**: Building robust Extract, Transform, and Load processes to move and prepare data from source systems into the warehouse reliably and efficiently.
3. **Data Modeling**: Constructing well-structured fact and dimension tables optimized for analytical queries and reporting performance.
4. **Analytics & Reporting**: Authoring SQL-based analyses and dashboards that surface key metrics to support strategic business decision-making.

> This repository serves as an excellent reference for professionals and students seeking to showcase expertise in:
> - SQL Development
> - Data Architecture
> - Data Engineering
> - ETL Pipeline Development
> - Data Modeling
> - Data Analytics

---

## 🔧 Important Links & Tools

All tools listed below are available free of charge:

| Tool | Purpose |
|------|---------|
| **[Datasets](#)** | Access the project's source data (CSV files) |
| **[SQL Server Express](#)** | Lightweight server for hosting the SQL database |
| **[SQL Server Management Studio (SSMS)](#)** | GUI for managing and interacting with the database |
| **[Git & GitHub](#)** | Version control to manage, track, and collaborate on code |
| **[DrawIO](#)** | Design data architecture diagrams, models, and data flows |
| **[Notion](#)** | All-in-one project management and task organization |
| **[Project Steps](#)** | Access all project phases and task breakdowns |

---

## 📐 Project Requirements

### Building the Data Warehouse (Data Engineering)

**Objective**

Develop a modern data warehouse using SQL Server to consolidate sales data from multiple source systems, enabling reliable analytical reporting and informed decision-making across the organization.

**Specifications**

- **Data Sources**: Import data from two independent source systems — an ERP system and a CRM system — both provided as CSV files.
- **Data Quality**: Identify, investigate, and resolve data quality issues (e.g., nulls, duplicates, inconsistent formats) prior to analysis.
- **Integration**: Combine both source systems into a single, unified data model designed for analytical queries, ensuring seamless joins and consistent identifiers.
- **Scope**: Focus exclusively on the latest available dataset. Historization and slowly changing dimensions are out of scope for this project.
- **Documentation**: Deliver clear, comprehensive documentation of the data model, schema design, and transformation logic to support both business stakeholders and analytics teams.

---

### BI: Analytics & Reporting (Data Analytics)

**Objective**

Develop SQL-based analytics solutions to deliver detailed, repeatable insights across three key business domains:

- **Customer Behavior** — Understand purchasing patterns, customer segmentation, and retention signals.
- **Product Performance** — Evaluate which products drive revenue, identify top and bottom performers, and monitor category-level trends.
- **Sales Trends** — Track revenue growth, seasonal patterns, and period-over-period performance.

These insights equip stakeholders with the key business metrics needed for confident, data-driven decision-making.

For full specifications, refer to [`docs/requirements.md`](docs/requirements.md).

---

## 🏗️ Data Architecture

The data architecture follows the **Medallion Architecture** pattern, structured across three progressive layers: **Bronze**, **Silver**, and **Gold**.

```
┌──────────┐    ┌───────────────┐    ┌───────────────┐    ┌──────────────┐    ┌─────────────────┐
│  Sources │───▶│  Bronze Layer │───▶│  Silver Layer │───▶│  Gold Layer  │───▶│     Consume     │
│          │    │   Raw Data    │    │ Cleaned Data  │    │Business-Ready│    │  BI & Reporting │
│  CRM CSV │    │   (Tables)    │    │   (Tables)    │    │   (Views)    │    │ Ad-Hoc Queries  │
│  ERP CSV │    │               │    │               │    │              │    │ Machine Learning│
└──────────┘    └───────────────┘    └───────────────┘    └──────────────┘    └─────────────────┘
```

---

### 🟫 Bronze Layer — Raw Data Ingestion

The Bronze layer serves as the **landing zone** for all incoming data. Data is ingested directly from source CSV files into SQL Server tables with no modifications, preserving a faithful copy of the original source records.

| Property | Detail |
|----------|--------|
| **Object Type** | Tables |
| **Load Strategy** | Batch Processing · Full Load · Truncate & Insert |
| **Transformations** | None — data stored as-is |
| **Data Model** | None (raw as-is) |
| **Interface** | Files in Folders (CSV) |

**Purpose:** Establish a reliable, auditable raw data foundation that can be reprocessed at any time without returning to the source systems.

---

### 🥈 Silver Layer — Cleansed & Standardized Data

The Silver layer is the **transformation zone**, where raw Bronze data is cleaned, standardized, and integrated. This layer makes data trustworthy and consistent, ready for analytical modeling downstream.

| Property | Detail |
|----------|--------|
| **Object Type** | Tables |
| **Load Strategy** | Batch Processing · Full Load · Truncate & Insert |
| **Transformations** | Data Cleansing · Data Standardization · Data Normalization · Derived Columns · Data Enrichment |
| **Data Model** | None (as-is from Bronze, post-transformation) |

**Transformation details:**
- **Data Cleansing**: Remove nulls, fix broken records, and deduplicate rows.
- **Data Standardization**: Normalize date formats, casing conventions, and code values across both ERP and CRM sources.
- **Data Normalization**: Restructure data to eliminate redundancy and improve consistency.
- **Derived Columns**: Compute new fields from existing data (e.g., full name, age buckets, revenue categories).
- **Data Enrichment**: Augment records with additional context to improve analytical value.

---

### 🥇 Gold Layer — Business-Ready Data

The Gold layer is the **consumption zone**, where data is shaped into a business-friendly star schema optimized for reporting, dashboards, and advanced analytics. This layer exposes only curated, validated, business-ready views.

| Property | Detail |
|----------|--------|
| **Object Type** | Views |
| **Load Strategy** | No load — reads directly from Silver layer |
| **Transformations** | Data Integrations · Aggregations · Business Logic |
| **Data Model** | Star Schema · Flat Tables · Aggregated Tables |

**Data model types:**
- **Star Schema**: Central fact tables joined to dimension tables (e.g., `fact_sales`, `dim_customer`, `dim_product`), enabling fast, intuitive analytical queries.
- **Flat Tables**: Denormalized wide tables for self-service BI tools and simplified reporting.
- **Aggregated Tables**: Pre-computed summaries (e.g., monthly revenue, top customers) for performance-critical dashboards.

---

## 🔄 ETL Pipeline

The ETL pipeline moves data progressively through each Medallion layer using SQL Server Stored Procedures, ensuring modularity, reusability, and maintainability.

### Pipeline Overview

```
[Source CSV Files]
       │
       ▼
  ┌─────────────────────────────────────────┐
  │  EXTRACT                                │
  │  Bulk insert CSV files into Bronze      │
  │  tables via stored procedures           │
  └──────────────────┬──────────────────────┘
                     │
                     ▼
  ┌─────────────────────────────────────────┐
  │  TRANSFORM                              │
  │  Apply cleansing, standardization,      │
  │  normalization, and enrichment rules    │
  │  to populate Silver layer tables        │
  └──────────────────┬──────────────────────┘
                     │
                     ▼
  ┌─────────────────────────────────────────┐
  │  LOAD                                   │
  │  Integrate Silver data into Gold layer  │
  │  views using business logic and star    │
  │  schema modeling                        │
  └─────────────────────────────────────────┘
```

### Extract — Bronze Layer

- **Source**: ERP and CRM systems provided as flat CSV files.
- **Method**: Bulk insert via SQL Server stored procedures.
- **Strategy**: Full Load with Truncate & Insert — tables are cleared and fully reloaded on each pipeline run.
- **Goal**: Capture a complete, unmodified snapshot of source data to ensure full reprocessability.

### Transform — Silver Layer

Data cleansing and transformation rules are applied via stored procedures before writing to Silver tables:

- Resolve missing or null values using defined business rules (e.g., substitute defaults, flag anomalies for review).
- Standardize codes and labels across ERP and CRM (e.g., unified country names, consistent product category IDs).
- Parse and reformat all date fields into a consistent `YYYY-MM-DD` standard.
- Generate derived columns such as calculated customer age, tenure, and product margin bands.
- Enrich records by joining to reference tables (e.g., region mapping, product classifications).

### Load — Gold Layer

- **Method**: SQL Views — no physical data movement occurs; the Gold layer reads directly from Silver in real time.
- **Modeling**: Data is organized into a star schema with clearly separated fact and dimension tables to support efficient slicing and aggregation.
- **Business Logic**: KPI computations, aggregations, and business-specific filtering rules are applied at the view level, keeping them centralized and auditable.

---

## 📊 Analytics & Reporting

SQL-based analytics are built on top of the Gold layer and organized across three core reporting domains:

### 👥 Customer Behavior
- Customer segmentation by purchase frequency, recency, and total lifetime spend (RFM analysis).
- Identification of high-value customers and early signals of churn risk.
- Regional and demographic breakdown of purchasing patterns.

### 📦 Product Performance
- Revenue contribution ranked by product and product category.
- Top and bottom performing SKUs across configurable time periods.
- Inventory turnover rates and sales velocity metrics.
- Category mix analysis to inform assortment strategy.

### 📈 Sales Trends
- Month-over-month and year-over-year revenue growth comparisons.
- Identification of seasonal sales patterns and peak demand periods.
- Sales performance breakdowns by region, channel, and sales representative.
- Variance analysis between targets and actuals where applicable.

All reports are designed to be reproducible, auditable, and directly consumable via SSMS or connected BI tools.

---

## 🚀 Getting Started

1. **Clone this repository**
   ```bash
   git clone https://github.com/your-username/sql-data-warehouse-project.git
   ```

2. **Install the required tools**
   - [SQL Server Express](#)
   - [SQL Server Management Studio (SSMS)](#)

3. **Set up the database**
   - Open SSMS and connect to your local SQL Server instance.
   - Run the database initialization script located in `scripts/init_database.sql`.

4. **Load source data into the Bronze layer**
   - Place the CSV source files in the `datasets/` folder.
   - Execute the Bronze layer stored procedures to bulk-load the raw data.

5. **Run the Silver layer transformations**
   - Execute the Silver layer stored procedures to cleanse and standardize the data.

6. **Explore the Gold layer and analytics**
   - Query the Gold layer views to access business-ready data.
   - Open the analytics scripts in the `analytics/` folder to run reporting queries.

---

## 📁 Repository Structure

```
sql-data-warehouse-project/
│
├── datasets/               # Source CSV files (ERP & CRM)
├── docs/                   # Project documentation and architecture diagrams
│   ├── requirements.md
│   └── data_flow.pdf
├── scripts/
│   ├── bronze/             # Stored procedures for raw data ingestion
│   ├── silver/             # Stored procedures for cleansing & transformation
│   └── gold/               # Views for business-ready data
├── analytics/              # SQL reporting and analytics queries
└── README.md
```

---

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
