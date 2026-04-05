# Data Warehouse and Analytics Project

Welcome to the **Data Warehouse and Analytics Project** repository! 

This project demonstrates a comprehensive data warehousing and analytics solution - from building the warehouse infrastructure to generating actionable business insights. Designed as a portfolio project, it reflects industry best practices in data engineering and analytics.

---

## 📋 Project Overview

This project covers the following areas:

1. **Data Architecture**: Designing a modern data warehouse using the Medallion Architecture, structured across **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Building fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Writing SQL-based reports and dashboards to surface actionable insights.

> This repository is a great resource for anyone looking to build or showcase expertise in:
> - SQL Development
> - Data Architecture
> - Data Engineering
> - ETL Pipeline Development
> - Data Modeling
> - Data Analytics

---

## 🔧 Important Links & Tools

All tools listed below are free to use:

- **[Datasets](#)**: Access the project's source data (CSV files).
- **[SQL Server Express](#)**: A lightweight server for hosting your SQL database.
- **[SQL Server Management Studio (SSMS)](#)**: A GUI for managing and interacting with databases.
- **[Git & GitHub](#)**: Set up version control to manage and collaborate on your code efficiently.
- **[DrawIO](#)**: Design data architecture diagrams, models, and flows.
- **[Notion](#)**: An all-in-one tool for project management and organization.
- **[Project Steps](#)**: Access all project phases and tasks.

---

## 📐 Project Requirements

### Building the Data Warehouse (Data Engineering)

**Objective**

Build a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

**Specifications**

- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Identify and resolve data quality issues before analysis.
- **Integration**: Combine both sources into a single, user-friendly data model tailored for analytical queries.
- **Scope**: Focus on the current dataset only — historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

---

### BI: Analytics & Reporting (Data Analytics)

**Objective**

Develop SQL-based analytics to deliver detailed insights into:

- Customer Behavior
- Product Performance
- Sales Trends

These insights equip stakeholders with key business metrics, supporting strategic decision-making.

For more details, refer to `docs/requirements.md`.

---

## 🏗️ Data Architecture

The data architecture follows the **Medallion Architecture** with three layers: **Bronze**, **Silver**, and **Gold**.

```
Sources  →  [Bronze Layer]  →  [Silver Layer]  →  [Gold Layer]  →  Consume
CRM/ERP      Raw Data          Cleaned Data       Business-Ready    BI & Reporting
             (Tables)          (Tables)           Data (Views)      Ad-Hoc Queries
                                                                    Machine Learning
```

### Layer Breakdown

1. **Bronze Layer**
   - Stores raw data as-is from the source systems.
   - Data is ingested from CSV files directly into SQL Server.
   - Object Type: Tables
   - Load: Batch Processing, Full Load, Truncate & Insert
   - No transformations applied.

2. **Silver Layer**
   - Applies data cleansing, standardization, and normalization to prepare data for analysis.
   - Object Type: Tables
   - Load: Batch Processing, Full Load, Truncate & Insert
   - Transformations: Data Cleansing, Data Standardization, Data Normalization, Derived Columns, Data Enrichment.

3. **Gold Layer**
   - Houses business-ready data modeled into a star schema for reporting and analytics.
   - Object Type: Views
   - No Load (reads from Silver)
   - Transformations: Data Integrations, Aggregations, Business Logic
   - Data Model: Star Schema, Flat Tables, Aggregated Tables

---

## 🚀 Getting Started

1. Clone this repository.
2. Install SQL Server Express and SQL Server Management Studio.
3. Load the provided CSV datasets into the Bronze layer.
4. Run the ETL pipeline scripts to populate Silver and Gold layers.
5. Explore the analytics reports in the `analytics/` folder.

---

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
## About me

This project was developed by Sabrina, a data professional holding a Master's degree in Big Data and Data Science, passionate about transforming raw data into strategic insights.
