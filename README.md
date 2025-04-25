# Sql-Data-Warehouse-Project
Building a modern data warehouse with SQL Server, including ETL processes, data modeling and analytics
![High Level Architecture](https://github.com/user-attachments/assets/700f703d-f850-4652-9d57-61a0ec69b81f)

1. Bronze Layer: Stores raw data as-is from the source systems. Data is ingested from CSV Files into a SQL Server Database.
2. Silver Layer: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. Gold Layer: Houses business-ready data modeled into a star schema required for reporting and analytics.

Project Overview
This project involves:

1. Data Architecture: Designing a Modern Data Warehouse Using Medallion Architecture Bronze, Silver, and Gold layers.
2. ETL Pipelines: Extracting, transforming, and loading data from source systems into the warehouse.
3. Data Modeling: Developing fact and dimension tables optimized for analytical queries.
4. Analytics & Reporting: Creating SQL-based reports and dashboards for actionable insights.


🚀 Project Requirements

Building the Data Warehouse (Data Engineering)
Objective
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

Specifications
1. Data Sources: Import data from two source systems (ERP and CRM) provided as CSV files.
2. Data Quality: Cleanse and resolve data quality issues before analysis.
3. Integration: Combine both sources into a single, user-friendly data model designed for analytical queries.
4. Scope: Focus on the latest dataset only; historization of data is not required.
5. Documentation: Provide clear documentation of the data model to support both business stakeholders and analytics teams.
