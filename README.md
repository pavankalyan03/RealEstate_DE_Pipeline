
# 🏙️ End-to-End Real Estate Data Engineering & Analytics Pipeline

A complete data engineering project that transforms raw real estate data from [Housing.com](https://housing.com) into actionable insights for Hyderabad’s property market. The solution includes web scraping, cloud-based processing, Snowflake warehousing, and Power BI dashboards — delivering real-time business intelligence to buyers, investors, and analysts.

> ⚠️ **Note:** This entire repository is designed to be executed within an **Apache Airflow environment**. Please ensure Airflow is properly installed and configured before running the pipeline.


---

## 📌 Problem Statement

The Hyderabad property market data is:
- Fragmented across platforms
- Inconsistent and messy
- Lacking locality-wise insights
- Difficult to analyze manually

### ✅ This project solves:
- Data fragmentation with unified pipelines  
- Manual analysis with automated ETL  
- Missing insights with real-time dashboards  
- Poor quality data with domain-specific cleaning  

---

## ⚙️ Architecture Overview

```text
Housing.com → Apify → Python → AWS S3 → Databricks (PySpark) → Snowflake → Power BI
```

### 💽 Medallion Architecture
- Bronze (Staging): Raw JSON in AWS S3
- Silver (Processing): Cleaned, structured data in Databricks
- Gold (Analytics): Star schema in Snowflake

