
# 🏙️ End-to-End Real Estate Data Engineering & Analytics Pipeline

A complete data engineering project that transforms raw real estate data from [Housing.com](https://housing.com) into actionable insights for Hyderabad’s property market. The solution includes web scraping, cloud-based processing, Snowflake warehousing, and Power BI dashboards — delivering real-time business intelligence to buyers, investors, and analysts.

> ⚠️ **Note:** This entire repository is designed to be executed within an **Apache Airflow environment**.
>  - 🔹 The **input data is not uploaded** here due to large file size. 
>  - 🔹 You must **create your own connections to Snowflake and Databricks** in the Airflow UI or via environment variables.
>  - 🔹 This project assumes that Airflow is set up using the **Astro CLI** or any standard Airflow installation.

>  📚 Learn and install Airflow with Astro CLI:
>  👉 [Astro CLI Documentation](https://docs.astronomer.io/astro/cli/install-cli)



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

