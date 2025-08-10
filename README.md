

# SQL Data Warehouse 
## 📌 Objective

The goal of this project is to develop a **modern data warehouse** using **SQL Server** to consolidate data from multiple sources (ERP & CRM), enabling **analytical reporting** and **data-driven decision-making**.

---

## 📊 Specifications

* **Data Sources**:
  Import sales and customer data from two source systems (**ERP** and **CRM**) provided as CSV files.

* **Data Quality**:
  Perform data cleansing to resolve inconsistencies and data quality issues prior to analysis.

* **Integration**:
  Combine both sources into a single, user-friendly **data model** optimized for analytical queries.

* **Scope**:
  Focus on the latest dataset (no data historization required).

* **Documentation**:
  Provide clear documentation of the data model to support both **business stakeholders** and **analytics teams**.

---

## 🛠 High-Level Architecture

### **Source Layer**

* **CRM** & **ERP** export raw CSV files.

### **Data Warehouse Layers**

#### **1. Bronze Layer (Raw Data)**

* **Object Type**: Tables
* **Load**: Batch processing, full load, truncate & insert
* **Transformations**: None (data stored as-is)

#### **2. Silver Layer (Clean & Standardized Data)**

* **Object Type**: Tables
* **Load**: Batch processing, full load, truncate & insert
* **Transformations**:

  * Data cleaning
  * Data standardization
  * Data normalization
  * Derived columns
  * Data enrichment

#### **3. Gold Layer (Business-Ready Data)**

* **Object Type**: Views
* **Load**: Not applicable (views generated dynamically)
* **Transformations**:

  * Data integration
  * Aggregations
  * Business logic application
* **Data Model**: Star schema, flat tables, aggregated tables

---

## 📈 BI & Reporting

### Objective

Develop **SQL-based analytics** to deliver detailed insights into:

* **Customer Behavior**
* **Product Performance**
* **Sales Trends**

These insights empower decision-makers with **key business metrics**, enabling **strategic planning** and **performance optimization**.

### Tools Used

* **Power BI**
* **Tableau**

---

## 📜 License

This project is licensed under the **MIT License**. You are free to use, modify, and share this project with proper attribution.

---

## 🚀 How to Use

1. **Load the Source Data**

   * Place ERP and CRM CSV files in the designated input folder.
   * Execute the **ETL scripts** to load into the Bronze Layer.

2. **Run Transformations**

   * Use SQL scripts to cleanse, standardize, and integrate data into the Silver & Gold layers.

3. **Connect BI Tools**

   * Link Power BI or Tableau to the Gold Layer views for interactive dashboards and reports.

---

## 👤 About Me

**Kareem Younis** — Data Analyst and recent **Computer Science** graduate.
Passionate about transforming raw data into actionable insights through modern analytics and BI tools.

📞 **Phone**: +20 115 883 8504
📧 **Email**: [kareemyounis03@gmail.com](mailto:kareemyounis03@gmail.com)




