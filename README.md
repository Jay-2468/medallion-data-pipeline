# 🛒 E-Commerce Medallion Data Pipeline with PySpark & Databricks

An end-to-end **e-commerce data engineering pipeline** built using **PySpark and Databricks**, following the **Medallion Architecture**.

The project takes raw order data from a CSV source, stores it in a Bronze layer, cleans and enriches it in the Silver layer, and produces multiple business-oriented analytical datasets in the Gold layer.

---

## 📌 Project Overview

This project was built to practice the practical implementation of a **Medallion Architecture data pipeline** using PySpark in Databricks.

The pipeline consists of three layers:

```text
Raw CSV Data
     │
     ▼
┌─────────────┐
│   BRONZE    │
│ Raw Orders  │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   SILVER    │
│ Cleaned &   │
│  Enriched   │
│    Orders   │
└──────┬──────┘
       │
       ▼
┌───────────────────────────────────────┐
│                 GOLD                  │
│ Customer • Location • Product •       │
│ Category • Sales • Payment • Status   │
│ Top Customers • Segmentation • Daily │
└───────────────────────────────────────┘
```

The main focus was on learning how raw data can be progressively transformed into **clean, validated, enriched, and business-ready datasets**.

---

# 🏗️ Medallion Architecture

## 🥉 Bronze Layer — Raw Data

The Bronze layer contains the raw data ingested from the source CSV.

### Source

```text
orders.csv
```

### Bronze Table

```text
bronze.orders
```

The raw CSV is loaded using PySpark and stored as a **Delta table**.

```python
raw_df = spark.read \
    .option("header", True) \
    .csv("/path/to/orders.csv")

raw_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze.orders")
```

### Bronze Layer Purpose

* Ingest raw source data
* Preserve the source structure
* Store the data in Delta format
* Provide the input for Silver-layer processing

Minimal transformation is intentionally performed at this stage.

---

# 🥈 Silver Layer — Cleaning & Enrichment

The Silver layer performs data cleaning, validation, standardization, and enrichment.

The following transformations were implemented.

## 1. Data Type Conversion

Raw string columns were converted into appropriate data types.

| Column       | Transformation |
| ------------ | -------------- |
| `quantity`   | Integer        |
| `unit_price` | Integer        |
| `discount`   | Decimal        |
| `order_date` | Date           |

PySpark functions used:

```text
cast()
to_date()
withColumns()
```

---

## 2. Duplicate Removal

Duplicate records were removed using `order_id` as the duplicate key.

```python
df_silver = df_silver.dropDuplicates(["order_id"])
```

This prevents duplicate orders from being propagated to downstream analytical datasets.

---

## 3. Data Validation

Validation rules were implemented for important order fields.

```text
quantity <= 0
    → Invalid quantity

unit_price < 0
    → Invalid unit price

discount outside 0–1
    → Invalid discount

otherwise
    → Valid
```

A new column was created:

```text
validation_status
```

Possible values include:

```text
Valid
Invalid quantity
Invalid unit price
Invalid discount
```

PySpark conditional expressions were used to implement these rules.

---

## 4. Gross Amount Calculation

A `gross_amount` column was created:

```text
gross_amount = quantity × unit_price
```

This represents the order value before applying discounts.

---

## 5. Discount Amount Calculation

A `discount_amount` column was created:

```text
discount_amount = gross_amount × discount
```

---

## 6. Net Amount Calculation

The final order value after applying the discount was calculated as:

```text
net_amount = gross_amount - discount_amount
```

The resulting Silver data therefore contains both the original order information and derived financial metrics.

---

## 7. Location Standardization

Location values were standardized using:

```python
F.initcap(F.trim(F.col("location")))
```

This removes unnecessary whitespace and standardizes the capitalization of location values.

For example:

```text
" Bangalore "
```

becomes:

```text
"Bangalore"
```

---

## 8. Completed Order Flag

A Boolean column called:

```text
is_completed
```

was created.

The logic is:

```text
status = Completed
        → True

Other statuses
        → False
```

---

## 9. Order Size Classification

Orders were categorized according to `net_amount`.

```text
net_amount < 2,000
    → Small

2,000–10,000
    → Medium

> 10,000
    → Large
```

This creates an additional business-oriented classification for each order.

---

## 10. Null Handling

The pipeline checks for missing values using:

```text
isNull()
isNotNull()
```

Missing values were then handled using `fillna()`.

The following default values were applied:

| Column          | Replacement |
| --------------- | ----------- |
| `customer_name` | `Unknown`   |
| `quantity`      | `0`         |
| `gross_amount`  | `0`         |

---

## Silver Table

The transformed data is stored as a Delta table.

Current notebook table name:

```text
silver.customers
```

> Note: Although the table is currently named `silver.customers`, the data represents order-level records. Renaming it to `silver.orders` would make the project structure more descriptive before publishing the repository.

---

# 🥇 Gold Layer — Business Analytics

The Gold layer contains aggregated datasets designed for analytical use cases.

The project creates **11 Gold tables**.

---

## 1. Customer Summary

### Table

```text
gold.customer_summary
```

Provides customer-level spending information.

Metrics include:

* Customer ID
* Customer name
* Total quantity
* Total spending
* Average order value

The aggregation uses:

```text
groupBy()
sum()
avg()
```

---

## 2. Location Summary

### Table

```text
gold.location_summary
```

Provides location-level sales information.

Metrics include:

* Location
* Total quantity
* Total revenue
* Average order value

---

## 3. Product Performance

### Table

```text
gold.product_performance
```

Provides product-level performance metrics.

Metrics include:

* Product
* Total quantity
* Total revenue
* Average order value

This can be used to identify products generating higher sales volumes and revenue.

---

## 4. Category Performance

### Table

```text
gold.category_performance
```

Provides aggregated performance by product category.

Metrics include:

* Category
* Total quantity
* Total revenue
* Average order value

---

## 5. Yearly Sales

### Table

```text
gold.yearly_sales
```

Provides yearly sales analysis based on `order_date`.

Metrics include:

* Year
* Total revenue
* Average order sales

The PySpark `year()` function is used to derive the year from the order date.

---

## 6. Monthly Sales

### Table

```text
gold.monthly_sales
```

Provides monthly sales information.

Metrics include:

* Month
* Total revenue
* Average order sales

The month name is extracted using PySpark's `monthname()` function.

---

## 7. Payment Analysis

### Table

```text
gold.payment_analysis
```

Analyzes transactions based on payment method.

Metrics include:

* Payment method
* Transaction count
* Total revenue

The results are ordered by transaction count in descending order.

---

## 8. Order Status Summary

### Table

```text
gold.order_status_summary
```

Provides an overview of different order statuses.

Metrics include:

* Order status
* Order count
* Total revenue

This allows the dataset to be analyzed across statuses such as:

```text
Completed
Cancelled
Returned
```

---

## 9. Top Customers

### Table

```text
gold.top_customers
```

Identifies the top five customers based on total spending.

Metrics include:

* Customer ID
* Total quantity
* Total spending
* Average spending
* Last order date

The results are sorted by total spending in descending order and limited to the top five customers.

---

## 10. Customer Segmentation

### Table

```text
gold.customer_segmentation
```

Customers are segmented based on their total spending.

```text
Total spending < 5,000
    → Low Value

5,000–20,000
    → Medium Value

> 20,000
    → High Value
```

The resulting dataset contains:

```text
customer_id
customer_name
total_spent
customer_segment
```

---

## 11. Daily Sales

### Table

```text
gold.daily_sales
```

Provides daily sales metrics based on `order_date`.

Metrics include:

* Order date
* Total quantity
* Daily revenue
* Average daily revenue

Null aggregation results are handled using `coalesce()`.

---

# 🧰 Technologies Used

| Technology                    | Purpose                                        |
| ----------------------------- | ---------------------------------------------- |
| **Python**                    | Pipeline development                           |
| **PySpark**                   | Data ingestion, transformation and aggregation |
| **Apache Spark**              | Distributed data processing                    |
| **Databricks**                | Development and execution environment          |
| **Delta Lake / Delta Tables** | Storage for Bronze, Silver and Gold datasets   |

---

# 🔧 PySpark Concepts Practiced

### Data Ingestion

```text
spark.read
option()
csv()
```

### Data Transformation

```text
withColumn()
withColumns()
cast()
to_date()
alias()
```

### Data Cleaning

```text
dropDuplicates()
fillna()
isNull()
isNotNull()
trim()
initcap()
```

### Conditional Logic

```text
when()
otherwise()
between()
```

### Aggregation

```text
groupBy()
sum()
avg()
count()
max()
```

### Date Functions

```text
year()
monthname()
```

### Sorting & Limiting

```text
orderBy()
limit()
```

### Null-Safe Aggregation

```text
coalesce()
```

### Delta Table Operations

```text
format("delta")
saveAsTable()
spark.table()
```

---

# 📊 Gold Layer Overview

| Gold Table                   | Purpose                       |
| ---------------------------- | ----------------------------- |
| `gold.customer_summary`      | Customer spending analysis    |
| `gold.location_summary`      | Location-level sales analysis |
| `gold.product_performance`   | Product performance           |
| `gold.category_performance`  | Category performance          |
| `gold.yearly_sales`          | Yearly revenue analysis       |
| `gold.monthly_sales`         | Monthly revenue analysis      |
| `gold.payment_analysis`      | Payment method analysis       |
| `gold.order_status_summary`  | Order status analysis         |
| `gold.top_customers`         | Top five customers            |
| `gold.customer_segmentation` | Customer value segmentation   |
| `gold.daily_sales`           | Daily sales analysis          |

---

# 🔄 End-to-End Pipeline

<img width="1536" height="1024" alt="Medallion Data Pipeline Architecture" src="https://github.com/user-attachments/assets/f9afcc4a-182f-4907-8bcb-94716dc9bb51" />


---

# 📁 Repository Structure

A simple repository structure for this project:

```text
medallion-data-pipeline/
│
├── data/
│   └── orders.csv
│
├── notebooks/
│   └── Medallion_Data_Pipeline.ipynb
│
└── README.md
```

Adjust the structure above to match the actual files you decide to upload.

---

# 🎯 Project Objectives

The project was developed to gain practical experience with:

* Designing a Medallion Architecture
* Ingesting CSV data with PySpark
* Storing data as Delta tables
* Cleaning raw datasets
* Converting data types
* Removing duplicate records
* Implementing data validation rules
* Handling null values
* Creating derived columns
* Standardizing string values
* Performing aggregations
* Creating business-oriented analytical datasets
* Working with Databricks and PySpark

---

# 📚 Key Learnings

This project provided practical experience in moving data through a multi-layer data pipeline:

```text
Raw Data
   ↓
Data Cleaning
   ↓
Data Validation
   ↓
Data Enrichment
   ↓
Aggregation
   ↓
Business Analytics
```

It also helped develop familiarity with the role of the Bronze, Silver, and Gold layers and how each layer serves a different purpose within a data engineering pipeline.

---

# 🚀 Future Improvements

The current project focuses on batch processing and core PySpark transformations.

Potential future improvements include:

* Implementing incremental data ingestion
* Using Delta Lake `MERGE` for upserts
* Adding PySpark window functions
* Adding joins between multiple datasets
* Implementing data-quality checks
* Adding schema enforcement
* Adding pipeline logging
* Parameterizing input and output paths
* Implementing Databricks Workflows
* Adding automated pipeline execution
* Implementing partitioning and performance optimization

---

# 👨‍💻 Author

**Jay Falak**

Bachelor's Degree in Information Technology

### Areas of Interest

* Data Engineering
* SQL
* PySpark
* Databricks
* ETL / ELT
* Big Data Technologies
* Cloud Data Engineering
