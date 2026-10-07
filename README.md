# Day 22 — E-commerce Daily Sales Batch Pipeline

An end-to-end Apache Spark batch processing project that processes daily e-commerce transactions, cleans invalid records, enriches transactions with customer and product information, calculates revenue, aggregates sales, and writes partitioned Parquet output.

## Project Objective

This project demonstrates a complete batch ETL pipeline using Apache Spark.

The pipeline:

1. Reads raw transaction data.
2. Validates and cleans invalid records.
3. Joins transactions with customer data.
4. Joins transactions with product data.
5. Calculates transaction revenue.
6. Aggregates daily sales by product category.
7. Writes partitioned Parquet output.
8. Stores rejected transactions separately.

## Architecture

```text
Raw CSV Files
     |
     v
Spark DataFrames
     |
     v
Data Validation
     |
     +----> Rejected Transactions
     |
     v
Customer Join
     |
     v
Product Join
     |
     v
Revenue Calculation
     |
     v
Daily Category Aggregation
     |
     v
Partitioned Parquet
Technologies
Scala 2.12.18
Apache Spark 3.5.1
Spark SQL
sbt
CSV
Parquet
WSL2 / Linux
Input Data
Customers

data/customers.csv

Contains:

customer_id
customer_name
city
Products

data/products.csv

Contains:

product_id
product_name
category
price
Transactions

data/transactions.csv

Contains:

transaction_id
transaction_date
customer_id
product_id
quantity
Data Quality

The pipeline rejects transactions with:

Missing transaction ID
Missing transaction date
Missing customer ID
Missing product ID
Missing quantity
Quantity less than or equal to zero

The pipeline also uses inner joins to remove transactions referencing customers or products that do not exist in the master data.

Revenue Calculation
revenue = quantity × product price
Aggregation

Sales are aggregated by:

transaction date
product category

The following metrics are calculated:

total transactions
total units sold
total revenue
Output
Daily Sales

The final sales data is written as partitioned Parquet:

output/daily_sales/
└── transaction_date=2026-10-07/
    └── part-*.snappy.parquet

The output is partitioned by transaction_date.

Rejected Transactions

Rejected records are written separately:

output/rejected_transactions/
└── part-*.snappy.parquet
Sample Results

The sample dataset contains 15 raw transactions.

Raw transactions: 15
Valid transactions: 14
Rejected transactions: 1
After customer join: 13
After product join: 12
Daily Sales Summary
Date	Category	Transactions	Units Sold	Revenue
2026-10-07	Accessories	5	14	14,700
2026-10-07	Electronics	7	8	256,500
Total Revenue
271,200
Spark Concepts Demonstrated
SparkSession
Explicit schemas
DataFrame API
CSV ingestion
Data validation
Filtering
Inner joins
withColumn
groupBy
Aggregations
countDistinct
sum
round
Parquet
Partitioned output
Batch ETL processing
Project Structure
Day22-Batch-Mini-Project/
├── build.sbt
├── .gitignore
├── README.md
├── project/
│   └── build.properties
├── data/
│   ├── customers.csv
│   ├── products.csv
│   └── transactions.csv
└── src/
    └── main/
        └── scala/
            └── com/
                └── day22batch/
                    └── EcommerceBatchApp.scala
Execution
sbt clean
sbt run
Conclusion

This project demonstrates an end-to-end batch ETL workflow using Apache Spark, from raw e-commerce transaction ingestion through data quality processing, customer and product enrichment, revenue calculation, aggregation, and partitioned Parquet storage.
