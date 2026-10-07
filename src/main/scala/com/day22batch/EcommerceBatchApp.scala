package com.day22batch

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object EcommerceBatchApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Ecommerce Daily Sales Batch Pipeline")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val basePath = "."

    val transactionsPath = s"$basePath/data/transactions.csv"
    val customersPath = s"$basePath/data/customers.csv"
    val productsPath = s"$basePath/data/products.csv"

    val outputPath = s"$basePath/output/daily_sales"
    val rejectedPath = s"$basePath/output/rejected_transactions"

    println("==========================================")
    println(" E-COMMERCE DAILY SALES BATCH PIPELINE")
    println("==========================================")

    // ------------------------------------------------
    // 1. Define schemas
    // ------------------------------------------------

    val transactionSchema = StructType(Seq(
      StructField("transaction_id", StringType, false),
      StructField("transaction_date", StringType, false),
      StructField("customer_id", StringType, false),
      StructField("product_id", StringType, false),
      StructField("quantity", IntegerType, false)
    ))

    val customerSchema = StructType(Seq(
      StructField("customer_id", StringType, false),
      StructField("customer_name", StringType, false),
      StructField("city", StringType, false)
    ))

    val productSchema = StructType(Seq(
      StructField("product_id", StringType, false),
      StructField("product_name", StringType, false),
      StructField("category", StringType, false),
      StructField("price", DoubleType, false)
    ))

    // ------------------------------------------------
    // 2. Read raw transactions
    // ------------------------------------------------

    val transactions = spark.read
      .option("header", "true")
      .schema(transactionSchema)
      .csv(transactionsPath)

    val customers = spark.read
      .option("header", "true")
      .schema(customerSchema)
      .csv(customersPath)

    val products = spark.read
      .option("header", "true")
      .schema(productSchema)
      .csv(productsPath)

    println()
    println(s"Raw transactions: ${transactions.count()}")

    // ------------------------------------------------
    // 3. Clean invalid transactions
    // ------------------------------------------------

    val validTransactions = transactions
      .filter(
        col("transaction_id").isNotNull &&
        col("transaction_date").isNotNull &&
        col("customer_id").isNotNull &&
        col("product_id").isNotNull &&
        col("quantity").isNotNull &&
        col("quantity") > 0
      )

    val rejectedTransactions = transactions
      .filter(
        col("transaction_id").isNull ||
        col("transaction_date").isNull ||
        col("customer_id").isNull ||
        col("product_id").isNull ||
        col("quantity").isNull ||
        col("quantity") <= 0
      )

    println(s"Valid transactions: ${validTransactions.count()}")
    println(s"Rejected transactions: ${rejectedTransactions.count()}")

    // ------------------------------------------------
    // 4. Join customers
    // ------------------------------------------------

    val customerJoined = validTransactions
      .join(customers, Seq("customer_id"), "inner")

    println(s"After customer join: ${customerJoined.count()}")

    // ------------------------------------------------
    // 5. Join products
    // ------------------------------------------------

    val enrichedTransactions = customerJoined
      .join(products, Seq("product_id"), "inner")

    println(s"After product join: ${enrichedTransactions.count()}")

    // ------------------------------------------------
    // 6. Calculate revenue
    // ------------------------------------------------

    val salesWithRevenue = enrichedTransactions
      .withColumn(
        "revenue",
        round(col("quantity") * col("price"), 2)
      )

    println()
    println("Enriched transactions:")
    salesWithRevenue.show(false)

    // ------------------------------------------------
    // 7. Aggregate daily sales
    // ------------------------------------------------

    val dailySales = salesWithRevenue
      .groupBy(
        col("transaction_date"),
        col("category")
      )
      .agg(
        countDistinct("transaction_id").alias("total_transactions"),
        sum("quantity").alias("total_units_sold"),
        round(sum("revenue"), 2).alias("total_revenue")
      )
      .orderBy(
        col("transaction_date"),
        col("category")
      )

    println()
    println("==========================================")
    println(" DAILY SALES SUMMARY")
    println("==========================================")

    dailySales.show(false)

    // ------------------------------------------------
    // 8. Write rejected transactions
    // ------------------------------------------------

    rejectedTransactions
      .write
      .mode("overwrite")
      .option("header", "true")
      .parquet(rejectedPath)

    // ------------------------------------------------
    // 9. Write partitioned Parquet output
    // ------------------------------------------------

    dailySales
      .write
      .mode("overwrite")
      .partitionBy("transaction_date")
      .parquet(outputPath)

    println()
    println("==========================================")
    println(" PIPELINE COMPLETED SUCCESSFULLY")
    println("==========================================")

    println(s"Daily sales output: $outputPath")
    println(s"Rejected records: $rejectedPath")

    spark.stop()
  }
}
