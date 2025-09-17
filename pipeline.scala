// ✅ Features Demonstrated
// 
// Structured logging: logs counts and key steps.
// Reading CSV files: scalable readCSV helper.
// Data cleaning: null handling, default values, deduplication, filtering.
// Transformations: derived columns, UDFs for categorization, date extraction.
// Joins: broadcast join for small lookup table (customersDF).
// Partitioned write: parquet by category for query performance.
// Error handling & session management: try-catch-finally ensures Spark stops cleanly.

// ⚡ Notes & Best Practices
// 
// Use explicit schemas for large datasets instead of inferSchema.
// Avoid .count() in production if dataset is huge — use sample or monitoring metrics.
// Use broadcast joins only when one dataset is small (<500 MB).
// Partition output by logical keys (category, year, month) to speed up downstream analytics.
// Keep transformations modular — easy to test cleanTransactions, cleanCustomers, and addDerivedColumns separately.

// file: src/main/scala/com/example/Pipeline.scala
package com.example

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

object Pipeline {

  def main(args: Array[String]): Unit = {
    // ------------------------
    // 1. Setup logging and SparkSession
    // ------------------------
    Logger.getLogger("org").setLevel(Level.WARN)
    val logger = Logger.getLogger(getClass.getName)

    val spark = SparkSession.builder()
      .appName("Complete ETL Pipeline")
      .config("spark.sql.shuffle.partitions", "200")
      .getOrCreate()

    try {
      // ------------------------
      // 2. Read input datasets
      // ------------------------
      val transactionsPath = if (args.length > 0) args(0) else "s3://bucket/transactions/"
      val customersPath    = if (args.length > 1) args(1) else "s3://bucket/customers/"

      val transactionsDF = readCSV(spark, transactionsPath)
      val customersDF    = readCSV(spark, customersPath)
      logger.info(s"Transactions count: ${transactionsDF.count()}")
      logger.info(s"Customers count: ${customersDF.count()}")

      // ------------------------
      // 3. Clean and standardize
      // ------------------------
      val cleanedTransactions = cleanTransactions(transactionsDF)
      val cleanedCustomers    = cleanCustomers(customersDF)

      // ------------------------
      // 4. Transform / enrich
      // ------------------------
      val transformedTransactions = addDerivedColumns(cleanedTransactions)

      // ------------------------
      // 5. Join datasets
      // ------------------------
      val enrichedDF = transformedTransactions
        .join(broadcast(cleanedCustomers), Seq("customer_id"), "left")
        .withColumn("full_name", concat_ws(" ", $"first_name", $"last_name"))

      // ------------------------
      // 6. Write output
      // ------------------------
      val outputPath = if (args.length > 2) args(2) else "s3://bucket/output/"
      enrichedDF.write
        .mode("overwrite")
        .partitionBy("category")
        .parquet(outputPath)

      logger.info("ETL pipeline completed successfully!")

    } catch {
      case e: Exception =>
        logger.error("Pipeline failed!", e)
        throw e
    } finally {
      spark.stop()
      logger.info("Spark session stopped.")
    }
  }

  // ------------------------
  // Helper methods
  // ------------------------

  /** Read CSV with header and inferred schema */
  def readCSV(spark: SparkSession, path: String): DataFrame = {
    spark.read.option("header", "true").option("inferSchema", "true").csv(path)
  }

  /** Clean transactions: drop nulls, filter out negative amounts */
  def cleanTransactions(df: DataFrame): DataFrame = {
    df.na.drop(Seq("transaction_id", "customer_id", "amount"))
      .filter($"amount" > 0)
      .withColumn("transaction_date", to_date($"transaction_date", "yyyy-MM-dd"))
  }

  /** Clean customer data */
  def cleanCustomers(df: DataFrame): DataFrame = {
    df.na.fill(Map(
      "first_name" -> "Unknown",
      "last_name"  -> "Unknown",
      "category"   -> "Unknown"
    )).dropDuplicates("customer_id")
  }

  /** Add derived columns with transformations and UDFs */
  def addDerivedColumns(df: DataFrame): DataFrame = {
    val categorizeAmount = udf((amt: Double) => {
      if (amt > 1000) "High" else if (amt > 500) "Medium" else "Low"
    })

    df.withColumn("amount_category", categorizeAmount($"amount"))
      .withColumn("year", year($"transaction_date"))
      .withColumn("month", month($"transaction_date"))
  }
}

