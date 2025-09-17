// ✅ Key Features in this Skeleton
//
// Logging & verbosity control
// Suppresses excessive Spark and Akka logs while keeping ETL logs visible.
// SparkSession configuration
// Example tuning: shuffle.partitions, Kryo serialization.
// Structured try-catch-finally
// Ensures Spark session stops even on failures.
// Minimal ETL flow
// Extract → Transform → Load skeleton.
// Shows readData, transformData, and writeData helper methods.
// Scalable transformations
// Uses withColumn, filter, and trim/lower for data cleaning.

// Tips for Production Spark Apps
//
// Prefer explicit schemas (StructType) over inferSchema in production for speed.
// Partition writes by logical keys (e.g., date, category) for downstream analytics.
// Avoid .count() on massive datasets unless necessary — triggers a full job.
// Use broadcast(df) for small lookup tables to avoid shuffles.
// Include metrics/logs for monitoring (row counts, runtime, partition info).

// file: src/main/scala/com/example/SparkApp.scala
package com.example

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

object SparkApp {

  def main(args: Array[String]): Unit = {
    // ------------------------
    // 1. Configure logging
    // ------------------------
    Logger.getLogger("org").setLevel(Level.WARN) // suppress verbose Spark logs
    Logger.getLogger("akka").setLevel(Level.WARN)
    val logger = Logger.getLogger(getClass.getName)
    logger.info("Starting Spark application...")

    // ------------------------
    // 2. Create SparkSession
    // ------------------------
    val spark = SparkSession.builder()
      .appName("Generic ETL SparkApp")
      .config("spark.sql.shuffle.partitions", "200") // tune as needed
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    try {
      // ------------------------
      // 3. Read input data (Extract)
      // ------------------------
      val inputPath = if (args.length > 0) args(0) else "s3://bucket/input/"
      val df = readData(spark, inputPath)
      logger.info(s"Input schema:\n${df.printSchema()}")

      // ------------------------
      // 4. Transform data
      // ------------------------
      val transformedDF = transformData(df)
      logger.info(s"Transformed data count: ${transformedDF.count()}")

      // ------------------------
      // 5. Write output (Load)
      // ------------------------
      val outputPath = if (args.length > 1) args(1) else "s3://bucket/output/"
      writeData(transformedDF, outputPath)
      logger.info("Spark ETL job completed successfully!")

    } catch {
      case e: Exception =>
        logger.error("Spark ETL job failed!", e)
        throw e
    } finally {
      // ------------------------
      // 6. Stop Spark session
      // ------------------------
      spark.stop()
      logger.info("Spark session stopped.")
    }
  }

  // ------------------------
  // Helper methods
  // ------------------------

  /** Reads a CSV as a DataFrame with inferred schema */
  def readData(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }

  /** Example transformation */
  def transformData(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    df.withColumn("name_clean", lower(trim($"name")))
      .filter($"amount".isNotNull)
  }

  /** Write output as partitioned Parquet */
  def writeData(df: DataFrame, path: String): Unit = {
    df.write
      .mode("overwrite")
      .partitionBy("category")
      .parquet(path)
  }
}

