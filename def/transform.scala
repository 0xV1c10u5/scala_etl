// Selecting and Renaming

val selected = df
  .select($"id", $"name".alias("customer_name"))

// Creating New Columns

import org.apache.spark.sql.functions._

val enriched = df
  .withColumn("ingest_date", current_date())
  .withColumn("name_lower", lower($"name"))
  .withColumn("amount_usd", $"amount" * 1.1)

// Dropping Columns

val reduced = enriched.drop("raw_column")

// Filtering and Conditional Logic

// Simple Filter
val adults = df.filter($"age" > 21)

// SQL Style Conditions

val activeUsers = df.filter($"status" === "active" && $"last_login".isNotNull)

// Case When

val flagged = df.withColumn(
  "risk_level",
  when($"amount" > 1000, "HIGH")
    .when($"amount" > 500, "MEDIUM")
    .otherwise("LOW")
)

// Aggregations and Grouping

val summary = df
  .groupBy("category")
  .agg(
    count("*").as("total"),
    avg("amount").as("avg_amount"),
    max("amount").as("max_amount")
  )

// Sorting and Window Functions

// Sorting
val sorted = df.orderBy($"amount".desc)

// Window
import org.apache.spark.sql.expressions.Window

val windowSpec = Window.partitionBy("category").orderBy($"amount".desc)

val ranked = df.withColumn("rank", row_number().over(windowSpec))

// Joins
//
// Supported join types: inner, left, right, outer, semi, anti.

val joined = df1.join(df2, Seq("id"), "inner")

// Flattening Nested Structures

// JSON Nested Structures
val flattened = df
  .select($"id", $"details.address.city", $"details.phone")

// Arrays
val exploded = df.withColumn("item", explode($"items"))


