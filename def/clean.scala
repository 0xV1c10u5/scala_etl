// Dropping Nulls
//
// Useful when incomplete rows add no value.
// Be careful — you might throw away useful partial data.

// Drop rows with *any* null value
val cleaned = df.na.drop()

// Drop rows with nulls in specific columns
val cleanedSubset = df.na.drop(Seq("id", "amount"))

// Filling Nulls with Defaults
//
// Useful for categorical fields or missing numbers.

val filled = df.na.fill(Map(
  "amount" -> 0,              // numeric default
  "status" -> "Unknown"       // string default
))

// Replacing Specific Values

val replaced = df.na.replace("status", Map(
  "N/A" -> "Unknown",
  "-"   -> "Unknown"
))

// Deduplication

// Drop full duplicates
val deduped = df.dropDuplicates()

// Drop duplicates based on specific keys
val dedupedById = df.dropDuplicates("id")

// Standardizing Strings

import org.apache.spark.sql.functions._

val standardized = df
  .withColumn("name_clean", trim(lower($"name")))
  .withColumn("phone_clean", regexp_replace($"phone", "[^0-9]", ""))

// Handling Outliers
//
// Helps prevent skewed averages or exploding metrics.

val filtered = df.filter($"amount".between(0, 10000))

