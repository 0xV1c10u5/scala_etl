// User Defined Functions
//
// Custom logic you can plug into DataFrame transformations

// Creating a Simple UDF
//
// Useful for custom string cleaning, categorization, or parsing.

import org.apache.spark.sql.functions.udf

val normalizeString = udf((s: String) =>
  if (s != null) s.trim.toLowerCase else null
)

val dfWithUdf = df.withColumn("normalized_name", normalizeString($"name"))

// Registering a UDF for SQL
//
// Lets you reuse the UDF directly in Spark SQL queries.

spark.udf.register("normalizeString", (s: String) =>
  if (s != null) s.trim.toLowerCase else null
)

df.createOrReplaceTempView("people")

val result = spark.sql("""
  SELECT id, normalizeString(name) as clean_name FROM people
""")

// Typed UDFs with Encoders
//
// If you’re working with strongly typed datasets:

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val isAdult: UserDefinedFunction = udf((age: Int) => age >= 18)

val dfWithFlag = df.withColumn("is_adult", isAdult($"age"))

