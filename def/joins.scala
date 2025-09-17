// Basic Inner Join
//
// Keeps only rows with matching id in both DataFrames.
// Watch for duplicate column names → Spark auto-renames with suffixes (_1, _2).

val joined = df1.join(df2, Seq("id"), "inner")

// Other Join Types

val leftJoined  = df1.join(df2, Seq("id"), "left")
val rightJoined = df1.join(df2, Seq("id"), "right")
val fullJoined  = df1.join(df2, Seq("id"), "outer")

val semiJoined  = df1.join(df2, Seq("id"), "left_semi")   // keep only df1 rows with match
val antiJoined  = df1.join(df2, Seq("id"), "left_anti")   // keep only df1 rows with no match

// Joining with Multiple Keys

val multiKeyJoin = df1.join(
  df2,
  df1("id") === df2("cust_id") && df1("date") === df2("order_date"),
  "inner"
)

// Avoiding Duplicate Columns
//
// Explicitly selecting prevents messy duplicate column names.

val cleanJoin = df1.alias("a")
  .join(df2.alias("b"), $"a.id" === $"b.id")
  .select($"a.id", $"a.name", $"b.amount")

// Broadcasting Small Tables
//
// When one table is small, use a broadcast join to avoid costly shuffles:
// Essential optimization for large–small table joins (e.g., lookup tables).

import org.apache.spark.sql.functions.broadcast

val joined = dfLarge.join(broadcast(dfSmall), Seq("id"))

// Self Joins
//
// Useful for hierarchical data (employees ↔ managers).

val selfJoined = df.as("a")
  .join(df.as("b"), $"a.manager_id" === $"b.id")
  .select($"a.id", $"a.name", $"b.name".as("manager_name"))


