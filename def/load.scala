// Writing to Files

// Parquet

df.write
  .mode("overwrite")  // options: overwrite, append, ignore, errorIfExists
  .parquet("s3://bucket/output/parquet/")

// CSV

df.write
  .option("header", "true")
  .mode("overwrite")
  .csv("s3://bucket/output/csv/")

// JSON

df.write
  .mode("overwrite")
  .json("s3://bucket/output/json/")

// Partitioned Output
// Improves query efficiency in Hive/Athena/Presto.

df.write
  .partitionBy("year", "month")
  .parquet("s3://bucket/output/partitioned/")

// Writing to Databases(JDBC)

// Good for structured transactional targets.
df.write
  .format("jdbc")
  .option("url", "jdbc:postgresql://host:5432/dbname")
  .option("dbtable", "public.transactions_summary")
  .option("user", "username")
  .option("password", "password")
  .mode("append")
  .save()
// For big data → use batching:
  .option("batchsize", "5000")
  .option("isolationLevel", "READ_COMMITTED")

// Writing to Hive Tables
// Makes data queryable in Spark SQL, Hive, Presto, etc.

df.write
  .mode("overwrite")
  .saveAsTable("analytics.transactions_summary")

// Writing Streams(For real-time pipelines:)
// Ensures fault tolerance with checkpoints.

df.writeStream
  .format("parquet")
  .option("path", "s3://bucket/streaming/output/")
  .option("checkpointLocation", "s3://bucket/checkpoints/etl/")
  .start()

