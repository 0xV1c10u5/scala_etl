// Read CSV

val csvDF = spark.read
  .format("csv") // or simply .csv()
  .option("header", "true")        // first row as header
  .option("inferSchema", "true")   // auto-detect column types
  .option("delimiter", ",")        // custom delimiter if needed
  .load("s3://bucket/path/data.csv")

// Read Parquet

val parquetDF = spark.read
  .parquet("s3://bucket/path/data.parquet")

// Read JSON

val jsonDF = spark.read
  .option("multiLine", "true")   // for pretty-printed JSON
  .json("s3://bucket/path/data.json")

// Read Avro

val avroDF = spark.read
  .format("avro")
  .load("s3://bucket/path/data.avro")

// Reading with Explicit Schema
//
// Defining schema improves speed and prevents type ambiguity:

import org.apache.spark.sql.types._

val schema = StructType(Array(
  StructField("id", IntegerType, true),
  StructField("name", StringType, true),
  StructField("amount", DoubleType, true),
  StructField("date", DateType, true)
))

val df = spark.read
  .schema(schema)
  .option("header", "true")
  .csv("s3://bucket/path/data.csv")

// Read from DB

val jdbcDF = spark.read
  .format("jdbc")
  .option("url", "jdbc:postgresql://host:5432/dbname")
  .option("dbtable", "public.transactions")
  .option("user", "username")
  .option("password", "password")
  .load()
// Partitioning for large tables
  .option("partitionColumn", "id")
  .option("lowerBound", "1")
  .option("upperBound", "1000000")
  .option("numPartitions", "10")

// Read from Stream

// Kafka Example
val kafkaDF = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "broker:9092")
  .option("subscribe", "topic_name")
  .load()

