# Scala ETL
Scala is exceptionally good at distributed ETL.  Aside from this use case, currently cannot think of using it regularly.  Therefore, I figured a collection of Scala snippets that are designed for ETL as they are needed would be helpful for code reuse in the future, and might help speed up development of efficient ETL pipelines in the future.

## Tips
(Edit This)
📝 Scala Tips & Tricks
1. General Language Tips

Immutable by default: Prefer val over var to avoid side effects.

Use Option instead of null: Safely handle missing values.

val maybeValue: Option[String] = Some("Hello")
maybeValue.getOrElse("Default")


Pattern matching: Powerful alternative to if-else chains.

x match {
  case 0 => "Zero"
  case 1 => "One"
  case _ => "Other"
}


For-comprehensions: Clean syntax for working with monads (Option, Future, Either).

val result = for {
  a <- Some(2)
  b <- Some(3)
} yield a + b

2. Collections

Prefer Seq, List, Vector, Map, Set — immutable collections by default.

Functional operations:

val nums = List(1, 2, 3)
val doubled = nums.map(_ * 2)
val evens = nums.filter(_ % 2 == 0)
val sum = nums.reduce(_ + _)


Chaining transformations: keeps code concise and readable.

val result = nums.filter(_ > 1).map(_ * 10).sum

3. Functions & Lambdas

Anonymous functions: _ shorthand for single param.

val squared = nums.map(_ * _)


Multiple parameters

val add = (x: Int, y: Int) => x + y
add(2, 3)


Curried functions: Useful for partial application.

def multiply(x: Int)(y: Int) = x * y
val timesTwo = multiply(2) _

4. Option / Either / Try

Safe error handling:

val safeDivide = (x: Int, y: Int) => Try(x / y)
safeDivide(4, 0) match {
  case Success(v) => println(v)
  case Failure(e) => println("Error: " + e.getMessage)
}


Chain operations with map / flatMap:

val result = Some(5).map(_ * 2).getOrElse(0)

5. String Interpolation & Formatting

Interpolated strings:

val name = "Alice"
println(s"Hello, $name")
println(f"Pi ~ ${Math.PI}%.2f")


Multiline strings:

val text = """This is
             |a multiline
             |string""".stripMargin

6. Performance & Best Practices

Use Vector over List for large random-access sequences.

Lazy evaluation with lazy val or Stream for deferred computation.

Avoid var where possible; immutable data structures are easier to reason about.

Use .view for lazy collections:

val squares = (1 to 1000000).view.map(_ * _).take(5).toList

7. Spark-Specific Tips

Prefer DataFrame API over RDDs for performance and optimization.

Use broadcast joins for small lookup tables.

import org.apache.spark.sql.functions.broadcast
dfLarge.join(broadcast(dfSmall), "id")


Cache DataFrames only if reused multiple times:

df.cache()


Avoid .count() on huge datasets unless necessary — it triggers a full computation.

8. Debugging & Logging

Use Logger instead of println for production code:

import org.apache.log4j.Logger
val logger = Logger.getLogger(getClass)
logger.info("Starting ETL job")


Spark UI is invaluable for monitoring jobs, stages, and shuffles.

9. Implicits & Type Classes

Use implicit conversions carefully for type enrichment:

implicit class StringOps(s: String) {
  def shout = s.toUpperCase + "!"
}
println("hello".shout) // HELLO!


Use implicit parameters for context or configuration injection.

10. Testing & REPL

Use ScalaTest or MUnit for unit tests.

Test pure functions first (no side effects).

Use Ammonite REPL for quick experiments:

amm> val data = Seq(1,2,3)
amm> data.map(_*2)

---
📝 Advanced Scala Tips & Tricks
1. Performance Optimizations

Use Vector or Array over List for random access
List is great for prepending (::) but slow for indexed access.

val v = Vector(1,2,3)
println(v(2)) // O(1)


Avoid unnecessary object creation
Reuse immutable objects, especially in tight loops.

Lazy evaluation
Use lazy val or .view to defer expensive computation:

val largeRange = (1 to 1000000).view.map(_ * 2).take(5).toList


Use primitive arrays for numeric-heavy loops
Reduces boxing/unboxing overhead:

val arr: Array 

2. Functional Programming Patterns

Use higher-order functions instead of loops:

val doubled = List(1,2,3).map(_ * 2)
val evens   = List(1,2,3,4).filter(_ % 2 == 0)


Monads & chaining (Option, Either, Try):

val result = for {
  a <- Some(10)
  b <- Some(5)
} yield a / b


Pattern matching with guards:

x match {
  case n if n > 0 => "Positive"
  case 0 => "Zero"
  case _ => "Negative"
}


Partial functions:

val pf: PartialFunction[Int, String] = {
  case 1 => "One"
  case 2 => "Two"
}
println(pf.isDefinedAt(1)) // true

3. Spark-Specific Advanced Tips

Avoid wide transformations if possible (groupByKey is expensive). Use reduceByKey or aggregateByKey.

Use broadcast joins for small lookup tables:

import org.apache.spark.sql.functions.broadcast
dfLarge.join(broadcast(dfSmall), "id")


Partitioning

Repartition before expensive operations like joins or aggregations.

val dfPart = df.repartition($"key")


Avoid over-partitioning: 2–4 partitions per CPU core is a good rule of thumb.

Cache DataFrames strategically:

Use .cache() for reused datasets.

Use .persist(StorageLevel.MEMORY_AND_DISK) for large datasets that may not fit in memory.

Avoid count() or collect() on large datasets in production; prefer .limit() or sampling.

Use explicit schemas when reading files — it improves read speed and prevents type ambiguity:

import org.apache.spark.sql.types._
val schema = StructType(Array(
  StructField("id", IntegerType),
  StructField("name", StringType)
))
val df = spark.read.schema(schema).csv("s3://bucket/file.csv")

4. Concurrency & Parallelism

Futures for async tasks

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{Success, Failure}

val f = Future { Thread.sleep(1000); 42 }
f.onComplete {
  case Success(v) => println(s"Got $v")
  case Failure(e) => println(s"Error: ${e.getMessage}")
}


Parallel collections (.par) for CPU-bound tasks:

val nums = (1 to 1000000).par.map(_ * 2)


Akka or ZIO for advanced concurrency, streaming, and fault tolerance.

5. Implicits & Type Classes (Advanced)

Implicits for type enrichment:

implicit class StringOps(s: String) {
  def shout: String = s.toUpperCase + "!"
}
println("hello".shout) // HELLO!


Implicit parameters for dependency injection:

def greet(name: String)(implicit greeting: String) = s"$greeting, $name"
implicit val defaultGreeting = "Hello"
println(greet("Alice")) // Hello, Alice


Type classes for generic behavior:

trait JsonSerializable[T] { def toJson(obj: T): String }
implicit val intSerializer: JsonSerializable[Int] = (x: Int) => x.toString

6. Testing & REPL Productivity

Ammonite REPL for fast experimentation:

amm> val data = List(1,2,3)
amm> data.map(_ * 2)


Property-based testing with ScalaCheck:

import org.scalacheck.Prop.forAll
forAll { (n: Int) => n + 0 == n }


Unit-test pure functions first, side-effects later.

7. Debugging & Logging

Structured logging:

import org.apache.log4j.Logger
val logger = Logger.getLogger(getClass)
logger.info("Starting ETL job")


Spark UI is your friend for monitoring DAGs, stages, and shuffle operations.

Metrics: track row counts, stage duration, and partition sizes to detect skew.

8. Miscellaneous Tricks

Multi-line strings with stripMargin:

val query =
  """SELECT *
    |FROM table
    |WHERE amount > 100
  """.stripMargin


Try not to use null — prefer Option or Either.

Use .view or .iterator for lazy collections to reduce memory pressure.

Use foldLeft and foldRight for complex reductions:

val sum = List(1,2,3).foldLeft(0)(_ + _)
---

### Extract
- Prefer Parquet/Avro/ORC over CSV/JSON for efficiency.
- Use explicit schemas when possible.
- For databases: push filters (.option("query", "...")) to reduce load.
- For S3/Cloud storage: partition your data by date/hour for faster reads.
- For streaming: handle schema evolution and late-arriving data.

### Transform
- Keep transformations stateless where possible (scales better).
- Use window functions for ranking, deduplication, or time-series.
- Minimize shuffles (joins, groupBy) by partitioning data wisely.
- Chain transformations fluently but cache intermediate results if reused.
- For readability, break complex pipelines into smaller vals.

### Cleaning
- Drop rows only when absolutely necessary (consider downstream impact).
- Standardize strings early to avoid join mismatches.
- Track cleaning decisions (audit trail, logging).
- For large datasets: prefer fill over drop (retains more data).
- Outlier handling should be domain-driven (don’t just clip blindly).

### Load
- Use Parquet/ORC/Avro for analytics, CSV/JSON only for interoperability.
- Always define write mode explicitly (overwrite, append, etc.).
- For partitioned data lakes, pick natural partitions (date/hour).
- For JDBC, avoid single-thread writes → partition + batch inserts.
- For streaming, checkpoint frequently for recovery.

### UDFs (User Defined Functions)
Sometimes built-in Spark functions (lower, trim, date_add, etc.) aren’t enough. That’s where UDFs come in: custom logic you can plug into DataFrame transformations.
- Use UDFs only as a last resort when Spark doesn’t have a built-in function.
- Keep UDF logic simple and deterministic.
- Always handle null inputs safely.
- Reuse registered UDFs across SQL/DataFrame queries.
#### Performance Considerations
- UDFs break Spark’s optimization (Catalyst can’t optimize them).
- Prefer built-in functions (regexp_replace, split, substring, when) when possible.
- For heavy usage, consider Spark SQL functions in Scala (functions API) instead of UDFs.
- If you need high performance, use Spark SQL native functions or pandas UDFs (PySpark side). In Scala, explore Dataset map/flatMap for efficiency.

### Joins
- Always filter early before joining → reduces shuffle size.
- Use broadcast joins when one dataset is < ~500 MB.
- When possible, repartition both DataFrames on the join key:
> val df1Part = df1.repartition($"id")
> val df2Part = df2.repartition($"id")
- Be explicit with column selection to avoid ambiguous schemas.
- Monitor join performance in Spark UI (look for shuffles).

### Telemetry
- Use INFO for job milestones, DEBUG for details, ERROR for failures.
- Avoid logging entire DataFrames — log row counts, schema, and partitions.
- Use structured logs (JSON) if logs are consumed by observability platforms.
- Always log inputs/outputs (paths, schemas, counts) for auditability.
- For long pipelines → log elapsed time per stage.
#### Integrating with Monitoring Systems
- Spark Metrics System → publish to Graphite, Prometheus, InfluxDB.
- Custom metrics: you can log counts, throughput, error rates.
- Cloud-native monitoring: CloudWatch (AWS), Datadog, Grafana.

##### ADDITIONAL NOTES
- .sbt is (simple build tool)
