📝 Scala Cheat Sheet (Basic + Advanced)
1. Basics
Variables
val x = 5          // immutable
var y = 10         // mutable

Option / Null Safety
val maybe: Option[String] = Some("Hello")
maybe.getOrElse("Default")

Pattern Matching
x match {
  case 0 => "Zero"
  case 1 => "One"
  case _ => "Other"
}

For-comprehensions
val result = for {
  a <- Some(2)
  b <- Some(3)
} yield a + b

2. Collections
val nums = List(1,2,3)
nums.map(_*2)
nums.filter(_ % 2 == 0)
nums.reduce(_ + _)


Immutable by default: Seq, List, Vector, Map, Set

Use .view for lazy operations

3. Functions
val add = (x:Int,y:Int)=>x+y
val squared = nums.map(_*_)

def multiply(x:Int)(y:Int) = x*y
val timesTwo = multiply(2) _

4. Strings
val name = "Alice"
println(s"Hello, $name")    // interpolation
println(f"Pi ~ ${Math.PI}%.2f")
val multiline = """line1
                  |line2""".stripMargin

5. Error Handling
import scala.util.{Try, Success, Failure}

val safeDivide = (x:Int,y:Int)=>Try(x/y)
safeDivide(4,0) match {
  case Success(v)=> println(v)
  case Failure(e)=> println("Error")
}

6. Spark Tips
import org.apache.spark.sql.functions.broadcast

// Read CSV
val df = spark.read.option("header","true").csv("s3://bucket/file.csv")

// Transform
val transformed = df.withColumn("name_clean", lower(trim($"name")))

// Join with small table
val joined = dfLarge.join(broadcast(dfSmall), "id")

// Write partitioned Parquet
df.write.mode("overwrite").partitionBy("category").parquet("s3://bucket/output")


Avoid .count() on huge datasets

Use broadcast for small lookup tables

Cache only reused DataFrames

7. Advanced Functional Patterns
// Higher-order
nums.map(_*2).filter(_>2)

// Partial Function
val pf: PartialFunction[Int,String] = { case 1=>"One"; case 2=>"Two" }

// Futures (async)
import scala.concurrent._
import ExecutionContext.Implicits.global
val f = Future{42}
f.onComplete{ case Success(v)=>println(v); case Failure(e)=>println(e) }


Use Option, Either, Try for safe chaining

Pattern matching with guards:

x match { case n if n>0=>"Pos"; case 0=>"Zero"; case _=>"Neg" }

8. Performance & Concurrency

Use Vector for random-access sequences

Lazy evaluation: .view, lazy val, Stream

Parallel collections:

val pnums = (1 to 1000000).par.map(_*2)


Prefer immutable data structures

Avoid unnecessary object creation in loops

9. Implicits & Type Classes
implicit class StringOps(s:String){ def shout=s.toUpperCase+"!" }
println("hello".shout)

def greet(name:String)(implicit g:String)=s"$g, $name"
implicit val defaultG="Hello"
println(greet("Alice"))

10. Testing & REPL

Use Ammonite REPL for experimentation

amm> val data = List(1,2,3)
amm> data.map(_*2)


Unit test pure functions first

Use ScalaTest / MUnit for automated tests

Property-based testing with ScalaCheck:

import org.scalacheck.Prop.forAll
forAll{ (n:Int)=> n+0==n }

