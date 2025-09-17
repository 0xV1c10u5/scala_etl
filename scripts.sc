// ✅ Features Demonstrated
// 
// Standalone scripting: run with amm scripts.sc input.csv output.csv.
// File I/O: read and write CSV without Spark.
// Lightweight transformations: simple transform function with map logic.
// Error handling: uses Try to catch exceptions and print useful messages.
// Dependency imports: shows $ivy imports for external libraries (os-lib, scalatags) for file handling or HTML generation.
// CLI arguments: simple positional arguments for input/output paths.
// Logging: minimal [INFO] / [WARN] / [ERROR] style logs for clarity.

// ⚡ Notes
//
// Ammonite scripts are excellent for small, fast ETL utilities, DevOps scripts, or data prep tasks.
// For heavier workloads or distributed ETL, switch to Spark applications like pipeline.scala.
// You can add libraries via $ivy for JSON parsing, HTTP requests, database access, etc.

#!/usr/bin/env amm
import $ivy.`com.lihaoyi::os-lib:0.9.1`
import $ivy.`com.lihaoyi::scalatags:0.13.2`
import scala.util.{Try, Success, Failure}
import java.time.LocalDate

// ------------------------
// 1. Simple argument parsing
// ------------------------
val inputPath  = if (args.length > 0) args(0) else "data/input.csv"
val outputPath = if (args.length > 1) args(1) else "data/output/"

println(s"[INFO] Input: $inputPath")
println(s"[INFO] Output: $outputPath")

// ------------------------
// 2. Helper functions
// ------------------------
def readCSV(path: String): Seq[Map[String, String]] = {
  if (!os.exists(os.Path(path))) {
    println(s"[WARN] File not found: $path")
    Seq.empty
  } else {
    val lines = os.read.lines(os.Path(path))
    val header = lines.head.split(",").map(_.trim)
    lines.tail.map { line =>
      header.zip(line.split(",").map(_.trim)).toMap
    }
  }
}

def writeCSV(data: Seq[Map[String, String]], path: String): Unit = {
  if (data.isEmpty) {
    println(s"[INFO] No data to write to $path")
    return
  }
  val header = data.head.keys.mkString(",")
  val content = data.map(_.values.mkString(",")).mkString("\n")
  os.write.over(os.Path(path), s"$header\n$content")
  println(s"[INFO] Wrote ${data.size} rows to $path")
}

// Example transformation: uppercase all values
def transform(data: Seq[Map[String, String]]): Seq[Map[String, String]] = {
  data.map(row => row.map { case (k,v) => (k, v.toUpperCase) })
}

// ------------------------
// 3. Main script execution
// ------------------------
Try {
  val rawData = readCSV(inputPath)
  val transformed = transform(rawData)
  writeCSV(transformed, outputPath)
} match {
  case Success(_) =>
    println(s"[INFO] Script finished successfully at ${LocalDate.now()}")
  case Failure(e) =>
    println(s"[ERROR] Script failed: ${e.getMessage}")
    e.printStackTrace()
}

