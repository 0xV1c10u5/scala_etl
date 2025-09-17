// file: src/main/scala/com/example/SimpleMain.scala
package com.example

object SimpleMain {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: SimpleMain <input> <output>")
      System.exit(1)
    }

    val input = args(0)
    val output = args(1)

    println(s"[INFO] Starting job: input=$input output=$output")

    try {
      val result = doWork(input)
      println(s"[INFO] Success: $result -> written to $output")
      System.exit(0)
    } catch {
      case e: Exception =>
        System.err.println(s"[ERROR] Job failed: ${e.getMessage}")
        e.printStackTrace()
        System.exit(2)
    }
  }

  private def doWork(in: String): String = {
    // Your processing logic here (IO, compute, call services, etc.)
    // Keep heavy ops out of `main` for testability.
    s"processed:$in"
  }
}

// Very small surface area — easy to test and run.
// Uses System.exit codes: 0 = success, 1 = bad args, 2 = failure.
// Good for quick scripts or bootstrapping.
