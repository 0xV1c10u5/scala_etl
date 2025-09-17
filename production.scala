// Production-ready main.scala (with scopt + SLF4J + graceful shutdown)

// This version is suited for real ETL jobs where you want 
// structured logging, argument validation, a shutdown hook,
// and a single run entrypoint that returns an exit code.

// file: src/main/scala/com/example/Main.scala
package com.example

import org.slf4j.LoggerFactory
import scopt.OParser
import scala.util.{Try, Success, Failure}

case class Config(input: String = "", output: String = "", dryRun: Boolean = false)

object Main {

  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    // --- CLI parser setup (scopt)
    val builder = OParser.builder[Config]
    val parser = {
      import builder._
      OParser.sequence(
        programName("generic-etl"),
        head("generic-etl", "0.1"),
        opt[String]('i', "input")
          .required()
          .action((x, c) => c.copy(input = x))
          .text("input path or connection string"),
        opt[String]('o', "output")
          .required()
          .action((x, c) => c.copy(output = x))
          .text("output path or connection string"),
        opt[Unit]("dry-run")
          .action((_, c) => c.copy(dryRun = true))
          .text("perform all steps except writing output"),
        help("help").text("prints this usage text")
      )
    }

    // --- Parse args
    OParser.parse(parser, args, Config()) match {
      case Some(conf) =>
        // Register a shutdown hook for graceful termination
        sys.addShutdownHook {
          logger.info("Shutdown hook invoked — cleaning up.")
          // cleanup resources here if needed
        }

        val exit = run(conf)
        // Use explicit sys.exit to propagate exit code to the shell
        sys.exit(exit)

      case _ =>
        // Arguments invalid — scopt already printed usage
        sys.exit(2)
    }
  }

  /**
   * Main program logic delegated here.
   * Returns 0 on success, non-zero on error.
   */
  def run(conf: Config): Int = {
    logger.info(s"Starting job | input=${conf.input} output=${conf.output} dryRun=${conf.dryRun}")
    val start = System.nanoTime()

    // Wrap the work in Try to centralize error handling
    val outcome = Try {
      // 1) Validate sources (examples)
      require(conf.input.nonEmpty, "input must not be empty")
      require(conf.output.nonEmpty, "output must not be empty")

      // 2) Extract (placeholder)
      logger.debug("Extracting data...")
      val raw = extract(conf.input)

      // 3) Transform (placeholder)
      logger.debug("Transforming data...")
      val transformed = transform(raw)

      // 4) Load (respect dry-run)
      if (conf.dryRun) {
        logger.info("Dry-run enabled: skipping load/write step.")
      } else {
        logger.debug("Loading data...")
        load(transformed, conf.output)
      }

      // Could return a summary object for richer logging
      "OK"
    }

    val elapsedMs = (System.nanoTime() - start) / 1000000
    outcome match {
      case Success(v) =>
        logger.info(s"Job completed successfully in ${elapsedMs}ms: $v")
        0
      case Failure(ex) =>
        logger.error(s"Job failed after ${elapsedMs}ms", ex)
        1
    }
  }

  // --- Example stubs (replace with real logic)
  private def extract(path: String): Seq[String] = {
    // simulate reading data (files, DB, API)
    logger.info(s"Reading from $path")
    Seq("row1", "row2", "row3")
  }

  private def transform(rows: Seq[String]): Seq[String] = {
    // apply transformations, cleaning, enrichments
    rows.map(_.toUpperCase)
  }

  private def load(rows: Seq[String], out: String): Unit = {
    // persist results (files, DB, message bus)
    logger.info(s"Writing ${rows.size} rows to $out")
    // Example: write to file or call repository layer
  }
}

// Highlights & best practices shown
// ✅ Single run function that returns an Int exit code (easy to test).
// ✅ Graceful shutdown hook for cleanup if JVM is terminated.
// ✅ Structured logging (SLF4J + Logback) — don’t rely on println in prod.
// ✅ Centralized error handling using Try so uncaught exceptions can return proper exit codes.
// ✅ Dry-run flag — handy for testing pipelines without side-effects.
// ✅ Small, testable helper methods (extract, transform, load) keep main lean.

// Quick notes on running & testing
// 
// With SBT: sbt "runMain com.example.Main --input s3://in --output s3://out"
// Unit-test run by calling Main.run(Config(...)) and asserting return values or side effects.
// For production, supply a logback.xml or equivalent to configure log levels and outputs.
