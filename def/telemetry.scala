// Using Log4j in Scala
//
// Log key lifecycle events: start, checkpoints, success/failure.

import org.apache.log4j.Logger

object MyETLJob {
  val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("ETL job started...")

    try {
      // Run ETL steps
      logger.debug("Reading input data...")
      // val df = spark.read...

      logger.info("Transformation complete.")
    } catch {
      case e: Exception =>
        logger.error("ETL job failed!", e)
        throw e
    }

    logger.info("ETL job finished successfully.")
  }
}

// Logging Spark DataFrame Details
//
// Careful: .count() and .show() trigger Spark jobs (expensive at scale).
// Better: use .inputFiles.length, .columns.length, or sample .limit(5).collect().

logger.info(s"Input DataFrame count: ${df.count()}")
logger.info(s"Schema: ${df.printSchema()}")



