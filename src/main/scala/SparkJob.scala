import config.{Config, OptionsParser}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.{Failure, Try}

trait SparkJob {
  def main(args: Array[String]): Unit =
    new OptionsParser()
      .parse(args, Config())
      .fold[Try[Unit]](Failure(throw new Exception("Parsing Options failed")))(runPipeline)

  protected lazy val spark: SparkSession =
    SparkSession
      .builder
      .appName(appName)
      .getOrCreate()

  protected def appName: String

  protected def runPipeline(config: Config): Try[Unit]
}
