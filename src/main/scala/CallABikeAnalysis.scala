import Transformations._
import config.{Config, OptionsParser}
import storage._

import scala.util.{Failure, Success, Try}

object CallABikeAnalysis extends SparkJob {

  def main(args: Array[String]): Unit = {
    val csv = new CSV(spark)
    val text = new Text(spark)
    val optionsParser = new OptionsParser()

    optionsParser
      .parse(args, Config())
      .fold[Try[Unit]](Failure(throw new Exception("Parsing Options failed"))) {
        config => {
          val job = {
            val df = csv.read(config.input)
            val outputDir = config.output

            import spark.implicits._
            // Basic SQL on Dataframe approach
            val hamburgDF =
              df
                .select(
                  $"CITY_RENTAL_ZONE".as("city"),
                  $"START_RENTAL_ZONE".as("startStation"),
                  $"END_RENTAL_ZONE".as("endStation")
                )
                .filter("city = \"Hamburg\" AND startStation is not null AND endStation is not null")
                .cache()

            csv.write(top20("startStation", hamburgDF, spark), s"$outputDir/top20HamburgStartStations")
            csv.write(top20("endStation", hamburgDF, spark), s"$outputDir/top20HamburgEndStations")

            // Typed approach on Dataset
            val ds =
              df.select(
                $"CITY_RENTAL_ZONE".as("city"),
                $"START_RENTAL_ZONE".as("startStation"),
                $"END_RENTAL_ZONE".as("endStation")
              ).as[RentalData]

            csv.write(top20CitiesByUsage(ds, spark), s"$outputDir/topCities")
            csv.write(top20RoutesHamburg(ds, spark), s"$outputDir/topRoutesHamburg")
            text.write(averageTripLengthByDay(df, spark), s"$outputDir/averageTripLengthByDay")
          }
          Success(job)
        }
      }

  }

}