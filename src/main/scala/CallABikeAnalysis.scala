import Transformations.{averageTripLengthByDay, top20, top20RoutesHamburg, _}
import config.Config
import org.apache.spark.sql.{DataFrame, Dataset}
import storage._

import scala.util.Try

object CallABikeAnalysis extends SparkJob {
  override def appName: String = "Call a Bike Analysis"

  override protected def runPipeline(config: Config): Try[Unit] = {
    val csv = new CSV(spark)
    val text = new Text(spark)

    val df = csv.read(config.input)
    val outputDir = config.output

    val transformedDatasets = transform(df)

    Try {
      csv.write(transformedDatasets.top20StartStations, s"$outputDir/top20HamburgStartStations")
      csv.write(transformedDatasets.top20EndStations, s"$outputDir/top20HamburgEndStations")
      csv.write(transformedDatasets.top20Cities, s"$outputDir/topCities")
      csv.write(transformedDatasets.top20RoutesHamburg, s"$outputDir/topRoutesHamburg")
      text.write(transformedDatasets.averageTriplength, s"$outputDir/averageTripLengthByDay")
    }
  }

  case class TransformedDatasets(
                                top20StartStations: DataFrame,
                                top20EndStations: DataFrame,
                                top20Cities: DataFrame,
                                top20RoutesHamburg: Dataset[TopRouteResult],
                                averageTriplength: Dataset[String]
                                )

   def transform(dataframe: DataFrame): TransformedDatasets = {
    import spark.implicits._

    val hamburgDF =
      dataframe
        .select(
          $"CITY_RENTAL_ZONE".as("city"),
          $"START_RENTAL_ZONE".as("startStation"),
          $"END_RENTAL_ZONE".as("endStation")
        )
        .filter("city = \"Hamburg\" AND startStation is not null AND endStation is not null")
        .cache()

    val ds =
      dataframe.select(
        $"CITY_RENTAL_ZONE".as("city"),
        $"START_RENTAL_ZONE".as("startStation"),
        $"END_RENTAL_ZONE".as("endStation")
      ).as[RentalData]

    TransformedDatasets(
      top20("startStation", hamburgDF, spark),
      top20("endStation", hamburgDF, spark),
      top20CitiesByUsage(ds, spark),
      top20RoutesHamburg(ds, spark),
      averageTripLengthByDay(dataframe, spark)
    )
  }

}