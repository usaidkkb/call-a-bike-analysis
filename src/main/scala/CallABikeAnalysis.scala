import org.apache.spark.sql._
import storage.CSV

object CallABikeAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Call A Bike Analysis").getOrCreate()
    val csv = new CSV(spark)

    val df = csv.read(args(0))

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

    csv.write(top20("startStation", hamburgDF, spark), s"${args(1)}/top20HamburgStartStations")
    csv.write(top20("endStation", hamburgDF, spark), s"${args(1)}/top20HamburgEndStations")

    // Typed approach on Dataset
    val ds =
      df.select(
        $"CITY_RENTAL_ZONE".as("city"),
        $"START_RENTAL_ZONE".as("startStation"),
        $"END_RENTAL_ZONE".as("endStation")
      ).as[RentalData]

    csv.write(top20CitiesByUsage(ds, spark), s"${args(1)}/topCities")
    csv.write(top20RoutesHamburg(ds, spark), s"${args(1)}/topRoutesHamburg")
  }

  // Basic SQL on Dataframe approach
  def top20(column: String, dataFrame: DataFrame, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    dataFrame
      .groupBy(column)
      .count()
      .sort($"count".desc)
      .limit(20)
  }

  // Typed approach on Dataset
  def top20EndStations(dataset: Dataset[RentalData], sparkSession: SparkSession) = {
    import sparkSession.implicits._
    dataset
      .groupByKey(_.endStation)
      .count()
      // dataset loses column names and creates [key, count(1)] instead of [endStation, count]
      .sort($"count(1)".desc)
      .limit(20)
  }

  def top20CitiesByUsage(dataset: Dataset[RentalData], sparkSession: SparkSession) = {
    import sparkSession.implicits._
    dataset
      .filter(_.city.isDefined)
      .groupBy("city")
      .count()
      .sort($"count".desc)
      .limit(20)
  }

  def top20RoutesHamburg(dataset: Dataset[RentalData], sparkSession: SparkSession) = {
    import sparkSession.implicits._
    dataset
      .filter(booking => booking.city.contains("Hamburg") && booking.startStation.nonEmpty && booking.endStation.nonEmpty)
      .groupByKey(booking => (booking.startStation, booking.endStation))
      .count()
      .map { case (key, count) => (key._1, key._2, count) }
      .toDF(Seq("start", "end", "count"): _*)
      .as[TopRouteResult]
      .sort($"count".desc)
      .limit(20)
  }

  def trafficIncreaseHamburgHBF(dataset: Dataset[RentalData], sparkSession: SparkSession) = ???

  case class RentalData(city: Option[String], startStation: Option[String], endStation: Option[String])

  case class TopRouteResult(start: String, end: String, count: Long)
}