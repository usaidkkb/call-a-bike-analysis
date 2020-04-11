import org.apache.spark.sql._

object CallABikeAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Call A Bike Analysis").getOrCreate()
    import spark.implicits._
    val df =
      spark
        .read
        .format("csv")
        .option("delimiter", ";")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(args(0))

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

    println(top20("startStation", hamburgDF, spark).show())
    println(top20("endStation", hamburgDF, spark).show())

    val ds =
      df.select(
        $"CITY_RENTAL_ZONE".as("city"),
        $"START_RENTAL_ZONE".as("startStation"),
        $"END_RENTAL_ZONE".as("endStation")
      ).as[RentalData]

    val topCitiesByUsageDS = top20CitiesByUsage(ds, spark)
    println(topCitiesByUsageDS.show())

    topCitiesByUsageDS
      .write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(args(1))

    println(top20EndStations(ds, spark))
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

  // start and end stations count
  def top20RoutesHamburg(dataset: Dataset[RentalData], sparkSession: SparkSession) = {
    ???
  }

  def trafficIncreaseHamburgHBF(dataset: Dataset[RentalData], sparkSession: SparkSession) = ???

  case class RentalData(city: Option[String], startStation: Option[String], endStation: Option[String])
}