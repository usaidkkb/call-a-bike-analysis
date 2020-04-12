import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Transformations {

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

  def averageTripLengthByDay(dataframe: DataFrame, sparkSession: SparkSession) = {
    import org.apache.spark.sql.functions._
    import sparkSession.implicits._
    dataframe
      .select($"TRIP_LENGTH_MINUTES".as("length"), $"DATE_BOOKING".as("date"))
      .na
      .fill(0, Seq("length"))
      .groupBy(window($"date", "1 day"))
      .agg(mean($"length"))
      .sort($"avg(length)")
      .map(_.mkString(" "))
      .limit(50)
  }

  //Typed
  //  def averageTripLengthByDay(dataset: Dataset[TripData], sparkSession: SparkSession) = {
  //    import sparkSession.implicits._
  //    dataset
  //      // replacing non existing Length values with 0
  //      .map { case TripData(x, y) => TripData(x orElse Some(0), y) }
  //
  //  }


  case class TripData(length: Option[Int], date: java.sql.Timestamp)

  case class TopRouteResult(start: String, end: String, count: Long)

  case class RentalData(city: Option[String], startStation: Option[String], endStation: Option[String])

}
