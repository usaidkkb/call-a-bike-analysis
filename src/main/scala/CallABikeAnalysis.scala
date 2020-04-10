import org.apache.spark.sql._

object CallABikeAnalysis {
  case class CityDS (city: Option[String])
  def main (args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Call A Bike Analysis").getOrCreate()
    import spark.implicits._
    val df =
      spark
        .read
        .format("com.databricks.spark.csv")
        .option("delimiter",";")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(args(0))
        .select($"CITY_RENTAL_ZONE".as("city"))
        .as[CityDS]

    val topCitiesByUsageDS = top20CitiesByUsage(df, spark)
    println(topCitiesByUsageDS.show())

    topCitiesByUsageDS
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(args(1))
  }

  def top20CitiesByUsage(dataset: Dataset[CityDS], sparkSession: SparkSession) = {
    import sparkSession.implicits._
    dataset
      .filter(_.city.isDefined)
      .groupBy("city")
      .count()
      .sort($"count".desc)
      .limit(20)
  }
}