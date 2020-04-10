import org.apache.spark.sql._

object CallABikeAnalysis {
  def main (args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Call A Bike Analysis").getOrCreate()
    val df =
      spark
        .read
        .format("com.databricks.spark.csv")
        .option("delimiter",";")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(args(0))

    println(topCitiesByUsage(df, spark).show(20))
  }

  def topCitiesByUsage(dataFrame: DataFrame, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    dataFrame.groupBy("CITY_RENTAL_ZONE").count().sort($"count".desc)
  }
}