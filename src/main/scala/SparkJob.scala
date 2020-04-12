import org.apache.spark.sql.SparkSession

trait SparkJob {
  protected lazy val spark: SparkSession = SparkSession.builder.appName("Call A Bike Analysis").getOrCreate()
}
