import org.apache.spark.sql.SparkSession

trait SparkSessionTest {
  lazy val spark =
    SparkSession
      .builder()
      .master("local[2]")
      .appName("call-a-bike-analysis-test")
      .getOrCreate()
}
