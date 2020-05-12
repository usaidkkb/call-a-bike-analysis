import Transformations.top20
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TransformationsSpec extends AnyWordSpec with Matchers with SparkSessionTest {

  import spark.implicits._

  "Transformations" should {
    "return Top 20 stations" in {
      val df = Seq("HBF", "HBF", "HBF", "HBF", "HBF Koln").toDF("station")
      val transformedDf = top20("station", df, spark)
      val expectedData: Seq[Row] = Seq(
        Row("HBF", 4),
        Row("HBF Koln", 1)
      )

      val schema = StructType(
        List(
          StructField("station", StringType, false),
          StructField("count", IntegerType, false)
        )
      )
      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData), schema
      )

      transformedDf.collect() should contain theSameElementsAs expectedDf.collect()
    }
  }
}
