package storage

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

trait Storage {
  def read(location: String): DataFrame
  def write[T](dataset: Dataset[T], location: String): Unit
}

class CSV(spark: SparkSession) extends Storage {
  import spark.implicits._
  override def read(location: String): DataFrame =
    spark
      .read
      .format("csv")
      .option("delimiter", ";")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(location)

  override def write[T](dataset: Dataset[T], location: String) =
    dataset
      .write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(location)
}

class Text(spark: SparkSession) extends Storage {
  import spark.implicits._
  override def read(location: String): DataFrame =
    spark
      .read
      .format("csv")
      .option("delimiter", ";")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(location)

  override def write[T](dataset: Dataset[T], location: String) =
    dataset
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .text(location)
}