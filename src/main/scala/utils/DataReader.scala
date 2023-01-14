package utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataReader {

  val CSV_FORMAT = "csv"

  /**
   * Reads a comma-separated-values file into a [[DataFrame]] and infers the schema
   *
   * @param path the filepath of the csv to read
   * @param spark the [[SparkSession]]
   * @return a [[DataFrame]] containing csv data
   */
  def readCsv(path: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.format(CSV_FORMAT)
      .option("inferSchema", "true")
      .option("header", "true")
      .load(path)
  }
}
