package utils

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DataWriter {

  val DEFAULT_OUTPUT = "output/tmp"
  val DEFAULT_NUMBER_OF_PARTITIONS = 1

  def writeToCsv(df: DataFrame, path: String = DEFAULT_OUTPUT, partitions: Int = DEFAULT_NUMBER_OF_PARTITIONS)(implicit spark: SparkSession): Unit = {
    df.coalesce(partitions)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", value = true)
      .csv(path)
  }
}
