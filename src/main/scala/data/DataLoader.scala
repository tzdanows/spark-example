package data

import org.apache.spark.sql.{Dataset, SparkSession}

trait DataLoader[T] {
  def loadData()(implicit spark: SparkSession): Dataset[T]
}
