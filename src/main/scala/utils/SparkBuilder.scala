package utils

import org.apache.spark.sql.SparkSession

object SparkBuilder {
  /**
   * Creates a [[SparkSession]] with a given appName
   *
   * @param appName the name of the application
   * @return [[SparkSession]]
   */
  def create(appName: String): SparkSession = { // context that lives for entire duration of our program
    SparkSession
      .builder
      .appName(appName)
      .config("spark.master", "local")
      .getOrCreate()
  }
}
