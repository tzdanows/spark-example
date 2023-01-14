package examples.text

import org.apache.spark.sql.SparkSession
import utils.SparkBuilder

object TextCounter {
  def main(args: Array[String]): Unit = {
    // Path to the text file
    val logFile = "resources/text.txt"

    // Start the Spark session
    implicit val spark: SparkSession = SparkBuilder.create("TextCounter")

    // Read the input into a dataset
    val data = spark.read.textFile(logFile).cache()

    // Do some calculations
    val numAs = data.filter(line => line.contains("a")).count()
    val numFs = data.filter(line => line.contains("f")).count()

    // Print the output
    println(s"Lines with a: $numAs, Lines with f: $numFs")

    // Stop the Spark session
    spark.stop()
  }
}
