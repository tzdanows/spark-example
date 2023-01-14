package examples.countries

import data.AirportCodeData
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import utils.SparkBuilder

object Airports {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkBuilder.create("Airports")

    val airportsData = AirportCodeData.loadData()
      .filter(col(AirportCodeData.Col.KIND) === lit("large_airport"))

    airportsData.show(50, truncate = false)
  }
}
