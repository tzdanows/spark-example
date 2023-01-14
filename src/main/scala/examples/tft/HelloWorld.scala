package examples.tft

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import utils.SparkBuilder

object HelloWorld {

  val file = "resources/tft_data.json" // copy line ctrl d, go to location ctrl b
  val simpleFile = "resources/simplejson.json"

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkBuilder.create("HelloWorld")

    val df = spark.read.option("multiline","true").json(file)
      .select(col("info")).cache()
    val participants = df.select(explode(col("info.participants")))
    val x = participants.select("col.*","*")


    x.show(10)


    // ideally we dig into participants, then augments, puuid, placement, traits etc..
  }


}
