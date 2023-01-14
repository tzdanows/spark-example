package examples.countries

import data.RichestPeopleData
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, col}
import utils.SparkBuilder

object Billionaires {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkBuilder.create("Billionaires")

    val out = RichestPeopleData
      .loadData()
      .groupBy(RichestPeopleData.Col.NATIONALITY)
      .avg(RichestPeopleData.Col.AGE) // or .avg("age") but explicitness is helpful
      .select(
        col(RichestPeopleData.Col.NATIONALITY),
        col(s"avg(${RichestPeopleData.Col.AGE})").as(RichestPeopleData.Col.AGE)
      )
      .orderBy(asc(RichestPeopleData.Col.AGE))

    out.show()
  }
}
