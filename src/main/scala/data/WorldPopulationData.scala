package data

import org.apache.spark.sql.{Dataset, SparkSession}
import utils.DataReader

sealed case class WorldPopulationData(
                                  country_code: String,
                                  name: String,
                                  year_2022: String,
                                  year_2020: String,
                                  year_2015: String,
                                  year_2010: String,
                                  year_2000: String,
                                  year_1990: String,
                                  year_1980: String,
                                  year_1970: String,
                                  area: Long,
                                  density: Double,
                                  growth_rate: Double,
                                  world_population_percentage: Double,
                                  rank: Long
                                )


object WorldPopulationData extends DataLoader[WorldPopulationData] {
  val FILE_PATH = "resources/world_population_2022.csv"

  /**
   * The columns of the world population dataset
   */
  object Col extends Enumeration {
    type Column = String
    val COUNTRY_CODE = "country_code"
    val NAME = "name"
    val TWENTY_TWENTY_TWO = "year_2022"
    val TWENTY_TWENTY = "year_2020"
    val TWENTY_FIFTEEN = "year_2015"
    val TWENTY_TEN = "year_2010"
    val TWO_THOUSAND = "year_2000"
    val NINETEEN_NINETY = "year_1990"
    val NINETEEN_EIGHTY = "year_1980"
    val NINETEEN_SEVENTY = "year_1970"
    val AREA = "area"
    val DENSITY = "density"
    val GROWTH_RATE = "growth_rate"
    val WORLD_POPULATION_PERCENTAGE = "world_population_percentage"
    val RANK = "rank"
  }

  override def loadData()(implicit spark: SparkSession): Dataset[WorldPopulationData] = {
    import spark.implicits._
    DataReader.readCsv(FILE_PATH)
      .as[WorldPopulationData]
  }
}
