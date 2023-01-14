package data

import org.apache.spark.sql.{Dataset, SparkSession}
import utils.DataReader

sealed case class NumberOfBillionairesData(
                                            country: String,
                                            num_billionaires: Long,
                                            billionaire_per_million: Float
                                          )

object NumberOfBillionairesData extends DataLoader[NumberOfBillionairesData] {

  val FILE_PATH = "resources/wiki_number_of_billionaires.csv"

  object Col extends Enumeration {
    type Column = String
    val COUNTRY = "country"
    val NUM_BILLIONAIRES = "num_billionaires"
    val BILLIONAIRES_PER_MILLION = "billionaire_per_million"
  }

  override def loadData()(implicit spark: SparkSession): Dataset[NumberOfBillionairesData] = {
    import spark.implicits._
    DataReader.readCsv(FILE_PATH)
      .as[NumberOfBillionairesData]
  }
}
