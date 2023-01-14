package data
import org.apache.spark.sql.{Dataset, SparkSession}
import utils.DataReader


sealed case class RichestPeopleData(
                                     rank: Long,
                                     name: String,
                                     net_worth: String,
                                     bday: String,
                                     age: Long,
                                     nationality: String
                                   )

object RichestPeopleData extends DataLoader[RichestPeopleData] {

  val FILE_PATH = "resources/top_100_richest.csv"

  object Col extends Enumeration {
    type Column = String
    val RANK = "rank"
    val NAME = "name"
    val NET_WORTH = "net_worth"
    val BDAY = "bday"
    val AGE = "age"
    val NATIONALITY = "nationality"
  }

  override def loadData()(implicit spark: SparkSession): Dataset[RichestPeopleData] = {
    import spark.implicits._
    DataReader.readCsv(FILE_PATH)
      .as[RichestPeopleData]
  }
}
