package data
import org.apache.spark.sql.{Dataset, SparkSession}
import utils.DataReader

sealed case class AirportCodeData(
                                   identifier: String,
                                   kind: String,
                                   name: String,
                                   elevation_ft: Long,
                                   continent: String,
                                   iso_country: String,
                                   iso_region: String,
                                   municipality: String,
                                   gps_code: String,
                                   iata_code: String,
                                   local_code: String,
                                   coordinates: String
                                 )

object AirportCodeData extends DataLoader[AirportCodeData] {
  val FILE_PATH = "resources/airport-codes.csv"

  object Col extends Enumeration {
    type Column = String
    val IDENTIFIER = "identifier"
    val KIND = "kind"
    val NAME = "name"
    val ELEVATION_FT = "elevation_ft"
    val CONTINENT = "continent"
    val ISO_COUNTRY = "iso_country"
    val ISO_REGION = "iso_region"
    val MUNICIPALITY = "municipality"
    val GPS_CODE = "gps_code"
    val IATA_CODE = "iata_code"
    val LOCAL_CODE = "local_code"
    val COORDINATES = "coordinates"
  }

  override def loadData()(implicit spark: SparkSession): Dataset[AirportCodeData] = {
    import spark.implicits._
    DataReader.readCsv(FILE_PATH)
      .as[AirportCodeData]
  }
}
