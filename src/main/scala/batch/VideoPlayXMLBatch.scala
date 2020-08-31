/*
spark2-submit --class batch.VideoPlayXMLBatch \
--packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.12 \
--conf spark.cassandra.auth.username=edureka_735821 \
--conf spark.cassandra.auth.password=edureka_735821iyktd \
 target/scala-2.11/mid-project-2_2.11-0.1.jar VideoPlayLake/Inbound/xml videos.video_play_stg
 */

package batch

import model.VideoPlayRecord
import org.apache.spark.sql.functions.{max, unix_timestamp}
import org.xml.sax.SAXParseException

import scala.xml.{Elem, XML}
import utilities._

object VideoPlayXMLBatch {

  def main (args : Array[String]) : Unit = {

    // ----------------------- Spark session -----------------------
    val spark = Utils.get_spark_session("XML batch processing for enrichment")

    // ----------------------- load data to lookups -----------------------

    Utils.channelGeocd(spark)
    Utils.videoCreator(spark)

    // ----------------------- load data in dataframe -----------------------
    import spark.implicits._
    val input_df = spark.sparkContext.textFile(args(0)).
      map( x => xml_parser(x)).
      map( x => VideoPlayRecord(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9))).toDF()


    // ----------------------- Data validation and enrichment -----------------------

    val null_replaced_df = Processing.blank_replacement(input_df)

    val enriched = Processing.enrichment( null_replaced_df )

    // ----------------------- Lake ingestion -----------------------

    Processing.load(enriched, args(1))

    enriched.select(unix_timestamp(max("process_time"), "dd/MM/yyyy HH:mm:ss").as("last_updated_time"))
      .write.format("orc")
      .mode("overwrite")
      .insertInto("videos.video_play_stg_last_updated")
  }

  def xml_parser( record : String) : Array[String] = {
    var rec : Elem = null
    try {
      rec = XML.loadString(record)

      Array((rec \ "liked").text,
        (rec \ "user_id").text,
        (rec \ "video_end_type").text,
        (rec \ "minutes_played").text,
        (rec \ "video_id").text,
        (rec \ "geo_cd").text,
        (rec \ "channel_id").text,
        (rec \ "creator_id").text,
        (rec \ "timestamp").text,
        (rec \ "disliked").text
      )
    } catch {
      case exc : SAXParseException =>{
        Array(null, null, null, null, null, null, null, null, null, null)
      }
    }
  }

}
