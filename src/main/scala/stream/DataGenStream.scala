package stream

import org.apache.spark.sql.functions.{col, from_json, max, unix_timestamp}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import utilities.{Processing, Utils}

object DataGenStream {

def main (args : Array[String]) : Unit = {

  // ----------------------- Spark session -----------------------
  val spark = Utils.get_spark_session("Data gen streaming enrichment")
  val ssc = new StreamingContext(spark.sparkContext, Seconds(300))

  // ----------------------- configuring input stream -----------------------

  val input_stream = KafkaUtils.createStream(ssc,"ip-20-0-21-196.ec2.internal:2181", "group_1", Map("MobileLogTopic" -> 1))

  // ----------------------- load data to lookups -----------------------

  Utils.channelGeocd(spark)
  Utils.videoCreator(spark)

  // ----------------------- Schema definition -----------------------

  val jsonSchema =  Utils.getSchema()

  // ----------------------- stream processing -----------------------

  import spark.implicits._
  val stream_process = input_stream.
    map( x => x._2.toString()).
    foreachRDD( rdd => {

      if(!rdd.isEmpty()){

        val df = rdd.toDF("value")

        // ----------------------- validate json. Non adherent records are rejected  -----------------------
        val stream_process = df.select(from_json(col("value"), jsonSchema).as("value")).
          filter(col("value").isNotNull).
          select("value.*")

        // ----------------------- replace null by actual "Null"
        val stream_null_replaced = Processing.null_replacement(stream_process)

        // ----------------------- lookup values for empty values of creator_id and geo_cd
        val enriched = Processing.enrichment(stream_null_replaced)

        // ----------------------- Lake ingestion -----------------------
        Processing.load(enriched, args(0))
        enriched.select(unix_timestamp(max("process_time"), "dd/MM/yyyy HH:mm:ss").as("last_updated_time"))
          .write.format("orc")
          .mode("overwrite")
          .insertInto("videos.video_play_stg_last_updated")
      }
    }
    )

  // ----------------------- Begin stream  -----------------------
  try{
    ssc.start()
    ssc.awaitTermination()
  } finally {
    ssc.stop(true, true)
  }

}
}
