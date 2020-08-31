package utilities

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, when}

object Processing {

  def null_replacement(input_frame : DataFrame) : DataFrame ={

    input_frame.select(when(col("channel_id").isNull, "null").otherwise(col("channel_id")).as("channel_id"),
      when(col("creator_id").isNull, "null").otherwise(col("creator_id")).as("creator_id"),
      when(col("disliked").isNull, "null").otherwise(col("disliked")).as("disliked"),
      when(col("geo_cd").isNull, "null").otherwise(col("geo_cd")).as("geo_cd"),
      when(col("liked").isNull, "null").otherwise(col("liked")).as("liked"),
      when(col("minutes_played").isNull, "null").otherwise(col("minutes_played")).as("minutes_played"),
      when(col("timestamp").isNull, "null").otherwise(col("timestamp")).as("timestamp"),
      when(col("user_id").isNull, "null").otherwise(col("user_id")).as("user_id"),
      when(col("video_end_type").isNull, "null").otherwise(col("video_end_type")).as("video_end_type"),
      when(col("video_id").isNull, "null").otherwise(col("video_id")).as("video_id"))
  }

  def enrichment( null_replaced : DataFrame) : DataFrame = {

    null_replaced.select(
      col("channel_id"),
      when(col("creator_id") === "null", Utils.videoCreatorLkp(col("video_id").toString())).otherwise(col("creator_id")).as("creator_id"),
      col("disliked"),
      when(col("geo_cd") === "null", Utils.channelGeocdLKP(col("channel_id").toString())).otherwise(col("geo_cd")).as("geo_cd"),
      col("liked"),
      col("minutes_played"),
      col("timestamp"),
      col("user_id"),
      col("video_end_type"),
      col("video_id")
    ).filter( (col("creator_id") =!= "null").and(col("geo_cd") =!= "null")).
      withColumn("process_time", date_format(current_timestamp, "dd/MM/yyyy HH:mm:ss")).
      filter( col("creator_id").notEqual("Invalid").and(col("geo_cd").notEqual("Invalid")))
  }

  def blank_replacement(input_frame : DataFrame) : DataFrame ={

    input_frame.select(when(col("channel_id") === "", "null").otherwise(col("channel_id")).as("channel_id"),
      when(col("creator_id")  === "", "null").otherwise(col("creator_id")).as("creator_id"),
      when(col("disliked")  === "", "null").otherwise(col("disliked")).as("disliked"),
      when(col("geo_cd") === "", "null").otherwise(col("geo_cd")).as("geo_cd"),
      when(col("liked") === "", "null").otherwise(col("liked")).as("liked"),
      when(col("minutes_played") === "", "null").otherwise(col("minutes_played")).as("minutes_played"),
      when(col("timestamp") === "", "null").otherwise(col("timestamp")).as("timestamp"),
      when(col("user_id")  === "", "null").otherwise(col("user_id")).as("user_id"),
      when(col("video_end_type") === "", "null").otherwise(col("video_end_type")).as("video_end_type"),
      when(col("video_id") === "", "null").otherwise(col("video_id")).as("video_id"))
  }

  def load( load_hive : DataFrame, table : String) : Unit = {
    load_hive
      .coalesce(1)
      .write
      .format("orc")
      .mode("append")
      .insertInto(table)

  }
}
