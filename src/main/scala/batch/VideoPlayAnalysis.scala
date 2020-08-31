package batch

import java.text.SimpleDateFormat

import utilities._
import org.apache.spark.sql.functions.{col, countDistinct, sum, current_timestamp, rank, lit}
import org.apache.spark.sql.expressions.Window

object VideoPlayAnalysis {

  def main( args : Array[String] ) : Unit = {

    // ----------------------- Spark session -----------------------
    val spark = Utils.get_spark_session("Analysis for Video Plays table(hive)")

    // ----------------------- load data to lookups -----------------------

    Utils.userSubscn(spark)

    // ----------------------- load data to data frame -----------------------

    val video_play_df = spark.read.format("orc").table(args(0))

    // ----------------------- UDF registration -----------------------

    val user_category =
    spark.udf.register("user_category", (user_id : String, ts : String)  =>
    {
      val ret_val = Utils.userSubscnLkp(user_id)
      lazy val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      if (ret_val == null){
      "unsubscribed"
    } else {
      if ( format.parse(ts).compareTo(format.parse(ret_val)) == 1){
      "unsubscribed"
    } else {
      "subscribed"
    }
    }
    })

    // ----------------------- Data transformation -----------------------

    // 1. Most popular channels by the criteria of a maximum number of videos played and liked by unique users

    val popular_channels =
    video_play_df.groupBy(col("channel_id")).
      agg(countDistinct("video_id").as("distinct_video_count"), countDistinct("user_id").as("distict_user_id")).
      orderBy(col("distinct_video_count").desc, col("distict_user_id").desc).
      withColumn("process_time", current_timestamp())

    //2. The total duration of videos played by each type of user
    val category_wise_duration =
    video_play_df.select(col("user_id"), col("ts"), col("minutes_played")).
      withColumn("user_type", user_category(col("user_id").cast(org.apache.spark.sql.types.StringType), col("ts"))).
      groupBy(col("user_type")).agg(sum("minutes_played").as("duration")).
      orderBy(col("duration").desc).
      withColumn("process_time", current_timestamp())

    //3. Determine the list of connected creators
    val connected_creator =
    video_play_df.select( col("creator_id"), col("user_id"))
      .groupBy(col("creator_id")).agg(countDistinct("user_id").as("user_count"))
      .orderBy(col("user_count").desc)
      .withColumn("process_time", current_timestamp())

    //4. Determine which videos are generating maximum revenue
    val video_max_rev =
    video_play_df.select(col("video_id"), col("liked"), col("video_end_type"), col("minutes_played")).
      filter(col("liked") === true && col("video_end_type") === 0).
      drop("liked", "video_end_type").
      groupBy(col("video_id")).agg(sum("minutes_played").as("duration")).
      orderBy(col("duration").desc).
      withColumn("process_time", current_timestamp())

    //5. The unsubscribed users who watched the videos for the longest duration.

    val unsub_user_longest_dur =
        video_play_df.select(col("user_id"), col("ts"), col("minutes_played")).
      withColumn("user_type", user_category(col("user_id"), col("ts"))).
      drop("ts").
      filter( col("user_type") === "unsubscribed").
      drop(col("user_type")).
      groupBy( col("user_id") ).agg( sum("minutes_played").as("duration")).
          withColumn("rank", rank().over(Window.partitionBy(lit("1")).orderBy(col("duration").desc))).
          filter(col("rank") === 1).
          drop("rank").
          withColumn("process_time", current_timestamp())


    // ----------------------- load data into hive tables -----------------------

    Processing.load(popular_channels, "videos.popular_channel")
    Processing.load(category_wise_duration, "videos.category_duration")
    Processing.load(video_max_rev, "videos.video_max_revenue")
    Processing.load(connected_creator, "videos.connected_creator")
    Processing.load(unsub_user_longest_dur, "videos.unsub_top_user")
  }

}
