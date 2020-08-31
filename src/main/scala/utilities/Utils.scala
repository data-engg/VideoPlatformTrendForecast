package utilities

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.Map

object Utils {

  var channelGeoCdMap : Map[String, String] = null
  var videoCreatorMap : Map[String, String] = null
  var userSubscnMap : Map[String, String] = null

  def get_spark_session(app_name : String) : SparkSession ={
    val spark =
    SparkSession.builder().
      config("spark.cassandra.connection.host","cassandradb.edu.cloudlab.com").
      config("spark.cassandra.connection.port",9042).
      appName(app_name).
      enableHiveSupport().
      getOrCreate()

    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    return spark
  }

  //Return a map of channel_id -> geo_cd
  def channelGeocd( spark : SparkSession) : Unit = {
    if (this.channelGeoCdMap == null) {
      this.channelGeoCdMap =
        spark.read.format("org.apache.spark.sql.cassandra").
          options(Map("keyspace" -> "edureka_735821", "table" -> "channel_geocd")).load().
          rdd.
          map(x => (x(0).toString(), x(1).toString())).
          collectAsMap()
    }
  }

  def channelGeocdLKP( channel_id : String) : String ={
    try{
      channelGeoCdMap(channel_id)
    } catch {
      case exc : NoSuchElementException => {
        "Invalid"
      } case e : Exception => {
        "Invalid"
      }
    }
  }

  //Return a map of user_id -> creator_id
  def videoCreator( spark : SparkSession) : Unit = {
    if ( this.videoCreatorMap == null ){

      this.videoCreatorMap =
        spark.read.format("org.apache.spark.sql.cassandra").
          options(Map("keyspace"->"edureka_735821", "table"->"video_creator")).load().
          rdd.
          map( x => (x(1).toString(), x(0).toString()) ).
          collectAsMap()
    }
  }

  def videoCreatorLkp( video_id : String ) : String = {
    try{
      videoCreatorMap(video_id)
    } catch {
      case exc : NoSuchElementException => {
        "Invalid"
      } case e : Exception => {
        "Invalid"
      }
    }
  }

  //Return a map of user_id -> subsc_end_dt
  def userSubscn( spark : SparkSession) : Unit = {
    if ( this.userSubscnMap == null ){
      this.userSubscnMap =
        spark.read.format("org.apache.spark.sql.cassandra").
          options(Map("keyspace"->"edureka_735821", "table"->"user_subscn")).load().
          rdd.
          map( x => ((x(0).toString().substring(1)), x(1).toString()) ).
          collectAsMap()
    }
  }

  def userSubscnLkp(user_id : String): String ={
    try{
      userSubscnMap(user_id)
    } catch {
      case exc : NoSuchElementException => {
        null
      } case e : Exception => {
        null
      }
    }
  }
  def getSchema() : StructType = {
    StructType(
      Array(StructField("liked",StringType,true),
          StructField("user_id",StringType,true),
          StructField("video_end_type",StringType,true),
          StructField("minutes_played",StringType,true),
          StructField("video_id",StringType,true),
          StructField("geo_cd",StringType,true),
          StructField("channel_id",StringType,true),
          StructField("creator_id",StringType,true),
          StructField("timestamp",StringType,true),
          StructField("disliked",StringType,true)
      ))
  }
}
