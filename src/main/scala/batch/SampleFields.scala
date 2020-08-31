package batch

import utilities.Utils
import org.apache.hadoop.fs.{FileSystem, Path}

object SampleFields {

  def main(args : Array[String]) : Unit = {

    // ----------------------- Input validation -----------------------

    if (args.length != 3){
      println(" Usage : <jar> <source path> <target path <format>")
      System.exit(1)
    } else {
      if (args(2).trim != "csv" & args(2).trim != "xml") {
        println( "Valid input formats are csv and xml")
        System.exit(1)
      }
    }

    var filepath = args(1)+"/" + args(2) + "/"

    // ----------------------- Spark session -----------------------
    val spark = Utils.get_spark_session("Sample 20% records from dataset")

    // ----------------------- Creating file system -----------------------
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    // ----------------------- Sample data -----------------------

    val input_rdd =
    spark.sparkContext.textFile(args(0)).sample(false, 0.2)

    if (fs.exists(new Path(filepath))){
      fs.delete(new Path(filepath))
    }

    input_rdd
        .coalesce(1)
        .saveAsTextFile(filepath)

  }
}