package cn.cimba.sparkstream

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by ljy on 2020/3/15.
 * ok
 */
// spark-submit --class cn.cimba.sparkstream.StructedStream --master yarn --num-executors 8 --executor-cores 2 --deploy-mode client --driver-memory 1G ./SparkLearn.jar
object StructedStream {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
//      .master("local[2]")
      .getOrCreate()

    //初始化，创建sparkContext
    val sc = spark.sparkContext
    //初始化，创建StreamingContext，batchDuration为1秒
    val ssc = new StreamingContext(sc, Seconds(5))

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }

}
