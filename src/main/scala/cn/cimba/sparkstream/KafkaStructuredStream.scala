package cn.cimba.sparkstream

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by ljy on 2020/3/15.
 * ok
 */
// spark-submit --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/root/log4j.properties" --class cn.cimba.sparkstream.StructedStream --master yarn --num-executors 8 --executor-cores 2 --deploy-mode client --driver-memory 1G ./SparkLearn.jar
object KafkaStructuredStream {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder
      .appName("KafkaStructuredNetworkWordCount")
      .master("local[2]")
      .getOrCreate()

    //初始化，创建sparkContext
    val sc = sparkSession.sparkContext
//    sc.setLogLevel("INFO")
    //初始化，创建StreamingContext，batchDuration为5秒
    val ssc = new StreamingContext(sc, Seconds(5))

    import sparkSession.implicits._

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val kafkaDF: DataFrame = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.8.170:9092,192.168.8.171:9092,192.168.8.172:9092,192.168.8.173:9092")
      .option("subscribe", "test,topic1")
      .load()

//    val ds = linesDF
//      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//      .writeStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "192.168.8.170:9092,192.168.8.171:9092,192.168.8.172:9092,192.168.8.173:9092")
//      .option("topic", "topic2")
//      .option("checkpointLocation", "/Users/ljy/desktop/")
//      .start()

    // 从DataFrame中选取特定的列组成新的DataFrame
    // val linesDF: DataFrame = kafkaDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
    val linesDF: DataFrame = kafkaDF.selectExpr("CAST(value AS STRING)")

    // Split the lines into words
    val words: Dataset[String] = linesDF.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts: DataFrame = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query: StreamingQuery = wordCounts.writeStream
      .outputMode("complete")
//      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()

  }

}
