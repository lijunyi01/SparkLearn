package cn.cimba.sparkstream

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.functions._

/**
 * Created by ljy on 2020/3/15.
 * ok
 */
// spark-submit --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/root/log4j.properties" --class cn.cimba.sparkstream.StructedStream --master yarn --num-executors 8 --executor-cores 2 --deploy-mode client --driver-memory 1G ./SparkLearn.jar
object KafkaStructuredStream3 {

  def main(args: Array[String]): Unit = {

    //{"id":1,"year":2020,"rating":1.5,"duration":3}
    //{"id":2,"year":2019,"rating":1.3,"duration":4}
    val mySchema = StructType(Array(
      StructField("id", IntegerType),
      StructField("year", IntegerType),
      StructField("rating", DoubleType),
      StructField("duration", IntegerType)
    ))

    //{"id":1,"name":"george"}
    //{"id":2,"name":"james"}
    val mySchema2 = StructType(Array(
      StructField("id", IntegerType),
      StructField("name", StringType),
    ))

    val sparkSession = SparkSession
      .builder
      .appName("KafkaStructuredNetworkWordCount")
      .master("local[2]")
      .getOrCreate()

    //初始化，创建sparkContext
    val sc = sparkSession.sparkContext
//    sc.setLogLevel("INFO")
    //初始化，创建StreamingContext，batchDuration为5秒
    val ssc = new StreamingContext(sc, Seconds(1))

    import sparkSession.implicits._

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val kafkaDF: DataFrame = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.8.170:9092,192.168.8.171:9092,192.168.8.172:9092,192.168.8.173:9092")
      .option("subscribe", "test,topic1")
      .load()

    val t1ds: Dataset[String] = kafkaDF.where("topic = 'test'").selectExpr("CAST(value AS STRING)").as[String]
    val t1df: DataFrame = t1ds.select(from_json($"value", mySchema).as("data")).select("data.*")
    val t2ds: Dataset[String] = kafkaDF.where("topic = 'topic1'").selectExpr("CAST(value AS STRING)").as[String]
    val t2df: DataFrame = t2ds.select(from_json($"value", mySchema2).as("data")).select("data.*")

    t1df.createOrReplaceTempView("t1")
    t2df.createOrReplaceTempView("t2")
    val a: DataFrame = sparkSession.sql("select t1.id,t2.name,t1.year,t1.rating from t1,t2 where t1.id=t2.id")

    a.writeStream
      .format("console")
      .option("truncate","false")
//      .outputMode("complete")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

}
