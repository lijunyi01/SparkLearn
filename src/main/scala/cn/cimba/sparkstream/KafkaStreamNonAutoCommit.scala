package cn.cimba.sparkstream

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by ljy on 2020/3/15.
 * ok
 */
// spark-submit --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/root/log4j.properties" --class cn.cimba.sparkstream.StructedStream --master yarn --num-executors 8 --executor-cores 2 --deploy-mode client --driver-memory 1G ./SparkLearn.jar
object KafkaStreamNonAutoCommit {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder
      .appName("KafkaWordCountNonAutoCommit")
      .master("local[2]")
      .getOrCreate()

    //初始化，创建sparkContext
    val sc = sparkSession.sparkContext
//    sc.setLogLevel("INFO")
    //初始化，创建StreamingContext，batchDuration为5秒
    val ssc = new StreamingContext(sc, Seconds(5))


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.8.170:9092,192.168.8.171:9092,192.168.8.172:9092,192.168.8.173:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group03",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // val topics = Array("topicA", "topicB")
    val topics = Array("test")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    // 流(stream)中的每个项都是 ConsumerRecord
//    stream.map(record => (record.key, record.value))

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val lines: RDD[String] = rdd.map(record => (record.value))

      //对RDD进行操作，你操作这个抽象（代理，描述），就像操作一个本地的集合一样
      //切分压平
      val words: RDD[String] = lines.flatMap(_.split(" "))
      //单词和一组合在一起
      val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
      //聚合
      val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)
      //打印结果(Action)
      reduced.foreach(println(_))

      // some time later, after outputs have completed
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()

  }

}
