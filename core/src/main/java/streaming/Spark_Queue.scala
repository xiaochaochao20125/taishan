package streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author zc
 * @create 2021-09-03-16:17
 */
object Spark_Queue {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val rddQueue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()

    val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue, oneAtATime = false)

    val mappedStream: DStream[(Int, Int)] = inputStream.map((_, 1))

    val reduceStream: DStream[(Int, Int)] = mappedStream.reduceByKey(_ + _)

    reduceStream.print()

    ssc.start()

    for(i <- 1 to 5){
      rddQueue += ssc.sparkContext.makeRDD(1 to 300,10)
      Thread.sleep(2000)
    }
    ssc.awaitTermination()
  }
}
