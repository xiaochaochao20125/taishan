package rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zc
 * @create 2021-09-02-21:39
 */
object WordCount {
  def main(args: Array[String]): Unit = {

    //1.初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3.通过监控端口创建DStream，读进来的数据为一行行
    val lineStreams: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //4.将每一行数据做切分，形成一个个单词
    val wordStreams: DStream[String] = lineStreams.flatMap(_.split(" "))

    //5.将单词映射成元组(word,1)
    val wordAndOneStreams: DStream[(String, Int)] = wordStreams.map((_, 1))

    //6.将相同的单词次数做统计
    val wordAndCountStreams: DStream[(String, Int)] = wordAndOneStreams.reduceByKey(_ + _)

    //7.打印
    wordAndCountStreams.print()

    //8.启动SparkStreamingContext
    ssc.start()
    ssc.awaitTermination()
  }
}
