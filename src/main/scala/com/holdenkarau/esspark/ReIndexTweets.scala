/**
 * A sample streaming application which indexes tweets live into elastic search
 */

package com.holdenkarau.esspark

// Scala imports
import scala.collection.JavaConversions._
// Spark imports
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
// ES imports
import org.elasticsearch.hadoop.mr.EsOutputFormat
import org.elasticsearch.hadoop.mr.EsInputFormat
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
// Hadoop imports
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf, OutputFormat}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{MapWritable, Text, NullWritable}

object ReIndexTweets {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage IndexTweetsLive <master> <es-resource> <es-nodes>")
      System.exit(-1)
    }

    val Array(master, esResource) = args.take(2)
    val esNodes = args.length match {
        case x: Int if x > 2 => args(2)
        case _ => "localhost"
    }

    val conf = new SparkConf
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)
    val jobConf = SharedESConfig.setupEsOnSparkContext(sc, args(1), Some(args(6)), true)
    // RDD of input
    val currentTweets = sc.hadoopRDD(jobConf, classOf[EsInputFormat[Object, MapWritable]], classOf[Object], classOf[MapWritable])
    // Extract only the map
    // Convert the MapWritable[Text, Text] to Map[String, String]
    val tweets = currentTweets.map{ case (key, value) => SharedIndex.mapWritableToInput(value) }
    println(tweets.collect().mkString(":"))
    // Save the output
    // output.saveAsHadoopDataset(jobConf)
  }
}
