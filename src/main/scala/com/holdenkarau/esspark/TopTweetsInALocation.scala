/**
 * Determine the top tweets in a location from our es index
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

object TopTweetsInALocation {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage IndexTweetsLive <master> <es-resource> <es-nodes> <lat> <lon> <dist>")
      System.exit(-1)
    }

    val Array(master, esResource, esNodes, lat, lon, dist) = args.take(4)

    val conf = new SparkConf
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)
    val jobConf = SharedESConfig.setupEsOnSparkContext(sc, esResource, Some(esNodes))
    // Lets find nearby tweets
    jobConf.set("es.query", "{\"filtered\" : {\"query\" : { \"geo_distance\" : { \"distance\" : \""+ dist + "km\", \"location\" : { \"lat\" : "+ lat +", \"lon\" : "+ lon +" }}}}}")
    // rdd of input
    val currentTweets = sc.hadoopRDD(jobConf, classOf[EsInputFormat[Object, MapWritable]], classOf[Object], classOf[MapWritable])
    // Extract only the map
    // Convert the MapWritable[Text, Text] to Map[String, String]
    val tweets = currentTweets.map{ case (key, value) => SharedIndex.mapWritableToInput(value) }
    // Extract the hash tags
    val hashTags = tweets.map(_.get("hashTags").map(_.split(" ")).getOrElse(Nil))
    println(hashTags.countByValue())
  }
}
