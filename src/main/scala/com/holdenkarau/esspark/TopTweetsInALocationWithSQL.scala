/**
 * Determine the top tweets in a location from our es index
 */

package com.holdenkarau.esspark

// Scala imports
import scala.collection.JavaConversions._
import scala.util.Sorting
// Spark imports
import org.apache.spark._
import org.apache.spark.rdd._
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
// sqlcontext
import org.apache.spark.sql._
import org.elasticsearch.spark.sql._

object TopTweetsInALocationWithSQL {

  def main(args: Array[String]) {
    if (args.length < 6) {
      System.err.println("Usage IndexTweetsLive <master> <es-resource> <es-nodes> <lat> <lon> <dist>")
      System.exit(-1)
    }

    val Array(master, esResource, esNodes, lat, lon, dist) = args.take(6)

    val conf = new SparkConf
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)
    // SQL context
    val sqlCtx = new SQLContext(sc)
    import sqlCtx._
    val query = "{\"query\": {\"filtered\" : {\"query\" : {\"match_all\" : {}},\"filter\" : { \"geo_distance\" : { \"distance\" : \""+ dist + "km\", \"location\" : { \"lat\" : "+ lat +", \"lon\" : "+ lon +" }}}}}}"
    // Extract only the map
    // Convert the MapWritable[Text, Text] to Map[String, String]
    val tweets = sqlCtx.esRDD(esResource, query)
    println(tweets.schema)
    // Extract the hash tags
    val hashTags = tweets.select('hashTags)
    // Now we could keep going in SQL but I'm more familiar in Scala
    val hashTagCounts = hashTags.countByValue()
    val topTags = makeWordCount(hashTags).takeOrdered(40)(WordCountOrdering)
    println("top tags:")
    topTags.foreach{case (tag, count) => println(tag +"," + count)}
    println("some tweets")
    tweets.take(5).foreach(println)
  }

  object WordCountOrdering extends Ordering[(String, Int)] {
    def compare(a: (String, Int), b: (String, Int)) = {
      b._2 compare a._2
    }
  }
  def makeWordCount(input: SchemaRDD) = {
    input.map(_.getString(0)).flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey((x,y) => x+y)
  }
}
