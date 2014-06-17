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

object TopTweetsInALocation {

  def main(args: Array[String]) {
    if (args.length < 6) {
      System.err.println("Usage IndexTweetsLive <master> <es-resource> <es-nodes> <lat> <lon> <dist>")
      System.exit(-1)
    }

    val Array(master, esResource, esNodes, lat, lon, dist) = args.take(6)

    val conf = new SparkConf
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)
    val jobConf = SharedESConfig.setupEsOnSparkContext(sc, esResource, Some(esNodes))
    // Lets find nearby tweets
    val query = "{\"query\": {\"filtered\" : {\"query\" : {\"match_all\" : {}},\"filter\" : { \"geo_distance\" : { \"distance\" : \""+ dist + "km\", \"location\" : { \"lat\" : "+ lat +", \"lon\" : "+ lon +" }}}}}}"
    println("Using query "+query)
    jobConf.set("es.query", query)
    // rdd of input
    val currentTweets = sc.hadoopRDD(jobConf, classOf[EsInputFormat[Object, MapWritable]], classOf[Object], classOf[MapWritable])
    // Extract only the map
    // Convert the MapWritable[Text, Text] to Map[String, String]
    val tweets = currentTweets.map{ case (key, value) => SharedIndex.mapWritableToInput(value) }
    // Extract the hash tags
    val hashTags = tweets.flatMap{tweet =>
      tweet.getOrElse("hashTags", "").split(" ")
    }
    val hashTagCounts = hashTags.countByValue()
    // Extract the top words
    val words = tweets.flatMap{tweet =>
      tweet.flatMap{elem =>
      elem._2 match {
        case null => Nil
        case _ => elem._2.split(" ")
      }}}.filter(_ != "(null)")
    val topWords = makeWordCount(words).takeOrdered(40)(WordCountOrdering)
    val topTags = makeWordCount(hashTags).takeOrdered(40)(WordCountOrdering)
    println("top tags:")
    topTags.foreach(t => println(t._1 +"," + t._2))
    println("top words")
    topWords.foreach(t => println(t._1 +"," + t._2))
    println("top NC")
    topWords.foreach(t => println(t._1))
    println("some tweets")
    tweets.take(5).map(t => t.foreach{case (f, v) => println(f+":"+v)})
  }

  object WordCountOrdering extends Ordering[(String, Int)] {
    def compare(a: (String, Int), b: (String, Int)) = {
      b._2 compare a._2
    }
  }
  def makeWordCount(input: RDD[String]) = {
    input.map(x => (x, 1)).reduceByKey((x,y) => x+y)
  }
}
