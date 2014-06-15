/**
 * A sample streaming application which indexes tweets live into elastic search
 */

package com.holdenkarau.esspark

import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.elasticsearch.hadoop.mr.EsOutputFormat
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
// Hadoop imports
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf, OutputFormat}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{MapWritable, Text, NullWritable}


object IndexTweetsLive {
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage IndexTweetsLive <master> <key> <secret key> <access token> <access token secret>  <es-resource> [es-nodes]")
    }
    val Array(master, consumerKey, consumerSecret, accessToken, accessTokenSecret, esResource) = args.take(6)
    val esNodes = args.length match {
        case x: Int if x > 6 => args(6)
        case _ => "localhost"
    }

    // Set up the system properties for twitter
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val ssc = new StreamingContext(master, "IndexTweetsLive", Seconds(1))

    val tweets = TwitterUtils.createStream(ssc, None)
    tweets.print()
    val tweetsAsMap = tweets.map{ tweet =>
      val
      tweet.getGeoLocation() match {
        case null => HashMap("docid" -> tweet.getId().toString, "content" -> tweet.getContet())
        case _ => HashMap("docid" -> tweet.getId().toString, "content" -> tweet.getContet())
      }
    tweetsAsMap.foreachRDD{tweetRDD =>
      val sc = tweetRDD.context
      val jobConf = new JobConf(sc.hadoopConfiguration)
      jobConf.set("mapred.output.format.class", "org.elasticsearch.hadoop.mr.EsOutputFormat")
      jobConf.setOutputCommitter(classOf[FileOutputCommitter])
      jobConf.set(ConfigurationOptions.ES_RESOURCE_READ, args(1))
      jobConf.set(ConfigurationOptions.ES_RESOURCE_WRITE, args(1))
      jobConf.set(ConfigurationOptions.ES_NODES, args(2))
      tweetRDD.saveAsHadoopDataset(jobConf)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
