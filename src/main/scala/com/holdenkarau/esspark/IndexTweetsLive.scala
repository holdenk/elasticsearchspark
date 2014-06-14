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


object IndexTweetsLive {
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage IndexTweetsLive <master> <key> <secret key> <access token> <access token secret>  <es-resource> [es-nodes]")
    }
    val master = args(0)
    val twitterKey = args(1)
    val twitterSecret = args(2)
    val twitterAccessToken = args(3)
    val twitterAccessSecret = args(4)
    val esResource = args(5)
    val esNodes = args.length match {
        case x: Int if x > 6 => args(6)
        case _ => "localhost"
      }

    val ssc = new StreamingContext(master, "IndexTweetsLive", Seconds(1))
    val tweets = TwitterUtils.createStream(ssc, None)

    ssc.start()
    ssc.awaitTermination()
  }
}
