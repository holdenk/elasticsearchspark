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
      System.err.println("Usage IndexTweetsLive <key> <secret key> <access token> <access token secret>  <es-resource> <es-nodes>")
    }
    val conf = new SparkConf
    conf.setMaster(args(0))
    conf.set("es.source", args(4))
    conf.set(ConfigurationOptions.ES_NODES, args(5))
    val sc = new SparkContext(conf)


  }
}
