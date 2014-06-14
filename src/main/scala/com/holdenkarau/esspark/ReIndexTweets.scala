/**
 * A sample streaming application which indexes tweets live into elastic search
 */

package com.holdenkarau.esspark

// Spark imports
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
// ES imports
import org.elasticsearch.hadoop.mr.EsOutputFormat
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
// Hadoop imports
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf, OutputFormat}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{MapWritable, Text, NullWritable}

object ReIndexTweets {
  def mapToOutput(in: Map[String, String]): (Object, Object) = {
    val m = new MapWritable
    for ((k, v) <- in)
      m.put(new Text(k), new Text(v))
    (NullWritable.get, m)
  }

  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage IndexTweetsLive <master> <key> <secret key> <access token> <access token secret>  <es-resource> <es-nodes>")
      System.exit(-1)
    }

    println("Using master" + args(0))
    println("Using es write resource " + args(5))
    println("Using nodes "+args(6))
    val conf = new SparkConf
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)
    val tweets = sc.parallelize(List(Map("user" -> "holdenkarau",
      "message" -> "stress pandas")))
    val output = tweets.map(mapToOutput)
    val jobConf = new JobConf(sc.hadoopConfiguration)
    jobConf.set("mapred.output.format.class", "org.elasticsearch.hadoop.mr.EsOutputFormat")
    jobConf.setOutputCommitter(classOf[FileOutputCommitter])
    jobConf.set(ConfigurationOptions.ES_RESOURCE_WRITE, args(5))
    jobConf.set(ConfigurationOptions.ES_NODES, args(6))
    // This tells the ES MR output format to use the spark partition index as the node index
    // This will degrade performance unless you have the same partition layout as ES in which case
    // it should improve performance.
    // Note: this is currently implemented as kind of a hack.
    jobConf.set("es.sparkpartition", "true")
    FileOutputFormat.setOutputPath(jobConf, new Path("-"))
    output.saveAsHadoopDataset(jobConf)
  }
}
