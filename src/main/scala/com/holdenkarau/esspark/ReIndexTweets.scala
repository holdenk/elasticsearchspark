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
  def mapToOutput(in: Map[String, String]): (Object, Object) = {
    val m = new MapWritable
    for ((k, v) <- in)
      m.put(new Text(k), new Text(v))
    (NullWritable.get, m)
  }
  def mapWritableToInput(in: MapWritable): Map[String, String] = {
    in.map{case (k, v) => (k.toString, v.toString)}.toMap
  }

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage IndexTweetsLive <master> <es-resource> <es-nodes>")
      System.exit(-1)
    }

    println("Using master" + args(0))
    println("Using es write resource " + args(1))
    println("Using nodes "+args(2))
    val conf = new SparkConf
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)
    val jobConf = new JobConf(sc.hadoopConfiguration)
    jobConf.set("mapred.output.format.class", "org.elasticsearch.hadoop.mr.EsOutputFormat")
    jobConf.setOutputCommitter(classOf[FileOutputCommitter])
    jobConf.set(ConfigurationOptions.ES_RESOURCE_READ, args(1))
    jobConf.set(ConfigurationOptions.ES_RESOURCE_WRITE, args(1))
    jobConf.set(ConfigurationOptions.ES_NODES, args(2))
    // This tells the ES MR output format to use the spark partition index as the node index
    // This will degrade performance unless you have the same partition layout as ES in which case
    // it should improve performance.
    // Note: this is currently implemented as kind of a hack.
    jobConf.set("es.sparkpartition", "true")
    FileOutputFormat.setOutputPath(jobConf, new Path("-"))
    // RDD of input
    val currentTweets = sc.hadoopRDD(jobConf, classOf[EsInputFormat[Object, MapWritable]], classOf[Object], classOf[MapWritable])
    // Extract only the map
    // Convert the MapWritable[Text, Text] to Map[String, String]
    val tweets = currentTweets.map{ case (key, value) => mapWritableToInput(value) }
    println(tweets.collect().mkString(":"))
    // Save the output
    // output.saveAsHadoopDataset(jobConf)
  }
}
