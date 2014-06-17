/**
 * Shared utilities to convert setup elastic search
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


object SharedESConfig {
  def setupEsOnSparkContext(sc: SparkContext, esResource: String, esNodes: Option[String] = None,
    esSparkPartition: Boolean = false) = {
    println("panda Creating configuration to write to "+esResource+" on "+esNodes)
    val jobConf = new JobConf(sc.hadoopConfiguration)
    jobConf.set("mapred.output.format.class", "org.elasticsearch.hadoop.mr.EsOutputFormat")
    jobConf.setOutputCommitter(classOf[FileOutputCommitter])
    jobConf.set(ConfigurationOptions.ES_RESOURCE, esResource)
    esNodes match {
      case Some(node) => jobConf.set(ConfigurationOptions.ES_NODES, node)
      case _ => // Skip it
    }
    // This tells the ES MR output format to use the spark partition index as the node index
    // This will degrade performance unless you have the same partition layout as ES in which case
    // it should improve performance.
    // Note: this is currently implemented as kind of a hack.
    if (esSparkPartition) {
      jobConf.set("es.sparkpartition", "true")
    }
    FileOutputFormat.setOutputPath(jobConf, new Path("-"))
    jobConf
  }
}
