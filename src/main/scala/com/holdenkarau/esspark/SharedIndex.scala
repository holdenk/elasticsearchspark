/**
 * Shared utilities to convert a tweet to something we want to index
 */

package com.holdenkarau.esspark

// Scala imports
import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap
// Hadoop imports
import org.apache.hadoop.io.{MapWritable, Text, NullWritable}
// twitter imports
import twitter4j.Status
import twitter4j.TwitterFactory

object SharedIndex {
  // twitter helper methods
  def prepareTweets(tweet: twitter4j.Status) = {
    println("panda preparing tweet!")
    val fields = tweet.getGeoLocation() match {
        case null => HashMap(
          "docid" -> tweet.getId().toString,
          "message" -> tweet.getText(),
          "hashTags" -> tweet.getHashtagEntities().map(_.getText()).mkString(" ")
        )
        case loc => {
          val lat = loc.getLatitude()
          val lon = loc.getLongitude()
          HashMap(
            "docid" -> tweet.getId().toString,
            "message" -> tweet.getText(),
            "hashTags" -> tweet.getHashtagEntities().map(_.getText()).mkString(" "),
            "location" -> s"$lat,$lon"
          )
        }
      }
    val output = mapToOutput(fields)
    output
  }

  def setupTwitter(consumerKey: String, consumerSecret: String, accessToken: String, accessTokenSecret: String) ={
    // Set up the system properties for twitter
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
  }

  def fetchTweets(ids: Seq[String]) = {
    val twitter = new TwitterFactory().getInstance();
  }
  // hadoop helper methods
  def mapToOutput(in: Map[String, String]): (Object, Object) = {
    val m = new MapWritable
    for ((k, v) <- in)
      m.put(new Text(k), new Text(v))
    (NullWritable.get, m)
  }
  def mapWritableToInput(in: MapWritable): Map[String, String] = {
    in.map{case (k, v) => (k.toString, v.toString)}.toMap
  }

}
