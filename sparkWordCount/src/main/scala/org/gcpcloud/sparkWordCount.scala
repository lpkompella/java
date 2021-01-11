package org.gcpcloud
import org.apache.spark.{SparkConf, SparkContext}


object sparkWordCount {
  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("Spark Word Count"))
    val threshold = args(1).toInt

    // split each document into words
    val tokenized = sc.textFile(args(0)).flatMap(_.split(" "))

    // count the occurrence of each word
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)

    // filter out words with less than threshold occurrences
    val filtered = wordCounts.filter(_._2 >= threshold)

    println(filtered.collect().mkString(" "))

    // count characters
    //val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)

    // System.out.println(charCounts.collect().mkString(", "))
  }
}
