package com.tcs

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Hello world!
 *
 */
object WordCount{
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFile = args(1)
    val sc = new SparkContext(new SparkConf().setAppName("Spark Word Count"))
    val input = sc.textFile(inputFile)
    val words = input.flatMap(line => line.split(" "))
    val count = words.map(word => (word,1)).reduceByKey{case(x,y) => x + y}
    count.saveAsTextFile(outputFile)
  }
}
