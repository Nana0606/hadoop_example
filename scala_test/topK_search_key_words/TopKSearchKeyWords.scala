package com.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import java.io.PrintWriter
import java.io.File

object TopKSearchKeyWords {
  def main(args: Array[String]): Unit = {
    if(args.length < 2){
      println("Usage: TopKSearchKeyWords KeyWordsFile K")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Spark Exerceise: Top K Searching Key Words")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val srcData = sc.textFile(args(0))
    val countedData = srcData.map(line => (line.toLowerCase(), 1)).reduceByKey((a, b) => a+b )
    //for debug use
    countedData.foreach( x => println(x) )
    val sortedData = countedData.map{ case (k,v) => (v,k)}.sortByKey(false)
    val topKData = sortedData.take(args(1).toInt).map{ case (v,k) => (k,v) }
    topKData.foreach(println)
    val writer = new PrintWriter(new File("topKSearch_result.txt"))
    for (i <- 0 to topKData.length-1){
      writer.println(topKData(i))
    }
    writer.close()
    
  }
}
