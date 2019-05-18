package com.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkWordCount {
  
  //System.setProperty("hadoop.home.dir", "E:\\Hadoop\\hadoop-2.7.3"); //将此路径换成自己的
  
  def FILE_NAME: String = "word_count_results_"
  def main(args: Array[String]): Unit = {
    if(args.length < 1){
      println("Usage: SparkWordCount FileName")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Spark Exercise: Spark Version Word Count Program")
    //conf.setMaster("local")
    val sc = new SparkContext(conf)
    //args(0)控制输入文件，随便一个文件即可
    val textFile = sc.textFile(args(0))
    val wordCounts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a+b)
    
    //print the results, for debug use
    println("Wors Count progran running results:")
    wordCounts.collect().foreach(e => {
       val (k,v) = e
       println(k + "=" + v)
       });
     wordCounts.saveAsTextFile("hdfs://192.168.163.131:9000/output/SparkWordCount")
     println("Word Count program running results are sucessfully saved.")
    }
  
}
