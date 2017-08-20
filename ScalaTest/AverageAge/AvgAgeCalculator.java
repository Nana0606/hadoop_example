package com.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object AvgAgeCalculator {
  System.setProperty("hadoop.home.dir", "E:\\Hadoop\\hadoop-2.7.3"); //将此路径换成自己的
  
  def main(args: Array[String]): Unit = {
    if(args.length < 1){
      println("Usage: AvgAgeCalculator datafile")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Spark Exercise: Average Age Calculator")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val dataFile = sc.textFile(args(0),3)
    var count = dataFile.count()
    println("Count is " + count)
    var ageData = dataFile.map(line => line.split(" ")(1))
    val totalAge = ageData.map(age => Integer.parseInt(String.valueOf(age))).collect().reduce((a,b) => a+b)
    println("Total Age:" + totalAge + "; Number of People:" + count)
    val avgAge: Double = totalAge.toDouble / count.toDouble
    println("Average Age is " + avgAge)
  }
}
