import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object HDFSExample {
  
  def main(args: Array[String]): Unit = {
    
    System.setProperty("hadoop.home.dir", "E:\\Hadoop\\hadoop-2.7.3"); //将此路径换成自己的
    val path = "hdfs://192.168.163.131:9000/input/scalaTest.txt"
    val conf = new SparkConf().setAppName("HDFS Example")
    val sc = new SparkContext(conf)
    val data = sc.textFile(path, 2).cache()
    val numAs = data.filter(line => line.contains("a")).count();
    val numBs = data.filter(line => line.contains("b")).count();
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
  
}
