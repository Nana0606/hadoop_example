import java.io.FileWriter
import java.io.File
import scala.util.Random

//本代码主要用于生成年龄数据
object PeopleDataFileGenerator {
  
  def main(args: Array[String]): Unit = {
    val writer = new FileWriter(new File("sample_age_data.txt"), false)
    val rand = new Random()
    for ( i <- 1 to 30){
      writer.write(i + " " + rand.nextInt(100))
      writer.write(System.getProperty("line.separator"))
    }
    writer.flush()
    writer.close()
  }
}
