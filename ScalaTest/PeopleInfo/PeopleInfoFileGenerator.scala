package com.test

import java.io.FileWriter
import java.io.File
import scala.util.Random

object PeopleInfoFileGenerator {
  
   def main(args: Array[String]): Unit = {
    val writer = new FileWriter(new File("sample_people_data.txt"), false)
    val rand = new Random()
    for ( i <- 1 to 30){
     var height = rand.nextInt(220)
     if(height < 50){
       height = height + 50
     }
     var gender = getRandomGender
     if( height < 100 && gender == "M")
       height = height + 100
     if( height < 100 && gender == "F")
       height = height + 50
     writer.write( i + " " + getRandomGender + " " + height )
     writer.write( System.getProperty( "line.separator" ))
    }
    writer.flush()
    writer.close()
    println( "People Information File generated successfully." )
  }
   
   def getRandomGender():String = {
     val rand = new Random()
     val randNum = rand.nextInt(2) + 1
     if( randNum % 2 == 0){
       "M"
     }else{
       "F"
     }
   }
   
}
