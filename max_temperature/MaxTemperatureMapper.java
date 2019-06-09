package com.oss.maxtemperature;

import java.io.IOException; 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> { 

         @Override
         public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{                                    
                   String line = value.toString();  
                   if( !line.equals("") ){
                       try {
                                String year = line.substring(0,4);
                                int airTemperature = Integer.parseInt(line.substring(5));            
                                context.write(new Text(year),new IntWritable(airTemperature));                           
                       } catch (Exception e) {
                                System.out.print("Error in line:" + line);
                       }  
                    } else {
                          return;
                    }                   
         }        
}
