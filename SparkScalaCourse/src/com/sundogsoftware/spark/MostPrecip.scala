package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max

object MostPrecip {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val date = fields(1).substring(0, 4) + "-" + fields(1).substring(4, 6) + "-" + fields(1).substring(6, 8) 
    val entryType = fields(2)
    //val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    val value = fields(3).toFloat * 0.1f
    (date, entryType, value)
  }  
  
  def main(args: Array[String]) {
    // filter rows for just precip
    // get max precip value 
    // find all dates with the max precip value
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "Precip")
    
    val lines = sc.textFile("../source/1800.csv")
    val parsedLines = lines.map(parseLine)
  
    val precipRows = parsedLines.filter(x => x._2 == "PRCP")
    val dateRain = precipRows.map(x => (x._1, x._3))
    
    val maxRain = dateRain.map(x => x._2).max()
    println("maxRain is: " + maxRain)
  
    val results = dateRain.filter(x => x._2 == maxRain).collect()
    
    for (result <- results.sorted) {
       val date = result._1
       val rain = result._2
       println(s"$date precipitation: $rain") 
    }
  }
  
}