package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object PurchaseByCustomerSorted {
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local", "PurchaseByCustomerSorted")   
    
    // Load each line of my book into an RDD
    val input = sc.textFile("../source/customer-orders.csv")
    //input.take(10).foreach(println)
    
    // RDD with each row is an array of 3 strings
    val columns = input.map(f => f.split(","))
    //columns.take(10).foreach(a => println(a(0) + "\t" + a(1) + "\t" + a(2)))
    
    val kvp = columns.map(f => (f(0).toInt, f(2).toFloat) )
    //kvp.take(10).foreach(a => println(a._1 + "," + a._2))
        
    val agg = kvp.reduceByKey( (x,y) => x + y )
    
    val switched = agg.map(f => (f._2, f._1) )
    
    //switched.foreach(println)
    
    val sorted = switched.sortByKey(false)
    
    //sorted.foreach(println)
    for (result <- sorted) {
      val sales = result._1
      val cust = result._2
      println(s"$cust: $sales")
    }
    
  
  }
  
}