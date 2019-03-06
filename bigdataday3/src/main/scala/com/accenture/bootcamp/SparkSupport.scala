package com.accenture.bootcamp

import java.io.File

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait SparkSupport {

  System.setProperty("hadoop.home.dir", new File(".").getAbsolutePath )

  // Disable logs for org package
  LogManager.getLogger("org").setLevel(Level.OFF)

  // TODO: initialize Spark session
  val spark: SparkSession = {
    SparkSession.builder()
      .master("local")
      .appName("BidDataDay3")
      .config("spark.driver.host", "localhost")
      .getOrCreate()
  }
  protected val sc: SparkContext = spark.sparkContext

}
