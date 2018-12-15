package com.vinay.utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import  org.apache.spark.sql.SparkSession



/**
  * Created by ADMIN on 27-08-18.
  */
trait SparkConfig {
  val sparkSession = SparkSession.builder().getOrCreate()
  val Spark = SparkSession.builder().config("spark.sql.warehouse.dir","/home/hadoop/warehosue").enableHiveSupport().getOrCreate()
  val sQLContext = sparkSession.sqlContext
}
