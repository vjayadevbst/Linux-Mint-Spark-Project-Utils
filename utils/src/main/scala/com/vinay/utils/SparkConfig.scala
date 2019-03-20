package com.vinay.utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import  org.apache.spark.sql.SparkSession



/**
  * Created by ADMIN on 27-08-18.
  */
trait SparkConfig {
  /*val sparkSession = SparkSession.builder().getOrCreate()
  val Spark = SparkSession.builder().config("spark.sql.warehouse.dir","/home/hadoop/warehosue").enableHiveSupport().getOrCreate()
  val sQLContext = sparkSession.sqlContext*/

  va sparkSession:SparkSession = SparkSession.builder().
    config("spark.sql.warehouse.dir","/data/dev/warehouse/siu/dda_siu").
    config("spark.shuffle.spill","false").
    config("spark.rdd.compress","true").
    config("spark.storage.memoryFraction","1").
    config("saprk.shuffle.consolidateFiles","true").
    config("spark")
}
