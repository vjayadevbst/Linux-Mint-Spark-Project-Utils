package com.vinay.project_1
import java.sql.SQLException

import org.apache.spark.sql.{DataFrame, SQLContext}
import com.vinay.utils._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

//Linux mint
/**
  * Created by ADMIN on 27-08-18.
  */
object Logic_1 extends LoProvider with SparkConfig{
  var propertyFile:String = _
  var inputpath:String = _
  var outputpath:String =_
  var errorCode =0

  def main(args: Array[String]): Unit = {
    if(args.length == 3 && args(0)!=null && args(1)!=null && args(2)!=null ) {
      inputpath= args(0)
      outputpath=args(1)
      propertyFile=args(2)
    }else{
      log.error("Incorrect args lenght")
      System.exit(1)
    }
    ResourceBuilder.config("../Project_1.properties")
    val input = getData(inputpath)
    val output = transform(input)
    DataWriter.writeData(output,outputpath,ResourceBuilder.writeSchema)
  }

  def getData(inputPath:String):DataFrame = {
    try{
      val input = Spark.table("tablename")
      input
    }
    catch {
      case ex: SQLException => errorCode == 1; log.error("Exception").asInstanceOf
    }finally {
      if(errorCode==1) log.info("Reding failed")
      else
        log.info("Reading complete")
    }
  }

  def transform(input:DataFrame):DataFrame = {
    try{
      log.info("Logic here")
      val output = input
      output
    }
    catch {
      case ex: SQLException => errorCode == 1; log.error("Exception").asInstanceOf
    }finally {
      if(errorCode==1) log.info("Reding failed")
      else
        log.info("Reading complete")
    }
  }
}
