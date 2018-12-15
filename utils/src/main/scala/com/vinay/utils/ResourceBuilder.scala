package com.vinay.utils

import java.io.FileInputStream

import org.apache.spark.sql.types.{StructField, StructType,StringType}
import java.util.Properties
import java.io.{File, FileInputStream, FileNotFoundException, IOException}

/**
  * Created by ADMIN on 27-08-18.
  */
object ResourceBuilder extends LoProvider with SparkConfig{

  var readSchema:String = _
  var schema:StructType = _
  var writeSchema:String= _
  var error_code = 0

  var delim:String = _
  var charDeflector:String = _
  var enc:String = _


  private var properties:Properties = _

  def config(path:String):Unit = {
    try{
      properties = new Properties;
      properties.load(new FileInputStream(new File(path)))
      log.info("Read from properties file successfull")
    }
  catch{
    case io :IOException => log.error("IO Exception loading file" + io.printStackTrace())
    case fx: FileNotFoundException => log.error("File not found" + fx.printStackTrace())
      error_code=1
  }
    finally{
      if (error_code ==1) {
        log.info("Loading properties file failed")
      }else{
        log.info("Reading properties file succesful")
      }
    }
    readSchema = properties.getProperty("schema_String")
    delim = properties.getProperty("cntrlchr")
    enc = properties.getProperty("enc")
    charDeflector = properties.getProperty("chardef")
    writeSchema=properties.getProperty("write_schema")
    schema = StructType(readSchema.split(",").map(fieldName=>StructField(fieldName,StringType,nullable=true)))
  }


}
