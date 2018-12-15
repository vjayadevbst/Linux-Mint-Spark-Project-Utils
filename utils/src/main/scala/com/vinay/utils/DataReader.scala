package com.vinay.utils
import org.apache.spark.sql.{DataFrame, SparkSession,SQLContext}
import java.io.IOException

/**
  * Created by ADMIN on 27-08-18.
  */
object DataReader extends LoProvider{
  def loadData(filepath:String):DataFrame= {
    try{
      val df = ResourceBuilder.sparkSession.read.format("csv")
        .option("charset","ResourceBuilder.enc")
        .option("quote","ResourceBuilder.charDeflector")
          .option("header",true)
        .option("delimiter","ResourceBuilder.delim")
        .load(filepath)
      df
    }
    catch{
      case ex:IOException => log.error("Exception reading file").asInstanceOf
    }

  }

}
